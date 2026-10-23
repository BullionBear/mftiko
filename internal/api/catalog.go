package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"path"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	"github.com/BullionBear/mftiko/internal/platform/artifact"
)

// tagResolveParallel is the most tag resolutions one catalog request runs at once (D1057).
const tagResolveParallel = 8

const (
	pageDefault = 100
	pageMax     = 1000
)

// tagCatalog is the read-only registry surface (D1056). *artifact.Catalog implements it.
// Tests substitute a fake. There is no cache in front of it (D1057).
type tagCatalog interface {
	Tags(ctx context.Context, repository string) ([]string, error)
	Resolve(ctx context.Context, repository, reference string) (string, error)
	ReadManifest(ctx context.Context, repository, digest string) (artifact.ManifestInfo, error)
}

// sha256Ref is the contract's digest (sha256 and 64 lowercase hex digits).
var sha256Ref = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

var errStrategyNotFound = errors.New("strategy not found")

type strategyRepo struct {
	id         string
	path       string
	repository string // address/path, the value the response shows (D1060)
}

type resolveJob struct {
	key        string
	repository string
	reference  string
}

func (s *Server) useCatalog() error {
	if s.cfg.Catalog != nil {
		s.catalog = s.cfg.Catalog
		return nil
	}
	if s.cfg.Registry.Address == "" {
		return nil
	}
	c, err := artifact.OpenCatalog(s.cfg.Registry)
	if err != nil {
		return err
	}
	s.catalog = c
	return nil
}

func strategyPath(p string) bool {
	p = path.Clean("/" + p)
	return p == "/api/v1/strategies" || strings.HasPrefix(p, "/api/v1/strategies/")
}

func (s *Server) strategyRoutes() http.Handler {
	s.apiOnce.Do(func() {
		mux := http.NewServeMux()
		wrapper := &apiv1.ServerInterfaceWrapper{
			Handler:          s,
			ErrorHandlerFunc: s.strategyParamError,
		}
		mux.HandleFunc("GET /api/v1/strategies", wrapper.ListStrategies)
		mux.HandleFunc("GET /api/v1/strategies/{strategy_id}/versions", wrapper.ListStrategyVersions)
		mux.HandleFunc("GET /api/v1/strategies/{strategy_id}/versions/{ref}", wrapper.GetStrategyVersion)
		s.apiHandler = mux
	})
	return s.apiHandler
}

func (s *Server) strategyParamError(w http.ResponseWriter, _ *http.Request, err error) {
	writeProblem(w, http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, "Bad Request", "request is not valid", []apiv1.FieldError{{
		Path:   paramPointer(err),
		Reason: "is not valid",
	}})
}

func paramPointer(err error) string {
	name := ""
	switch e := err.(type) {
	case *apiv1.InvalidParamFormatError:
		name = e.ParamName
	case *apiv1.RequiredParamError:
		name = e.ParamName
	case *apiv1.UnmarshalingParamError:
		name = e.ParamName
	case *apiv1.TooManyValuesForParamError:
		name = e.ParamName
	}
	if name == "" {
		return "/"
	}
	return "/" + name
}

// ListStrategies lists the configured repositories (D1013). latest is the digest of the tag named
// latest, resolved only for this page (D1057, D1058). A missing latest tag omits the field.
func (s *Server) ListStrategies(w http.ResponseWriter, r *http.Request, params apiv1.ListStrategiesParams) {
	limit, after, fields := bindPage(params.Limit, params.Cursor)
	if len(fields) > 0 {
		writeProblem(w, http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, "Bad Request", "request is not valid", fields)
		return
	}
	repos, err := s.strategyRepos()
	if err != nil {
		s.log.Error().Err(err).Msg("catalog")
		writeProblem(w, http.StatusInternalServerError, apiv1.ErrorCodeInternal, "Internal Server Error", "internal error", nil)
		return
	}
	if len(repos) == 0 {
		writeJSON(w, http.StatusOK, apiv1.StrategyList{Items: []apiv1.Strategy{}})
		return
	}
	if !s.ensureCatalog(w) {
		return
	}
	keys := make([]string, len(repos))
	for i, repo := range repos {
		keys[i] = repo.id
	}
	start, end, next := pageKeys(keys, after, limit)
	page := repos[start:end]
	jobs := make([]resolveJob, len(page))
	for i, repo := range page {
		jobs[i] = resolveJob{key: repo.id, repository: repo.path, reference: "latest"}
	}
	digests, err := s.resolveAll(r.Context(), jobs)
	if err != nil {
		s.writeRegistry(w, r, err, "version not found")
		return
	}
	items := make([]apiv1.Strategy, 0, len(page))
	for _, repo := range page {
		item := apiv1.Strategy{Id: repo.id, Repository: repo.repository}
		if dig, ok := digests[repo.id]; ok {
			item.Latest = &apiv1.StrategyTag{Tag: "latest", Digest: dig}
		}
		items = append(items, item)
	}
	writeJSON(w, http.StatusOK, apiv1.StrategyList{Items: items, NextCursor: next})
}

// ListStrategyVersions lists every tag, sorted by name, each resolved to a digest (D1057, D1058).
func (s *Server) ListStrategyVersions(w http.ResponseWriter, r *http.Request, strategyID apiv1.StrategyID, params apiv1.ListStrategyVersionsParams) {
	limit, after, fields := bindPage(params.Limit, params.Cursor)
	if len(fields) > 0 {
		writeProblem(w, http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, "Bad Request", "request is not valid", fields)
		return
	}
	repo, err := s.strategyByID(strategyID)
	if errors.Is(err, errStrategyNotFound) {
		writeProblem(w, http.StatusNotFound, apiv1.ErrorCodeNotFound, "Not Found", "strategy not found", nil)
		return
	}
	if err != nil {
		s.log.Error().Err(err).Msg("catalog")
		writeProblem(w, http.StatusInternalServerError, apiv1.ErrorCodeInternal, "Internal Server Error", "internal error", nil)
		return
	}
	if !s.ensureCatalog(w) {
		return
	}
	tags, err := s.catalog.Tags(r.Context(), repo.path)
	if err != nil {
		s.writeRegistry(w, r, err, "strategy not found")
		return
	}
	tags = uniqueSorted(tags)
	jobs := make([]resolveJob, len(tags))
	for i, tag := range tags {
		jobs[i] = resolveJob{key: tag, repository: repo.path, reference: tag}
	}
	digests, err := s.resolveAll(r.Context(), jobs)
	if err != nil {
		s.writeRegistry(w, r, err, "version not found")
		return
	}
	present := make([]string, 0, len(tags))
	for _, tag := range tags {
		if _, ok := digests[tag]; ok {
			present = append(present, tag)
		}
	}
	start, end, next := pageKeys(present, after, limit)
	items := make([]apiv1.StrategyTag, 0, end-start)
	for _, tag := range present[start:end] {
		items = append(items, apiv1.StrategyTag{Tag: tag, Digest: digests[tag]})
	}
	writeJSON(w, http.StatusOK, apiv1.StrategyTagList{Items: items, NextCursor: next})
}

// GetStrategyVersion returns one manifest. A tag is resolved to a digest first (D934).
func (s *Server) GetStrategyVersion(w http.ResponseWriter, r *http.Request, strategyID apiv1.StrategyID, ref string) {
	repo, err := s.strategyByID(strategyID)
	if errors.Is(err, errStrategyNotFound) {
		writeProblem(w, http.StatusNotFound, apiv1.ErrorCodeNotFound, "Not Found", "strategy not found", nil)
		return
	}
	if err != nil {
		s.log.Error().Err(err).Msg("catalog")
		writeProblem(w, http.StatusInternalServerError, apiv1.ErrorCodeInternal, "Internal Server Error", "internal error", nil)
		return
	}
	if !s.ensureCatalog(w) {
		return
	}
	dig, err := s.versionDigest(r.Context(), repo.path, ref)
	if err != nil {
		s.writeRegistry(w, r, err, "version not found")
		return
	}
	info, err := s.catalog.ReadManifest(r.Context(), repo.path, dig)
	if err != nil {
		s.writeRegistry(w, r, err, "version not found")
		return
	}
	if info.Manifest.Kind != artifact.KindStrategy {
		s.log.Warn().Str("kind", info.Manifest.Kind).Str("digest", info.Digest).Msg("catalog")
		writeProblem(w, http.StatusBadGateway, apiv1.ErrorCodeInvalidArtifact, "Invalid Artifact", "manifest is not a strategy artifact", nil)
		return
	}
	tags, err := s.catalog.Tags(r.Context(), repo.path)
	if err != nil {
		s.writeRegistry(w, r, err, "version not found")
		return
	}
	tags = uniqueSorted(tags)
	jobs := make([]resolveJob, len(tags))
	for i, tag := range tags {
		jobs[i] = resolveJob{key: tag, repository: repo.path, reference: tag}
	}
	digests, err := s.resolveAll(r.Context(), jobs)
	if err != nil {
		s.writeRegistry(w, r, err, "version not found")
		return
	}
	names := make([]string, 0)
	for _, tag := range tags {
		if digests[tag] == info.Digest {
			names = append(names, tag)
		}
	}
	contracts := info.Manifest.Contracts
	if contracts == nil {
		contracts = []string{}
	}
	size := info.Size
	writeJSON(w, http.StatusOK, apiv1.StrategyVersion{
		StrategyId: repo.id,
		Repository: repo.repository,
		Digest:     info.Digest,
		Tags:       names,
		Name:       info.Manifest.Name,
		Runtime:    info.Manifest.Runtime,
		Os:         info.Manifest.OS,
		Arch:       info.Manifest.Arch,
		Sdk:        formatSDK(info.Manifest.SDK),
		Contracts:  contracts,
		SizeBytes:  &size,
	})
}

func (s *Server) versionDigest(ctx context.Context, repository, ref string) (string, error) {
	if strings.HasPrefix(ref, "sha256:") {
		if !sha256Ref.MatchString(ref) {
			return "", &artifact.Error{Reason: artifact.ReasonNotFound, Permanent: true, Err: errors.New("digest is not sha256")}
		}
		return ref, nil
	}
	return s.catalog.Resolve(ctx, repository, ref)
}

func (s *Server) ensureCatalog(w http.ResponseWriter) bool {
	if s.catalog != nil {
		return true
	}
	s.log.Warn().Msg("catalog: registry is not configured")
	writeProblem(w, http.StatusServiceUnavailable, apiv1.ErrorCodeRegistryUnavailable, "Registry Unavailable", "registry is not configured", nil)
	return false
}

func (s *Server) strategyRepos() ([]strategyRepo, error) {
	out := make([]strategyRepo, 0, len(s.cfg.Repositories))
	addr := s.cfg.Registry.Address
	for _, p := range s.cfg.Repositories {
		id, err := strategyID(p)
		if err != nil {
			return nil, err
		}
		ref := p
		if addr != "" {
			ref = addr + "/" + p
		}
		out = append(out, strategyRepo{id: id, path: p, repository: ref})
	}
	slices.SortFunc(out, func(a, b strategyRepo) int { return strings.Compare(a.id, b.id) })
	return out, nil
}

func (s *Server) strategyByID(id string) (strategyRepo, error) {
	repos, err := s.strategyRepos()
	if err != nil {
		return strategyRepo{}, err
	}
	for _, repo := range repos {
		if repo.id == id {
			return repo, nil
		}
	}
	return strategyRepo{}, errStrategyNotFound
}

// resolveAll resolves every job. At most tagResolveParallel requests are in flight (D1057).
// A missing tag is omitted. The first other error cancels the rest and is returned.
// Cancellation of ctx is returned as itself, so a shutdown is not reported as registry_unavailable (D1059).
func (s *Server) resolveAll(ctx context.Context, jobs []resolveJob) (map[string]string, error) {
	out := make(map[string]string, len(jobs))
	if len(jobs) == 0 {
		return out, nil
	}
	parent := ctx
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	var (
		mu   sync.Mutex
		fail error
	)
	sem := make(chan struct{}, tagResolveParallel)
	var wg sync.WaitGroup
	for _, job := range jobs {
		wg.Add(1)
		go func(job resolveJob) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()
			dig, err := s.catalog.Resolve(ctx, job.repository, job.reference)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				if artifact.ReasonOf(err) == artifact.ReasonNotFound {
					return
				}
				if fail == nil {
					fail = err
					cancel()
				}
				return
			}
			out[job.key] = dig
		}(job)
	}
	wg.Wait()
	if fail != nil {
		return nil, fail
	}
	if err := parent.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Server) writeRegistry(w http.ResponseWriter, r *http.Request, err error, missing string) {
	if err == nil {
		return
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		if r.Context().Err() != nil {
			return
		}
		s.log.Warn().Err(err).Msg("catalog")
		writeProblem(w, http.StatusInternalServerError, apiv1.ErrorCodeInternal, "Internal Server Error", "internal error", nil)
		return
	}
	status, code, title, detail := registryProblem(err, missing)
	s.log.Warn().Err(err).Int("status", status).Str("code", string(code)).Msg("catalog")
	writeProblem(w, status, code, title, detail, nil)
}

// registryProblem maps a registry error onto the contract (D1059). 401 and 403 stay on the existing
// internal code: D1059 does not name a code for them.
func registryProblem(err error, missing string) (int, apiv1.ErrorCode, string, string) {
	if missing == "" {
		missing = "version not found"
	}
	switch artifact.ReasonOf(err) {
	case artifact.ReasonNotFound:
		return http.StatusNotFound, apiv1.ErrorCodeNotFound, "Not Found", missing
	case artifact.ReasonUnavailable:
		if artifact.Permanent(err) {
			return http.StatusInternalServerError, apiv1.ErrorCodeInternal, "Internal Server Error", "internal error"
		}
		return http.StatusServiceUnavailable, apiv1.ErrorCodeRegistryUnavailable, "Registry Unavailable", "registry unavailable"
	case artifact.ReasonNotImage, artifact.ReasonWrongKind, artifact.ReasonWrongMediaType:
		return http.StatusBadGateway, apiv1.ErrorCodeInvalidArtifact, "Invalid Artifact", "manifest is not a strategy artifact"
	case artifact.ReasonBadManifest, artifact.ReasonDigestMismatch, artifact.ReasonTooLarge:
		return http.StatusBadGateway, apiv1.ErrorCodeInvalidArtifact, "Invalid Artifact", "manifest could not be read"
	default:
		return http.StatusInternalServerError, apiv1.ErrorCodeInternal, "Internal Server Error", "internal error"
	}
}

func bindPage(limit *apiv1.Limit, cursor *apiv1.Cursor) (int, string, []apiv1.FieldError) {
	n := pageDefault
	var fields []apiv1.FieldError
	if limit != nil {
		if *limit < 1 || *limit > pageMax {
			fields = append(fields, apiv1.FieldError{Path: "/limit", Reason: "must be between 1 and 1000"})
		} else {
			n = *limit
		}
	}
	after := ""
	if cursor != nil && *cursor != "" {
		b, err := base64.RawURLEncoding.DecodeString(*cursor)
		if err != nil || len(b) == 0 {
			fields = append(fields, apiv1.FieldError{Path: "/cursor", Reason: "is not a cursor from this API"})
		} else {
			after = string(b)
		}
	}
	return n, after, fields
}

func pageKeys(keys []string, after string, limit int) (start, end int, next *string) {
	start = 0
	if after != "" {
		start = sort.Search(len(keys), func(i int) bool { return keys[i] > after })
	}
	end = start + limit
	if end > len(keys) {
		end = len(keys)
	}
	if end < len(keys) && end > start {
		c := base64.RawURLEncoding.EncodeToString([]byte(keys[end-1]))
		next = &c
	}
	return start, end, next
}

func uniqueSorted(tags []string) []string {
	if len(tags) == 0 {
		return []string{}
	}
	out := slices.Clone(tags)
	slices.Sort(out)
	return slices.Compact(out)
}

// formatSDK is how the catalog shows the manifest's sdk object. Both fields set is "lang version".
// One field set is that field. Both empty (md.conn and td.account, D989) omits sdk.
func formatSDK(sdk artifact.SDK) *string {
	switch {
	case sdk.Lang != "" && sdk.Version != "":
		s := sdk.Lang + " " + sdk.Version
		return &s
	case sdk.Lang != "":
		s := sdk.Lang
		return &s
	case sdk.Version != "":
		s := sdk.Version
		return &s
	default:
		return nil
	}
}

func writeProblem(w http.ResponseWriter, status int, code apiv1.ErrorCode, title, detail string, fields []apiv1.FieldError) {
	p := apiv1.Problem{
		Type:   "about:blank",
		Title:  title,
		Status: status,
		Code:   code,
	}
	if detail != "" {
		p.Detail = &detail
	}
	if len(fields) > 0 {
		p.Errors = &fields
	}
	b, err := json.Marshal(p)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_, _ = w.Write(append(b, '\n'))
}
