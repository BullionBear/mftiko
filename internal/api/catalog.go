package api

import (
	"context"
	"encoding/base64"
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

// tagResolveParallel is the most tag resolutions one catalog request runs at once (E10-D057).
const tagResolveParallel = 8

const (
	pageDefault = 100
	pageMax     = 1000
)

// tagCatalog is the read-only registry surface (E10-D056). *artifact.Catalog implements it.
// Tests substitute a fake. There is no cache in front of it (E10-D057).
type tagCatalog interface {
	Tags(ctx context.Context, repository string) ([]string, error)
	Resolve(ctx context.Context, repository, reference string) (string, error)
	ReadManifest(ctx context.Context, repository, digest string) (artifact.ManifestInfo, error)
	ReadData(ctx context.Context, repository, digest string) (artifact.DataInfo, error)
}

// sha256Ref is the contract's digest (sha256 and 64 lowercase hex digits).
var sha256Ref = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

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
	s.strategyOnce.Do(func() {
		mux := http.NewServeMux()
		wrapper := &apiv1.ServerInterfaceWrapper{
			Handler:          s,
			ErrorHandlerFunc: s.strategyParamError,
		}
		mux.HandleFunc("GET /api/v1/strategies", wrapper.ListStrategies)
		s.strategyHandler = mux
	})
	return s.strategyHandler
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

// ListStrategies lists tags of one repository (E10-D121). latest is the tag named latest, included
// even when that tag is not on this page. A missing latest tag omits the field. Kind is not checked.
func (s *Server) ListStrategies(w http.ResponseWriter, r *http.Request, params apiv1.ListStrategiesParams) {
	limit, after, fields := bindPage(params.Limit, params.Cursor)
	full := strings.TrimRight(params.Repository, "/")
	if params.Repository == "" {
		fields = append(fields, apiv1.FieldError{Path: "/repository", Reason: "required"})
	}
	if len(fields) > 0 {
		writeProblem(w, http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, "Bad Request", "request is not valid", fields)
		return
	}
	path, admitted, err := s.admitRepository("/repository", full)
	if errors.Is(err, errRegistry) {
		s.log.Warn().Msg("catalog: registry is not configured")
		writeProblem(w, http.StatusServiceUnavailable, apiv1.ErrorCodeRegistryUnavailable, "Registry Unavailable", "registry is not configured", nil)
		return
	}
	if err != nil {
		s.writeRegistry(w, r, err, "repository not found")
		return
	}
	if len(admitted) > 0 {
		writeProblem(w, http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, "Bad Request", "request is not valid", admitted)
		return
	}
	if !s.ensureCatalog(w) {
		return
	}
	tags, err := s.catalog.Tags(r.Context(), path)
	if err != nil {
		s.writeRegistry(w, r, err, "repository not found")
		return
	}
	tags = uniqueSorted(tags)
	jobs := make([]resolveJob, len(tags))
	for i, tag := range tags {
		jobs[i] = resolveJob{key: tag, repository: path, reference: tag}
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
	body := apiv1.StrategyRepository{Repository: full, Items: items, NextCursor: next}
	if dig, ok := digests["latest"]; ok {
		body.Latest = &apiv1.StrategyTag{Tag: "latest", Digest: dig}
	}
	writeJSON(w, http.StatusOK, body)
}

// admitRepository checks shape and the allowlist (E10-D119, E10-D120, E10-D122).
// The allowlist is checked before the registry is used, including when the registry is down.
// The returned value is the full repository, host included. It is what the catalog reads and
// what a session spec stores. A name that is not host/path is not a repository.
func (s *Server) admitRepository(pointer, repository string) (string, fieldList, error) {
	repository = strings.TrimRight(repository, "/")
	if repository == "" {
		return "", fieldList{{Path: pointer, Reason: "required"}}, nil
	}
	if err := repositoryShape(repository); err != nil {
		return "", fieldList{{Path: pointer, Reason: "is not a repository"}}, nil
	}
	if !allowMatches(s.cfg.Allow, repository) {
		return "", fieldList{{Path: pointer, Reason: "is not allowed"}}, nil
	}
	if _, _, err := artifact.SplitRepository(repository); err != nil {
		return "", fieldList{{Path: pointer, Reason: "is not a repository"}}, nil
	}
	return repository, nil, nil
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

// resolveAll resolves every job. At most tagResolveParallel requests are in flight (E10-D057).
// A missing tag is omitted. The first other error cancels the rest and is returned.
// Cancellation of ctx is returned as itself, so a shutdown is not reported as registry_unavailable (E10-D059).
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

// registryProblem maps a registry error onto the contract (E10-D059, E10-D127).
// 401 and 403 are registry_unavailable, the same code as a registry that cannot be reached.
func registryProblem(err error, missing string) (int, apiv1.ErrorCode, string, string) {
	if missing == "" {
		missing = "version not found"
	}
	switch artifact.ReasonOf(err) {
	case artifact.ReasonNotFound:
		return http.StatusNotFound, apiv1.ErrorCodeNotFound, "Not Found", missing
	case artifact.ReasonUnauthorized:
		return http.StatusServiceUnavailable, apiv1.ErrorCodeRegistryUnavailable, "Registry Unavailable", "registry unavailable"
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
