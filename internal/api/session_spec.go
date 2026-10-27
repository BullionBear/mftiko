package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/artifact"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// errAlgoOverlap is a best-effort cross-session algo_id collision (E10-D012). It is 409 algo_overlap.
// errIdempotencyConflict is the same Idempotency-Key with a different body (E10-D068). It is 409 conflict.
// errIdempotentReplay is internal: the key already names this spec, and the caller finishes that row.
var (
	errStorage              = errors.New("api: storage unavailable")
	errRegistry             = errors.New("api: registry unavailable")
	errRegistryUnconfigured = fmt.Errorf("%w: registry is not configured", errRegistry)
	errAlgoOverlap          = errors.New("api: algo overlap")
	errIdempotencyConflict  = errors.New("api: idempotency key does not match")
	errIdempotentReplay     = errors.New("api: idempotent replay")
	// errNotTerminal is DELETE of a session that is not DONE or FAILED (E10-D017, E10-D084).
	errNotTerminal = errors.New("api: session is not terminal")
	// errNoRecord is a session etcd still has and session_records does not (E10-D081).
	// It is not inserted and not deleted.
	errNoRecord = errors.New("api: session has no record")
)

var (
	labelKey     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]{0,62}$`)
	inputNamePat = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)
	instancePat  = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)
)

const (
	maxLabels     = 32
	maxLabelValue = 256
	maxNameLen    = 128
)

type fieldList []apiv1.FieldError

func (f *fieldList) add(path, reason string) {
	*f = append(*f, apiv1.FieldError{Path: path, Reason: reason})
}

// validIdempotencyKey accepts 1 to 255 bytes of visible ASCII. E10-D068 names the header and the
// 24 hour retention, not the character set. A space or a control byte is rejected so the key
// cannot be blank by whitespace.
func validIdempotencyKey(k string) bool {
	if len(k) < 1 || len(k) > 255 {
		return false
	}
	for i := 0; i < len(k); i++ {
		if k[i] < 0x21 || k[i] > 0x7e {
			return false
		}
	}
	return true
}

func pointerToken(s string) string {
	s = strings.ReplaceAll(s, "~", "~0")
	s = strings.ReplaceAll(s, "/", "~1")
	return s
}

func decodeCreate(body []byte) (apiv1.SessionCreate, fieldList) {
	var req apiv1.SessionCreate
	var fields fieldList
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		var syn *json.SyntaxError
		if errors.As(err, &syn) {
			fields.add("/", "is not valid JSON")
			return req, fields
		}
		msg := err.Error()
		const unknown = `json: unknown field "`
		if strings.HasPrefix(msg, unknown) {
			name := strings.TrimSuffix(strings.TrimPrefix(msg, unknown), `"`)
			fields.add("/"+pointerToken(name), "is not accepted")
			return req, fields
		}
		fields.add("/", "is not valid")
		return req, fields
	}
	if dec.More() {
		fields.add("/", "is not valid JSON")
	}
	return req, fields
}

// specContentEqual reports whether two spec payloads are the same aside from desired_state.
// E13-D003 makes that the only mutable field, so a hash of the raw bytes changes when a session
// ends. E10-D071's mismatch is every other difference (E10-D031).
func specContentEqual(a, b []byte) bool {
	if bytes.Equal(a, b) {
		return true
	}
	var sa, sb sessionv1.SessionSpec
	if proto.Unmarshal(a, &sa) != nil || proto.Unmarshal(b, &sb) != nil {
		return false
	}
	sa.DesiredState = 0
	sb.DesiredState = 0
	return proto.Equal(&sa, &sb)
}

func desiredOfSpec(b []byte) string {
	var spec sessionv1.SessionSpec
	if proto.Unmarshal(b, &spec) != nil {
		return desRunning
	}
	if spec.DesiredState == sessionv1.DesiredState_DESIRED_STATE_ENDED {
		return desEnded
	}
	return desRunning
}

func (s *Server) buildSessionSpec(ctx context.Context, req apiv1.SessionCreate) (*sessionv1.SessionSpec, fieldList, error) {
	fields := s.structural(req)
	if len(fields) > 0 {
		return nil, fields, nil
	}
	spec := &sessionv1.SessionSpec{
		Name:         req.Name,
		DesiredState: sessionv1.DesiredState_DESIRED_STATE_RUNNING,
	}
	if req.Labels != nil {
		spec.Labels = map[string]string{}
		for k, v := range *req.Labels {
			spec.Labels[k] = v
		}
	}
	if err := s.fillNumbers(req, spec, &fields); err != nil {
		return nil, fields, err
	}
	if len(fields) > 0 {
		return nil, fields, nil
	}
	if err := s.fillStrategy(ctx, req, spec, &fields); err != nil || len(fields) > 0 {
		return nil, fields, err
	}
	if err := s.fillInputs(ctx, req, spec, &fields); err != nil || len(fields) > 0 {
		return nil, fields, err
	}
	views, err := s.availableViews(req.StsInstance)
	if err != nil {
		return nil, nil, err
	}
	if err := s.fillAccounts(views, req, spec, &fields); err != nil || len(fields) > 0 {
		return nil, fields, err
	}
	return spec, nil, nil
}

func (s *Server) structural(req apiv1.SessionCreate) fieldList {
	var fields fieldList
	if req.Name == "" {
		fields.add("/name", "required")
	} else if utf8.RuneCountInString(req.Name) > maxNameLen {
		fields.add("/name", "must be at most 128 characters")
	}
	if req.StsInstance == "" {
		fields.add("/sts_instance", "required")
	} else {
		s.checkInstance(req.StsInstance, "/sts_instance", state.PlaneSTS, &fields)
	}
	repo := strings.TrimRight(req.Strategy.Repository, "/")
	if req.Strategy.Repository == "" {
		fields.add("/strategy/repository", "required")
	} else if err := repositoryShape(repo); err != nil {
		fields.add("/strategy/repository", "is not a repository")
	} else if _, _, err := artifact.SplitRepository(repo); err != nil {
		fields.add("/strategy/repository", "is not a repository")
	} else if !allowMatches(s.cfg.Allow, repo) {
		fields.add("/strategy/repository", "is not allowed")
	}
	if req.Strategy.Ref == "" {
		fields.add("/strategy/ref", "required")
	}
	if req.Labels != nil {
		if len(*req.Labels) > maxLabels {
			fields.add("/labels", "must have at most 32 keys")
		}
		for k, v := range *req.Labels {
			p := "/labels/" + pointerToken(k)
			if !labelKey.MatchString(k) {
				fields.add(p, "key is not valid")
			}
			if utf8.RuneCountInString(v) > maxLabelValue {
				fields.add(p, "value must be at most 256 characters")
			}
		}
	}
	if req.Params != nil {
		if _, err := compactObject(*req.Params); err != nil {
			fields.add("/params", "must be a JSON object")
		}
	}
	feeds := map[string]struct{}{}
	if req.Feeds != nil {
		for i, f := range *req.Feeds {
			p := fmt.Sprintf("/feeds/%d", i)
			if f.Feed == "" {
				fields.add(p+"/feed", "required")
			} else if _, ok := feeds[f.Feed]; ok {
				fields.add(p+"/feed", "duplicate")
			} else {
				feeds[f.Feed] = struct{}{}
			}
			s.checkInstance(f.MdInstance, p+"/md_instance", state.PlaneMD, &fields)
		}
	}
	seenAcct := map[int64]struct{}{}
	if req.Accounts != nil {
		for i, a := range *req.Accounts {
			p := fmt.Sprintf("/accounts/%d", i)
			if a.Name == "" {
				fields.add(p+"/name", "required")
			}
			if a.ApiId < 1 || a.ApiId > maxAPIID {
				fields.add(p+"/api_id", "must be between 1 and 9007199254740991")
			} else if _, ok := seenAcct[a.ApiId]; ok {
				fields.add(p+"/api_id", "duplicate")
			} else {
				seenAcct[a.ApiId] = struct{}{}
			}
			if len(a.AlgoIds) == 0 {
				fields.add(p+"/algo_ids", "required")
			}
			algos := map[int]struct{}{}
			for _, id := range a.AlgoIds {
				if id < 1 || id > 1023 {
					fields.add(p+"/algo_ids", "must be between 1 and 1023")
					break
				}
				if _, ok := algos[id]; ok {
					fields.add(p+"/algo_ids", "duplicate")
					break
				}
				algos[id] = struct{}{}
			}
		}
	}
	if req.Restart != nil && req.Restart.Mode != nil && !req.Restart.Mode.Valid() {
		fields.add("/restart/mode", "is not valid")
	}
	names := map[string]struct{}{}
	if req.Inputs != nil {
		for i, in := range *req.Inputs {
			p := fmt.Sprintf("/inputs/%d", i)
			if !inputNamePat.MatchString(in.Name) {
				fields.add(p+"/name", "is not a valid name")
			} else if _, ok := names[in.Name]; ok {
				fields.add(p+"/name", "duplicate")
			} else {
				names[in.Name] = struct{}{}
			}
			if !sha256Ref.MatchString(in.Digest) {
				fields.add(p+"/digest", "must be a sha256 digest")
			}
			inRepo := strings.TrimRight(in.Repository, "/")
			if in.Repository == "" || strings.ContainsAny(in.Repository, " \t\r\n") {
				fields.add(p+"/repository", "required")
			} else if _, _, err := artifact.SplitRepository(inRepo); err != nil {
				fields.add(p+"/repository", "is not a repository")
			} else if !allowMatches(s.cfg.Allow, inRepo) {
				fields.add(p+"/repository", "is not allowed")
			}
		}
	}
	checkU32(req.DepsTimeoutS, "/deps_timeout_s", &fields)
	checkU32(req.StartTimeoutS, "/start_timeout_s", &fields)
	checkU32(req.RecvWindowMs, "/recv_window_ms", &fields)
	checkU32(req.GraceMs, "/grace_ms", &fields)
	if req.Restart != nil {
		checkU32(req.Restart.MaxRestarts, "/restart/max_restarts", &fields)
		checkU32(req.Restart.WindowS, "/restart/window_s", &fields)
	}
	if req.Limits != nil {
		checkU32(req.Limits.MemoryMb, "/limits/memory_mb", &fields)
		checkU32(req.Limits.Gomaxprocs, "/limits/gomaxprocs", &fields)
		checkU32(req.Limits.VolumeMb, "/limits/volume_mb", &fields)
		checkU32(req.Limits.HookTimeoutS, "/limits/hook_timeout_s", &fields)
	}
	return fields
}

func checkU32(n *int, path string, fields *fieldList) {
	if n == nil {
		return
	}
	if *n < 0 || *n > math.MaxUint32 {
		fields.add(path, "must be between 0 and 4294967295")
	}
}

func (s *Server) checkInstance(name, path string, want state.Plane, fields *fieldList) {
	if !instancePat.MatchString(name) {
		fields.add(path, "is not a valid instance name")
		return
	}
	if _, err := s.cfg.Topology.RegionOf(name); err != nil {
		fields.add(path, "is not in the topology")
		return
	}
	if s.cfg.Planes[name] != want {
		fields.add(path, "must be a "+string(want)+" instance")
	}
}

func (s *Server) fillNumbers(req apiv1.SessionCreate, spec *sessionv1.SessionSpec, fields *fieldList) error {
	if req.Params != nil {
		b, err := compactObject(*req.Params)
		if err != nil {
			fields.add("/params", "must be a JSON object")
		} else {
			spec.Params = b
		}
	}
	var err error
	spec.DepsTimeoutS, err = mustU32(req.DepsTimeoutS)
	if err != nil {
		fields.add("/deps_timeout_s", "must be between 0 and 4294967295")
	}
	spec.StartTimeoutS, err = mustU32(req.StartTimeoutS)
	if err != nil {
		fields.add("/start_timeout_s", "must be between 0 and 4294967295")
	}
	spec.RecvWindowMs, err = mustU32(req.RecvWindowMs)
	if err != nil {
		fields.add("/recv_window_ms", "must be between 0 and 4294967295")
	}
	spec.GraceMs, err = mustU32(req.GraceMs)
	if err != nil {
		fields.add("/grace_ms", "must be between 0 and 4294967295")
	}
	if req.Restart != nil {
		rp := &sessionv1.RestartPolicy{}
		if req.Restart.Mode != nil {
			switch *req.Restart.Mode {
			case apiv1.OnFailure:
				rp.Mode = sessionv1.RestartMode_RESTART_MODE_ON_FAILURE
			case apiv1.Never:
				rp.Mode = sessionv1.RestartMode_RESTART_MODE_NEVER
			default:
				fields.add("/restart/mode", "is not valid")
			}
		}
		rp.MaxRestarts, err = mustU32(req.Restart.MaxRestarts)
		if err != nil {
			fields.add("/restart/max_restarts", "must be between 0 and 4294967295")
		}
		rp.WindowS, err = mustU32(req.Restart.WindowS)
		if err != nil {
			fields.add("/restart/window_s", "must be between 0 and 4294967295")
		}
		spec.Restart = rp
	}
	if req.Limits != nil {
		lim := &sessionv1.Limits{}
		lim.MemoryMb, err = mustU32(req.Limits.MemoryMb)
		if err != nil {
			fields.add("/limits/memory_mb", "must be between 0 and 4294967295")
		}
		lim.Gomaxprocs, err = mustU32(req.Limits.Gomaxprocs)
		if err != nil {
			fields.add("/limits/gomaxprocs", "must be between 0 and 4294967295")
		}
		lim.VolumeMb, err = mustU32(req.Limits.VolumeMb)
		if err != nil {
			fields.add("/limits/volume_mb", "must be between 0 and 4294967295")
		}
		lim.HookTimeoutS, err = mustU32(req.Limits.HookTimeoutS)
		if err != nil {
			fields.add("/limits/hook_timeout_s", "must be between 0 and 4294967295")
		}
		spec.Limits = lim
	}
	if req.Feeds != nil {
		for _, f := range *req.Feeds {
			spec.Feeds = append(spec.Feeds, &sessionv1.FeedDecl{Feed: f.Feed, MdInstance: f.MdInstance})
		}
	}
	return nil
}

func mustU32(n *int) (uint32, error) {
	if n == nil {
		return 0, nil
	}
	if *n < 0 || *n > math.MaxUint32 {
		return 0, errors.New("range")
	}
	return uint32(*n), nil
}

func compactObject(raw json.RawMessage) ([]byte, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, errors.New("not an object")
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, trimmed); err != nil {
		return nil, err
	}
	if buf.Len() == 0 || buf.Bytes()[0] != '{' {
		return nil, errors.New("not an object")
	}
	return buf.Bytes(), nil
}

func (s *Server) fillStrategy(ctx context.Context, req apiv1.SessionCreate, spec *sessionv1.SessionSpec, fields *fieldList) error {
	path, admitted, err := s.admitRepository("/strategy/repository", req.Strategy.Repository)
	if err != nil {
		return err
	}
	if len(admitted) > 0 {
		*fields = append(*fields, admitted...)
		return nil
	}
	if s.catalog == nil {
		return errRegistryUnconfigured
	}
	dig, err := s.versionDigest(ctx, path, req.Strategy.Ref)
	if ferrs, ferr := registryFields(err, "/strategy/ref", "was not found"); ferr != nil || len(ferrs) > 0 {
		*fields = append(*fields, ferrs...)
		return ferr
	}
	info, err := s.catalog.ReadManifest(ctx, path, dig)
	if ferrs, ferr := registryFields(err, "/strategy/ref", "was not found"); ferr != nil || len(ferrs) > 0 {
		*fields = append(*fields, ferrs...)
		return ferr
	}
	if info.Manifest.Kind != artifact.KindStrategy {
		fields.add("/strategy/ref", "is not a strategy artifact")
		return nil
	}
	spec.Strategy = &sessionv1.StrategyRef{
		Name:       info.Manifest.Name,
		Code:       info.Digest,
		Repository: path,
	}
	return nil
}

func (s *Server) fillInputs(ctx context.Context, req apiv1.SessionCreate, spec *sessionv1.SessionSpec, fields *fieldList) error {
	if req.Inputs == nil {
		return nil
	}
	if s.catalog == nil {
		return errRegistryUnconfigured
	}
	for i, in := range *req.Inputs {
		p := fmt.Sprintf("/inputs/%d", i)
		repo := strings.TrimRight(in.Repository, "/")
		if _, _, err := artifact.SplitRepository(repo); err != nil {
			fields.add(p+"/repository", "is not a repository")
			continue
		}
		info, err := s.catalog.ReadData(ctx, repo, in.Digest)
		if ferrs, ferr := registryFields(err, p+"/digest", "is not a data artifact"); ferr != nil {
			return ferr
		} else if len(ferrs) > 0 {
			*fields = append(*fields, ferrs...)
			continue
		}
		if info.Config.Schema != artifact.DataSchema {
			fields.add(p+"/digest", "is not a data artifact")
			continue
		}
		if info.Size > s.maxInputBytes() {
			fields.add(p+"/digest", "is too large")
			continue
		}
		spec.Inputs = append(spec.Inputs, &sessionv1.InputDecl{
			Name: in.Name, Digest: info.Digest, Repository: repo,
		})
	}
	return nil
}

func registryFields(err error, path, missing string) (fieldList, error) {
	if err == nil {
		return nil, nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil, err
	}
	switch artifact.ReasonOf(err) {
	case artifact.ReasonUnavailable, artifact.ReasonUnauthorized:
		return nil, fmt.Errorf("%w: %w", errRegistry, err)
	case "":
		if errors.Is(err, artifact.ErrRequest) {
			return fieldList{{Path: path, Reason: missing}}, nil
		}
		return nil, err
	default:
		return fieldList{{Path: path, Reason: missing}}, nil
	}
}

func (s *Server) fillAccounts(views map[string]state.View, req apiv1.SessionCreate, spec *sessionv1.SessionSpec, fields *fieldList) error {
	if req.Accounts == nil {
		return nil
	}
	down := len(views) < len(s.clusters)
	for i, a := range *req.Accounts {
		p := fmt.Sprintf("/accounts/%d/api_id", i)
		inst, ok := findAccount(views, a.ApiId)
		if !ok {
			if down {
				return fmt.Errorf("%w: account %d", errRegionUnavailable, a.ApiId)
			}
			fields.add(p, "was not found")
			continue
		}
		if s.cfg.Planes[inst] != state.PlaneTD {
			fields.add(p, "must be a td instance")
			continue
		}
		decl := &sessionv1.AccountDecl{Name: a.Name, ApiId: a.ApiId, TdInstance: inst}
		for _, id := range a.AlgoIds {
			decl.AlgoIds = append(decl.AlgoIds, uint32(id))
		}
		spec.Accounts = append(spec.Accounts, decl)
	}
	return nil
}

func findAccount(views map[string]state.View, apiID int64) (string, bool) {
	var found string
	for _, view := range views {
		for key := range view.Entries {
			p, inst, err := state.Owner(key)
			if err != nil || p != state.PlaneTD {
				continue
			}
			if key == state.AccountKey(inst, apiID) {
				if found == "" || inst < found {
					found = inst
				}
			}
		}
	}
	return found, found != ""
}

// algoOverlap is the best-effort check against the mirrors (E10-D012). except is a session being
// replayed, so its own algos are not a conflict. A region that is down is skipped.
func algoOverlap(views map[string]state.View, spec *sessionv1.SessionSpec, except string) error {
	want := map[algoUse]struct{}{}
	for _, a := range spec.GetAccounts() {
		for _, id := range a.GetAlgoIds() {
			want[algoUse{api: a.GetApiId(), algo: id}] = struct{}{}
		}
	}
	if len(want) == 0 {
		return nil
	}
	for _, view := range views {
		for key, entry := range view.Entries {
			p, inst, err := state.Owner(key)
			if err != nil || p != state.PlaneSTS || key != state.SessionSpecKey(inst, sessionIDOfSpec(key, inst)) {
				continue
			}
			id := sessionIDOfSpec(key, inst)
			if id == "" || id == except {
				continue
			}
			// A retired STS instance cannot write a terminal status (W1). The
			// retirement key, present in this same region mirror, releases the
			// algo (E12-D034). Without the key the check is unchanged.
			if _, retired := view.Entries[state.RetiredKey(inst)]; retired {
				continue
			}
			var other sessionv1.SessionSpec
			if proto.Unmarshal(entry.Value, &other) != nil {
				continue
			}
			if sessionTerminal(statusOf(view, inst, id)) {
				continue
			}
			for _, a := range other.GetAccounts() {
				for _, algo := range a.GetAlgoIds() {
					if _, ok := want[algoUse{api: a.GetApiId(), algo: algo}]; ok {
						return errAlgoOverlap
					}
				}
			}
		}
	}
	return nil
}

type algoUse struct {
	api  int64
	algo uint32
}

func sessionIDOfSpec(key, inst string) string {
	id, ok := strings.CutPrefix(key, state.SessionSpecPrefix(inst))
	if !ok || id == "" || strings.Contains(id, "/") {
		return ""
	}
	return id
}

func statusOf(view state.View, inst, id string) *statev1.SessionStatus {
	entry, ok := view.Entries[state.SessionStatusKey(inst, id)]
	if !ok {
		return nil
	}
	var st statev1.SessionStatus
	if proto.Unmarshal(entry.Value, &st) != nil {
		return nil
	}
	return &st
}

func sessionTerminal(st *statev1.SessionStatus) bool {
	if st == nil {
		return false
	}
	return st.Phase == statev1.Phase_PHASE_DONE || st.Phase == statev1.Phase_PHASE_FAILED
}

// maxInputBytes is the per-input limit on the declared config plus layer (E15-D006).
func (s *Server) maxInputBytes() int64 {
	if s.cfg.MaxInputBytes > 0 {
		return s.cfg.MaxInputBytes
	}
	return artifact.DefaultMaxBytes
}
