package api

import (
	"cmp"
	"context"
	"encoding/base64"
	"maps"
	"net/http"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	openapi_types "github.com/oapi-codegen/runtime/types"
	"google.golang.org/protobuf/proto"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

func (s *Server) ListSessions(w http.ResponseWriter, r *http.Request, params apiv1.ListSessionsParams) {
	limit, after, fields := bindPage(params.Limit, params.Cursor)
	if len(fields) > 0 {
		writeProblem(w, http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, http.StatusText(http.StatusBadRequest), "invalid request", fields)
		return
	}
	views, unavail := s.regionViews()
	refs := s.sessionRefs(views, params.Phase, params.Active, params.StsInstance)
	keys := make([]string, len(refs))
	for i, ref := range refs {
		keys[i] = ref.pageKey
	}
	start, end, next := pageKeys(keys, after, limit)
	page := refs[start:end]
	rows := s.recordsForPage(r.Context(), page)
	items := make([]apiv1.Session, 0, len(page))
	for _, ref := range page {
		rec := pairRecord(rows[ref.id], ref.id, ref.createRev, ref.spec)
		sess, err := s.sessionIn(views, ref.sts, ref.id, ref.spec, ref.rev, rec)
		if err != nil {
			s.writeSessionProblem(w, err)
			return
		}
		items = append(items, sess)
	}
	writeJSON(w, http.StatusOK, apiv1.SessionList{
		Freshness: s.freshness(views, unavail), Items: items, NextCursor: next,
	})
}

func (s *Server) GetSession(w http.ResponseWriter, r *http.Request, id apiv1.SessionID) {
	sts, err := s.findSpec(string(id))
	if err != nil {
		s.writeSessionProblem(w, err)
		return
	}
	views, unavail := s.regionViews()
	region, err := s.cfg.Topology.RegionOf(sts)
	if err != nil {
		s.writeSessionProblem(w, err)
		return
	}
	view, ok := views[region]
	if !ok {
		s.writeSessionProblem(w, errRegionUnavailable)
		return
	}
	e, ok := view.Entries[state.SessionSpecKey(sts, string(id))]
	if !ok {
		s.notFound(w)
		return
	}
	rec, err := s.rowForView(r.Context(), string(id), e.CreateRevision, e.Value)
	if err != nil {
		rec = nil
	}
	sess, err := s.sessionIn(views, sts, string(id), append([]byte(nil), e.Value...), e.ModRevision, rec)
	if err != nil {
		s.writeSessionProblem(w, err)
		return
	}
	writeJSON(w, http.StatusOK, detailFrom(sess, s.freshness(views, unavail)))
}

type sessionRef struct {
	id, sts, pageKey string
	rev, createRev   int64
	spec             []byte
}

func (s *Server) sessionRefs(views map[string]state.View, phases *[]apiv1.Phase, active *bool, stsInst *string) []sessionRef {
	var wantPhase []apiv1.Phase
	if phases != nil {
		wantPhase = *phases
	}
	var out []sessionRef
	for region, view := range views {
		for key, e := range view.Entries {
			plane, inst, err := state.Owner(key)
			if err != nil || plane != state.PlaneSTS {
				continue
			}
			home, err := s.cfg.Topology.RegionOf(inst)
			if err != nil || home != region {
				continue
			}
			id := sessionIDOfSpec(key, inst)
			if id == "" {
				continue
			}
			if stsInst != nil && *stsInst != "" && *stsInst != inst {
				continue
			}
			st := statusOf(view, inst, id)
			if !phaseOK(st, phases, wantPhase) || !activeOK(st, active) {
				continue
			}
			out = append(out, sessionRef{
				id: id, sts: inst, pageKey: id + "\x00" + inst,
				rev: e.ModRevision, createRev: e.CreateRevision,
				spec: append([]byte(nil), e.Value...),
			})
		}
	}
	slices.SortFunc(out, func(a, b sessionRef) int { return strings.Compare(a.pageKey, b.pageKey) })
	return out
}

func phaseOK(st *statev1.SessionStatus, set *[]apiv1.Phase, phases []apiv1.Phase) bool {
	if set == nil {
		return true
	}
	if st == nil {
		return false
	}
	return slices.Contains(phases, phaseOf(st.GetPhase()))
}

func activeOK(st *statev1.SessionStatus, active *bool) bool {
	if active == nil {
		return true
	}
	term := sessionTerminal(st)
	if *active {
		return !term
	}
	return term
}

func (s *Server) recordsForPage(ctx context.Context, refs []sessionRef) map[string][]sessionRecord {
	if len(refs) == 0 {
		return nil
	}
	ids := make([]string, len(refs))
	seen := map[string]struct{}{}
	n := 0
	for _, ref := range refs {
		if _, ok := seen[ref.id]; ok {
			continue
		}
		seen[ref.id] = struct{}{}
		ids[n] = ref.id
		n++
	}
	rows, err := s.recordsBySessionIDs(ctx, ids[:n])
	if err != nil {
		s.log.Warn().Err(err).Msg("session list records")
		return nil
	}
	out := map[string][]sessionRecord{}
	for _, row := range rows {
		out[row.SessionID] = append(out[row.SessionID], row)
	}
	return out
}

func (s *Server) ListInstances(w http.ResponseWriter, _ *http.Request, params apiv1.ListInstancesParams) {
	if params.Plane != nil && !params.Plane.Valid() {
		writeProblem(w, http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, http.StatusText(http.StatusBadRequest), "invalid request", []apiv1.FieldError{{
			Path: "/plane", Reason: "is not a plane",
		}})
		return
	}
	views, unavail := s.regionViews()
	names := slices.Sorted(maps.Keys(s.cfg.Planes))
	items := make([]apiv1.Instance, 0, len(names))
	for _, name := range names {
		plane := s.cfg.Planes[name]
		if params.Plane != nil && string(*params.Plane) != string(plane) {
			continue
		}
		item, ok := s.instanceSummary(views, name, plane)
		if !ok {
			continue
		}
		items = append(items, item)
	}
	writeJSON(w, http.StatusOK, apiv1.InstanceList{
		Controller: s.controllerStatus(views),
		Freshness:  s.freshness(views, unavail),
		Items:      items,
	})
}

func (s *Server) GetInstance(w http.ResponseWriter, _ *http.Request, name apiv1.InstanceName) {
	plane, ok := s.cfg.Planes[string(name)]
	if !ok || string(name) == state.ControllerInstance {
		s.notFound(w)
		return
	}
	views, unavail := s.regionViews()
	region, err := s.cfg.Topology.RegionOf(string(name))
	if err != nil {
		s.notFound(w)
		return
	}
	if _, avail := views[region]; !avail {
		writeProblem(w, http.StatusServiceUnavailable, apiv1.ErrorCodeRegionUnavailable, "Region Unavailable", "region is unavailable", nil)
		return
	}
	detail, ok := s.instanceDetail(views, string(name), plane, s.freshness(views, unavail))
	if !ok {
		s.notFound(w)
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

func (s *Server) instanceSummary(views map[string]state.View, name string, plane state.Plane) (apiv1.Instance, bool) {
	apiP, ok := apiPlane(plane)
	if !ok {
		return apiv1.Instance{}, false
	}
	region, err := s.cfg.Topology.RegionOf(name)
	if err != nil {
		return apiv1.Instance{}, false
	}
	view, avail := views[region]
	return apiv1.Instance{
		Name: name, Plane: apiP, Region: region, RegionAvailable: avail,
		Agent: s.agentOf(view, avail, plane, name),
	}, true
}

func (s *Server) controllerStatus(views map[string]state.View) apiv1.ControllerStatus {
	home, err := s.cfg.Topology.RegionOf(state.ControllerInstance)
	out := apiv1.ControllerStatus{}
	if err != nil {
		return out
	}
	out.HomeRegion = home
	view, avail := views[home]
	if !avail {
		return out
	}
	_, out.Present = view.Entries[state.AgentKey(state.PlaneCtl, state.ControllerInstance)]
	out.Agent = s.agentOf(view, true, state.PlaneCtl, state.ControllerInstance)
	return out
}

func (s *Server) instanceDetail(views map[string]state.View, name string, plane state.Plane, fresh apiv1.Freshness) (apiv1.InstanceDetail, bool) {
	apiP, ok := apiPlane(plane)
	if !ok {
		return apiv1.InstanceDetail{}, false
	}
	region, err := s.cfg.Topology.RegionOf(name)
	if err != nil {
		return apiv1.InstanceDetail{}, false
	}
	view, avail := views[region]
	out := apiv1.InstanceDetail{
		Name: name, Plane: apiP, Region: region, RegionAvailable: avail,
		Freshness: fresh, Workers: []apiv1.Worker{},
		Agent: s.agentOf(view, avail, plane, name),
	}
	switch plane {
	case state.PlaneMD, state.PlaneTD:
		demands := []apiv1.DemandStatus{}
		if avail {
			demands = s.instanceDemands(views, view, plane, name)
		}
		out.Demands = &demands
	case state.PlaneSTS:
		sessions := []apiv1.InstanceSession{}
		if avail {
			sessions = instanceSessions(view, name)
		}
		out.Sessions = &sessions
	}
	if avail {
		out.Workers = workersOf(view, plane, name)
	}
	return out, true
}

func apiPlane(p state.Plane) (apiv1.Plane, bool) {
	switch p {
	case state.PlaneMD:
		return apiv1.Md, true
	case state.PlaneSTS:
		return apiv1.Sts, true
	case state.PlaneTD:
		return apiv1.Td, true
	default:
		return "", false
	}
}

func instanceSessions(view state.View, inst string) []apiv1.InstanceSession {
	prefix := state.SessionSpecPrefix(inst)
	out := []apiv1.InstanceSession{}
	for key, e := range view.Entries {
		id, ok := strings.CutPrefix(key, prefix)
		if !ok || id == "" || strings.Contains(id, "/") {
			continue
		}
		spec := &sessionv1.SessionSpec{}
		if proto.Unmarshal(e.Value, spec) != nil {
			continue
		}
		item := apiv1.InstanceSession{SessionId: id, Name: spec.GetName()}
		if st := statusOf(view, inst, id); st != nil {
			if p := phaseOf(st.GetPhase()); p != "" {
				item.Phase = &p
			}
		}
		out = append(out, item)
	}
	slices.SortFunc(out, func(a, b apiv1.InstanceSession) int { return strings.Compare(a.SessionId, b.SessionId) })
	return out
}

func (s *Server) instanceDemands(views map[string]state.View, view state.View, plane state.Plane, inst string) []apiv1.DemandStatus {
	prefix := state.DemandPrefix(plane, inst)
	silent := s.agentMissing(views, plane, inst)
	out := []apiv1.DemandStatus{}
	for key, e := range view.Entries {
		rest, ok := strings.CutPrefix(key, prefix)
		if !ok {
			continue
		}
		sessionID, entry, ok := strings.Cut(rest, "/")
		if !ok || sessionID == "" || entry == "" {
			continue
		}
		dem := &statev1.DemandEntry{}
		if proto.Unmarshal(e.Value, dem) != nil {
			continue
		}
		item, ok := demandItem(plane, inst, sessionID, entry, view)
		if !ok {
			continue
		}
		if silent {
			item.State = apiv1.EntryStateSilent
		}
		out = append(out, item)
	}
	slices.SortFunc(out, func(a, b apiv1.DemandStatus) int {
		if c := strings.Compare(a.SessionId, b.SessionId); c != 0 {
			return c
		}
		if c := strings.Compare(string(a.Kind), string(b.Kind)); c != 0 {
			return c
		}
		if c := strings.Compare(deref(a.Feed), deref(b.Feed)); c != 0 {
			return c
		}
		return cmp.Compare(derefInt(a.ApiId), derefInt(b.ApiId))
	})
	return out
}

func demandItem(plane state.Plane, inst, sessionID, entry string, view state.View) (apiv1.DemandStatus, bool) {
	item := apiv1.DemandStatus{SessionId: sessionID, State: apiv1.EntryStateSilent}
	var ackKey string
	switch plane {
	case state.PlaneMD:
		feed, ok := strings.CutPrefix(entry, "feed/")
		if !ok || feed == "" {
			return apiv1.DemandStatus{}, false
		}
		item.Kind = apiv1.DemandStatusKindFeed
		item.Feed = &feed
		ackKey = state.MDAckKey(inst, sessionID, entry)
	case state.PlaneTD:
		id, err := strconv.ParseInt(entry, 10, 64)
		if err != nil || strconv.FormatInt(id, 10) != entry {
			return apiv1.DemandStatus{}, false
		}
		item.Kind = apiv1.DemandStatusKindAccount
		item.ApiId = &id
		ackKey = state.TDAckKey(inst, sessionID, id)
	default:
		return apiv1.DemandStatus{}, false
	}
	if st := entryStatusAt(view, ackKey); st != nil {
		item.State = entryStateOf(st.GetState())
		if st.GetReason() != "" {
			reason := st.GetReason()
			item.Reason = &reason
		}
		if st.GetSubject() != "" {
			subject := st.GetSubject()
			item.Subject = &subject
		}
		if st.GetUpdatedAtNs() != 0 {
			at := time.Unix(0, st.GetUpdatedAtNs()).UTC()
			item.UpdatedAt = &at
		}
	}
	return item, true
}

func entryStatusAt(view state.View, key string) *statev1.EntryStatus {
	e, ok := view.Entries[key]
	if !ok {
		return nil
	}
	st := &statev1.EntryStatus{}
	if proto.Unmarshal(e.Value, st) != nil {
		return nil
	}
	return st
}

func workersOf(view state.View, plane state.Plane, inst string) []apiv1.Worker {
	prefix := state.WorkerStatusPrefix(plane, inst)
	out := []apiv1.Worker{}
	for key, e := range view.Entries {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		st := &statev1.WorkerStatus{}
		if proto.Unmarshal(e.Value, st) != nil {
			continue
		}
		phase, ok := workerPhase(st.GetPhase())
		if !ok {
			continue
		}
		w := apiv1.Worker{
			Key:         st.GetWorker().GetKey(),
			Incarnation: int(st.GetWorker().GetIncarnation()),
			Phase:       phase,
			Restarts:    int(st.GetRestarts()),
		}
		if w.Key == "" {
			w.Key = strings.TrimPrefix(key, prefix)
		}
		if st.GetRelease() != "" {
			rel := st.GetRelease()
			w.Release = &rel
		}
		if st.GetShimRelease() != "" {
			rel := st.GetShimRelease()
			w.ShimRelease = &rel
		}
		if st.GetStage() != "" {
			stage := st.GetStage()
			w.Stage = &stage
		}
		if st.GetRssBytes() != 0 {
			n := st.GetRssBytes()
			w.RssBytes = &n
		}
		if st.GetStartedAtNs() != 0 {
			at := time.Unix(0, st.GetStartedAtNs()).UTC()
			w.StartedAt = &at
		}
		if exit := st.GetLastExit(); exit != nil {
			view := exitView(exit)
			w.LastExit = &view
		}
		out = append(out, w)
	}
	slices.SortFunc(out, func(a, b apiv1.Worker) int { return strings.Compare(a.Key, b.Key) })
	return out
}

func workerPhase(raw string) (apiv1.WorkerPhase, bool) {
	p := apiv1.WorkerPhase(strings.ToLower(raw))
	if !p.Valid() {
		return "", false
	}
	return p, true
}

func (s *Server) ListSessionRecords(w http.ResponseWriter, r *http.Request, params apiv1.ListSessionRecordsParams) {
	limit, after, fields := bindPage(params.Limit, params.Cursor)
	var cursorAt *time.Time
	var cursorID *string
	if after != "" {
		at, id, err := parseRecordCursor(after)
		if err != nil {
			fields = append(fields, apiv1.FieldError{Path: "/cursor", Reason: "is not a cursor from this API"})
		} else {
			cursorAt = &at
			cursorID = &id
		}
	}
	if len(fields) > 0 {
		writeProblem(w, http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, http.StatusText(http.StatusBadRequest), "invalid request", fields)
		return
	}
	var strategy string
	if params.StrategyId != nil {
		strategy = *params.StrategyId
	}
	rows, more, err := s.pageSessionRecords(r.Context(), recordPage{
		limit: limit, sessionID: params.SessionId, createdBy: params.CreatedBy,
		after: params.CreatedAfter, before: params.CreatedBefore,
		cursorAt: cursorAt, cursorID: cursorID,
		keep: func(rec sessionRecord) bool {
			return strategy == "" || strategyIDOfBytes(rec.Spec) == strategy
		},
	})
	if err != nil {
		s.writeSessionProblem(w, err)
		return
	}
	items := make([]apiv1.SessionRecord, 0, len(rows))
	for _, rec := range rows {
		item, err := s.sessionRecordView(rec)
		if err != nil {
			s.writeSessionProblem(w, err)
			return
		}
		items = append(items, item)
	}
	var next *string
	if more && len(rows) > 0 {
		last := rows[len(rows)-1]
		c := encodeRecordCursor(last.CreatedAt, last.ID)
		next = &c
	}
	writeJSON(w, http.StatusOK, apiv1.SessionRecordList{Items: items, NextCursor: next})
}

func (s *Server) GetSessionRecord(w http.ResponseWriter, r *http.Request, id openapi_types.UUID) {
	rec, err := s.recordByID(r.Context(), id.String())
	if err != nil {
		s.writeSessionProblem(w, err)
		return
	}
	item, err := s.sessionRecordView(*rec)
	if err != nil {
		s.writeSessionProblem(w, err)
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (s *Server) sessionRecordView(rec sessionRecord) (apiv1.SessionRecord, error) {
	id, err := uuid.Parse(rec.ID)
	if err != nil {
		return apiv1.SessionRecord{}, err
	}
	spec := &sessionv1.SessionSpec{}
	if err := proto.Unmarshal(rec.Spec, spec); err != nil {
		return apiv1.SessionRecord{}, err
	}
	state, ok := recordState(rec.State)
	if !ok {
		return apiv1.SessionRecord{}, errNotFound
	}
	var createdBy *string
	if rec.CreatedBy != nil && *rec.CreatedBy != "" {
		createdBy = rec.CreatedBy
	}
	return apiv1.SessionRecord{
		RecordId:         id,
		SessionId:        rec.SessionID,
		StsInstance:      rec.STS,
		Spec:             s.specView(spec),
		State:            state,
		CreatedAt:        rec.CreatedAt.UTC(),
		CreatedBy:        createdBy,
		ClosedAt:         utcPtr(rec.ClosedAt),
		EndedRequestedAt: endedRequestedAt(rec),
		Final:            s.finalView(rec),
	}, nil
}

func recordState(s string) (apiv1.SessionRecordState, bool) {
	switch s {
	case recPending:
		return apiv1.SessionRecordStatePending, true
	case recCreated:
		return apiv1.SessionRecordStateCreated, true
	case recAbandoned:
		return apiv1.SessionRecordStateAbandoned, true
	case recClosed:
		return apiv1.SessionRecordStateClosed, true
	default:
		return "", false
	}
}

func endedRequestedAt(rec sessionRecord) *time.Time {
	if rec.EndedAt == nil {
		return nil
	}
	wanted := rec.Desired == desEnded || (rec.EndReason != nil && *rec.EndReason == endReasonSpec)
	if !wanted {
		return nil
	}
	t := rec.EndedAt.UTC()
	return &t
}

func utcPtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

func strategyIDOfBytes(b []byte) string {
	spec := &sessionv1.SessionSpec{}
	if proto.Unmarshal(b, spec) != nil {
		return ""
	}
	repo := spec.GetStrategy().GetRepository()
	if repo == "" {
		return ""
	}
	return path.Base(strings.TrimRight(repo, "/"))
}

func encodeRecordCursor(t time.Time, id string) string {
	raw := t.UTC().Format(time.RFC3339Nano) + "\n" + id
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func parseRecordCursor(raw string) (time.Time, string, error) {
	ts, id, ok := strings.Cut(raw, "\n")
	if !ok {
		return time.Time{}, "", errNotFound
	}
	at, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return time.Time{}, "", err
	}
	if _, err := uuid.Parse(id); err != nil {
		return time.Time{}, "", err
	}
	return at.UTC(), id, nil
}

func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func derefInt(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}

// finalView is the status kept when the row closed (E10-D117). A row closed before the column existed
// has none. Bytes that do not decode are logged and left out, so the record stays readable.
func (s *Server) finalView(rec sessionRecord) *apiv1.SessionStatus {
	if len(rec.Final) == 0 {
		return nil
	}
	st := &statev1.SessionStatus{}
	if err := proto.Unmarshal(rec.Final, st); err != nil {
		s.log.Error().Err(err).Str("record_id", rec.ID).Str("session_id", rec.SessionID).Msg("session record final status decode failed")
		return nil
	}
	return statusView(st)
}
