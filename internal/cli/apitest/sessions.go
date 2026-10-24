package apitest

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"sort"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

func cloneJSON[T any](in T) T {
	b, err := json.Marshal(in)
	if err != nil {
		panic(err)
	}
	var out T
	if err := json.Unmarshal(b, &out); err != nil {
		panic(err)
	}
	return out
}

func (s *Server) installSessions(opt Option) {
	s.agents = map[string]apiv1.AgentStatus{}
	for name, agent := range opt.Agents {
		s.agents[name] = cloneJSON(agent)
	}
	for _, item := range opt.Sessions {
		s.sessions = append(s.sessions, cloneJSON(item))
	}
	for _, item := range opt.Records {
		s.records = append(s.records, cloneJSON(item))
	}
}

func (a *api) agentInstance(ctx context.Context, name string) (apiv1.GetInstanceResponseObject, bool) {
	a.s.mu.Lock()
	agent, ok := a.s.agents[name]
	inst, hasInst := a.s.instances[name]
	if ok {
		agent = cloneJSON(agent)
	}
	a.s.mu.Unlock()
	if !ok {
		return nil, false
	}
	if !a.allowed(ctx) {
		return apiv1.GetInstance401ApplicationProblemPlusJSONResponse{
			UnauthenticatedApplicationProblemPlusJSONResponse: apiv1.UnauthenticatedApplicationProblemPlusJSONResponse(unauthProblem()),
		}, true
	}
	det := apiv1.InstanceDetail{
		Name:            name,
		Plane:           apiv1.Sts,
		RegionAvailable: true,
		Agent:           &agent,
		Workers:         []apiv1.Worker{},
		Freshness:       apiv1.Freshness{UnavailableRegions: []string{}},
	}
	if hasInst {
		det.Plane = inst.Plane
		det.Region = inst.Region
		det.RegionAvailable = inst.RegionAvailable
	}
	return apiv1.GetInstance200JSONResponse(det), true
}

func (a *api) ListSessions(ctx context.Context, req apiv1.ListSessionsRequestObject) (apiv1.ListSessionsResponseObject, error) {
	if !a.allowed(ctx) {
		return apiv1.ListSessions401ApplicationProblemPlusJSONResponse{
			UnauthenticatedApplicationProblemPlusJSONResponse: apiv1.UnauthenticatedApplicationProblemPlusJSONResponse(unauthProblem()),
		}, nil
	}
	if req.Params.Phase != nil {
		for _, phase := range *req.Params.Phase {
			if !phase.Valid() {
				p := fieldsProblem(http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, http.StatusText(http.StatusBadRequest), "invalid request", []apiv1.FieldError{{
					Path: "/phase", Reason: "is not a session phase",
				}})
				return apiv1.ListSessions400ApplicationProblemPlusJSONResponse{
					ValidationFailedApplicationProblemPlusJSONResponse: apiv1.ValidationFailedApplicationProblemPlusJSONResponse(p),
				}, nil
			}
		}
	}
	a.s.mu.Lock()
	defer a.s.mu.Unlock()
	var phases []apiv1.Phase
	phaseSet := req.Params.Phase != nil
	if phaseSet {
		phases = *req.Params.Phase
	}
	var kept []apiv1.SessionDetail
	for _, row := range a.s.sessions {
		if sessionKeep(row, phases, phaseSet, req.Params.Active, req.Params.StsInstance) {
			kept = append(kept, row)
		}
	}
	sort.Slice(kept, func(i, j int) bool {
		if kept[i].Id != kept[j].Id {
			return kept[i].Id < kept[j].Id
		}
		return kept[i].StsInstance < kept[j].StsInstance
	})
	lim, cur := limitCursor(req.Params.Limit, req.Params.Cursor)
	page, next, prob := pageSlice(kept, lim, cur, a.s.pageSize)
	if prob != nil {
		return apiv1.ListSessions400ApplicationProblemPlusJSONResponse{
			ValidationFailedApplicationProblemPlusJSONResponse: apiv1.ValidationFailedApplicationProblemPlusJSONResponse(*prob),
		}, nil
	}
	items := make([]apiv1.Session, 0, len(page))
	for _, d := range page {
		items = append(items, asSession(d))
	}
	pageN := a.s.sessionPages
	a.s.sessionPages++
	fresh := a.s.fresh
	if pageN > 0 && a.s.nextFresh != nil {
		fresh = *a.s.nextFresh
	}
	return apiv1.ListSessions200JSONResponse{Items: items, NextCursor: next, Freshness: copyFresh(fresh)}, nil
}

func sessionKeep(d apiv1.SessionDetail, phases []apiv1.Phase, phaseSet bool, active *bool, sts *string) bool {
	if sts != nil && *sts != "" && d.StsInstance != *sts {
		return false
	}
	if phaseSet {
		if d.Status == nil || !slices.Contains(phases, d.Status.Phase) {
			return false
		}
	}
	if active != nil {
		term := d.Status != nil && (d.Status.Phase == apiv1.PhaseDone || d.Status.Phase == apiv1.PhaseFailed)
		if *active == term {
			return false
		}
	}
	return true
}

func limitCursor(limit *apiv1.Limit, cursor *apiv1.Cursor) (*int, *string) {
	var lim *int
	if limit != nil {
		v := int(*limit)
		lim = &v
	}
	var cur *string
	if cursor != nil {
		v := string(*cursor)
		cur = &v
	}
	return lim, cur
}

func asSession(d apiv1.SessionDetail) apiv1.Session {
	b, err := json.Marshal(d)
	if err != nil {
		panic(err)
	}
	var s apiv1.Session
	if err := json.Unmarshal(b, &s); err != nil {
		panic(err)
	}
	return s
}

func (a *api) GetSession(ctx context.Context, req apiv1.GetSessionRequestObject) (apiv1.GetSessionResponseObject, error) {
	if !a.allowed(ctx) {
		return apiv1.GetSession401ApplicationProblemPlusJSONResponse{
			UnauthenticatedApplicationProblemPlusJSONResponse: apiv1.UnauthenticatedApplicationProblemPlusJSONResponse(unauthProblem()),
		}, nil
	}
	a.s.mu.Lock()
	defer a.s.mu.Unlock()
	d, ok := a.s.sessionByID(string(req.SessionId))
	if !ok {
		return apiv1.GetSession404ApplicationProblemPlusJSONResponse{
			NotFoundApplicationProblemPlusJSONResponse: apiv1.NotFoundApplicationProblemPlusJSONResponse(sessionMissing()),
		}, nil
	}
	d.Freshness = copyFresh(a.s.fresh)
	return apiv1.GetSession200JSONResponse(d), nil
}

func (a *api) EndSession(ctx context.Context, req apiv1.EndSessionRequestObject) (apiv1.EndSessionResponseObject, error) {
	if !a.allowed(ctx) {
		return apiv1.EndSession401ApplicationProblemPlusJSONResponse{
			UnauthenticatedApplicationProblemPlusJSONResponse: apiv1.UnauthenticatedApplicationProblemPlusJSONResponse(unauthProblem()),
		}, nil
	}
	a.s.mu.Lock()
	defer a.s.mu.Unlock()
	i, ok := a.s.findSession(string(req.SessionId))
	if !ok {
		return apiv1.EndSession404ApplicationProblemPlusJSONResponse{
			NotFoundApplicationProblemPlusJSONResponse: apiv1.NotFoundApplicationProblemPlusJSONResponse(sessionMissing()),
		}, nil
	}
	a.s.sessions[i].Spec.DesiredState = apiv1.DesiredStateEnded
	d := cloneJSON(a.s.sessions[i])
	d.Freshness = copyFresh(a.s.fresh)
	return apiv1.EndSession200JSONResponse(d), nil
}

func (a *api) DeleteSession(ctx context.Context, req apiv1.DeleteSessionRequestObject) (apiv1.DeleteSessionResponseObject, error) {
	if !a.allowed(ctx) {
		return apiv1.DeleteSession401ApplicationProblemPlusJSONResponse{
			UnauthenticatedApplicationProblemPlusJSONResponse: apiv1.UnauthenticatedApplicationProblemPlusJSONResponse(unauthProblem()),
		}, nil
	}
	a.s.mu.Lock()
	defer a.s.mu.Unlock()
	i, ok := a.s.findSession(string(req.SessionId))
	if !ok {
		return apiv1.DeleteSession404ApplicationProblemPlusJSONResponse{
			NotFoundApplicationProblemPlusJSONResponse: apiv1.NotFoundApplicationProblemPlusJSONResponse(sessionMissing()),
		}, nil
	}
	st := a.s.sessions[i].Status
	term := st != nil && (st.Phase == apiv1.PhaseDone || st.Phase == apiv1.PhaseFailed)
	if !term {
		return apiv1.DeleteSession409ApplicationProblemPlusJSONResponse{
			ConflictApplicationProblemPlusJSONResponse: apiv1.ConflictApplicationProblemPlusJSONResponse(notTerminalProblem()),
		}, nil
	}
	a.s.sessions = append(a.s.sessions[:i], a.s.sessions[i+1:]...)
	return apiv1.DeleteSession204Response{}, nil
}

func (s *Server) findSession(id string) (int, bool) {
	for i := range s.sessions {
		if s.sessions[i].Id == id {
			return i, true
		}
	}
	return 0, false
}

func (s *Server) sessionByID(id string) (apiv1.SessionDetail, bool) {
	i, ok := s.findSession(id)
	if !ok {
		return apiv1.SessionDetail{}, false
	}
	return cloneJSON(s.sessions[i]), true
}

func sessionMissing() apiv1.Problem {
	return problem(http.StatusNotFound, apiv1.ErrorCodeNotFound, http.StatusText(http.StatusNotFound), "session not found")
}

func notTerminalProblem() apiv1.Problem {
	return problem(http.StatusConflict, apiv1.ErrorCodeNotTerminal, http.StatusText(http.StatusConflict), "session is not terminal")
}

func (a *api) ListSessionRecords(ctx context.Context, req apiv1.ListSessionRecordsRequestObject) (apiv1.ListSessionRecordsResponseObject, error) {
	if !a.allowed(ctx) {
		return apiv1.ListSessionRecords401ApplicationProblemPlusJSONResponse{
			UnauthenticatedApplicationProblemPlusJSONResponse: apiv1.UnauthenticatedApplicationProblemPlusJSONResponse(unauthProblem()),
		}, nil
	}
	a.s.mu.Lock()
	defer a.s.mu.Unlock()
	var kept []apiv1.SessionRecord
	for _, row := range a.s.records {
		if recordKeep(row, req.Params) {
			kept = append(kept, row)
		}
	}
	sort.Slice(kept, func(i, j int) bool {
		if !kept[i].CreatedAt.Equal(kept[j].CreatedAt) {
			return kept[i].CreatedAt.After(kept[j].CreatedAt)
		}
		return kept[i].RecordId.String() > kept[j].RecordId.String()
	})
	lim, cur := limitCursor(req.Params.Limit, req.Params.Cursor)
	page, next, prob := pageSlice(kept, lim, cur, a.s.pageSize)
	if prob != nil {
		return apiv1.ListSessionRecords400ApplicationProblemPlusJSONResponse{
			ValidationFailedApplicationProblemPlusJSONResponse: apiv1.ValidationFailedApplicationProblemPlusJSONResponse(*prob),
		}, nil
	}
	return apiv1.ListSessionRecords200JSONResponse{Items: page, NextCursor: next}, nil
}

func recordKeep(r apiv1.SessionRecord, p apiv1.ListSessionRecordsParams) bool {
	if p.SessionId != nil && *p.SessionId != "" && r.SessionId != *p.SessionId {
		return false
	}
	if p.StrategyId != nil && *p.StrategyId != "" {
		id := ""
		if r.Spec.Strategy.Id != nil {
			id = *r.Spec.Strategy.Id
		}
		if id != *p.StrategyId {
			return false
		}
	}
	if p.CreatedBy != nil && *p.CreatedBy != "" && (r.CreatedBy == nil || *r.CreatedBy != *p.CreatedBy) {
		return false
	}
	if p.CreatedAfter != nil && !r.CreatedAt.After(*p.CreatedAfter) {
		return false
	}
	if p.CreatedBefore != nil && !r.CreatedAt.Before(*p.CreatedBefore) {
		return false
	}
	return true
}

func (a *api) GetSessionRecord(ctx context.Context, req apiv1.GetSessionRecordRequestObject) (apiv1.GetSessionRecordResponseObject, error) {
	if !a.allowed(ctx) {
		return apiv1.GetSessionRecord401ApplicationProblemPlusJSONResponse{
			UnauthenticatedApplicationProblemPlusJSONResponse: apiv1.UnauthenticatedApplicationProblemPlusJSONResponse(unauthProblem()),
		}, nil
	}
	a.s.mu.Lock()
	defer a.s.mu.Unlock()
	for _, row := range a.s.records {
		if row.RecordId == req.RecordId {
			return apiv1.GetSessionRecord200JSONResponse(cloneJSON(row)), nil
		}
	}
	return apiv1.GetSessionRecord404ApplicationProblemPlusJSONResponse{
		NotFoundApplicationProblemPlusJSONResponse: apiv1.NotFoundApplicationProblemPlusJSONResponse(recordMissing()),
	}, nil
}

func recordMissing() apiv1.Problem {
	return problem(http.StatusNotFound, apiv1.ErrorCodeNotFound, http.StatusText(http.StatusNotFound), "record not found")
}
