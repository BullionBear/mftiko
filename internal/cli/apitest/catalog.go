package apitest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

// Catalog is the read-only data behind the fake server (E11-03).
// Set every field before Start. The handler reads it and does not mutate it.
type Catalog struct {
	Strategies []apiv1.Strategy
	// Versions is keyed by strategy id. An empty slice is a known strategy
	// with no tags. A strategy listed in Strategies is known even when this
	// map has no entry.
	Versions map[string][]apiv1.StrategyTag
	// Manifests is keyed by strategy id, a NUL byte, and the ref (tag or digest).
	Manifests  map[string]apiv1.StrategyVersion
	Instances  []apiv1.Instance
	Controller apiv1.ControllerStatus
	Freshness  apiv1.Freshness
	Details    map[string]apiv1.InstanceDetail
	Kinds      []apiv1.AccountKind
	// PageSize, when greater than 0, caps a strategy page below the requested
	// limit and still sets next_cursor when more rows remain. Tests use it so
	// a short page is not the end of the list.
	PageSize int
	// FailAt is the 1-based count of authenticated catalog calls. The call at
	// that count, and later calls, return Fail. Auth failures are not counted.
	FailAt int
	Fail   *apiv1.Problem
	// StrategyPages, when non-nil, is the raw JSON for GET /strategies keyed by
	// cursor. The empty string is the first page. Status 0 is 200.
	StrategyPages map[string]RawPage
}

// RawPage is one strategy list response written unchanged.
type RawPage struct {
	Status int
	Body   []byte
}

// begin admits one catalog call. A nil catalog leaves the route unused.
// The problem is copied before the lock is released.
func (s *Server) begin() (*Catalog, *apiv1.Problem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cat == nil {
		return nil, nil, errUnused
	}
	if s.lastAuth != "Bearer "+s.token {
		p := problem(http.StatusUnauthorized, apiv1.ErrorCodeUnauthenticated, "Unauthorized", "bearer token was not accepted")
		return nil, &p, nil
	}
	s.hits++
	if s.cat.Fail != nil && s.cat.FailAt > 0 && s.hits >= s.cat.FailAt {
		p := *s.cat.Fail
		return nil, &p, nil
	}
	return s.cat, nil, nil
}

func (a *api) ListStrategies(_ context.Context, req apiv1.ListStrategiesRequestObject) (apiv1.ListStrategiesResponseObject, error) {
	cat, p, err := a.s.begin()
	if err != nil {
		return nil, err
	}
	if p != nil {
		return listStrategiesProblem(p), nil
	}
	cursor := ""
	if req.Params.Cursor != nil {
		cursor = *req.Params.Cursor
	}
	if cat.StrategyPages != nil {
		page, ok := cat.StrategyPages[cursor]
		if !ok {
			return listStrategiesProblem(invalidCursor()), nil
		}
		return rawPage{status: page.Status, body: page.Body}, nil
	}
	items, next, bad := pageSlice(cat.Strategies, req.Params.Limit, req.Params.Cursor, cat.PageSize)
	if bad != nil {
		return listStrategiesProblem(bad), nil
	}
	return apiv1.ListStrategies200JSONResponse{Items: items, NextCursor: next}, nil
}

func (a *api) ListStrategyVersions(_ context.Context, req apiv1.ListStrategyVersionsRequestObject) (apiv1.ListStrategyVersionsResponseObject, error) {
	cat, p, err := a.s.begin()
	if err != nil {
		return nil, err
	}
	if p != nil {
		return listVersionsProblem(p), nil
	}
	tags, ok := cat.Versions[req.StrategyId]
	if !ok && !strategyKnown(cat, req.StrategyId) {
		return listVersionsProblem(notFound("strategy not found")), nil
	}
	items, next, bad := pageSlice(tags, req.Params.Limit, req.Params.Cursor, cat.PageSize)
	if bad != nil {
		return listVersionsProblem(bad), nil
	}
	return apiv1.ListStrategyVersions200JSONResponse{Items: items, NextCursor: next}, nil
}

func (a *api) GetStrategyVersion(_ context.Context, req apiv1.GetStrategyVersionRequestObject) (apiv1.GetStrategyVersionResponseObject, error) {
	cat, p, err := a.s.begin()
	if err != nil {
		return nil, err
	}
	if p != nil {
		return getVersionProblem(p), nil
	}
	ver, ok := cat.Manifests[req.StrategyId+"\x00"+req.Ref]
	if !ok {
		return getVersionProblem(notFound("version not found")), nil
	}
	if ver.Tags == nil {
		ver.Tags = []string{}
	}
	if ver.Contracts == nil {
		ver.Contracts = []string{}
	}
	return apiv1.GetStrategyVersion200JSONResponse(ver), nil
}

func (a *api) ListInstances(context.Context, apiv1.ListInstancesRequestObject) (apiv1.ListInstancesResponseObject, error) {
	cat, p, err := a.s.begin()
	if err != nil {
		return nil, err
	}
	if p != nil {
		return listInstancesProblem(p), nil
	}
	items := append([]apiv1.Instance(nil), cat.Instances...)
	if items == nil {
		items = []apiv1.Instance{}
	}
	return apiv1.ListInstances200JSONResponse{
		Items: items, Controller: cat.Controller, Freshness: normFreshness(cat.Freshness),
	}, nil
}

func (a *api) GetInstance(ctx context.Context, req apiv1.GetInstanceRequestObject) (apiv1.GetInstanceResponseObject, error) {
	if a.s.cat == nil {
		return a.getInstanceAccount(ctx, req)
	}
	cat, p, err := a.s.begin()
	if err != nil {
		return nil, err
	}
	if p != nil {
		return getInstanceProblem(p), nil
	}
	det, ok := cat.Details[req.Name]
	if !ok {
		return getInstanceProblem(notFound("instance not found")), nil
	}
	if det.Workers == nil {
		det.Workers = []apiv1.Worker{}
	}
	det.Freshness = normFreshness(det.Freshness)
	return apiv1.GetInstance200JSONResponse(det), nil
}

func (a *api) ListAccountKinds(ctx context.Context, req apiv1.ListAccountKindsRequestObject) (apiv1.ListAccountKindsResponseObject, error) {
	if a.s.cat == nil {
		return a.listKindsAccount(ctx, req)
	}
	cat, p, err := a.s.begin()
	if err != nil {
		return nil, err
	}
	if p != nil {
		return listKindsProblem(p), nil
	}
	return apiv1.ListAccountKinds200JSONResponse{Items: normKinds(cat.Kinds)}, nil
}

func strategyKnown(cat *Catalog, id string) bool {
	for _, s := range cat.Strategies {
		if s.Id == id {
			return true
		}
	}
	return false
}

func normFreshness(fr apiv1.Freshness) apiv1.Freshness {
	if fr.UnavailableRegions == nil {
		fr.UnavailableRegions = []string{}
	}
	return fr
}

func normKinds(in []apiv1.AccountKind) []apiv1.AccountKind {
	out := append([]apiv1.AccountKind(nil), in...)
	if out == nil {
		out = []apiv1.AccountKind{}
	}
	for i := range out {
		if out[i].Trades == nil {
			out[i].Trades = []string{}
		}
		if out[i].KeyTypes == nil {
			out[i].KeyTypes = []apiv1.KeyType{}
		}
	}
	return out
}

func pageSlice[T any](items []T, limit *int, cursor *string, pageSize int) ([]T, *string, *apiv1.Problem) {
	n := 100
	if limit != nil {
		if *limit < 1 || *limit > 1000 {
			return nil, nil, invalidLimit()
		}
		n = *limit
	}
	if pageSize > 0 && pageSize < n {
		n = pageSize
	}
	start := 0
	if cursor != nil && *cursor != "" {
		v, err := strconv.Atoi(*cursor)
		if err != nil || v < 0 {
			return nil, nil, invalidCursor()
		}
		start = v
	}
	if start > len(items) {
		start = len(items)
	}
	end := start + n
	if end > len(items) {
		end = len(items)
	}
	out := append([]T(nil), items[start:end]...)
	if out == nil {
		out = []T{}
	}
	var next *string
	if end < len(items) {
		s := strconv.Itoa(end)
		next = &s
	}
	return out, next, nil
}

func invalidLimit() *apiv1.Problem {
	p := problem(http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, "Validation Failed", "limit must be between 1 and 1000")
	errs := []apiv1.FieldError{{Path: "/limit", Reason: "must be between 1 and 1000"}}
	p.Errors = &errs
	return &p
}

func invalidCursor() *apiv1.Problem {
	p := problem(http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, "Validation Failed", "cursor is not a cursor from this API")
	errs := []apiv1.FieldError{{Path: "/cursor", Reason: "is not a cursor from this API"}}
	p.Errors = &errs
	return &p
}

func notFound(detail string) *apiv1.Problem {
	p := problem(http.StatusNotFound, apiv1.ErrorCodeNotFound, "Not Found", detail)
	return &p
}

type rawPage struct {
	status  int
	body    []byte
	problem bool
}

func (p rawPage) write(w http.ResponseWriter) error {
	if p.problem {
		w.Header().Set("Content-Type", "application/problem+json")
	} else {
		w.Header().Set("Content-Type", "application/json")
	}
	st := p.status
	if st == 0 {
		st = http.StatusOK
	}
	w.WriteHeader(st)
	if len(p.body) == 0 {
		return nil
	}
	_, err := w.Write(p.body)
	return err
}

func (p rawPage) VisitListStrategiesResponse(w http.ResponseWriter) error {
	return p.write(w)
}
func (p rawPage) VisitListStrategyVersionsResponse(w http.ResponseWriter) error {
	return p.write(w)
}
func (p rawPage) VisitGetStrategyVersionResponse(w http.ResponseWriter) error {
	return p.write(w)
}
func (p rawPage) VisitListInstancesResponse(w http.ResponseWriter) error {
	return p.write(w)
}
func (p rawPage) VisitGetInstanceResponse(w http.ResponseWriter) error { return p.write(w) }
func (p rawPage) VisitListAccountKindsResponse(w http.ResponseWriter) error {
	return p.write(w)
}

func problemRaw(p *apiv1.Problem) rawPage {
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(p)
	st := p.Status
	if st == 0 {
		st = http.StatusInternalServerError
	}
	return rawPage{status: st, body: buf.Bytes(), problem: true}
}

func listStrategiesProblem(p *apiv1.Problem) apiv1.ListStrategiesResponseObject {
	switch p.Status {
	case http.StatusBadRequest:
		return apiv1.ListStrategies400ApplicationProblemPlusJSONResponse{ValidationFailedApplicationProblemPlusJSONResponse: apiv1.ValidationFailedApplicationProblemPlusJSONResponse(*p)}
	case http.StatusUnauthorized:
		return apiv1.ListStrategies401ApplicationProblemPlusJSONResponse{UnauthenticatedApplicationProblemPlusJSONResponse: apiv1.UnauthenticatedApplicationProblemPlusJSONResponse(*p)}
	case http.StatusServiceUnavailable:
		return apiv1.ListStrategies503ApplicationProblemPlusJSONResponse{UnavailableApplicationProblemPlusJSONResponse: apiv1.UnavailableApplicationProblemPlusJSONResponse(*p)}
	default:
		return problemRaw(p)
	}
}

func listVersionsProblem(p *apiv1.Problem) apiv1.ListStrategyVersionsResponseObject {
	switch p.Status {
	case http.StatusBadRequest:
		return apiv1.ListStrategyVersions400ApplicationProblemPlusJSONResponse{ValidationFailedApplicationProblemPlusJSONResponse: apiv1.ValidationFailedApplicationProblemPlusJSONResponse(*p)}
	case http.StatusUnauthorized:
		return apiv1.ListStrategyVersions401ApplicationProblemPlusJSONResponse{UnauthenticatedApplicationProblemPlusJSONResponse: apiv1.UnauthenticatedApplicationProblemPlusJSONResponse(*p)}
	case http.StatusNotFound:
		return apiv1.ListStrategyVersions404ApplicationProblemPlusJSONResponse{NotFoundApplicationProblemPlusJSONResponse: apiv1.NotFoundApplicationProblemPlusJSONResponse(*p)}
	case http.StatusServiceUnavailable:
		return apiv1.ListStrategyVersions503ApplicationProblemPlusJSONResponse{UnavailableApplicationProblemPlusJSONResponse: apiv1.UnavailableApplicationProblemPlusJSONResponse(*p)}
	default:
		return problemRaw(p)
	}
}

func getVersionProblem(p *apiv1.Problem) apiv1.GetStrategyVersionResponseObject {
	switch p.Status {
	case http.StatusBadRequest:
		return apiv1.GetStrategyVersion400ApplicationProblemPlusJSONResponse{ValidationFailedApplicationProblemPlusJSONResponse: apiv1.ValidationFailedApplicationProblemPlusJSONResponse(*p)}
	case http.StatusUnauthorized:
		return apiv1.GetStrategyVersion401ApplicationProblemPlusJSONResponse{UnauthenticatedApplicationProblemPlusJSONResponse: apiv1.UnauthenticatedApplicationProblemPlusJSONResponse(*p)}
	case http.StatusNotFound:
		return apiv1.GetStrategyVersion404ApplicationProblemPlusJSONResponse{NotFoundApplicationProblemPlusJSONResponse: apiv1.NotFoundApplicationProblemPlusJSONResponse(*p)}
	case http.StatusBadGateway:
		return apiv1.GetStrategyVersion502ApplicationProblemPlusJSONResponse{InvalidArtifactApplicationProblemPlusJSONResponse: apiv1.InvalidArtifactApplicationProblemPlusJSONResponse(*p)}
	case http.StatusServiceUnavailable:
		return apiv1.GetStrategyVersion503ApplicationProblemPlusJSONResponse{UnavailableApplicationProblemPlusJSONResponse: apiv1.UnavailableApplicationProblemPlusJSONResponse(*p)}
	default:
		return problemRaw(p)
	}
}

func listInstancesProblem(p *apiv1.Problem) apiv1.ListInstancesResponseObject {
	switch p.Status {
	case http.StatusBadRequest:
		return apiv1.ListInstances400ApplicationProblemPlusJSONResponse{ValidationFailedApplicationProblemPlusJSONResponse: apiv1.ValidationFailedApplicationProblemPlusJSONResponse(*p)}
	case http.StatusUnauthorized:
		return apiv1.ListInstances401ApplicationProblemPlusJSONResponse{UnauthenticatedApplicationProblemPlusJSONResponse: apiv1.UnauthenticatedApplicationProblemPlusJSONResponse(*p)}
	default:
		return problemRaw(p)
	}
}

func getInstanceProblem(p *apiv1.Problem) apiv1.GetInstanceResponseObject {
	switch p.Status {
	case http.StatusUnauthorized:
		return apiv1.GetInstance401ApplicationProblemPlusJSONResponse{UnauthenticatedApplicationProblemPlusJSONResponse: apiv1.UnauthenticatedApplicationProblemPlusJSONResponse(*p)}
	case http.StatusNotFound:
		return apiv1.GetInstance404ApplicationProblemPlusJSONResponse{NotFoundApplicationProblemPlusJSONResponse: apiv1.NotFoundApplicationProblemPlusJSONResponse(*p)}
	case http.StatusServiceUnavailable:
		return apiv1.GetInstance503ApplicationProblemPlusJSONResponse{UnavailableApplicationProblemPlusJSONResponse: apiv1.UnavailableApplicationProblemPlusJSONResponse(*p)}
	default:
		return problemRaw(p)
	}
}

func listKindsProblem(p *apiv1.Problem) apiv1.ListAccountKindsResponseObject {
	switch p.Status {
	case http.StatusUnauthorized:
		return apiv1.ListAccountKinds401ApplicationProblemPlusJSONResponse{UnauthenticatedApplicationProblemPlusJSONResponse: apiv1.UnauthenticatedApplicationProblemPlusJSONResponse(*p)}
	default:
		return problemRaw(p)
	}
}
