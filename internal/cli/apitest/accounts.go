package apitest

import (
	"context"
	"encoding/base64"
	"net/http"
	"regexp"
	"sort"
	"strconv"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

// bodyKey and authKey carry the request body and Authorization into the
// strict handler. The body is how tests observe credential bytes.
var (
	bodyKey = &struct{ name string }{"body"}
	authKey = &struct{ name string }{"auth"}
)

type accountRow struct {
	rec apiv1.Account
}

var instanceNameRe = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)

func (s *Server) install(opt Option) {
	s.kinds = append([]apiv1.AccountKind(nil), opt.Kinds...)
	s.instances = make(map[string]apiv1.InstanceDetail, len(opt.Instances))
	for _, inst := range opt.Instances {
		s.instances[inst.Name] = apiv1.InstanceDetail{
			Name:            inst.Name,
			Plane:           inst.Plane,
			Region:          inst.Region,
			RegionAvailable: inst.RegionAvailable,
			Workers:         []apiv1.Worker{},
			Freshness:       apiv1.Freshness{UnavailableRegions: []string{}},
		}
	}
	var maxID int64
	for _, acct := range opt.Accounts {
		if acct.ApiId > maxID {
			maxID = acct.ApiId
		}
		s.accounts = append(s.accounts, accountRow{rec: copyAccount(acct)})
	}
	s.nextID = maxID
	s.pageSize = opt.PageSize
	s.fresh = copyFresh(opt.Freshness)
	if opt.NextFreshness != nil {
		f := copyFresh(*opt.NextFreshness)
		s.nextFresh = &f
	}
	s.conflict = opt.ConflictCAS
	s.echoSecret = opt.EchoCredential
}

func copyFresh(f apiv1.Freshness) apiv1.Freshness {
	f.UnavailableRegions = append([]string(nil), f.UnavailableRegions...)
	if f.UnavailableRegions == nil {
		f.UnavailableRegions = []string{}
	}
	return f
}

func copyAccount(in apiv1.Account) apiv1.Account {
	out := in
	out.ReferencedBy = append([]apiv1.SessionIDValue(nil), in.ReferencedBy...)
	if out.ReferencedBy == nil {
		out.ReferencedBy = []apiv1.SessionIDValue{}
	}
	if in.Settings.CancelOnDisconnectS != nil {
		v := *in.Settings.CancelOnDisconnectS
		out.Settings.CancelOnDisconnectS = &v
	}
	if out.Revision == 0 {
		out.Revision = 1
	}
	return out
}

func (a *api) allowed(ctx context.Context) bool {
	got, _ := ctx.Value(authKey).(string)
	a.s.mu.Lock()
	defer a.s.mu.Unlock()
	return got == "Bearer "+a.s.token
}

func unauthProblem() apiv1.Problem {
	return problem(http.StatusUnauthorized, apiv1.ErrorCodeUnauthenticated, "Unauthorized", "bearer token was not accepted")
}

func fieldsProblem(status int, code apiv1.ErrorCode, title, detail string, fields []apiv1.FieldError) apiv1.Problem {
	p := problem(status, code, title, detail)
	if len(fields) > 0 {
		p.Errors = &fields
	}
	return p
}

func (a *api) listKindsAccount(ctx context.Context, _ apiv1.ListAccountKindsRequestObject) (apiv1.ListAccountKindsResponseObject, error) {
	if !a.allowed(ctx) {
		return apiv1.ListAccountKinds401ApplicationProblemPlusJSONResponse{
			UnauthenticatedApplicationProblemPlusJSONResponse: apiv1.UnauthenticatedApplicationProblemPlusJSONResponse(unauthProblem()),
		}, nil
	}
	a.s.mu.Lock()
	items := append([]apiv1.AccountKind(nil), a.s.kinds...)
	a.s.mu.Unlock()
	if items == nil {
		items = []apiv1.AccountKind{}
	}
	return apiv1.ListAccountKinds200JSONResponse{Items: items}, nil
}

func (a *api) ListAccounts(ctx context.Context, req apiv1.ListAccountsRequestObject) (apiv1.ListAccountsResponseObject, error) {
	if !a.allowed(ctx) {
		return apiv1.ListAccounts401ApplicationProblemPlusJSONResponse{
			UnauthenticatedApplicationProblemPlusJSONResponse: apiv1.UnauthenticatedApplicationProblemPlusJSONResponse(unauthProblem()),
		}, nil
	}
	a.s.mu.Lock()
	defer a.s.mu.Unlock()
	if req.Params.TdInstance != nil && *req.Params.TdInstance != "" {
		if fields := a.s.tdFields(*req.Params.TdInstance); len(fields) > 0 {
			return apiv1.ListAccounts400ApplicationProblemPlusJSONResponse{
				ValidationFailedApplicationProblemPlusJSONResponse: apiv1.ValidationFailedApplicationProblemPlusJSONResponse(fieldsProblem(
					http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, http.StatusText(http.StatusBadRequest), "invalid request", fields)),
			}, nil
		}
	}
	limit := 1000
	if req.Params.Limit != nil {
		if *req.Params.Limit < 1 || *req.Params.Limit > 1000 {
			fields := []apiv1.FieldError{{Path: "/limit", Reason: "must be between 1 and 1000"}}
			return apiv1.ListAccounts400ApplicationProblemPlusJSONResponse{
				ValidationFailedApplicationProblemPlusJSONResponse: apiv1.ValidationFailedApplicationProblemPlusJSONResponse(fieldsProblem(
					http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, http.StatusText(http.StatusBadRequest), "invalid request", fields)),
			}, nil
		}
		limit = *req.Params.Limit
	}
	if a.s.pageSize > 0 && limit > a.s.pageSize {
		limit = a.s.pageSize
	}
	var after int64
	hasAfter := false
	if req.Params.Cursor != nil && *req.Params.Cursor != "" {
		b, err := base64.RawURLEncoding.DecodeString(*req.Params.Cursor)
		id, nerr := strconv.ParseInt(string(b), 10, 64)
		if err != nil || nerr != nil || strconv.FormatInt(id, 10) != string(b) {
			fields := []apiv1.FieldError{{Path: "/cursor", Reason: "is not a cursor from this API"}}
			return apiv1.ListAccounts400ApplicationProblemPlusJSONResponse{
				ValidationFailedApplicationProblemPlusJSONResponse: apiv1.ValidationFailedApplicationProblemPlusJSONResponse(fieldsProblem(
					http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, http.StatusText(http.StatusBadRequest), "invalid request", fields)),
			}, nil
		}
		after, hasAfter = id, true
	}
	rows := append([]accountRow(nil), a.s.accounts...)
	sort.Slice(rows, func(i, j int) bool { return rows[i].rec.ApiId < rows[j].rec.ApiId })
	var items []apiv1.Account
	for _, row := range rows {
		if req.Params.TdInstance != nil && *req.Params.TdInstance != "" && row.rec.TdInstance != *req.Params.TdInstance {
			continue
		}
		if hasAfter && row.rec.ApiId <= after {
			continue
		}
		items = append(items, copyAccount(row.rec))
	}
	var next *string
	if len(items) > limit {
		c := base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(items[limit-1].ApiId, 10)))
		next = &c
		items = items[:limit]
	}
	if items == nil {
		items = []apiv1.Account{}
	}
	page := a.s.listPages
	a.s.listPages++
	fresh := a.s.fresh
	if page > 0 && a.s.nextFresh != nil {
		fresh = *a.s.nextFresh
	}
	fresh = copyFresh(fresh)
	return apiv1.ListAccounts200JSONResponse{Items: items, NextCursor: next, Freshness: fresh}, nil
}

func (s *Server) tdFields(name string) []apiv1.FieldError {
	if !instanceNameRe.MatchString(name) {
		return []apiv1.FieldError{{Path: "/td_instance", Reason: "is not an instance name"}}
	}
	inst, ok := s.instances[name]
	if !ok {
		return []apiv1.FieldError{{Path: "/td_instance", Reason: "is not in the topology"}}
	}
	if inst.Plane != apiv1.Td {
		return []apiv1.FieldError{{Path: "/td_instance", Reason: "is not a td instance"}}
	}
	return nil
}

func (a *api) CreateAccount(ctx context.Context, req apiv1.CreateAccountRequestObject) (apiv1.CreateAccountResponseObject, error) {
	if !a.allowed(ctx) {
		return apiv1.CreateAccount401ApplicationProblemPlusJSONResponse{
			UnauthenticatedApplicationProblemPlusJSONResponse: apiv1.UnauthenticatedApplicationProblemPlusJSONResponse(unauthProblem()),
		}, nil
	}
	if req.Body == nil {
		return create400(fieldsProblem(http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, http.StatusText(http.StatusBadRequest), "invalid request", nil)), nil
	}
	body := *req.Body
	a.s.mu.Lock()
	defer a.s.mu.Unlock()
	if a.s.echoSecret {
		detail := ""
		if body.Credentials != nil {
			detail = body.Credentials.Secret
		}
		return create400(problem(http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, http.StatusText(http.StatusBadRequest), detail)), nil
	}
	for _, row := range a.s.accounts {
		if row.rec.Name == body.Name {
			return create409(fieldsProblem(http.StatusConflict, apiv1.ErrorCodeConflict, http.StatusText(http.StatusConflict), "name is already in use", []apiv1.FieldError{{
				Path: "/name", Reason: "already in use",
			}})), nil
		}
	}
	a.s.nextID++
	if a.s.nextID < 1 {
		a.s.nextID = 1
	}
	region := ""
	if inst, ok := a.s.instances[string(body.TdInstance)]; ok {
		region = inst.Region
	}
	rec := copyAccount(apiv1.Account{
		ApiId:        a.s.nextID,
		Name:         body.Name,
		Kind:         body.Kind,
		TdInstance:   body.TdInstance,
		Region:       region,
		Revision:     1,
		ReferencedBy: []apiv1.SessionIDValue{},
	})
	a.s.accounts = append(a.s.accounts, accountRow{rec: rec})
	loc := "/api/v1/accounts/" + strconv.FormatInt(rec.ApiId, 10)
	return apiv1.CreateAccount201JSONResponse{
		Body:    copyAccount(rec),
		Headers: apiv1.CreateAccount201ResponseHeaders{Location: &loc},
	}, nil
}

func create400(p apiv1.Problem) apiv1.CreateAccount400ApplicationProblemPlusJSONResponse {
	return apiv1.CreateAccount400ApplicationProblemPlusJSONResponse{
		ValidationFailedApplicationProblemPlusJSONResponse: apiv1.ValidationFailedApplicationProblemPlusJSONResponse(p),
	}
}

func create409(p apiv1.Problem) apiv1.CreateAccount409ApplicationProblemPlusJSONResponse {
	return apiv1.CreateAccount409ApplicationProblemPlusJSONResponse{
		ConflictApplicationProblemPlusJSONResponse: apiv1.ConflictApplicationProblemPlusJSONResponse(p),
	}
}

func (a *api) GetAccount(ctx context.Context, req apiv1.GetAccountRequestObject) (apiv1.GetAccountResponseObject, error) {
	if !a.allowed(ctx) {
		return apiv1.GetAccount401ApplicationProblemPlusJSONResponse{
			UnauthenticatedApplicationProblemPlusJSONResponse: apiv1.UnauthenticatedApplicationProblemPlusJSONResponse(unauthProblem()),
		}, nil
	}
	a.s.mu.Lock()
	defer a.s.mu.Unlock()
	_, rec, ok := a.s.findAccount(int64(req.ApiId))
	if !ok {
		return account404(), nil
	}
	return apiv1.GetAccount200JSONResponse(rec), nil
}

func (a *api) UpdateAccount(ctx context.Context, req apiv1.UpdateAccountRequestObject) (apiv1.UpdateAccountResponseObject, error) {
	if !a.allowed(ctx) {
		return apiv1.UpdateAccount401ApplicationProblemPlusJSONResponse{
			UnauthenticatedApplicationProblemPlusJSONResponse: apiv1.UnauthenticatedApplicationProblemPlusJSONResponse(unauthProblem()),
		}, nil
	}
	if req.Body == nil {
		return update400(fieldsProblem(http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, http.StatusText(http.StatusBadRequest), "invalid request", nil)), nil
	}
	body := *req.Body
	a.s.mu.Lock()
	defer a.s.mu.Unlock()
	i, rec, ok := a.s.findAccount(int64(req.ApiId))
	if !ok {
		return apiv1.UpdateAccount404ApplicationProblemPlusJSONResponse{
			NotFoundApplicationProblemPlusJSONResponse: apiv1.NotFoundApplicationProblemPlusJSONResponse(notFoundProblem()),
		}, nil
	}
	if a.s.conflict || body.Revision != rec.Revision {
		return update409(revisionProblem()), nil
	}
	if body.Credentials != nil {
		rec.Revision++
	}
	a.s.accounts[i].rec = rec
	return apiv1.UpdateAccount200JSONResponse(copyAccount(rec)), nil
}

func update400(p apiv1.Problem) apiv1.UpdateAccount400ApplicationProblemPlusJSONResponse {
	return apiv1.UpdateAccount400ApplicationProblemPlusJSONResponse{
		ValidationFailedApplicationProblemPlusJSONResponse: apiv1.ValidationFailedApplicationProblemPlusJSONResponse(p),
	}
}

func update409(p apiv1.Problem) apiv1.UpdateAccount409ApplicationProblemPlusJSONResponse {
	return apiv1.UpdateAccount409ApplicationProblemPlusJSONResponse{
		ConflictApplicationProblemPlusJSONResponse: apiv1.ConflictApplicationProblemPlusJSONResponse(p),
	}
}

func (a *api) DeleteAccount(ctx context.Context, req apiv1.DeleteAccountRequestObject) (apiv1.DeleteAccountResponseObject, error) {
	if !a.allowed(ctx) {
		return apiv1.DeleteAccount401ApplicationProblemPlusJSONResponse{
			UnauthenticatedApplicationProblemPlusJSONResponse: apiv1.UnauthenticatedApplicationProblemPlusJSONResponse(unauthProblem()),
		}, nil
	}
	a.s.mu.Lock()
	defer a.s.mu.Unlock()
	i, rec, ok := a.s.findAccount(int64(req.ApiId))
	if !ok {
		return apiv1.DeleteAccount404ApplicationProblemPlusJSONResponse{
			NotFoundApplicationProblemPlusJSONResponse: apiv1.NotFoundApplicationProblemPlusJSONResponse(notFoundProblem()),
		}, nil
	}
	if len(rec.ReferencedBy) > 0 {
		return apiv1.DeleteAccount409ApplicationProblemPlusJSONResponse{
			ConflictApplicationProblemPlusJSONResponse: apiv1.ConflictApplicationProblemPlusJSONResponse(fieldsProblem(
				http.StatusConflict, apiv1.ErrorCodeReferenced, http.StatusText(http.StatusConflict), "account is referenced by a session", nil)),
		}, nil
	}
	if a.s.conflict || req.Params.Revision != rec.Revision {
		return apiv1.DeleteAccount409ApplicationProblemPlusJSONResponse{
			ConflictApplicationProblemPlusJSONResponse: apiv1.ConflictApplicationProblemPlusJSONResponse(revisionProblem()),
		}, nil
	}
	a.s.accounts = append(a.s.accounts[:i], a.s.accounts[i+1:]...)
	return apiv1.DeleteAccount204Response{}, nil
}

func (a *api) getInstanceAccount(ctx context.Context, req apiv1.GetInstanceRequestObject) (apiv1.GetInstanceResponseObject, error) {
	if !a.allowed(ctx) {
		return apiv1.GetInstance401ApplicationProblemPlusJSONResponse{
			UnauthenticatedApplicationProblemPlusJSONResponse: apiv1.UnauthenticatedApplicationProblemPlusJSONResponse(unauthProblem()),
		}, nil
	}
	a.s.mu.Lock()
	inst, ok := a.s.instances[req.Name]
	a.s.mu.Unlock()
	if !ok {
		return apiv1.GetInstance404ApplicationProblemPlusJSONResponse{
			NotFoundApplicationProblemPlusJSONResponse: apiv1.NotFoundApplicationProblemPlusJSONResponse(notFoundProblem()),
		}, nil
	}
	inst.Workers = append([]apiv1.Worker(nil), inst.Workers...)
	inst.Freshness = copyFresh(inst.Freshness)
	return apiv1.GetInstance200JSONResponse(inst), nil
}

func (s *Server) findAccount(id int64) (int, apiv1.Account, bool) {
	for i, row := range s.accounts {
		if row.rec.ApiId == id {
			return i, copyAccount(row.rec), true
		}
	}
	return 0, apiv1.Account{}, false
}

func account404() apiv1.GetAccount404ApplicationProblemPlusJSONResponse {
	return apiv1.GetAccount404ApplicationProblemPlusJSONResponse{
		NotFoundApplicationProblemPlusJSONResponse: apiv1.NotFoundApplicationProblemPlusJSONResponse(notFoundProblem()),
	}
}

func notFoundProblem() apiv1.Problem {
	return problem(http.StatusNotFound, apiv1.ErrorCodeNotFound, http.StatusText(http.StatusNotFound), "account not found")
}

func revisionProblem() apiv1.Problem {
	return fieldsProblem(http.StatusConflict, apiv1.ErrorCodeConflict, http.StatusText(http.StatusConflict), "revision conflict", []apiv1.FieldError{{
		Path: "/revision", Reason: "does not match",
	}})
}
