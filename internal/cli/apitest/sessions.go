package apitest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

// SessionIDs returns the session ids stored by POST /sessions, in order.
func (s *Server) SessionIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.sessionOrder))
	copy(out, s.sessionOrder)
	return out
}

// takeDrop records and consumes one planned connection drop for POST /sessions.
// The caller closes the connection so the client sees a network error.
func (s *Server) takeDrop(r *http.Request, raw []byte) bool {
	if r.Method != http.MethodPost || r.URL.Path != "/api/v1/sessions" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dropLeft <= 0 {
		return false
	}
	s.dropLeft--
	s.calls = append(s.calls, Call{
		Operation:      "CreateSession",
		Method:         r.Method,
		Path:           r.URL.Path,
		Authorization:  r.Header.Get("Authorization"),
		UserAgent:      r.Header.Get("User-Agent"),
		ContentType:    r.Header.Get("Content-Type"),
		Cookie:         r.Header.Get("Cookie"),
		RawQuery:       r.URL.RawQuery,
		Query:          r.URL.RawQuery,
		Body:           append([]byte(nil), raw...),
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
	})
	return true
}

func hijackClose(w http.ResponseWriter) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		panic("apitest: response writer cannot hijack")
	}
	conn, _, err := hj.Hijack()
	if err != nil {
		panic(err)
	}
	_ = conn.Close()
}

func (a *api) createSession(ctx context.Context, req apiv1.CreateSessionRequestObject) (apiv1.CreateSessionResponseObject, error) {
	if !a.allowed(ctx) {
		return apiv1.CreateSession401ApplicationProblemPlusJSONResponse{
			UnauthenticatedApplicationProblemPlusJSONResponse: apiv1.UnauthenticatedApplicationProblemPlusJSONResponse(unauthProblem()),
		}, nil
	}
	raw, _ := ctx.Value(bodyKey).([]byte)
	raw = append([]byte(nil), raw...)
	key := ""
	if req.Params.IdempotencyKey != nil {
		key = *req.Params.IdempotencyKey
	}
	a.s.mu.Lock()
	defer a.s.mu.Unlock()
	if a.s.storageDown {
		p := problem(http.StatusServiceUnavailable, apiv1.ErrorCodeStorageUnavailable, http.StatusText(http.StatusServiceUnavailable), "storage is unavailable")
		return apiv1.CreateSession503ApplicationProblemPlusJSONResponse{
			UnavailableApplicationProblemPlusJSONResponse: apiv1.UnavailableApplicationProblemPlusJSONResponse(p),
		}, nil
	}
	if a.s.algoOverlap {
		p := problem(http.StatusConflict, apiv1.ErrorCodeAlgoOverlap, http.StatusText(http.StatusConflict), "algorithms overlap")
		return apiv1.CreateSession409ApplicationProblemPlusJSONResponse{
			ConflictApplicationProblemPlusJSONResponse: apiv1.ConflictApplicationProblemPlusJSONResponse(p),
		}, nil
	}
	if a.s.createFields != nil {
		fields := append([]apiv1.FieldError(nil), a.s.createFields...)
		p := fieldsProblem(http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, http.StatusText(http.StatusBadRequest), "invalid request", fields)
		return apiv1.CreateSession400ApplicationProblemPlusJSONResponse{
			ValidationFailedApplicationProblemPlusJSONResponse: apiv1.ValidationFailedApplicationProblemPlusJSONResponse(p),
		}, nil
	}
	if prev, ok := a.s.sessions[key]; ok {
		if bytes.Equal(prev.body, raw) {
			return sessionCreated(prev.id), nil
		}
		p := problem(http.StatusConflict, apiv1.ErrorCodeConflict, http.StatusText(http.StatusConflict), "the request conflicts with existing state")
		return apiv1.CreateSession409ApplicationProblemPlusJSONResponse{
			ConflictApplicationProblemPlusJSONResponse: apiv1.ConflictApplicationProblemPlusJSONResponse(p),
		}, nil
	}
	a.s.nextSession++
	id := fmt.Sprintf("%06x", a.s.nextSession)
	a.s.sessions[key] = savedSession{id: id, body: raw}
	a.s.sessionOrder = append(a.s.sessionOrder, id)
	return sessionCreated(id), nil
}

func sessionCreated(id string) apiv1.CreateSession201JSONResponse {
	digest := "sha256:" + strings.Repeat("ab", 32)
	strategyID := "chase"
	return apiv1.CreateSession201JSONResponse{Body: apiv1.Session{
		Id:           id,
		Region:       "r1",
		Revision:     1,
		StsInstance:  "sts1",
		Dependencies: []apiv1.Dependency{},
		Spec: apiv1.SessionSpecView{
			Name:         "demo",
			Accounts:     []apiv1.AccountDecl{},
			Feeds:        []apiv1.FeedDeclInput{},
			Inputs:       []apiv1.InputDecl{},
			Labels:       apiv1.Labels{},
			DesiredState: apiv1.DesiredStateRunning,
			Params:       json.RawMessage(`{}`),
			Restart:      apiv1.RestartPolicy{},
			Strategy:     apiv1.StrategyRef{Name: "chase", Digest: digest, Repository: "example/chase", Id: &strategyID},
		},
	}}
}
