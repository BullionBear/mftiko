package api

import (
	"net/http"

	openapi_types "github.com/oapi-codegen/runtime/types"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

// The generated server interface is what internal/api serves (D1003). Operations that belong to
// later tickets answer 404 not_found once the caller is authenticated, so a valid token is not a 401.
var _ apiv1.ServerInterface = (*Server)(nil)

func (s *Server) ListAccountKinds(w http.ResponseWriter, _ *http.Request) {
	s.notFound(w)
}

func (s *Server) ListAccounts(w http.ResponseWriter, _ *http.Request, _ apiv1.ListAccountsParams) {
	s.notFound(w)
}

func (s *Server) CreateAccount(w http.ResponseWriter, _ *http.Request) { s.notFound(w) }

func (s *Server) DeleteAccount(w http.ResponseWriter, _ *http.Request, _ apiv1.APIID, _ apiv1.DeleteAccountParams) {
	s.notFound(w)
}

func (s *Server) GetAccount(w http.ResponseWriter, _ *http.Request, _ apiv1.APIID) {
	s.notFound(w)
}

func (s *Server) UpdateAccount(w http.ResponseWriter, _ *http.Request, _ apiv1.APIID) {
	s.notFound(w)
}

// StreamEvents holds the response open until the client goes away. Event payloads, Last-Event-ID
// resume, and the 15s heartbeat are E10-13 (#318). This exists so the audit row is written when
// the stream ends, with the connection time as the duration (D1010).
func (s *Server) StreamEvents(w http.ResponseWriter, r *http.Request, params apiv1.StreamEventsParams) {
	if len(params.Topics) == 0 {
		writeProblem(w, http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, http.StatusText(http.StatusBadRequest), "missing parameter", []apiv1.FieldError{{
			Path:   "topics",
			Reason: "required",
		}})
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	<-r.Context().Done()
}

func (s *Server) ListInstances(w http.ResponseWriter, _ *http.Request, _ apiv1.ListInstancesParams) {
	s.notFound(w)
}

func (s *Server) GetInstance(w http.ResponseWriter, _ *http.Request, _ apiv1.InstanceName) {
	s.notFound(w)
}

func (s *Server) ListSessionRecords(w http.ResponseWriter, _ *http.Request, _ apiv1.ListSessionRecordsParams) {
	s.notFound(w)
}

func (s *Server) GetSessionRecord(w http.ResponseWriter, _ *http.Request, _ openapi_types.UUID) {
	s.notFound(w)
}

func (s *Server) ListSessions(w http.ResponseWriter, _ *http.Request, _ apiv1.ListSessionsParams) {
	s.notFound(w)
}

func (s *Server) CreateSession(w http.ResponseWriter, _ *http.Request) { s.notFound(w) }

func (s *Server) DeleteSession(w http.ResponseWriter, _ *http.Request, _ apiv1.SessionID) {
	s.notFound(w)
}

func (s *Server) GetSession(w http.ResponseWriter, _ *http.Request, _ apiv1.SessionID) {
	s.notFound(w)
}

func (s *Server) EndSession(w http.ResponseWriter, _ *http.Request, _ apiv1.SessionID) {
	s.notFound(w)
}
