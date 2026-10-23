package api

import (
	"net/http"

	openapi_types "github.com/oapi-codegen/runtime/types"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

// Server implements apiv1.ServerInterface so the generated strategy wrappers can be mounted.
// Only the three strategy routes are registered. Every other operation stays 404 until its ticket (D1024).
var _ apiv1.ServerInterface = (*Server)(nil)

func (s *Server) notRouted(w http.ResponseWriter, r *http.Request) {
	http.NotFound(w, r)
}

func (s *Server) ListAccountKinds(w http.ResponseWriter, r *http.Request) {
	s.notRouted(w, r)
}

func (s *Server) ListAccounts(w http.ResponseWriter, r *http.Request, _ apiv1.ListAccountsParams) {
	s.notRouted(w, r)
}

func (s *Server) CreateAccount(w http.ResponseWriter, r *http.Request) { s.notRouted(w, r) }

func (s *Server) DeleteAccount(w http.ResponseWriter, r *http.Request, _ apiv1.APIID, _ apiv1.DeleteAccountParams) {
	s.notRouted(w, r)
}

func (s *Server) GetAccount(w http.ResponseWriter, r *http.Request, _ apiv1.APIID) {
	s.notRouted(w, r)
}

func (s *Server) UpdateAccount(w http.ResponseWriter, r *http.Request, _ apiv1.APIID) {
	s.notRouted(w, r)
}

func (s *Server) Login(w http.ResponseWriter, r *http.Request)  { s.notRouted(w, r) }
func (s *Server) Logout(w http.ResponseWriter, r *http.Request) { s.notRouted(w, r) }
func (s *Server) GetMe(w http.ResponseWriter, r *http.Request)  { s.notRouted(w, r) }

func (s *Server) StreamEvents(w http.ResponseWriter, r *http.Request, _ apiv1.StreamEventsParams) {
	s.notRouted(w, r)
}

func (s *Server) ListInstances(w http.ResponseWriter, r *http.Request, _ apiv1.ListInstancesParams) {
	s.notRouted(w, r)
}

func (s *Server) GetInstance(w http.ResponseWriter, r *http.Request, _ apiv1.InstanceName) {
	s.notRouted(w, r)
}

func (s *Server) ListSessionRecords(w http.ResponseWriter, r *http.Request, _ apiv1.ListSessionRecordsParams) {
	s.notRouted(w, r)
}

func (s *Server) GetSessionRecord(w http.ResponseWriter, r *http.Request, _ openapi_types.UUID) {
	s.notRouted(w, r)
}

func (s *Server) ListSessions(w http.ResponseWriter, r *http.Request, _ apiv1.ListSessionsParams) {
	s.notRouted(w, r)
}

func (s *Server) CreateSession(w http.ResponseWriter, r *http.Request) { s.notRouted(w, r) }

func (s *Server) DeleteSession(w http.ResponseWriter, r *http.Request, _ apiv1.SessionID) {
	s.notRouted(w, r)
}

func (s *Server) GetSession(w http.ResponseWriter, r *http.Request, _ apiv1.SessionID) {
	s.notRouted(w, r)
}

func (s *Server) EndSession(w http.ResponseWriter, r *http.Request, _ apiv1.SessionID) {
	s.notRouted(w, r)
}
