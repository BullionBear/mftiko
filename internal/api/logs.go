package api

import (
	"net/http"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

// GetSessionLogs is the generated SSE handler (E14-D011). #440 replaces this body
// with the agent proxy. This build answers 503 logs_unavailable.
func (s *Server) GetSessionLogs(w http.ResponseWriter, r *http.Request, _ apiv1.SessionID, _ apiv1.GetSessionLogsParams) {
	logsUnavailable(w)
}

// ExportSessionLogs is the generated export handler (E14-D014). #440 replaces this
// body with the agent proxy. This build answers 503 logs_unavailable.
func (s *Server) ExportSessionLogs(w http.ResponseWriter, r *http.Request, _ apiv1.SessionID) {
	logsUnavailable(w)
}

func logsUnavailable(w http.ResponseWriter) {
	writeProblem(w, http.StatusServiceUnavailable, apiv1.ErrorCodeLogsUnavailable, "Logs Unavailable", "logs are unavailable", nil)
}
