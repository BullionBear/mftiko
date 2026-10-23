package api

import (
	"encoding/json"
	"errors"
	"net/http"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

// writeProblem sends an RFC 9457 body (D1004). detail is a fixed sentence; it must not echo a
// token, a cookie, or a credential.
func writeProblem(w http.ResponseWriter, status int, code apiv1.ErrorCode, detail string, fields []apiv1.FieldError) {
	p := apiv1.Problem{
		Type:   "about:blank",
		Title:  http.StatusText(status),
		Status: status,
		Code:   code,
	}
	if detail != "" {
		p.Detail = &detail
	}
	if len(fields) > 0 {
		p.Errors = &fields
	}
	b, err := json.Marshal(p)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_, _ = w.Write(append(b, '\n'))
}

func (s *Server) notFound(w http.ResponseWriter) {
	writeProblem(w, http.StatusNotFound, apiv1.ErrorCodeNotFound, "not found", nil)
}

// openAPIError is the generated router's parameter-binding failure. The text of err can quote the
// caller's value, so the response does not include it.
func (s *Server) openAPIError(w http.ResponseWriter, _ *http.Request, err error) {
	var missing *apiv1.RequiredParamError
	if errors.As(err, &missing) {
		writeProblem(w, http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, "missing parameter", []apiv1.FieldError{{
			Path:   missing.ParamName,
			Reason: "required",
		}})
		return
	}
	writeProblem(w, http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, "invalid request", nil)
}
