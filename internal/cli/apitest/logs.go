package apitest

import (
	"context"
	"net/http"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

// GetSessionLogs is the strict default (E14-D011). Option.Logs replaces it.
func (a *api) GetSessionLogs(ctx context.Context, _ apiv1.GetSessionLogsRequestObject) (apiv1.GetSessionLogsResponseObject, error) {
	if !a.allowed(ctx) {
		return apiv1.GetSessionLogs401ApplicationProblemPlusJSONResponse{
			UnauthenticatedApplicationProblemPlusJSONResponse: apiv1.UnauthenticatedApplicationProblemPlusJSONResponse(unauthProblem()),
		}, nil
	}
	p := problem(http.StatusServiceUnavailable, apiv1.ErrorCodeLogsUnavailable, "Logs Unavailable", "logs are unavailable")
	return apiv1.GetSessionLogs503ApplicationProblemPlusJSONResponse{
		LogsUnavailableApplicationProblemPlusJSONResponse: apiv1.LogsUnavailableApplicationProblemPlusJSONResponse(p),
	}, nil
}

// ExportSessionLogs is the strict default (E14-D014). Option.Logs replaces it.
func (a *api) ExportSessionLogs(ctx context.Context, _ apiv1.ExportSessionLogsRequestObject) (apiv1.ExportSessionLogsResponseObject, error) {
	if !a.allowed(ctx) {
		return apiv1.ExportSessionLogs401ApplicationProblemPlusJSONResponse{
			UnauthenticatedApplicationProblemPlusJSONResponse: apiv1.UnauthenticatedApplicationProblemPlusJSONResponse(unauthProblem()),
		}, nil
	}
	p := problem(http.StatusServiceUnavailable, apiv1.ErrorCodeLogsUnavailable, "Logs Unavailable", "logs are unavailable")
	return apiv1.ExportSessionLogs503ApplicationProblemPlusJSONResponse{
		LogsUnavailableApplicationProblemPlusJSONResponse: apiv1.LogsUnavailableApplicationProblemPlusJSONResponse(p),
	}, nil
}
