package apitest

import (
	"context"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

// ListScopes keeps the generated strict server compiling. The CLI has no scopes
// command; PMS calls the API directly (E21-D068). The fixture list is empty.
func (a *api) ListScopes(ctx context.Context, _ apiv1.ListScopesRequestObject) (apiv1.ListScopesResponseObject, error) {
	if !a.allowed(ctx) {
		return apiv1.ListScopes401ApplicationProblemPlusJSONResponse{
			UnauthenticatedApplicationProblemPlusJSONResponse: apiv1.UnauthenticatedApplicationProblemPlusJSONResponse(unauthProblem()),
		}, nil
	}
	return apiv1.ListScopes200JSONResponse{
		Items:     []apiv1.Scope{},
		Freshness: apiv1.Freshness{UnavailableRegions: []string{}},
	}, nil
}
