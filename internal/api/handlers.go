package api

import (
	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

// The generated server interface is what internal/api serves (D1003). Session and instance reads,
// session records, and the event stream are read.go and events.go (D1092–D1097).
var _ apiv1.ServerInterface = (*Server)(nil)
