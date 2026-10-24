package api

import (
	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

// The generated server interface is what internal/api serves (E10-D003). Session and instance reads,
// session records, and the event stream are read.go and events.go (E10-D092–E10-D097).
var _ apiv1.ServerInterface = (*Server)(nil)
