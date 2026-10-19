// Package agent is the strategy-plane agent (D916, D942–D949). It runs the sessions
// the controller assigns to this instance and writes each one's SessionStatus.
//
// Spawn is the worker executable named at deployment (D943). Pulling the strategy
// artifact, the full InitPayload, and bindings are a later change; this package
// sends Init with an empty payload and Start when the assignment's start is set.
// A session that has left the plan is stopped by the agent skeleton.
//
// A session with no usable spec is not started and gets no status (D944). A session
// already DONE or FAILED is not started again (D945). Overlapping algo_ids on one
// api_id end the later session with algo_conflict (D917).
package agent
