// Package agent is the strategy-plane agent (D916, D942–D984). It runs the sessions
// the controller assigns to this instance and writes each one's SessionStatus.
//
// Spawn pulls the strategy artifact and its inputs and unpacks them outside
// reconcile, then launches that rootfs (D958, D960, D974). The volume is created
// before the first spawn and kept across incarnations (D977). A volume over its
// limit is a normal Stop and FAILED / volume_over_limit (D976). InitPayload carries
// the spec, the resolved limits, and a session JWT (D918, D924). Start carries the
// first bindings; later changes are Command snapshots (D961). Stop uses the spec's
// grace_ms (D962). A missing beat is a SIGKILL with no Stop (D966), recorded as
// HUNG / no_beat (D967).
//
// A session with no usable spec is not started and gets no status (D944). A session
// already DONE or FAILED is not started again (D945). Overlapping algo_ids on one
// api_id end the later session with algo_conflict (D917). DONE, FAILED, and
// INIT_FAILED are not restarted; CRASHED, HUNG, and an unrequested signal are,
// until the restart window is used up (D919, D964, D969). A lost shim is not
// restarted (D970). After the worker process has ended, a non-empty volume is
// packed and pushed, then state_out is filled in; an empty volume is removed
// without a push. Cleanup then drops the session directory, the shim logs, and
// the incarnation key (D978–D984).
package agent
