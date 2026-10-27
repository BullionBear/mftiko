// Package agent is the strategy-plane agent (E09-D016, E09-D042–E09-D084). It runs the sessions
// the controller assigns to this instance and writes each one's SessionStatus.
//
// Spawn pulls the strategy artifact and its inputs and unpacks them outside
// reconcile, then launches that rootfs (E09-D058, E09-D060, E09-D074). The volume is created
// before the first spawn and kept across incarnations (E09-D077). A volume over its
// limit is a normal Stop and FAILED / volume_over_limit (E09-D076). InitPayload carries
// the spec, the resolved limits, and a session JWT (E09-D018, E09-D024). Start carries the
// first bindings; later changes are Command snapshots (E09-D061). Stop uses the spec's
// grace_ms (E09-D062). A missing beat is a SIGKILL with no Stop (E09-D066), recorded as
// HUNG / no_beat (E09-D067).
//
// A session with no usable spec is not started and gets no status (E09-D044). A session
// already DONE or FAILED is not started again (E09-D045). Overlapping algo_ids on one
// api_id end the later session with algo_conflict (E09-D017). DONE, FAILED, and
// INIT_FAILED are not restarted; CRASHED, HUNG, and an unrequested signal are,
// until the restart window is used up (E09-D019, E09-D064, E09-D069). A lost shim is not
// restarted (E09-D070). After the worker process has ended, a non-empty volume is
// packed and pushed, then state_out is filled in; an empty volume is removed
// without a push. That step deletes the volume and the inputs. Shim logs,
// terminal.json, and the incarnation key stay until the spec is gone
// (E09-D078–E09-D084, E10-D016, E10-D020). While the term holds it, the log
// endpoint serves those files (E14-D019–E14-D024).
package agent
