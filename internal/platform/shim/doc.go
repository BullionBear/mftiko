// Package shim implements the shim: the C3 server for agents, the C4 link to its worker on fd 3, event buffering, epoch fencing and the exit file (PROTOCOL §4, §5).
//
// Implemented so far: the Launch on stdin and the spawn (PROTOCOL §4.6, E1-10); the C3 socket, Attach with epoch
// fencing and major negotiation, and Signal (§4.1, §4.2, E1-09); the lifecycle verbs, Stop's timers and the
// shim's own SIGTERM (§4.3, E1-11). Linux only.
package shim
