// Package shim implements the shim: the C3 server for agents, the C4 link to its worker on fd 3, event buffering, epoch fencing and the exit file (PROTOCOL §4, §5).
//
// Implemented so far: the Launch on stdin and the spawn (PROTOCOL §4.6, E1-10). Linux only.
package shim
