//go:build unix

// Package logread reads one shim log stream across size-based rotation
// (PROTOCOL §4.5 S4, E14-03).
//
// It does not know which plane produced the files and it does not use the
// network. The live file is the path the caller passes, typically
// {h}.stdout or {h}.stderr. Rotated files are that name plus .1, .2, … with
// .1 the newest and .99 the oldest the shim can leave behind.
//
// A Cursor resumes a later read (E14-D016). When it does not land, the next
// record is a gap and bytes continue from the oldest file still present
// (E14-D017). Follow holds the current file's descriptor and, once that file
// is exhausted, checks the path every 250ms (E14-D018). Pause, when set, ends
// that wait early. Prime waits until Cursor is the resolved start and does
// not consume bytes.
package logread
