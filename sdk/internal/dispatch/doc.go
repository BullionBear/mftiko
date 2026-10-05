// Package dispatch is the single dispatcher: the TD buffer (never drops, FAILED on overflow), the MD
// ring (drops the oldest), TD-before-MD ordering, and the recv_window_ms prehook (DECISIONS D24).
package dispatch
