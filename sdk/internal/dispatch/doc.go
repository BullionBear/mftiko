// Package dispatch is the session worker's single dispatcher (DECISIONS E08-D003, E08-D015–E08-D017).
//
// One goroutine runs every hook, so a strategy needs no locks. Two buffers feed it. The TD buffer
// holds orders, timers, Post, and read replies: FIFO, never dropped, 65536 entries, and a full
// buffer ends the session FAILED. The MD buffer is one preallocated 32 MiB byte ring of
// variable-length messages: when a message does not fit, the oldest messages are dropped until it
// does, except the message a hook is reading. A message larger than the ring is dropped and counted.
//
// After each hook the TD buffer is taken first. An MD message is taken only when TD is empty. The
// prehook then reads the envelope timestamp and drops a message older than recv_window_ms without
// decoding it. A message that is not a v1 envelope (short, wrong version, or a non-zero reserved
// byte) is also dropped, counted in QueueStat.dropped, and logged; the session does not end (E08-D049).
// orderbook_delta does not take the age path (E8-15 applies it); a bad envelope is still dropped.
//
// A hook longer than 100ms counts one hook_slow. hook_blocked_ms is how long the current hook has
// been running. A hook longer than hook_timeout_s makes Run return *c4.Exit with outcome HUNG;
// this package does not send Exiting. OnStop is c4.Stopper: c4 calls it after Exiting, and the
// TD drain and the strategy callback run on the dispatcher goroutine.
//
// With the event log on, each hook's input is written to it before the hook runs, stamped with the
// hook's start, and a hook_end follows when the hook returns or panics (E20-D003, E20-D005). The log
// copies the bytes into its own ring and never waits, so recording does not slow the loop. With the
// log off nothing is kept for it. Every hook's start is also the session clock that Context.Now reads
// (E20-D004).
package dispatch
