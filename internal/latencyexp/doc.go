// Package latencyexp is the mftiko-latency-exporter (DECISIONS E17-D001–E17-D006).
//
// One process per STS host tails every session event log under the agent's data
// directory, sessions/<session_id>/volume/eventlog/*.mel (PROTOCOL §12), and
// serves Prometheus text on -metrics-addr. Frames are decoded with the
// sdk/eventlog reader. The SDK and the hot path are not involved.
//
// A session that has not turned the log on has no eventlog directory and is
// skipped. An incomplete frame at the end of the file currently being written
// waits until the writer finishes it. After a rotation the next file is read
// in order. Frames that were deleted before they were read, or dropped by the
// writer, show up as a rec_seq gap.
//
// The process reads what is already on disk and then follows new bytes, so the
// metrics describe the retained logs. A restart starts the counters over;
// Prometheus treats that as a counter reset.
package latencyexp
