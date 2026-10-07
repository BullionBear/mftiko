// Package frame is the C3/C4 framing: a 4-byte big-endian length followed by one protobuf message, whose body is at
// most 1 MiB (PROTOCOL §4.1, §5.1). C3 runs it over the shim's unix socket, C4 over the socketpair whose worker end
// is fd 3; both ends dying show up as EOF.
//
// A Writer is safe for concurrent use and writes each frame whole. A Reader is for one goroutine and tells a clean
// end between frames (io.EOF) from one inside a frame (ErrTruncated), and rejects an oversized frame from its
// header alone (ErrTooLarge), before allocating for it.
package frame
