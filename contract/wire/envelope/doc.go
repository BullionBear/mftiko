// Package envelope is the fixed binary prefix of every md.f message (E08-D003, E04-D005-E04-D011, PROTOCOL §7.1).
//
// Version 1 is 40 bytes, little-endian. The payload starts at byte 40. There is no length byte: the
// version fixes the size, and the seven bytes after the version are reserved and must be zero. Append
// writes one message, header and then payload. Parse reads one. MDTsNs reads only the publish time, which
// is what a prehook needs. All three reject a short buffer, a version other than 1, or a reserved byte
// that is not zero.
//
// The digest is xxHash64 of the payload alone, seed 0. It does not cover the envelope. Parse returns the
// stored digest and does not recompute it; deduplication is not this package's job.
//
// Reads and writes go through encoding/binary and do not use unsafe. A NATS msg.Data is not guaranteed to
// be 8-byte aligned. Append allocates only when dst lacks room for the envelope and the payload. Parse
// does not allocate on a valid message: the payload aliases the input.
package envelope
