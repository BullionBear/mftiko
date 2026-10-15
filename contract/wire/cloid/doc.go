// Package cloid is the client_order_id layout ver(4) | algo(10) | ts_ms(40) | seq(10) (D602,
// PROTOCOL §2): Encode, Decode and AlgoOf.
//
// The SDK encodes and filters with AlgoOf. TD does not decode: on the trade path client_order_id is
// an opaque string (D633). Persistence still decodes, to attribute a stored order (D601, D602, E7).
// Encode defines the layout; the session worker's generator, which keeps ts_ms monotonic and borrows
// a millisecond when seq runs out, is the SDK's (PROTOCOL §9.4).
//
// Standard library only.
package cloid
