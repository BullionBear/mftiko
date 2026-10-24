// Package cloid is the client_order_id layout ver(4) | algo(10) | ts_ms(40) | seq(10) (E06-D002,
// PROTOCOL §2): Encode, Decode and AlgoOf.
//
// The SDK encodes and filters with AlgoOf. TD does not decode: on the trade path client_order_id is
// an opaque string (E06-D033). Persistence still decodes, to attribute a stored order (E06-D001, E06-D002, E7).
// Encode defines the layout; the session worker's generator, which keeps ts_ms monotonic and borrows
// a millisecond when seq runs out, is sdk/internal/cloidgen (PROTOCOL §9.4, E08-D011–E08-D014). Borrowing can
// put ts_ms ahead of the wall clock.
//
// Standard library only.
package cloid
