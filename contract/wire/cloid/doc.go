// Package cloid is the client_order_id layout ver(4) | algo(10) | ts_ms(40) | seq(10) (D602,
// PROTOCOL §2): Encode, Decode and AlgoOf.
//
// The TD account worker decodes: AlgoOf checks a Submit's or Cancel's algo_id, picks a BatchCancel's
// orders, and tells hand-placed orders (no algo) apart. Encode defines the layout; the session
// worker's generator, which keeps ts_ms monotonic and borrows a millisecond when seq runs out, is
// the SDK's (PROTOCOL §9.4).
//
// Standard library only.
package cloid
