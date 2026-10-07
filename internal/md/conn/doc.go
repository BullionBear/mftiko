// Package conn is the MD conn worker: one websocket per (venue, route, n), the local full order book, per-atom tape rings, the md.f envelope, and the md.view server (D21, D24, D26).
//
// Venue specifics come from exchange: Market.NewSession, BookSync, PublicREST.Depth.
//
// Shape only: not implemented yet.
package conn
