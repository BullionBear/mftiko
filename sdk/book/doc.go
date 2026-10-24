// Package book is the tick-indexed order book shared by the MD conn worker and the STS worker (E04-D030).
//
// An OrderBook is two fixed rings, bids and asks, each of length N (default 2048, a power of two).
// A price is a tick, round(price/tickSize), stored at index tick&(N-1). Slots are never cleared.
// A slot whose stored tick is not the one being read is empty, and a size of zero is a delete.
// The session folds md.view snapshots and orderbook_delta onto this type in sdk/internal/bookpath.
package book
