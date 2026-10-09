package exchange

import (
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
)

// RouteDefault is the route of a venue whose public data all lives on one websocket path (PROTOCOL §2).
const RouteDefault = "default"

// Market is a venue's public websocket market data.
//
// The planning half (Topics, Atom, Routes, Budget, Cost) is pure: no I/O, no state, safe to call from
// the MD agent's planner. The connection half (Endpoint, NewSession, Book) is used by the conn worker.
type Market interface {
	// Topics is the capability table (D401). A topic missing here is unsupported on this venue.
	Topics() []TopicCaps

	// Atom maps a feed to its atom (D401, D202, D203). A spec names the feed with a UniversalTicker; the
	// caller resolves that to *symbol.Info at load (symbol.Resolver.Get) and passes what came back,
	// so this stays a pure function. The Info's UID is the instrument inside the system. The answer
	// is the venue channel to subscribe and the route (websocket
	// path) it lives on (D403); several topics may land on one atom (orderbook and orderbook_delta,
	// D402; Bybit's linear tickers carry ticker, funding, open interest and mark price).
	// ErrUnsupported when the venue does not push this topic: the lease item becomes REJECTED
	// (PROTOCOL §3.5), and query-only values go to md.fetch instead.
	Atom(topic Topic, inst *symbol.Info) (AtomRef, error)

	// Routes lists the venue's public routes. A single-path venue returns []string{RouteDefault}.
	Routes() []string

	// Budget is how much one connection on route can carry; Cost is what one channel takes out of it.
	// A new atom goes on the lowest n of (venue, route) with room left; n+1 opens only when all are
	// full (D403). Binance counts streams (cost 1, budget 1024); Bybit limits the total length of the
	// subscribed args. Budget <= 0 means the venue sets no limit.
	Budget(route string) int
	Cost(channel string) int

	// Endpoint is the websocket URL for route. override is mdctl.InitPayload.url when the MD instance
	// picked an alternative host of the same venue (D403); empty means the default.
	Endpoint(route, override string) (string, error)

	// NewSession returns the codec state of one connection. Not safe for concurrent use: one per
	// websocket, owned by the conn worker's read loop.
	NewSession(route string) (Session, error)

	// Book returns the sequencing rules of a book channel; false if channel carries no book.
	Book(channel string) (BookSync, bool)
}

// AtomRef is an atom's identity within its venue: one subscribe and one venue ack on one
// connection (D401). The atom id is <venue>:<route>:<channel> (D202). Route is part of it because a
// channel string can repeat across routes: Bybit's "publicTrade.BTCUSDT" is both a spot and a linear
// atom.
type AtomRef struct {
	Route   string // one lowercase word, e.g. public, market, linear; RouteDefault on a single-path venue
	Channel string // the venue's subscribe argument, verbatim
}

// Session encodes requests and decodes frames for one public websocket.
type Session interface {
	Limits() SessionLimits

	// Subscribe and Unsubscribe split channels into as many requests as Limits allows. The conn worker
	// paces them by Limits.Rate and matches each Request.ID to the Ack it gets back.
	Subscribe(channels []string) ([]Request, error)
	Unsubscribe(channels []string) ([]Request, error)

	// Ping is the application-level ping to send every Limits.Keepalive.Interval; nil when the venue
	// pings the client instead, or a websocket ping frame is enough.
	Ping() []byte

	// Decode parses one text or binary frame and calls sink once per event in it. One frame can carry
	// several events of several kinds (atom -> events is 1:N, D401). frame is lent for the call: it is
	// a pooled buffer the conn worker releases once Decode returns (D302).
	Decode(frame []byte, sink Sink) error
}

// Request is one subscribe or unsubscribe message on the wire.
type Request struct {
	ID       uint64
	Channels []string
	Frame    []byte
}

// SessionLimits are the venue's rules for one connection.
type SessionLimits struct {
	PerRequest      int         // channels per subscribe message; 0 = no limit
	PerRequestBytes int         // bytes of channel names per subscribe message; 0 = no limit
	Rate            []RateLimit // outgoing messages, all must hold
	Keepalive       Keepalive
	// MaxLifetime is when the venue drops the connection by itself (Binance: 24h). The conn worker
	// reconnects before that, which bumps conn_epoch like any reconnect. 0 = none.
	MaxLifetime time.Duration
}

// RateLimit is at most N events per window.
type RateLimit struct {
	N   int
	Per time.Duration
}

type Keepalive struct {
	Interval   time.Duration // client ping period; 0 = the client does not ping
	ServerPing bool          // the venue pings and expects a pong
	Timeout    time.Duration // no frame for this long = connection dead
}

// SnapshotSource says where a book's first full state comes from (D402).
type SnapshotSource uint8

const (
	SnapshotUnspecified SnapshotSource = iota
	// SnapshotREST: buffer stream updates, fetch PublicREST.Depth, drop updates the snapshot already
	// covers, then apply the rest (Binance).
	SnapshotREST
	// SnapshotStream: the first message after subscribe is the full book (OKX, Bybit).
	SnapshotStream
)

// BookSync holds a venue's book sequencing rules. The local full book and the fold live in the conn
// worker (D402); the adapter only judges each update against the sequence state.
type BookSync interface {
	Snapshot() SnapshotSource
	// Depth is the REST snapshot depth to ask for under SnapshotREST.
	Depth() int
	// Check classifies u given the last applied sequence state.
	Check(last BookSeq, u *BookUpdate) BookVerdict
}

// BookSeq is the sequence state of a local book after the last applied update.
type BookSeq struct {
	Last  uint64
	Valid bool // false until a snapshot is applied
}

type BookVerdict uint8

const (
	BookApply  BookVerdict = iota + 1 // in sequence: apply
	BookSkip                          // already covered by the snapshot: drop
	BookResync                        // gap or bad checksum: discard the book, fetch again, push a reset
)

// BookChecksum is implemented by a BookSync whose venue sends a checksum of the top of the book
// with each update (OKX). The conn worker calls it after applying u.
type BookChecksum interface {
	Verify(bids, asks []Level, u *BookUpdate) bool
}
