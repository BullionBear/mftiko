package exchange

import (
	"fmt"
	"sort"
	"time"
)

// TradeStream is the codec of one websocket that carries order requests and their answers (D303):
// Binance's WebSocket API, and the order channels OKX and Bybit offer. Like every codec it is sans-I/O
// (D302): it turns a request into a frame and a frame into answers. The account worker owns the
// connection, the request ids, the time a request may wait for its answer, and the reconnect.
//
// An Account that has one returns it from NewTradeStream, and the worker sends Submit and Cancel over
// it instead of REST. CancelBatch, Snapshot, Leverage and History stay on REST.
//
// Not safe for concurrent use: one per connection, owned by the connection's read loop.
type TradeStream interface {
	// Dial is the URL to connect to and the frames to send once connected: the logon, and on a
	// connection that carries private events the subscription to them. Signed at now. The answers to
	// these frames are the codec's own; it reports Ready once the stream is live.
	Dial(now time.Time) (*Dial, error)

	Limits() TradeLimits

	// Ping is the application-level ping to send every Limits().Keepalive.Interval; nil when a
	// websocket ping frame is enough.
	Ping() []byte

	// Submit and Cancel encode one request under the worker's id. Ids are the worker's: at least 1,
	// never reused on a connection. Frames the codec makes itself (Dial's) use ids of their own that
	// cannot collide with them.
	//
	// Decode needs no memory of what was sent: the codec writes into the venue's request id whatever
	// it needs to read the answer (Binance takes a string: "o17" for an order, "c18" for a cancel). An
	// answer is then readable after a reconnect, and a recorded session replays (E3-04).
	Submit(now time.Time, id uint64, req *OrderRequest) ([]byte, error)
	Cancel(now time.Time, id uint64, ref CancelRef) ([]byte, error)

	// Decode parses one frame and calls sink: the answer to a request, by id; private events, when
	// the connection carries them; the usage the venue reports. A refused logon is ErrAuth. frame is
	// lent for the call (D302).
	Decode(frame []byte, sink TradeSink) error
}

// TradeLimits are the venue's rules for one trade connection.
type TradeLimits struct {
	// Events: the connection also carries the account's private events (Binance spot after
	// userDataStream.subscribe). The worker then opens no PrivateStream: Order, Fill, Balances and
	// Positions arrive through the TradeSink.
	Events      bool
	Rate        []RateLimit // requests on the connection, all must hold
	Keepalive   Keepalive
	MaxLifetime time.Duration // when the venue drops the connection by itself; 0 = none
	ReadLimit   int64         // as SessionLimits.ReadLimit
}

// TradeSink receives what TradeStream.Decode finds. It is an AccountSink as well: Ready says the
// stream is live (logged on, and subscribed when it carries events), and the private events come
// through it on a connection that carries them. Pointers are valid only during the call (D302).
type TradeSink interface {
	AccountSink

	// Submitted answers Submit id: the venue's synchronous answer, or why it refused, classified as
	// Parse classifies a REST answer (*Reject, *RateLimited, ErrAuth).
	Submitted(id uint64, res *OrderResult, err error)
	// Canceled answers Cancel id; err is nil when the venue accepted it.
	Canceled(id uint64, err error)
	// Used is the usage the venue reports with an answer (Binance's rateLimits).
	Used(u []Used)
}

// Inflight tracks the requests a worker sent on a TradeStream until they are answered (D303). It is
// pure: the worker passes the time. A request not answered by its deadline, or still waiting when the
// connection drops, has an unknown outcome (ErrUnknownOutcome): the order may or may not be on the
// book, and the private stream or the next recon tells. Not safe for concurrent use.
type Inflight[T any] struct {
	next    uint64
	waiting map[uint64]waiting[T]
}

type waiting[T any] struct {
	v        T
	deadline time.Time
}

// Unanswered is a request whose answer did not come. Err wraps ErrUnknownOutcome.
type Unanswered[T any] struct {
	ID  uint64
	V   T
	Err error
}

// Add registers a request that must be answered by deadline, and returns its id: the id to encode
// it under. Ids start at 1 and are never reused.
func (f *Inflight[T]) Add(v T, deadline time.Time) uint64 {
	if f.waiting == nil {
		f.waiting = map[uint64]waiting[T]{}
	}
	f.next++
	f.waiting[f.next] = waiting[T]{v: v, deadline: deadline}
	return f.next
}

// Answer takes the request id off the list. ok is false for an id that is not waiting: never sent,
// already answered, or already given up on, so an answer arriving after its deadline is not mistaken
// for a live one. The worker treats such a late answer as news about the order, as it would a private
// event.
func (f *Inflight[T]) Answer(id uint64) (v T, ok bool) {
	w, ok := f.waiting[id]
	if ok {
		delete(f.waiting, id)
	}
	return w.v, ok
}

// Expire gives up on every request whose deadline is not after now, oldest id first.
func (f *Inflight[T]) Expire(now time.Time) []Unanswered[T] {
	var out []Unanswered[T]
	for id, w := range f.waiting {
		if !w.deadline.After(now) {
			out = append(out, Unanswered[T]{ID: id, V: w.v,
				Err: fmt.Errorf("exchange: request %d not answered by %s: %w", id, w.deadline.Format(time.RFC3339Nano), ErrUnknownOutcome)})
			delete(f.waiting, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Lost gives up on every waiting request: the connection dropped. Oldest id first.
func (f *Inflight[T]) Lost() []Unanswered[T] {
	out := make([]Unanswered[T], 0, len(f.waiting))
	for id, w := range f.waiting {
		out = append(out, Unanswered[T]{ID: id, V: w.v,
			Err: fmt.Errorf("exchange: request %d lost with its connection: %w", id, ErrUnknownOutcome)})
	}
	f.waiting = nil
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Waiting is how many requests wait for an answer.
func (f *Inflight[T]) Waiting() int { return len(f.waiting) }

// Next is the earliest deadline among the waiting requests; zero when none waits. The worker sleeps
// until then, or until a frame arrives.
func (f *Inflight[T]) Next() time.Time {
	var t time.Time
	for _, w := range f.waiting {
		if t.IsZero() || w.deadline.Before(t) {
			t = w.deadline
		}
	}
	return t
}
