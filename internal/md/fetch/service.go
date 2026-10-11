package fetch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/internal/platform/natsx"
)

const (
	// SubjectPrefix is md.fetch.{venue}. Venue is the human-readable name, the
	// same token sym.changed uses (D418).
	SubjectPrefix = "md.fetch."

	// MaxKlines is the most closed candles one reply carries (D419).
	MaxKlines = 5000

	// CodeInvalid, CodeUnsupported, CodeRateLimited and CodeVenueError are the
	// business errors carried in FetchReply.error. A success leaves error empty.
	// These are not natsx error replies: the proto puts them in the message.
	CodeInvalid     = "invalid"
	CodeUnsupported = "unsupported"
	CodeRateLimited = "rate_limited"
	CodeVenueError  = "venue_error"

	// maxReplyBytes is one byte under NATS's default 1 MiB payload, so a
	// published reply is accepted (D419). A larger reply is invalid rather
	// than sent.
	maxReplyBytes = 1<<20 - 1

	// DefaultRateHold is how long a venue stays closed after a 429 or 418 that
	// carries neither Retry-After nor a bucket ResetAt (D420).
	DefaultRateHold = time.Minute
)

// Subject is the request subject for one venue.
func Subject(venue string) string { return SubjectPrefix + venue }

// Options tunes New. The zero RateLimit does not refuse early (D420). A venue
// that answers *RateLimited is still rate_limited, and stays closed until the
// venue's reset time.
type Options struct {
	Log zerolog.Logger
	// RateLimit is the reported usage at which the next call to that venue is
	// refused. Usage is the exchange's number (PublicREST.Usage), which already
	// includes every other client on the same IP, including a conn worker on
	// this host. Zero disables the pre-check. Negative is rejected. A bucket
	// stops counting at its ResetAt.
	RateLimit int
	// RateHold is the fallback wait after a 429 or 418 that has no Retry-After
	// and no ResetAt. Zero uses DefaultRateHold. Negative is rejected.
	RateHold time.Duration
	// Now is the clock. Nil uses time.Now. Tests pass a fake.
	Now func() time.Time
}

// Service answers md.fetch.{venue} for the venues it was built with.
type Service struct {
	venues map[string]exchange.PublicREST
	order  []string
	res    symbol.Resolver
	doer   exchange.Doer
	limit  int
	hold   time.Duration
	now    func() time.Time
	log    zerolog.Logger

	mu   sync.Mutex
	used map[string]map[string]sample
	ban  map[string]time.Time
}

// sample is one bucket's last reported count and when it returns to zero.
// A zero resetAt does not expire.
type sample struct {
	n       int
	resetAt time.Time
}

// New serves venues, in that order, through res and doer. res is the active
// table (D422). doer sends the Calls; production uses the fasthttp Doer.
func New(venues []Venue, res symbol.Resolver, doer exchange.Doer, opt Options) (*Service, error) {
	if res == nil {
		return nil, errors.New("fetch: nil resolver")
	}
	if doer == nil {
		return nil, errors.New("fetch: nil doer")
	}
	if opt.RateLimit < 0 {
		return nil, errors.New("fetch: negative rate limit")
	}
	hold := opt.RateHold
	if hold == 0 {
		hold = DefaultRateHold
	}
	if hold < 0 {
		return nil, errors.New("fetch: negative rate hold")
	}
	if len(venues) == 0 {
		return nil, errors.New("fetch: no venues")
	}
	now := opt.Now
	if now == nil {
		now = time.Now
	}
	s := &Service{
		venues: map[string]exchange.PublicREST{},
		res:    res,
		doer:   doer,
		limit:  opt.RateLimit,
		hold:   hold,
		now:    now,
		log:    opt.Log,
		used:   map[string]map[string]sample{},
		ban:    map[string]time.Time{},
	}
	seen := map[string]struct{}{}
	for _, v := range venues {
		if v.Name == "" || v.REST == nil {
			return nil, errors.New("fetch: venue is missing a name or a reader")
		}
		if _, dup := seen[v.Name]; dup {
			return nil, fmt.Errorf("fetch: duplicate venue %s", v.Name)
		}
		seen[v.Name] = struct{}{}
		s.order = append(s.order, v.Name)
		s.venues[v.Name] = v.REST
	}
	return s, nil
}

// Serve joins the queue group of each configured venue. The queue name is the
// subject, which is what natsx does when the queue is empty (D417, D418).
// The handlers stop when ctx is cancelled. Serve returns once every interest
// is registered.
func (s *Service) Serve(ctx context.Context, c *natsx.Conn) ([]*natsx.Subscription, error) {
	if s == nil {
		return nil, errors.New("fetch: nil service")
	}
	if c == nil {
		return nil, errors.New("fetch: nil connection")
	}
	var subs []*natsx.Subscription
	for _, name := range s.order {
		venue := name
		sub, err := c.Serve(ctx, Subject(venue), "", func() proto.Message {
			return &mdv1.FetchRequest{}
		}, func(ctx context.Context, m proto.Message) (proto.Message, error) {
			return s.serve(ctx, venue, m)
		})
		if err != nil {
			for _, prev := range subs {
				_ = prev.Unsubscribe()
			}
			return nil, err
		}
		subs = append(subs, sub)
	}
	return subs, nil
}

func (s *Service) serve(ctx context.Context, venue string, m proto.Message) (proto.Message, error) {
	req, ok := m.(*mdv1.FetchRequest)
	if !ok || req == nil || req.GetReq() == nil {
		return reply(CodeInvalid, "request is empty"), nil
	}
	var (
		out *mdv1.FetchReply
		err error
	)
	switch r := req.GetReq().(type) {
	case *mdv1.FetchRequest_Klines:
		out, err = s.klines(ctx, venue, r.Klines)
	case *mdv1.FetchRequest_OpenInterest:
		out, err = s.openInterest(ctx, venue, r.OpenInterest)
	default:
		out = reply(CodeInvalid, "request is empty")
	}
	if err != nil {
		s.log.Error().Err(err).Str("venue", venue).Msg("fetch")
		return nil, err
	}
	if out.GetError() == CodeVenueError {
		s.log.Error().Str("venue", venue).Str("reason", out.GetReason()).Msg("venue")
	}
	return out, nil
}

func (s *Service) klines(ctx context.Context, venue string, req *mdv1.FetchKlines) (*mdv1.FetchReply, error) {
	if req == nil {
		return reply(CodeInvalid, "request is empty"), nil
	}
	limit := req.GetLimit()
	if limit < 1 || limit > MaxKlines {
		return reply(CodeInvalid, fmt.Sprintf("limit %d is not between 1 and %d", limit, MaxKlines)), nil
	}
	inst, bad, err := s.instrument(ctx, venue, req.GetTicker())
	if err != nil || bad != nil {
		return bad, err
	}
	rest := s.venues[venue]
	rows, err := pageKlines(ctx, s, venue, rest, inst, req.GetInterval(), int(limit))
	if err != nil {
		return s.fail(err), nil
	}
	msg := &mdv1.Klines{Klines: make([]*mdv1.Kline, 0, len(rows))}
	for _, row := range rows {
		msg.Klines = append(msg.Klines, klineMessage(row))
	}
	out := &mdv1.FetchReply{Result: &mdv1.FetchReply_Klines{Klines: msg}}
	if n := proto.Size(out); n > maxReplyBytes {
		return reply(CodeInvalid, fmt.Sprintf("reply is %d bytes, above %d", n, maxReplyBytes)), nil
	}
	return out, nil
}

func (s *Service) openInterest(ctx context.Context, venue string, req *mdv1.FetchOpenInterest) (*mdv1.FetchReply, error) {
	if req == nil {
		return reply(CodeInvalid, "request is empty"), nil
	}
	inst, bad, err := s.instrument(ctx, venue, req.GetTicker())
	if err != nil || bad != nil {
		return bad, err
	}
	call, err := s.venues[venue].OpenInterest(inst)
	if err != nil {
		return s.fail(err), nil
	}
	oi, err := callOne(ctx, s, venue, s.venues[venue], call)
	if err != nil {
		return s.fail(err), nil
	}
	if oi == nil {
		return reply(CodeVenueError, "open interest is empty"), nil
	}
	msg := &mdv1.OpenInterest{
		Meta:         &mdv1.Meta{ExchTsNs: oi.ExchTsNs},
		OpenInterest: oi.OpenInterest,
	}
	if decimal.Has(oi.OpenInterestValue) {
		v := oi.OpenInterestValue
		msg.OpenInterestValue = &v
	}
	out := &mdv1.FetchReply{Result: &mdv1.FetchReply_OpenInterest{OpenInterest: msg}}
	if n := proto.Size(out); n > maxReplyBytes {
		return reply(CodeInvalid, fmt.Sprintf("reply is %d bytes, above %d", n, maxReplyBytes)), nil
	}
	return out, nil
}

// pageKlines walks backward from the latest closed candle (D419). The first
// call asks for min(limit, KlinePage()). Each later call asks for a full page
// ending at the earliest open time already seen, which is Klines(endNs).
// A venue whose end is inclusive returns that candle again; it is dropped.
// A page that adds nothing steps one nanosecond earlier, once, so a page of
// one still moves, and endNs stays non-zero: zero means the latest. A short
// page or an open time that does not move ends the walk. A failure discards
// the partial page: the reply is the error, not a short success.
func pageKlines(ctx context.Context, s *Service, venue string, rest exchange.PublicREST, inst *symbol.Info, interval string, limit int) ([]exchange.Kline, error) {
	page := rest.KlinePage()
	var (
		got   []exchange.Kline
		seen  = map[int64]struct{}{}
		endNs int64
	)
	// Two calls per candle covers an inclusive page of one, plus the final empty page.
	// A cursor that never moves stops here instead of walking forever.
	stopped := false
	for pages := 0; len(got) < limit && pages <= limit*2+2; pages++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ask := limit - len(got)
		if page > 0 && (ask > page || endNs != 0) {
			ask = page
		}
		call, err := rest.Klines(inst, interval, endNs, ask)
		if err != nil {
			return nil, err
		}
		rows, err := callOne(ctx, s, venue, rest, call)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			stopped = true
			break
		}
		earliest := rows[0].OpenTimeNs
		fresh := make([]exchange.Kline, 0, len(rows))
		for _, row := range rows {
			if row.OpenTimeNs < earliest {
				earliest = row.OpenTimeNs
			}
			if !row.Closed {
				continue
			}
			if _, ok := seen[row.OpenTimeNs]; ok {
				continue
			}
			seen[row.OpenTimeNs] = struct{}{}
			fresh = append(fresh, row)
		}
		got = append(fresh, got...)
		if len(got) >= limit {
			stopped = true
			break
		}
		if len(fresh) == 0 {
			next := earliest - 1
			if next > 0 && next != endNs {
				endNs = next
				continue
			}
			stopped = true
			break
		}
		if earliest <= 0 || (endNs != 0 && earliest >= endNs) || len(rows) < ask {
			stopped = true
			break
		}
		endNs = earliest
	}
	if !stopped {
		return nil, errors.New("fetch: kline page did not advance")
	}
	if len(got) > limit {
		got = got[len(got)-limit:]
	}
	return got, nil
}

// instrument resolves ticker on the active table. A terminal listing is
// ErrNotFound and is invalid, the same as a ticker the table has never held
// (D422). A venue that does not match the subject is invalid before the lookup.
func (s *Service) instrument(ctx context.Context, venue, ticker string) (*symbol.Info, *mdv1.FetchReply, error) {
	t, err := symbol.Parse(ticker)
	if err != nil {
		return nil, reply(CodeInvalid, fmt.Sprintf("ticker %q is not a universal ticker", ticker)), nil
	}
	if t.Venue != venue {
		return nil, reply(CodeInvalid, fmt.Sprintf("ticker %s venue %s does not match subject venue %s", t, t.Venue, venue)), nil
	}
	info, err := s.res.Get(ctx, t, false)
	if errors.Is(err, symbol.ErrNotFound) {
		return nil, reply(CodeInvalid, fmt.Sprintf("ticker %s is not in the active table", t)), nil
	}
	if err != nil {
		return nil, nil, err
	}
	if info == nil {
		return nil, reply(CodeInvalid, fmt.Sprintf("ticker %s is not in the active table", t)), nil
	}
	return info, nil, nil
}

func (s *Service) fail(err error) *mdv1.FetchReply {
	var lim *limited
	var rl *exchange.RateLimited
	switch {
	case errors.As(err, &lim):
		return reply(CodeRateLimited, lim.reason)
	case errors.As(err, &rl):
		return reply(CodeRateLimited, "venue rate limited")
	case errors.Is(err, exchange.ErrUnsupported):
		return reply(CodeUnsupported, err.Error())
	default:
		return reply(CodeVenueError, err.Error())
	}
}

func reply(code, reason string) *mdv1.FetchReply {
	return &mdv1.FetchReply{Error: code, Reason: reason}
}

func klineMessage(row exchange.Kline) *mdv1.Kline {
	msg := &mdv1.Kline{
		Meta:       &mdv1.Meta{ExchTsNs: row.ExchTsNs},
		Interval:   row.Interval,
		OpenTimeNs: row.OpenTimeNs,
		Open:       row.Open,
		High:       row.High,
		Low:        row.Low,
		Close:      row.Close,
		Volume:     row.Volume,
		Closed:     true,
	}
	if decimal.Has(row.QuoteVolume) {
		v := row.QuoteVolume
		msg.QuoteVolume = &v
	}
	return msg
}

// limited is the pre-check refusing a call that would go out (D420).
type limited struct{ reason string }

func (e *limited) Error() string {
	if e == nil {
		return "rate limited"
	}
	return e.reason
}

// callOne sends one Call. It refuses first when this venue is held or a bucket
// that has not yet reset is already at the threshold, and records Usage from
// the response even when Parse fails. A 429 or 418 holds the venue until
// Retry-After, or until ResetAt, and sends nothing in between. It does not
// retry and it does not probe.
func callOne[T any](ctx context.Context, s *Service, venue string, rest exchange.PublicREST, c exchange.Call[T]) (T, error) {
	var zero T
	if reason, ok := s.over(venue); ok {
		return zero, &limited{reason: reason}
	}
	if c.Parse == nil {
		return zero, errors.New("fetch: nil parse")
	}
	parse := c.Parse
	c.Parse = func(resp *exchange.HTTPResponse) (T, error) {
		s.record(venue, rest.Usage(resp, s.now()))
		return parse(resp)
	}
	out, err := exchange.Run(ctx, s.doer, c)
	s.arm(venue, err)
	return out, err
}

func (s *Service) record(venue string, used []exchange.Used) {
	if len(used) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.used[venue]
	if m == nil {
		m = map[string]sample{}
		s.used[venue] = m
	}
	for _, u := range used {
		if u.Bucket == "" {
			continue
		}
		m[u.Bucket] = sample{n: u.N, resetAt: u.ResetAt}
	}
}

// arm closes the venue after a 429 or 418. Retry-After wins. Without it, the
// hold lasts until the latest reported ResetAt. With neither, it lasts RateHold.
// Nothing is sent until that time, including after a 418.
func (s *Service) arm(venue string, err error) {
	var rl *exchange.RateLimited
	if !errors.As(err, &rl) || rl == nil {
		return
	}
	now := s.now()
	until := time.Time{}
	fromRetry := false
	if rl.RetryAfterMs > 0 {
		until = now.Add(time.Duration(rl.RetryAfterMs) * time.Millisecond)
		fromRetry = true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !fromRetry {
		var saw bool
		for _, sample := range s.used[venue] {
			if sample.resetAt.IsZero() {
				continue
			}
			saw = true
			if sample.resetAt.After(until) {
				until = sample.resetAt
			}
		}
		if !saw {
			until = now.Add(s.hold)
		}
	}
	if prev, ok := s.ban[venue]; ok && !until.After(prev) {
		return
	}
	s.ban[venue] = until
}

func (s *Service) over(venue string) (string, bool) {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if until, ok := s.ban[venue]; ok {
		if now.Before(until) {
			return fmt.Sprintf("venue is held until %s", until.UTC().Format(time.RFC3339Nano)), true
		}
		delete(s.ban, venue)
	}
	if s.limit <= 0 {
		return "", false
	}
	m := s.used[venue]
	for bucket, sample := range m {
		if !sample.resetAt.IsZero() && !now.Before(sample.resetAt) {
			delete(m, bucket)
			continue
		}
		if sample.n >= s.limit {
			return fmt.Sprintf("bucket %q used %d has reached the threshold %d", bucket, sample.n, s.limit), true
		}
	}
	return "", false
}
