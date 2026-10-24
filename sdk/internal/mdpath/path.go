package mdpath

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	stsctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/stsctl/v1"
	"github.com/BullionBear/mftiko/contract/wire/envelope"
	"github.com/BullionBear/mftiko/sdk/internal/dispatch"
)

const flushTimeout = 5 * time.Second

var (
	// ErrSelector means the snapshot contains selectors. Phase 1 does not resolve them (E08-D046, E08-D022).
	ErrSelector = errors.New("mdpath: selectors are not supported")
	// ErrNoFeed means this session has no subscribed feed for that instrument and topic.
	ErrNoFeed = errors.New("mdpath: the session has no such feed")
	// ErrUnknownUID means the resolver does not know the instrument.
	ErrUnknownUID = errors.New("mdpath: the instrument is not known to this session")
	// ErrBadSubject means the feed subject has no md.v1.f. prefix, so md.view cannot be named.
	ErrBadSubject = errors.New("mdpath: feed subject has no md.v1.f. prefix")
	// ErrStopping means Close has been called.
	ErrStopping = errors.New("mdpath: the session is stopping")
)

// Tap is the dispatcher's inlet for market data. Pass its Feed and Book to dispatch.New,
// then pass the same Tap to Open.
type Tap struct {
	p atomic.Pointer[Path]
}

// Feed decodes one market-data message the prehook kept. The bytes alias the ring and are
// valid only until Feed returns.
func (t *Tap) Feed(msg []byte) {
	if t == nil {
		return
	}
	if p := t.p.Load(); p != nil {
		p.decodeFeed(msg)
	}
}

// Book receives one orderbook_delta message. The bytes alias the ring and are valid only
// until Book returns. The age prehook does not apply to this path (E08-D003).
func (t *Tap) Book(msg []byte) {
	if t == nil {
		return
	}
	if p := t.p.Load(); p != nil {
		p.decodeBook(msg)
	}
}

// Config opens a Path. NC is not closed by the path. Disp is the session dispatcher:
// live messages are pushed onto it, and read replies use its TD buffer. Tap must be the
// inlet whose methods were passed to that dispatcher. RecvWindow is InitPayload.recv_window_ms
// and must be positive (E08-D048).
//
// Resolve returns the ticker and venue of a UID this session has already resolved. It reads
// memory; mdpath does not call SYM (E08-D024). An unknown UID returns an error. Nil is allowed
// until Tape, BookSnapshot, or a fetch is called.
//
// Book receives orderbook_delta on the dispatcher. The bytes alias the ring and are valid
// only until Book returns. Nil discards the message after it has been taken off the ring.
// Log nil discards lines.
type Config struct {
	NC         *nats.Conn
	RecvWindow time.Duration
	Disp       *dispatch.Disp
	Tap        *Tap
	Resolve    func(uid uint32) (ticker, venue string, err error)
	Hooks      Hooks
	Book       func(msg []byte)
	Log        func(string)
}

// Path is one session's market-data path. Close drops the subscriptions and waits for
// in-flight md.view and md.fetch calls. Do not call Close from inside one of those callbacks:
// Close waits for the call that is running the callback.
type Path struct {
	op       sync.Mutex
	mu       sync.Mutex
	nc       *nats.Conn
	recv     time.Duration
	resolve  func(uint32) (string, string, error)
	hooks    Hooks
	book     func([]byte)
	logFn    func(string)
	pushMD   func([]byte) error
	pushBook func([]byte) error
	pushTD   func(func()) error
	bySub    map[string]*feed
	byKey    map[string]*feed
	epoch    int64
	seq      uint64
	have     bool
	closed   bool
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// feed is one subscribed subject. topic, key, and view are fixed for the life of the
// subscription. live is false once the subject has left the set.
type feed struct {
	key     string
	topic   string
	subject string
	view    string
	sub     *nats.Subscription
	live    atomic.Bool
}

// Open checks the session and registers the path on Tap. It does not subscribe; call Apply
// with the start bindings.
func Open(cfg Config) (*Path, error) {
	if cfg.NC == nil {
		return nil, errors.New("mdpath: nil nats conn")
	}
	if cfg.RecvWindow <= 0 {
		return nil, errors.New("mdpath: recv window must be positive")
	}
	if cfg.Disp == nil {
		return nil, errors.New("mdpath: nil dispatcher")
	}
	if cfg.Tap == nil {
		return nil, errors.New("mdpath: nil tap")
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &Path{
		nc:       cfg.NC,
		recv:     cfg.RecvWindow,
		resolve:  cfg.Resolve,
		hooks:    cfg.Hooks,
		book:     cfg.Book,
		logFn:    cfg.Log,
		pushMD:   cfg.Disp.PushMD,
		pushBook: cfg.Disp.PushBook,
		pushTD:   cfg.Disp.PushTD,
		bySub:    map[string]*feed{},
		byKey:    map[string]*feed{},
		ctx:      ctx,
		cancel:   cancel,
	}
	if p.logFn == nil {
		p.logFn = func(string) {}
	}
	cfg.Tap.p.Store(p)
	return p, nil
}

// Close unsubscribes every feed and cancels in-flight reads. The NATS connection stays open.
func (p *Path) Close() {
	p.op.Lock()
	defer p.op.Unlock()
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	feeds := p.bySub
	p.bySub = map[string]*feed{}
	p.byKey = map[string]*feed{}
	p.mu.Unlock()
	p.cancel()
	for _, f := range feeds {
		f.live.Store(false)
		if f.sub != nil {
			_ = f.sub.Unsubscribe()
		}
	}
	p.wg.Wait()
}

// Apply installs a bindings snapshot (E08-D046). A snapshot with selectors returns ErrSelector
// and changes nothing, whatever its Version. A snapshot without selectors is installed only
// when its Version is strictly newer than the one already applied; the first one is installed
// even if its Version is zero. The subscribed set is the subjects listed on that snapshot.
func (p *Path) Apply(b *stsctlv1.Bindings) error {
	if b == nil {
		return errors.New("mdpath: nil bindings")
	}
	if len(b.GetSelectors()) > 0 {
		return ErrSelector
	}
	next, err := indexFeeds(b.GetFeeds())
	if err != nil {
		return err
	}
	p.op.Lock()
	defer p.op.Unlock()

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrStopping
	}
	if p.have && !newer(p.epoch, p.seq, b.GetVersion()) {
		p.mu.Unlock()
		return nil
	}
	cur := p.bySub
	p.mu.Unlock()

	add, drop, merged := classify(cur, next)
	if err := p.subscribe(add); err != nil {
		return err
	}
	if len(add) > 0 {
		if err := p.nc.FlushTimeout(flushTimeout); err != nil {
			p.abandon(add)
			return err
		}
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		p.abandon(add)
		return ErrStopping
	}
	for _, f := range drop {
		f.live.Store(false)
	}
	p.bySub = merged
	p.byKey = keysOf(merged)
	p.epoch, p.seq = 0, 0
	if v := b.GetVersion(); v != nil {
		p.epoch, p.seq = v.GetEpoch(), v.GetSeq()
	}
	p.have = true
	p.mu.Unlock()

	for _, f := range drop {
		if f.sub != nil {
			_ = f.sub.Unsubscribe()
		}
	}
	return nil
}

// Applied is the epoch and seq of the snapshot last installed. ok is false before the first Apply.
func (p *Path) Applied() (epoch int64, seq uint64, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.epoch, p.seq, p.have
}

// subjects is the subscribed subject set, sorted. Tests use it to check an exact diff.
func (p *Path) subjects() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.bySub))
	for s := range p.bySub {
		out = append(out, s)
	}
	slices.Sort(out)
	return out
}

func (p *Path) subscribe(list []*feed) error {
	var up []*feed
	for _, f := range list {
		sub, err := p.nc.Subscribe(f.subject, func(m *nats.Msg) {
			p.onMsg(f, m.Data)
		})
		if err != nil {
			p.abandon(up)
			return err
		}
		f.sub = sub
		up = append(up, f)
	}
	return nil
}

func (p *Path) abandon(list []*feed) {
	for _, f := range list {
		f.live.Store(false)
		if f.sub != nil {
			_ = f.sub.Unsubscribe()
			f.sub = nil
		}
	}
}

// onMsg runs on the NATS receive goroutine. It does not decode a payload.
func (p *Path) onMsg(f *feed, data []byte) {
	if !f.live.Load() {
		return
	}
	if _, err := envelope.MDTsNs(data); err != nil {
		// The dispatcher drops it, counts QueueStat.dropped, and logs (E08-D049).
		_ = p.pushMD(data)
		return
	}
	if f.topic == topicBook {
		_ = p.pushBook(data)
		return
	}
	if !wants(p.hooks, f.topic) {
		return
	}
	_ = p.pushMD(data)
}

func (p *Path) decodeFeed(msg []byte) {
	mdTs, epoch, seq, _, payload, err := envelope.Parse(msg)
	if err != nil {
		p.logFn("md envelope rejected: " + err.Error())
		return
	}
	ev := &mdv1.FeedEvent{}
	if err := proto.Unmarshal(payload, ev); err != nil {
		p.logFn("md payload rejected: " + err.Error())
		return
	}
	base := MD{UID: ev.GetUid(), MdTsNs: mdTs, ConnEpoch: epoch, Seq: seq}
	switch e := ev.GetEvent().(type) {
	case *mdv1.FeedEvent_BestQuote:
		if p.hooks.BestQuote != nil {
			p.hooks.BestQuote(BestQuote{MD: base, Data: e.BestQuote})
		}
	case *mdv1.FeedEvent_Trade:
		if p.hooks.Trade != nil {
			p.hooks.Trade(Trade{MD: base, Data: e.Trade})
		}
	case *mdv1.FeedEvent_AggTrade:
		if p.hooks.AggTrade != nil {
			p.hooks.AggTrade(AggTrade{MD: base, Data: e.AggTrade})
		}
	case *mdv1.FeedEvent_Kline:
		if p.hooks.Kline != nil {
			p.hooks.Kline(Kline{MD: base, Data: e.Kline})
		}
	case *mdv1.FeedEvent_Ticker:
		if p.hooks.Ticker != nil {
			p.hooks.Ticker(Ticker{MD: base, Data: e.Ticker})
		}
	case *mdv1.FeedEvent_Liquidation:
		if p.hooks.Liquidation != nil {
			p.hooks.Liquidation(Liquidation{MD: base, Data: e.Liquidation})
		}
	case *mdv1.FeedEvent_FundingRate:
		if p.hooks.FundingRate != nil {
			p.hooks.FundingRate(FundingRate{MD: base, Data: e.FundingRate})
		}
	case *mdv1.FeedEvent_OpenInterest:
		if p.hooks.OpenInterest != nil {
			p.hooks.OpenInterest(OpenInterest{MD: base, Data: e.OpenInterest})
		}
	case *mdv1.FeedEvent_Greeks:
		if p.hooks.Greeks != nil {
			p.hooks.Greeks(Greeks{MD: base, Data: e.Greeks})
		}
	case *mdv1.FeedEvent_MarkPrice:
		if p.hooks.MarkPrice != nil {
			p.hooks.MarkPrice(MarkPrice{MD: base, Data: e.MarkPrice})
		}
	default:
		// orderbook_delta is delivered on the book path. An empty oneof is not an event.
	}
}

func (p *Path) decodeBook(msg []byte) {
	if p.book != nil {
		p.book(msg)
	}
}

func (p *Path) enqueue(fn func()) {
	if fn == nil {
		return
	}
	if err := p.pushTD(fn); err != nil {
		p.logFn("md reply dropped: " + err.Error())
	}
}

func (p *Path) track(fn func()) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		fn()
	}()
}

func (p *Path) viewOf(uid uint32, topic string) (string, error) {
	if topic == "" {
		return "", ErrNoFeed
	}
	ticker, err := p.ticker(uid)
	if err != nil {
		return "", err
	}
	key := topic + "." + ticker
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return "", ErrStopping
	}
	f := p.byKey[key]
	if f == nil {
		return "", ErrNoFeed
	}
	if f.view == "" {
		return "", ErrBadSubject
	}
	return f.view, nil
}

func (p *Path) venueOf(uid uint32) (string, error) {
	if p.resolve == nil {
		return "", errors.New("mdpath: no instrument resolver")
	}
	_, venue, err := p.resolve(uid)
	if err != nil {
		return "", err
	}
	subject, err := fetchSubject(venue)
	if err != nil {
		return "", err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return "", ErrStopping
	}
	return subject, nil
}

func (p *Path) ticker(uid uint32) (string, error) {
	if p.resolve == nil {
		return "", errors.New("mdpath: no instrument resolver")
	}
	ticker, _, err := p.resolve(uid)
	if err != nil {
		return "", err
	}
	if ticker == "" {
		return "", ErrUnknownUID
	}
	return ticker, nil
}

func (p *Path) request(subject string, body []byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(p.ctx, p.recv)
	defer cancel()
	msg, err := p.nc.RequestWithContext(ctx, subject, body)
	if err != nil {
		return nil, err
	}
	return msg.Data, nil
}

func wrap(kind string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("mdpath: %s: %w", kind, err)
}
