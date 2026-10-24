package conformance

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/BullionBear/mftiko/exchange"
)

// FrameConn is one websocket the recorder reads and writes. The transport provides it (E3-02).
type FrameConn interface {
	Read(ctx context.Context) ([]byte, error)
	Write(ctx context.Context, frame []byte) error
	Close() error
}

// Dialer opens a websocket to url.
type Dialer func(ctx context.Context, url string) (FrameConn, error)

// Writer writes a session fixture, redacting as it goes: every secret given to it, and the values of
// headers and query parameters that carry credentials. A fixture is committed, so nothing secret
// may reach it, testnet keys included.
type Writer struct {
	mu      sync.Mutex
	out     io.Writer
	secrets []string
	now     func() time.Time
	err     error
}

// Redacted replaces what Writer removes.
const Redacted = "REDACTED"

// NewWriter starts a fixture on out with its meta record. now stamps records; nil is time.Now.
func NewWriter(out io.Writer, meta Record, now func() time.Time, secrets ...string) (*Writer, error) {
	if now == nil {
		now = time.Now
	}
	w := &Writer{out: out, now: now}
	for _, s := range secrets {
		if s != "" {
			w.secrets = append(w.secrets, s)
		}
	}
	meta.Kind = KindMeta
	meta.AtNs = now().UnixNano()
	return w, w.write(meta)
}

// Frame records a frame sent (KindOut) or received (KindIn).
func (w *Writer) Frame(kind string, b []byte) error {
	return w.write(FrameRecord(kind, w.now(), []byte(w.redact(string(b)))))
}

// HTTP records a REST exchange during the session: a book snapshot of channel ch, or with check set
// the snapshot the folded book is compared with at the end.
func (w *Writer) HTTP(ch string, check bool, req *exchange.HTTPRequest, resp *exchange.HTTPResponse) error {
	return w.write(Record{Kind: KindHTTP, AtNs: w.now().UnixNano(), Ch: ch, Check: check,
		Exchange: &Exchange{Request: w.request(req), Response: w.response(resp)}})
}

// Err is the first write error.
func (w *Writer) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

func (w *Writer) write(r Record) error {
	b, err := marshal(r, false)
	if err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	if _, err := w.out.Write(append(b, '\n')); err != nil {
		w.err = err
	}
	return w.err
}

// credentialName reports whether a header or query parameter name carries credentials.
func credentialName(name string) bool {
	n := strings.ToLower(name)
	for _, s := range []string{"key", "sign", "passphrase", "token", "secret", "auth"} {
		if strings.Contains(n, s) {
			return true
		}
	}
	return false
}

func (w *Writer) redact(s string) string {
	for _, sec := range w.secrets {
		s = strings.ReplaceAll(s, sec, Redacted)
	}
	return s
}

func (w *Writer) query(q string) string {
	parts := strings.Split(q, "&")
	for i, p := range parts {
		if k, _, ok := strings.Cut(p, "="); ok && credentialName(k) {
			parts[i] = k + "=" + Redacted
		}
	}
	return w.redact(strings.Join(parts, "&"))
}

func (w *Writer) headers(hs []exchange.Header) []exchange.Header {
	if hs == nil {
		return nil
	}
	out := make([]exchange.Header, len(hs))
	for i, h := range hs {
		out[i] = exchange.Header{Name: h.Name, Value: w.redact(h.Value)}
		if credentialName(h.Name) {
			out[i].Value = Redacted
		}
	}
	return out
}

func (w *Writer) request(q *exchange.HTTPRequest) *Request {
	if q == nil {
		return nil
	}
	clean := *q
	clean.Header = w.headers(q.Header)
	r := RequestOf(&clean)
	r.Query, r.Body = w.query(r.Query), w.redact(r.Body)
	return r
}

func (w *Writer) response(p *exchange.HTTPResponse) *Response {
	if p == nil {
		return nil
	}
	clean := *p
	clean.Header = w.headers(p.Header)
	clean.Body = []byte(w.redact(string(p.Body)))
	return ResponseOf(&clean)
}

// SaveExchange writes http/<name>.json for a REST case: the request as sent and the response, both
// redacted.
func SaveExchange(out io.Writer, req *exchange.HTTPRequest, resp *exchange.HTTPResponse, secrets ...string) error {
	w := &Writer{secrets: secrets}
	b, err := marshal(Exchange{Request: w.request(req), Response: w.response(resp)}, true)
	if err != nil {
		return err
	}
	_, err = out.Write(append(b, '\n'))
	return err
}

// Snapshot is a REST depth request to record during a market session (SnapshotREST books).
type Snapshot struct {
	Ch  string
	Req exchange.HTTPRequest
}

// MarketRecording is one market session to record.
type MarketRecording struct {
	Venue    string
	Market   exchange.Market
	Route    string
	Override string   // Endpoint's override; "" for the default host
	Channels []string // subscribed in the order given
	Dial     Dialer
	Out      io.Writer
	Duration time.Duration
	// Snapshots are taken after the first SnapshotAfter frames arrived, and again at the end as
	// check records. Doer sends them.
	Snapshots     []Snapshot
	SnapshotAfter int
	Doer          exchange.Doer
	Now           func() time.Time
	Note          string
}

// RecordMarket connects, subscribes, and writes every frame both ways to Out for Duration. The
// application ping the codec asks for is sent and recorded too.
func RecordMarket(ctx context.Context, m MarketRecording) error {
	if m.Market == nil || m.Dial == nil || m.Out == nil {
		return errors.New("conformance: MarketRecording needs Market, Dial and Out")
	}
	if len(m.Snapshots) > 0 && m.Doer == nil {
		return errors.New("conformance: snapshots need a Doer")
	}
	url, err := m.Market.Endpoint(m.Route, m.Override)
	if err != nil {
		return err
	}
	sess, err := m.Market.NewSession(m.Route)
	if err != nil {
		return err
	}
	w, err := NewWriter(m.Out, Record{Venue: m.Venue, Side: SideMkt, Route: m.Route, Note: m.Note}, m.Now)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, m.Duration)
	defer cancel()
	conn, err := m.Dial(ctx, url)
	if err != nil {
		return fmt.Errorf("conformance: dial %s: %w", url, err)
	}
	defer conn.Close()
	reqs, err := sess.Subscribe(m.Channels)
	if err != nil {
		return err
	}
	for _, q := range reqs {
		if err := send(ctx, conn, w, q.Frame); err != nil {
			return err
		}
	}
	frames := make(chan []byte, 1024)
	readErr := make(chan error, 1)
	rctx := ctx
	go func() {
		for {
			b, err := conn.Read(rctx)
			if err != nil {
				readErr <- err
				return
			}
			select {
			case frames <- b:
			case <-rctx.Done():
				return
			}
		}
	}()
	var ping <-chan time.Time
	if ka := sess.Limits().Keepalive; ka.Interval > 0 && sess.Ping() != nil {
		t := time.NewTicker(ka.Interval)
		defer t.Stop()
		ping = t.C
	}
	n, snapped := 0, false
	snapCtx := ctx
	snap := func(check bool) error {
		for _, s := range m.Snapshots {
			resp, err := m.Doer.Do(snapCtx, &s.Req)
			if err != nil {
				return fmt.Errorf("conformance: snapshot %s: %w", s.Ch, err)
			}
			if err := w.HTTP(s.Ch, check, &s.Req, resp); err != nil {
				return err
			}
		}
		return nil
	}
	for {
		select {
		case b := <-frames:
			if err := w.Frame(KindIn, b); err != nil {
				return err
			}
			n++
			if !snapped && n >= m.SnapshotAfter && len(m.Snapshots) > 0 {
				snapped = true
				if err := snap(false); err != nil {
					return err
				}
			}
		case <-ping:
			if err := send(ctx, conn, w, sess.Ping()); err != nil {
				return err
			}
		case err := <-readErr:
			if ctx.Err() == nil {
				return fmt.Errorf("conformance: read: %w", err)
			}
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
	}
	for done := false; !done; { // what arrived before the deadline
		select {
		case b := <-frames:
			if err := w.Frame(KindIn, b); err != nil {
				return err
			}
		default:
			done = true
		}
	}
	if snapped {
		end, cancelEnd := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancelEnd()
		snapCtx = end
		if err := snap(true); err != nil {
			return err
		}
	}
	return w.Err()
}

func send(ctx context.Context, conn FrameConn, w *Writer, b []byte) error {
	if err := w.Frame(KindOut, b); err != nil {
		return err
	}
	return conn.Write(ctx, b)
}

// PrivateRecording is one private session to record, with real (testnet) credentials. They are
// redacted from the fixture; the fixture is replayed later with the test credentials.
type PrivateRecording struct {
	Venue       string
	Account     exchange.Account
	Credentials exchange.Credentials // only to redact them
	Doer        exchange.Doer        // runs Prepare's call
	Dial        Dialer
	Out         io.Writer
	Duration    time.Duration
	Now         func() time.Time
	Note        string
}

// RecordPrivate prepares, dials, sends the login frames, and records every frame for Duration.
func RecordPrivate(ctx context.Context, p PrivateRecording) error {
	if p.Account == nil || p.Dial == nil || p.Out == nil {
		return errors.New("conformance: PrivateRecording needs Account, Dial and Out")
	}
	now := p.Now
	if now == nil {
		now = time.Now
	}
	sess, err := p.Account.NewStream()
	if err != nil {
		return err
	}
	w, err := NewWriter(p.Out, Record{Venue: p.Venue, Side: SidePriv, Note: p.Note}, now,
		p.Credentials.Key, p.Credentials.Secret, p.Credentials.Passphrase)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, p.Duration)
	defer cancel()
	token := ""
	if c, err := sess.Prepare(now()); err != nil {
		return err
	} else if c != nil {
		if p.Doer == nil {
			return errors.New("conformance: the stream needs a Prepare call and there is no Doer")
		}
		v, err := exchange.Run(ctx, p.Doer, *c)
		if err != nil {
			return fmt.Errorf("conformance: prepare: %w", err)
		}
		token = v
		w.secrets = append(w.secrets, token)
	}
	d, err := sess.Dial(now(), token)
	if err != nil {
		return err
	}
	conn, err := p.Dial(ctx, d.URL)
	if err != nil {
		return fmt.Errorf("conformance: dial: %w", err)
	}
	defer conn.Close()
	for _, f := range d.Frames {
		if err := send(ctx, conn, w, f); err != nil {
			return err
		}
	}
	for ctx.Err() == nil {
		b, err := conn.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			return fmt.Errorf("conformance: read: %w", err)
		}
		if err := w.Frame(KindIn, b); err != nil {
			return err
		}
	}
	return w.Err()
}

// TradeRecording is one trade session to record (E03-D003), with real (testnet) credentials, redacted
// from the fixture. Orders are sent once the first frame arrives (the logon's answer), under ids
// 1, 2, ... in order; a TradeSample may be a Cancel of an order sent earlier in the session.
type TradeRecording struct {
	Venue       string
	Stream      exchange.TradeStream
	Credentials exchange.Credentials // only to redact them
	Dial        Dialer
	Out         io.Writer
	Duration    time.Duration
	Requests    []TradeSample
	Gap         time.Duration // between requests; 0 sends them back to back
	Now         func() time.Time
	Note        string
}

// RecordTrade connects, sends the logon, the requests, and records every frame both ways for
// Duration.
func RecordTrade(ctx context.Context, p TradeRecording) error {
	if p.Stream == nil || p.Dial == nil || p.Out == nil {
		return errors.New("conformance: TradeRecording needs Stream, Dial and Out")
	}
	now := p.Now
	if now == nil {
		now = time.Now
	}
	w, err := NewWriter(p.Out, Record{Venue: p.Venue, Side: SideTrade, Note: p.Note}, now,
		p.Credentials.Key, p.Credentials.Secret, p.Credentials.Passphrase)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, p.Duration)
	defer cancel()
	d, err := p.Stream.Dial(now())
	if err != nil {
		return err
	}
	conn, err := p.Dial(ctx, d.URL)
	if err != nil {
		return fmt.Errorf("conformance: dial: %w", err)
	}
	defer conn.Close()
	for _, f := range d.Frames {
		if err := send(ctx, conn, w, f); err != nil {
			return err
		}
	}
	sent := false
	for ctx.Err() == nil {
		b, err := conn.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			return fmt.Errorf("conformance: read: %w", err)
		}
		if err := w.Frame(KindIn, b); err != nil {
			return err
		}
		if sent {
			continue
		}
		sent = true
		for i, q := range p.Requests {
			id := uint64(i + 1)
			var f []byte
			switch {
			case q.Submit != nil:
				f, err = p.Stream.Submit(now(), id, q.Submit)
			case q.Cancel != nil:
				f, err = p.Stream.Cancel(now(), id, *q.Cancel)
			default:
				err = fmt.Errorf("conformance: request %q is neither a Submit nor a Cancel", q.Name)
			}
			if err != nil {
				return err
			}
			if err := send(ctx, conn, w, f); err != nil {
				return err
			}
			if p.Gap > 0 {
				select {
				case <-time.After(p.Gap):
				case <-ctx.Done():
				}
			}
		}
	}
	return w.Err()
}
