package conn

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	mdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/mdctl/v1"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/internal/platform/c4"
)

// Contract is the C5 contract this worker speaks (PROTOCOL §5.2, §6.2).
const Contract = "mdctl.v1"

const exitInit = c4.ExitInit // PROTOCOL §5.3, INIT_FAILED

// ServeConfig is the process around one C4 connection. Lookup and Publish come from the
// binary; tests pass fakes. The venue, route, url and starting epoch come from Init.
type ServeConfig struct {
	// Lookup returns the venue's public market. The bool is false when the venue is unknown.
	Lookup func(venue string) (exchange.Market, bool)
	// Publish sends one md.f message. It runs on the websocket read, so it must return promptly.
	Publish func(subject string, msg []byte) error
	// Now is the clock written into md_ts_ns. Nil means time.Now.
	Now func() time.Time
	// RetryAfter and RetryMax are Conn's dial backoff. Zero means 1s growing to 30s.
	RetryAfter time.Duration
	RetryMax   time.Duration
	// REST is the venue's public REST, used for SnapshotREST depth. Nil leaves depth without a
	// reader. Doer sends it. RateHold is the 429/418 fallback (D424): zero means one minute,
	// negative fails init. It is a process flag, not an Init field.
	REST     func(venue string) (exchange.PublicREST, bool)
	Doer     exchange.Doer
	RateHold time.Duration
	// Symbols loads this venue's active table before Init completes. Nil leaves exch_ticker and
	// price_tick on the assignment. The returned value is closed when it has a Close method.
	Symbols func(ctx context.Context, venue string) (Instruments, error)
	// Subscribe registers one md.view responder. Nil leaves the worker without md.view.
	// The handler runs off the websocket read. The returned function removes the interest.
	Subscribe func(subject string, handler func(reply string, req []byte)) (func(), error)
	// Log is optional. Nil discards.
	Log *zerolog.Logger
	// Release is Hello.release. Empty leaves it empty.
	Release string
}

// Serve speaks C4 on rw, which is the worker's end of fd 3, and runs one connection until Stop,
// EOF, or ctx (PROTOCOL §5). It closes rw when it returns. The lifecycle is internal/platform/c4's.
//
// Init.payload is mdctl.InitPayload; its conn_epoch is the last number reported, and the dial uses
// one more. That number is written as a Report before Dial, so a peer that reads it before
// accepting the socket has it before the connection is up. Live.payload is an mdctl.Desired;
// Desired only arrives as Live, never in Init.
//
// The int is an exit code: 0 done, 10 init failed, 20 the connection loop failed, 1 the control
// connection could not be spoken.
func Serve(ctx context.Context, rw io.ReadWriteCloser, cfg ServeConfig) int {
	p := &plane{cfg: cfg, log: zerolog.Nop()}
	if cfg.Log != nil {
		p.log = *cfg.Log
	}
	defer func() {
		if p.closeSymbols != nil {
			p.closeSymbols()
		}
	}()
	return c4.Serve(ctx, rw, c4.Config{Kind: "conn", Contract: Contract, Release: cfg.Release, Log: cfg.Log}, p)
}

// plane is the MD part of one C4 session.
type plane struct {
	cfg          ServeConfig
	log          zerolog.Logger
	sess         *c4.Session
	closeSymbols func()

	mu     sync.Mutex
	worker *Conn
}

func (s *plane) Init(ctx context.Context, sess *c4.Session, payload []byte) error {
	s.sess = sess
	p := &mdctlv1.InitPayload{}
	if err := proto.Unmarshal(payload, p); err != nil {
		return errors.New("init payload")
	}
	if p.GetVenue() == "" || p.GetRoute() == "" || p.GetMdInstance() == "" {
		return errors.New("venue, route and md_instance are required")
	}
	if s.cfg.RateHold < 0 {
		return errors.New("rate hold is negative")
	}
	if s.cfg.Lookup == nil || s.cfg.Publish == nil {
		return errors.New("lookup and publish are required")
	}
	market, found := s.cfg.Lookup(p.GetVenue())
	if !found || market == nil {
		return errors.New("unknown venue " + p.GetVenue())
	}
	var rest exchange.PublicREST
	if s.cfg.REST != nil {
		rest, _ = s.cfg.REST(p.GetVenue())
	}
	var bufAge time.Duration
	if ms := p.GetBookBufferMs(); ms > 0 {
		bufAge = time.Duration(ms) * time.Millisecond
	}
	var tapeWindow time.Duration
	if ms := p.GetTapeWindowMs(); ms > 0 {
		tapeWindow = time.Duration(ms) * time.Millisecond
	}
	var symbols Instruments
	if s.cfg.Symbols != nil {
		loaded, err := s.cfg.Symbols(ctx, p.GetVenue())
		if err != nil || loaded == nil {
			if err == nil {
				err = errors.New("symbol table is missing")
			}
			return fmt.Errorf("symbols: %w", err)
		}
		symbols = loaded
		if cl, ok := loaded.(interface{ Close() }); ok {
			s.closeSymbols = cl.Close
		}
	}
	s.log.Info().Str("venue", p.GetVenue()).Str("route", p.GetRoute()).Uint32("n", p.GetN()).
		Uint64("conn_epoch", p.GetConnEpoch()).Msg("init")
	c := New(Config{
		Venue: p.GetVenue(), Route: p.GetRoute(), Instance: p.GetMdInstance(), URL: p.GetUrl(),
		Market: market, Publish: s.cfg.Publish, Now: s.cfg.Now, Epoch: p.GetConnEpoch(),
		Report: s.report, RetryAfter: s.cfg.RetryAfter, RetryMax: s.cfg.RetryMax, Log: s.cfg.Log,
		REST: rest, Doer: s.cfg.Doer, RateHold: s.cfg.RateHold,
		BufferMax: int(p.GetBookBufferMax()), BufferAge: bufAge,
		Symbols: symbols, Subscribe: s.cfg.Subscribe,
		TapeWindow: tapeWindow, TapeMaxEvents: int(p.GetTapeMaxEvents()), TapeBudget: int64(p.GetTapeBudgetBytes()),
	})
	s.mu.Lock()
	s.worker = c
	s.mu.Unlock()
	return nil
}

// Start has nothing to do before Done(START): Run dials.
func (s *plane) Start(context.Context, []byte) error { return nil }

func (s *plane) Run(ctx context.Context) error {
	s.mu.Lock()
	c := s.worker
	s.mu.Unlock()
	return c.Run(ctx)
}

// Live applies one Desired.
func (s *plane) Live(payload []byte) error {
	d := &mdctlv1.Desired{}
	if err := proto.Unmarshal(payload, d); err != nil {
		return err
	}
	s.mu.Lock()
	c := s.worker
	s.mu.Unlock()
	c.SetDesired(d)
	return nil
}

// report writes one mdctl.Report. The pre-dial call carries the next conn_epoch, and Dial is
// not called until it returns. Later calls carry the same number plus each feed's status.
// The write returns only once the frame has been handed to rw.
func (s *plane) report(rep *mdctlv1.Report) error {
	if rep == nil {
		return nil
	}
	return s.sess.Report(rep)
}
