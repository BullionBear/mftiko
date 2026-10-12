package account

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	tdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/tdctl/v1"
	"github.com/BullionBear/mftiko/contract/wire/c4"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/transport"
)

// Contract is the C5 contract this worker speaks (PROTOCOL §6.3).
const Contract = "tdctl.v1"

// Reasons the worker gives: in Report.reason, and as the reason of an INIT_FAILED or CRASHED
// ending. The TD agent's Policy does not restart a worker that ended with ReasonAuth or
// ReasonHedgeMode (D608).
const (
	ReasonAuth            = "auth"             // the venue refused the credentials
	ReasonHedgeMode       = "hedge_mode"       // the account holds long and short apart (D607, D612)
	ReasonKindUnsupported = "kind_unsupported" // an account kind phase 1 does not run (D614)
	ReasonStreamLost      = "stream_lost"      // the events connection dropped (DEGRADED, D603)
)

// ServeConfig is the process around one account worker. The binary fills it from its flags and
// the exchange registry; tests pass fakes.
type ServeConfig struct {
	// Kinds returns an account kind by name; false when it is unknown.
	Kinds func(name string) (exchange.AccountKind, bool)
	// Doer sends the REST Calls. Net dials the websockets; nil connects directly.
	Doer exchange.Doer
	Net  *transport.Net
	// RESTEndpoint, StreamEndpoint and Simulated go to exchange.AccountConfig: the TD instance's
	// choice of host, the way MD's url is (D403). Empty is the venue's default.
	RESTEndpoint   string
	StreamEndpoint string
	Simulated      bool
	// Ledger receives the account's state. Nil discards it, until E6-07.
	Ledger Ledger
	// Now is the clock that signs requests and stamps recons. Nil is time.Now.
	Now func() time.Time
	// RetryAfter and RetryMax bound the waits between failed dials and failed recons: the first
	// waits RetryAfter, each next one twice as long, up to RetryMax. Zero is 1s and 30s.
	RetryAfter time.Duration
	RetryMax   time.Duration
	// ReadyTimeout bounds a connection's dial and logon, until the codec reports Ready. Zero is
	// 10s.
	ReadyTimeout time.Duration
	Log          *zerolog.Logger // nil discards
	Release      string          // Hello.release
}

// Serve speaks C4 on rw, the worker's end of fd 3, and runs one account until Stop, the shim's EOF,
// or ctx. It closes rw when it returns. The int is the exit code (PROTOCOL §5.3).
//
// Init.payload is a tdctl.InitPayload: the api_id, its kind and its unsealed credentials. Start
// connects, recons, and checks the position mode; Done(START, ok) goes out once the account is
// READY, after a Report saying so. Live config carries nothing for TD yet: a Live is applied as is.
func Serve(ctx context.Context, rw io.ReadWriteCloser, cfg ServeConfig) int {
	log := zerolog.Nop()
	if cfg.Log != nil {
		log = *cfg.Log
	}
	p := &plane{cfg: cfg, log: log}
	return c4.Serve(ctx, rw, c4.Config{
		Kind: "account", Contract: Contract, Release: cfg.Release,
		Log: func(msg string, err error) { log.Info().Err(err).Msg(msg) },
	}, p)
}

type plane struct {
	cfg ServeConfig
	log zerolog.Logger
	w   *Worker
}

func (p *plane) Init(_ context.Context, s *c4.Session, payload []byte) error {
	in := &tdctlv1.InitPayload{}
	if err := proto.Unmarshal(payload, in); err != nil {
		return errors.New("init payload")
	}
	if in.GetApiId() <= 0 || in.GetKind() == "" {
		return errors.New("api_id and kind are required")
	}
	if p.cfg.Kinds == nil || p.cfg.Doer == nil {
		return errors.New("kinds and doer are required")
	}
	kind, ok := p.cfg.Kinds(in.GetKind())
	if !ok || kind.Account == nil {
		return fmt.Errorf("unknown account kind %s", in.GetKind())
	}
	// A kind that trades several venues routes orders by venue, over one trade connection each;
	// Account has one NewTradeStream. Phase 1 does not run them (BinancePM, D614).
	if len(kind.Trades) != 1 {
		return errors.New(ReasonKindUnsupported)
	}
	creds, err := credentials(&kind, in.GetCredentials())
	if err != nil {
		return err
	}
	acct, err := kind.Account(exchange.AccountConfig{
		APIID: in.GetApiId(), Credentials: creds,
		RESTEndpoint: p.cfg.RESTEndpoint, StreamEndpoint: p.cfg.StreamEndpoint, Simulated: p.cfg.Simulated,
	})
	if err != nil || acct == nil {
		// The factory's error may quote what it could not parse.
		return fmt.Errorf("credentials: %s cannot build an account from them", kind.Name)
	}
	p.log.Info().Int64("api_id", in.GetApiId()).Str("kind", kind.Name).Msg("init")
	p.w = newWorker(p.cfg, p.log, acct, func(r *tdctlv1.Report) error { return s.Report(r) })
	return nil
}

// credentials checks the unsealed credentials against what the kind takes. The errors never quote
// them.
func credentials(kind *exchange.AccountKind, c *tdctlv1.Credentials) (exchange.Credentials, error) {
	out := exchange.Credentials{Type: exchange.KeyType(c.GetKeyType()), Key: c.GetKey(), Secret: c.GetSecret(), Passphrase: c.GetPassphrase()}
	switch {
	case !slices.Contains(kind.KeyTypes, out.Type):
		return exchange.Credentials{}, fmt.Errorf("credentials: %s does not take key type %q", kind.Name, c.GetKeyType())
	case out.Key == "" || out.Secret == "":
		return exchange.Credentials{}, errors.New("credentials: key and secret are required")
	case kind.Passphrase && out.Passphrase == "":
		return exchange.Credentials{}, fmt.Errorf("credentials: %s needs a passphrase", kind.Name)
	}
	return out, nil
}

func (p *plane) Start(ctx context.Context, _ []byte) error { return p.w.Start(ctx) }

func (p *plane) Run(ctx context.Context) error { return p.w.Run(ctx) }

// Live: TD has no live config yet; cancel_on_disconnect_s is in Init (E6-09).
func (p *plane) Live([]byte) error { return nil }
