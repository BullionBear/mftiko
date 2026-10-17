package conformance

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	stsctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/stsctl/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/contract/wire/envelope"
	"github.com/BullionBear/mftiko/internal/platform/natsdev"
	"github.com/BullionBear/mftiko/internal/sts"
	"github.com/BullionBear/mftiko/internal/sts/issuer"
)

const (
	sessionID       = "c0ffee"
	apiID     int64 = 7
	algoID          = uint32(1)
	uid             = uint32(1001)
	feedKey         = "trade.Binance_Spot_BTCUSDT"
	venue           = "Binance"
	ticker          = "Binance_Spot_BTCUSDT"

	// tdFull is one past the TD buffer cap (PROTOCOL §9.5, D815). The constant
	// lives in the SDK; the harness only publishes this many events.
	tdFull = 65536 + 1

	ringN    = 70
	ringSize = 512 << 10
	sentinel = uint64(10000)

	opTimeout = 3 * time.Second
	longWait  = 60 * time.Second
)

// bus is the embedded server plus the fake MD, TD, and md.fetch peers (D830).
type bus struct {
	dir          string
	url          string
	inst         string
	ns           *server.Server
	jwt          string
	seed         string
	md           *nats.Conn
	td           *nats.Conn
	fetch        *nats.Conn
	feedSubject  string
	orderSubject string
	evtSubject   string
	logSubject   string

	mu     sync.Mutex
	cloids []string
}

func startBus() (*bus, error) {
	dir, err := os.MkdirTemp("", "mftiko-conform-")
	if err != nil {
		return nil, err
	}
	b := &bus{dir: dir, logSubject: "log.sts." + sessionID}
	if err := os.MkdirAll(filepath.Join(dir, "vol"), 0o755); err != nil {
		b.close()
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(dir, "in"), 0o755); err != nil {
		b.close()
		return nil, err
	}
	if err := natsdev.Generate(dir, natsdev.Options{}); err != nil {
		b.close()
		return nil, err
	}
	lay, err := natsdev.Load(dir)
	if err != nil {
		b.close()
		return nil, err
	}
	if len(lay.Manifest.MD) == 0 || len(lay.Manifest.Fetch) == 0 {
		b.close()
		return nil, errors.New("conformance: natsdev layout has no MD or fetch user")
	}
	b.inst = lay.Manifest.MD[0].Inst
	if b.inst == "" {
		b.close()
		return nil, errors.New("conformance: empty MD instance")
	}
	b.feedSubject = "md.v1.f." + b.inst + "." + feedKey
	b.orderSubject = "td.order." + strconv.FormatInt(apiID, 10)
	b.evtSubject = "td.evt." + strconv.FormatInt(apiID, 10)

	conf, err := natsdev.FormatServerConfig(natsdev.ServerConfig{
		OperatorFile:  filepath.Join(dir, "operator.jwt"),
		SystemAccount: lay.Manifest.System,
		ResolverDir:   filepath.Join(dir, "resolver"),
		Host:          "127.0.0.1",
		Port:          -1,
		ServerName:    "conform",
	})
	if err != nil {
		b.close()
		return nil, err
	}
	confPath := filepath.Join(dir, "nats-server.conf")
	if err := os.WriteFile(confPath, []byte(conf), 0o644); err != nil {
		b.close()
		return nil, err
	}
	opts, err := server.ProcessConfigFile(confPath)
	if err != nil {
		b.close()
		return nil, err
	}
	if opts.MaxControlLine != natsdev.MaxControlLine {
		b.close()
		return nil, fmt.Errorf("conformance: max_control_line %d", opts.MaxControlLine)
	}
	opts.NoLog = true
	opts.NoSigs = true
	ns, err := server.NewServer(opts)
	if err != nil {
		b.close()
		return nil, err
	}
	b.ns = ns
	ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		b.close()
		return nil, errors.New("conformance: nats-server did not accept connections")
	}
	b.url = ns.ClientURL()

	spec := &sessionv1.SessionSpec{
		Accounts: []*sessionv1.AccountDecl{{Name: "a", ApiId: apiID, AlgoIds: []uint32{algoID}}},
		Feeds:    []*sessionv1.FeedDecl{{Feed: feedKey, MdInstance: b.inst}},
	}
	perms, err := sts.SessionPermissions(sessionID, spec, func(string) string { return b.inst })
	if err != nil {
		b.close()
		return nil, err
	}
	seed, err := os.ReadFile(filepath.Join(lay.Dir, filepath.FromSlash(lay.Manifest.SigningKeyFile)))
	if err != nil {
		b.close()
		return nil, err
	}
	skp, err := nkeys.FromSeed(bytes.TrimSpace(seed))
	if err != nil {
		b.close()
		return nil, err
	}
	token, userSeed, err := issuer.SignSession(sessionID, perms, lay.Manifest.Account, skp)
	if err != nil {
		b.close()
		return nil, err
	}
	b.jwt, b.seed = token, userSeed

	b.md, err = dialCreds(b.url, filepath.Join(lay.Dir, filepath.FromSlash(lay.Manifest.MD[0].Creds)))
	if err != nil {
		b.close()
		return nil, fmt.Errorf("conformance: md: %w", err)
	}
	b.td, err = b.accountConn(lay)
	if err != nil {
		b.close()
		return nil, err
	}
	b.fetch, err = dialCreds(b.url, filepath.Join(lay.Dir, filepath.FromSlash(lay.Manifest.Fetch[0].Creds)))
	if err != nil {
		b.close()
		return nil, fmt.Errorf("conformance: fetch: %w", err)
	}
	if err := b.servePeers(); err != nil {
		b.close()
		return nil, err
	}
	return b, nil
}

func (b *bus) accountConn(lay natsdev.Layout) (*nats.Conn, error) {
	signer, err := natsdev.LoadSigner(lay.Manifest.Account, filepath.Join(lay.Dir, filepath.FromSlash(lay.Manifest.SigningKeyFile)))
	if err != nil {
		return nil, err
	}
	userJWT, seed, inbox, err := signer.AccountWorker(apiID)
	if err != nil {
		return nil, err
	}
	return dialJWT(b.url, userJWT, seed, inbox)
}

func dialCreds(url, path string) (*nats.Conn, error) {
	return nats.Connect(url,
		nats.UserCredentials(path),
		nats.NoReconnect(),
		nats.Timeout(opTimeout),
		nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {}),
	)
}

func dialJWT(url, userJWT, seed, inbox string) (*nats.Conn, error) {
	return nats.Connect(url,
		nats.UserJWT(
			func() (string, error) { return userJWT, nil },
			func(nonce []byte) ([]byte, error) {
				kp, err := nkeys.FromSeed([]byte(seed))
				if err != nil {
					return nil, err
				}
				return kp.Sign(nonce)
			},
		),
		nats.CustomInboxPrefix(inbox),
		nats.NoReconnect(),
		nats.Timeout(opTimeout),
		nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {}),
	)
}

func (b *bus) servePeers() error {
	if _, err := b.td.Subscribe(b.orderSubject, b.onOrder); err != nil {
		return err
	}
	view := "td.view." + strconv.FormatInt(apiID, 10)
	if _, err := b.td.Subscribe(view, b.onView); err != nil {
		return err
	}
	if _, err := b.md.Subscribe("md.v1.view."+b.inst+".>", func(m *nats.Msg) {
		if m.Reply == "" {
			return
		}
		raw, _ := proto.Marshal(&mdv1.ViewReply{Result: &mdv1.ViewReply_Tape{Tape: &mdv1.TapePage{}}})
		_ = m.Respond(raw)
	}); err != nil {
		return err
	}
	if _, err := b.fetch.Subscribe("md.v1.fetch."+venue, func(m *nats.Msg) {
		if m.Reply == "" {
			return
		}
		raw, _ := proto.Marshal(&mdv1.FetchReply{Uid: uid})
		_ = m.Respond(raw)
	}); err != nil {
		return err
	}
	for _, nc := range []*nats.Conn{b.td, b.md, b.fetch} {
		if err := nc.FlushTimeout(opTimeout); err != nil {
			return err
		}
	}
	return nil
}

func (b *bus) onOrder(m *nats.Msg) {
	var req tdv1.OrderRequest
	if proto.Unmarshal(m.Data, &req) == nil {
		if s := req.GetSubmit(); s != nil {
			b.mu.Lock()
			b.cloids = append(b.cloids, s.GetClientOrderId())
			b.mu.Unlock()
		}
	}
	if m.Reply == "" {
		return
	}
	id := ""
	if req.GetSubmit() != nil {
		id = req.GetSubmit().GetClientOrderId()
	}
	raw, _ := proto.Marshal(&tdv1.OrderReply{Accepted: true, ClientOrderId: id})
	_ = m.Respond(raw)
}

// onView publishes the early recon events and then answers, on this same
// connection, so they are queued before the worker's Request returns.
// The snapshot is incarnation 3, seq 5. Seq 4 of that incarnation is at or
// below the cursor. Seq 6 is the one event past it. Seq 6 of incarnation 1
// is older than the reply and is dropped. A newer incarnation would ask
// again (D844); this fixture must not publish one.
func (b *bus) onView(m *nats.Msg) {
	_ = b.publishAccount(4, 3, false)
	_ = b.publishAccount(6, 3, false)
	_ = b.publishAccount(6, 1, false)
	if m.Reply == "" {
		return
	}
	raw, _ := proto.Marshal(&tdv1.ViewReply{
		Reply: &tdv1.ViewReply_Recon{Recon: &tdv1.Recon{
			Incarnation: 3,
			Reason:      tdv1.ReconReason_RECON_REASON_REQUESTED,
		}},
		State:       tdv1.AccountState_ACCOUNT_STATE_READY,
		Incarnation: 3,
		Seq:         5,
		CancelBatch: 1,
	})
	_ = m.Respond(raw)
}

func (b *bus) close() {
	for _, nc := range []*nats.Conn{b.md, b.td, b.fetch} {
		if nc != nil {
			nc.Close()
		}
	}
	if b.ns != nil {
		b.ns.Shutdown()
		b.ns.WaitForShutdown()
	}
	if b.dir != "" {
		_ = os.RemoveAll(b.dir)
	}
}

func (b *bus) workerEnv() []string {
	home := os.Getenv("HOME")
	if home == "" {
		home = b.dir
	}
	return []string{
		"MFTIKO_CTL_FD=3",
		"MFTIKO_NATS_URL=" + b.url,
		"MFTIKO_PLANE=sts",
		"MFTIKO_INSTANCE=sts-dev-1",
		"MFTIKO_WORKER_KEY=sts/" + sessionID,
		"MFTIKO_INCARNATION=1",
		"MFTIKO_VOLUME_DIR=" + filepath.Join(b.dir, "vol"),
		"MFTIKO_INPUT_DIR=" + filepath.Join(b.dir, "in"),
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + home,
		"TMPDIR=" + os.TempDir(),
	}
}

func (b *bus) initDown(p Params, deadline uint32, digest string) (*workerv1.WorkerDown, error) {
	rawParams, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	body, err := proto.Marshal(&stsctlv1.InitPayload{
		SessionId:    sessionID,
		Incarnation:  1,
		StsInstance:  "sts-dev-1",
		Name:         "conform",
		Params:       rawParams,
		Feeds:        []*sessionv1.FeedDecl{{Feed: feedKey, MdInstance: b.inst}},
		Accounts:     []*sessionv1.AccountDecl{{Name: "a", ApiId: apiID, AlgoIds: []uint32{algoID}}},
		Nats:         &stsctlv1.NatsCredentials{UserJwt: b.jwt, Seed: b.seed, InboxPrefix: "_INBOX_" + sessionID},
		RecvWindowMs: 500,
		HookTimeoutS: 1,
	})
	if err != nil {
		return nil, err
	}
	return &workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Init{Init: &workerv1.Init{
		Contract:   contractSTS,
		Payload:    body,
		DeadlineMs: deadline,
		LiveDigest: digest,
	}}}, nil
}

func (b *bus) startDown(deadline uint32) (*workerv1.WorkerDown, error) {
	body, err := proto.Marshal(&stsctlv1.StartPayload{Bindings: &stsctlv1.Bindings{
		Version: &commonv1.Version{Epoch: 1, Seq: 1},
		Feeds: []*stsctlv1.FeedBinding{{
			Feed:       feedKey,
			MdInstance: b.inst,
			Subject:    b.feedSubject,
			State:      stsctlv1.BindingState_BINDING_STATE_READY,
		}},
		Accounts: []*stsctlv1.AccountBinding{{
			Name:   "a",
			ApiId:  apiID,
			State:  stsctlv1.BindingState_BINDING_STATE_READY,
			Worker: &commonv1.WorkerRef{Key: "td/7", Incarnation: 1},
		}},
	}})
	if err != nil {
		return nil, err
	}
	return &workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Start{Start: &workerv1.Start{
		Contract:   contractSTS,
		Payload:    body,
		DeadlineMs: deadline,
	}}}, nil
}

func (b *bus) publishTrade(seq uint64, ts time.Time, size int) error {
	ev := &mdv1.FeedEvent{Uid: uid, Event: &mdv1.FeedEvent_Trade{Trade: &mdv1.Trade{
		TradeId: strconv.FormatUint(seq, 10),
		Price:   1,
		Qty:     1,
	}}}
	pb, err := proto.Marshal(ev)
	if err != nil {
		return err
	}
	if size > len(pb) {
		pb = append(pb, make([]byte, size-len(pb))...)
	}
	msg := envelope.Append(nil, ts.UnixNano(), 1, seq, pb)
	return b.md.Publish(b.feedSubject, msg)
}

func (b *bus) publishAccount(seq uint64, inc uint32, position bool) error {
	ev := &tdv1.AccountEvent{
		Seq:    seq,
		Source: &commonv1.WorkerRef{Key: "td/7", Incarnation: inc},
	}
	if position {
		ev.Event = &tdv1.AccountEvent_PositionUpdate{PositionUpdate: &tdv1.PositionUpdate{
			Positions: []*tdv1.Position{{Uid: uid, Size: 2}},
		}}
	} else {
		ev.Event = &tdv1.AccountEvent_OrderUpdate{OrderUpdate: &tdv1.OrderUpdate{
			Cause: tdv1.OrderCause_ORDER_CAUSE_ACCEPTED,
			Order: &tdv1.Order{
				Uid:           uid,
				ClientOrderId: "c",
				Status:        tdv1.OrderStatus_ORDER_STATUS_ACCEPTED,
				Qty:           1,
				Side:          commonv1.Side_SIDE_BUY,
			},
		}}
	}
	raw, err := proto.Marshal(ev)
	if err != nil {
		return err
	}
	return b.td.Publish(b.evtSubject, raw)
}

func (b *bus) flush() error {
	if err := b.md.FlushTimeout(opTimeout); err != nil {
		return err
	}
	return b.td.FlushTimeout(opTimeout)
}

func (b *bus) mark() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.cloids)
}

func (b *bus) since(n int) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.cloids[n:]...)
}

// denied reports whether the session user is refused for do.
func (b *bus) denied(do func(*nats.Conn) error) error {
	nc, err := dialJWT(b.url, b.jwt, b.seed, "_INBOX_"+sessionID)
	if err != nil {
		return err
	}
	defer nc.Close()
	if err := do(nc); err != nil {
		if errors.Is(err, nats.ErrPermissionViolation) {
			return nil
		}
		return err
	}
	if err := nc.FlushTimeout(opTimeout); err != nil {
		if errors.Is(err, nats.ErrPermissionViolation) {
			return nil
		}
		return err
	}
	if errors.Is(nc.LastError(), nats.ErrPermissionViolation) {
		return nil
	}
	return fmt.Errorf("expected permission violation, got %v", nc.LastError())
}
