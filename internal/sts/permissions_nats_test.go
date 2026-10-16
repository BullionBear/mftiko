package sts_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"

	"github.com/BullionBear/mftiko/internal/platform/natsdev"
	"github.com/BullionBear/mftiko/internal/sts"
)

const opTimeout = 2 * time.Second

// devServer is one embedded nats-server with the dev operator layout (D412).
// max_control_line is the server default: the generated config does not set it.
type devServer struct {
	url string
	lay natsdev.Layout
	ns  *server.Server
}

func startDevServer(t *testing.T) *devServer {
	t.Helper()
	dir := t.TempDir()
	if err := natsdev.Generate(dir, natsdev.Options{}); err != nil {
		t.Fatal(err)
	}
	lay, err := natsdev.Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	conf, err := natsdev.FormatServerConfig(natsdev.ServerConfig{
		OperatorFile:  filepath.Join(dir, "operator.jwt"),
		SystemAccount: lay.Manifest.System,
		ResolverDir:   filepath.Join(dir, "resolver"),
		Host:          "127.0.0.1",
		Port:          -1,
		ServerName:    "dev",
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(conf, "max_control_line") {
		t.Fatalf("config sets max_control_line:\n%s", conf)
	}
	confPath := filepath.Join(t.TempDir(), "nats-server.conf")
	if err := os.WriteFile(confPath, []byte(conf), 0o644); err != nil {
		t.Fatal(err)
	}
	opts, err := server.ProcessConfigFile(confPath)
	if err != nil {
		t.Fatal(err)
	}
	opts.NoLog = true
	opts.NoSigs = true
	ns, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	ns.Start()
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	if !ns.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats-server did not accept connections")
	}
	return &devServer{url: ns.ClientURL(), lay: lay, ns: ns}
}

// signedUser is a session user whose allow lists are Permissions. The JWT is
// signed with the dev account signing key, the same key natsdev.Signer uses
// for an account worker. There is no deny list and no response permission:
// a session calls, it does not answer (D821).
type signedUser struct {
	jwt   string
	kp    nkeys.KeyPair
	inbox string
}

func signSession(t *testing.T, lay natsdev.Layout, sessionID string, perms sts.Permissions) signedUser {
	t.Helper()
	ukp, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := ukp.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	seed, err := os.ReadFile(filepath.Join(lay.Dir, filepath.FromSlash(lay.Manifest.SigningKeyFile)))
	if err != nil {
		t.Fatal(err)
	}
	skp, err := nkeys.FromSeed(bytes.TrimSpace(seed))
	if err != nil {
		t.Fatal(err)
	}
	uc := jwt.NewUserClaims(pub)
	uc.Name = "session-" + sessionID
	uc.IssuerAccount = lay.Manifest.Account
	uc.Pub.Allow.Add(perms.Pub...)
	uc.Sub.Allow.Add(perms.Sub...)
	token, err := uc.Encode(skp)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := jwt.DecodeUserClaims(token)
	if err != nil {
		t.Fatal(err)
	}
	if len(claims.Pub.Deny) != 0 || len(claims.Sub.Deny) != 0 || claims.Resp != nil {
		t.Fatalf("deny pub %v sub %v resp %v", claims.Pub.Deny, claims.Sub.Deny, claims.Resp)
	}
	return signedUser{jwt: token, kp: ukp, inbox: "_INBOX_" + sessionID}
}

func dialUser(t *testing.T, url string, u signedUser) (*nats.Conn, error) {
	t.Helper()
	return nats.Connect(url,
		nats.UserJWT(
			func() (string, error) { return u.jwt, nil },
			func(nonce []byte) ([]byte, error) { return u.kp.Sign(nonce) },
		),
		nats.CustomInboxPrefix(u.inbox),
		nats.NoReconnect(),
		nats.Timeout(opTimeout),
		nats.Name("sts-session"),
		nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {}),
	)
}

func dialMust(t *testing.T, url string, u signedUser) *nats.Conn {
	t.Helper()
	nc, err := dialUser(t, url, u)
	if err != nil {
		t.Fatalf("connect jwt %d bytes: %v", len(u.jwt), err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func dialMD(t *testing.T, url string, lay natsdev.Layout) *nats.Conn {
	t.Helper()
	u := lay.Manifest.MD[0]
	if u.Inst != "dev" {
		t.Fatalf("md inst %s", u.Inst)
	}
	nc, err := nats.Connect(url,
		nats.UserCredentials(filepath.Join(lay.Dir, filepath.FromSlash(u.Creds))),
		nats.NoReconnect(),
		nats.Timeout(opTimeout),
		nats.Name("sts-md"),
		nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func expectDenied(t *testing.T, nc *nats.Conn, do func(*nats.Conn) error) {
	t.Helper()
	if err := do(nc); err != nil {
		if errors.Is(err, nats.ErrPermissionViolation) {
			return
		}
		t.Fatal(err)
	}
	if err := nc.FlushTimeout(opTimeout); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(nc.LastError(), nats.ErrPermissionViolation) {
		t.Fatalf("expected a permission violation, got %v", nc.LastError())
	}
}

func TestSessionJWTAllowsExactFeeds(t *testing.T) {
	srv := startDevServer(t)
	const (
		session = "a1b2c3"
		feed    = "trade.Binance_Spot_BTCUSDT"
		subject = "md.v1.f.dev.trade.Binance_Spot_BTCUSDT"
		view    = "md.v1.view.dev.trade.Binance_Spot_BTCUSDT"
	)
	p := perms(t, session, specOf([]int64{7}, []string{feed}), inst("dev"))
	user := signSession(t, srv.lay, session, p)
	sess := dialMust(t, srv.url, user)
	md := dialMD(t, srv.url, srv.lay)

	sub, err := sess.SubscribeSync(subject)
	if err != nil {
		t.Fatal(err)
	}
	if err := sess.FlushTimeout(opTimeout); err != nil {
		t.Fatal(err)
	}
	if err := sess.LastError(); err != nil {
		t.Fatal(err)
	}
	payload := []byte("tape")
	if err := md.Publish(subject, payload); err != nil {
		t.Fatal(err)
	}
	if err := md.FlushTimeout(opTimeout); err != nil {
		t.Fatal(err)
	}
	msg, err := sub.NextMsg(opTimeout)
	if err != nil {
		t.Fatal(err)
	}
	if string(msg.Data) != string(payload) {
		t.Fatalf("payload %q", msg.Data)
	}

	// Allows are flushed on this connection. Denials each use a new one: a
	// permission error stays on the connection as LastError.
	if err := sess.Publish(view, []byte("q")); err != nil {
		t.Fatal(err)
	}
	if err := sess.Publish("td.order.7", []byte("q")); err != nil {
		t.Fatal(err)
	}
	if err := sess.Publish("td.view.7", []byte("q")); err != nil {
		t.Fatal(err)
	}
	if err := sess.Publish("sym", []byte("q")); err != nil {
		t.Fatal(err)
	}
	if err := sess.Publish("log.sts."+session, []byte("q")); err != nil {
		t.Fatal(err)
	}
	if err := sess.FlushTimeout(opTimeout); err != nil {
		t.Fatal(err)
	}
	if err := sess.LastError(); err != nil {
		t.Fatal(err)
	}
	if _, err := sess.SubscribeSync("td.evt.7"); err != nil {
		t.Fatal(err)
	}
	if _, err := sess.SubscribeSync("sym.v1.changed.>"); err != nil {
		t.Fatal(err)
	}
	if _, err := sess.SubscribeSync("_INBOX_" + session + ".>"); err != nil {
		t.Fatal(err)
	}
	if err := sess.FlushTimeout(opTimeout); err != nil {
		t.Fatal(err)
	}

	denies := []struct {
		what string
		do   func(*nats.Conn) error
	}{
		{"other feed", func(nc *nats.Conn) error {
			_, err := nc.SubscribeSync("md.v1.f.dev.trade.Binance_Spot_ETHUSDT")
			return err
		}},
		{"instance feed wildcard", func(nc *nats.Conn) error {
			_, err := nc.SubscribeSync("md.v1.f.dev.>")
			return err
		}},
		{"other instance", func(nc *nats.Conn) error {
			_, err := nc.SubscribeSync("md.v1.f.other.trade.Binance_Spot_BTCUSDT")
			return err
		}},
		{"view wildcard", func(nc *nats.Conn) error { return nc.Publish("md.v1.view.dev.>", []byte("q")) }},
		{"other account order", func(nc *nats.Conn) error { return nc.Publish("td.order.8", []byte("q")) }},
		{"other account events", func(nc *nats.Conn) error {
			_, err := nc.SubscribeSync("td.evt.8")
			return err
		}},
		{"default inbox", func(nc *nats.Conn) error {
			_, err := nc.SubscribeSync("_INBOX.>")
			return err
		}},
	}
	for _, d := range denies {
		t.Run(d.what, func(t *testing.T) {
			nc, err := dialUser(t, srv.url, user)
			if err != nil {
				t.Fatal(err)
			}
			defer nc.Close()
			expectDenied(t, nc, d.do)
		})
	}

	bare := signSession(t, srv.lay, "b2c3d4", perms(t, "b2c3d4", specOf(nil, nil), nil))
	nc, err := dialUser(t, srv.url, bare)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	expectDenied(t, nc, func(nc *nats.Conn) error { return nc.Publish("td.order.7", []byte("q")) })
}

// controlLineObs is what one shape did against the default 4096-byte control
// line: the last feed count that connected, the first that was refused, and
// the user JWT a 500-feed session would present. jwt500 is signed and not connected.
type controlLineObs struct {
	okN, okJWT, failN, failJWT, jwt500 int
}

// controlLineObserved is the measurement on nats-server v2.15.0 with no
// max_control_line in the dev config (the 4096 default). It is not a cap this
// package chose. A session id is 6 lowercase hex, one account is api_id 7,
// and the instance is md-tw-1.
var controlLineObserved = map[string]controlLineObs{
	"perp":   {okN: 23, okJWT: 3706, failN: 24, failJWT: 3830, jwt500: 62854},
	"option": {okN: 14, okJWT: 3710, failN: 15, failJWT: 3914, jwt500: 102854},
}

// controlLineShape is one realistic feed spelling used to measure the CONNECT
// line. The ticker is unique per index and contains no D819 character.
type controlLineShape struct {
	name   string
	topic  string
	ticker func(i int) string
}

func (s controlLineShape) feeds(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = s.topic + "." + s.ticker(i)
	}
	return out
}

func TestSessionJWTControlLine(t *testing.T) {
	srv := startDevServer(t)
	shapes := []controlLineShape{
		{
			name:  "perp",
			topic: "trade",
			ticker: func(i int) string {
				return fmt.Sprintf("BinanceUM_Perp_S%04d", i)
			},
		},
		{
			name:  "option",
			topic: "orderbook_delta",
			ticker: func(i int) string {
				return fmt.Sprintf("BinanceUM_Option_BTCUSDT-260925-%06d-C", 100000+i)
			},
		},
	}
	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			if server.MAX_CONTROL_LINE_SIZE != 4096 {
				t.Fatalf("nats-server default max control line is %d", server.MAX_CONTROL_LINE_SIZE)
			}
			okN, okJWT, failN, failJWT, failErr := controlLineLimit(t, srv, shape, 512)
			jwt500 := jwtLen(t, srv.lay, shape, 500)
			t.Logf("%s: %d feeds connect (jwt %d bytes); %d feeds refused (jwt %d bytes): %v; 500 feeds would be jwt %d bytes",
				shape.name, okN, okJWT, failN, failJWT, failErr, jwt500)
			// The counts are the measurement against the server default.
			// This test does not raise max_control_line.
			want, ok := controlLineObserved[shape.name]
			if !ok {
				t.Fatalf("no observation for %s", shape.name)
			}
			if okN != want.okN || okJWT != want.okJWT || failN != want.failN || failJWT != want.failJWT || jwt500 != want.jwt500 {
				t.Fatalf("got ok %d jwt %d, fail %d jwt %d, jwt500 %d; want %+v", okN, okJWT, failN, failJWT, jwt500, want)
			}
			if failErr == nil || !controlLineExceeded(failErr) {
				t.Fatalf("refusal was not max_control_line: %v", failErr)
			}
			// The server is still up after the refusal.
			again := signSession(t, srv.lay, "c3d4e5", perms(t, "c3d4e5", specOf(nil, shape.feeds(1)), inst("md-tw-1")))
			nc, err := dialUser(t, srv.url, again)
			if err != nil {
				t.Fatal(err)
			}
			nc.Close()
		})
	}
}

// controlLineLimit is the largest feed count at or below cap that connects,
// and the next count, which does not. cap is inclusive.
func controlLineLimit(t *testing.T, srv *devServer, shape controlLineShape, cap int) (okN, okJWT, failN, failJWT int, failErr error) {
	t.Helper()
	try := func(n int) (int, error) {
		t.Helper()
		session := fmt.Sprintf("%06x", n) // 6 lowercase hex, the session_id form (PROTOCOL §2)
		p, err := sts.SessionPermissions(session, specOf([]int64{7}, shape.feeds(n)), inst("md-tw-1"))
		if err != nil {
			t.Fatalf("permissions for %d feeds: %v", n, err)
		}
		user := signSession(t, srv.lay, session, p)
		nc, err := dialUser(t, srv.url, user)
		if nc != nil {
			nc.Close()
		}
		return len(user.jwt), err
	}
	// Exponential search, then the gap between the last success and the first failure.
	lastOK, lastJWT := -1, 0
	failAt := cap + 1
	for n := 0; n <= cap; n = nextN(n) {
		jwtLen, err := try(n)
		if err != nil {
			failAt = n
			failJWT = jwtLen
			failErr = err
			break
		}
		lastOK, lastJWT = n, jwtLen
	}
	if failErr == nil {
		return lastOK, lastJWT, cap, lastJWT, nil
	}
	lo, hi := lastOK, failAt
	for lo+1 < hi {
		mid := (lo + hi) / 2
		jwtLen, err := try(mid)
		if err != nil {
			hi = mid
			failJWT = jwtLen
			failErr = err
			continue
		}
		lo = mid
		lastJWT = jwtLen
	}
	return lo, lastJWT, hi, failJWT, failErr
}

func jwtLen(t *testing.T, lay natsdev.Layout, shape controlLineShape, n int) int {
	t.Helper()
	session := fmt.Sprintf("%06x", n)
	p, err := sts.SessionPermissions(session, specOf([]int64{7}, shape.feeds(n)), inst("md-tw-1"))
	if err != nil {
		t.Fatalf("permissions for %d feeds: %v", n, err)
	}
	return len(signSession(t, lay, session, p).jwt)
}

func nextN(n int) int {
	if n == 0 {
		return 1
	}
	return n * 2
}

func controlLineExceeded(err error) bool {
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "maximum control line") || strings.Contains(s, "max control line") || strings.Contains(s, "control line exceeded")
}
