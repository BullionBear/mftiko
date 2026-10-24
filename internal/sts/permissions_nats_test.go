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
	"github.com/BullionBear/mftiko/internal/sts/issuer"
)

const opTimeout = 2 * time.Second

// devServer is one embedded nats-server with the dev operator layout (E04-D012).
// The generated config sets max_control_line to 262144 (E08-D027).
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
	wantLine := fmt.Sprintf("max_control_line: %d\n", natsdev.MaxControlLine)
	if !strings.Contains(conf, wantLine) {
		t.Fatalf("config missing %q\n%s", wantLine, conf)
	}
	confPath := filepath.Join(t.TempDir(), "nats-server.conf")
	if err := os.WriteFile(confPath, []byte(conf), 0o644); err != nil {
		t.Fatal(err)
	}
	opts, err := server.ProcessConfigFile(confPath)
	if err != nil {
		t.Fatal(err)
	}
	if opts.MaxControlLine != natsdev.MaxControlLine {
		t.Fatalf("server max_control_line %d", opts.MaxControlLine)
	}
	if server.MAX_CONTROL_LINE_SIZE != 4096 {
		t.Fatalf("nats-server default max control line is %d", server.MAX_CONTROL_LINE_SIZE)
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
// a session calls, it does not answer (E08-D021).
type signedUser struct {
	jwt   string
	kp    nkeys.KeyPair
	inbox string
}

func signSession(t *testing.T, lay natsdev.Layout, sessionID string, perms sts.Permissions) signedUser {
	t.Helper()
	seed, err := os.ReadFile(filepath.Join(lay.Dir, filepath.FromSlash(lay.Manifest.SigningKeyFile)))
	if err != nil {
		t.Fatal(err)
	}
	skp, err := nkeys.FromSeed(bytes.TrimSpace(seed))
	if err != nil {
		t.Fatal(err)
	}
	token, userSeed, err := issuer.SignSession(sessionID, perms, lay.Manifest.Account, skp)
	if err != nil {
		t.Fatal(err)
	}
	ukp, err := nkeys.FromSeed([]byte(userSeed))
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
	if claims.Name != "session-"+sessionID || claims.IssuerAccount != lay.Manifest.Account {
		t.Fatalf("name %s issuer account %s", claims.Name, claims.IssuerAccount)
	}
	return signedUser{jwt: token, kp: ukp, inbox: "_INBOX_" + sessionID}
}

func dialUser(t *testing.T, url string, u signedUser) (*nats.Conn, error) {
	t.Helper()
	// The permission checks set a client name. The size check measures a
	// connection with no name (E08-D027); TestSessionJWTFiveHundredFeeds dials that way.
	return dialSession(t, url, u, "sts-session")
}

func dialSession(t *testing.T, url string, u signedUser, name string) (*nats.Conn, error) {
	t.Helper()
	opts := []nats.Option{
		nats.UserJWT(
			func() (string, error) { return u.jwt, nil },
			func(nonce []byte) ([]byte, error) { return u.kp.Sign(nonce) },
		),
		nats.CustomInboxPrefix(u.inbox),
		nats.NoReconnect(),
		nats.Timeout(opTimeout),
		nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {}),
	}
	if name != "" {
		opts = append(opts, nats.Name(name))
	}
	return nats.Connect(url, opts...)
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
		{"log subject", func(nc *nats.Conn) error { return nc.Publish("log.sts."+session, []byte("q")) }},
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

// TestSessionJWTFiveHundredFeeds connects a session that lists 500 feeds on
// the generated server (max_control_line 262144, E08-D027). The client sets no
// connection name, which is the CONNECT argument SignSession measures.
// The MD user is the dev instance, so the feed instance is dev and one
// message is delivered on the first and last feed.
func TestSessionJWTFiveHundredFeeds(t *testing.T) {
	srv := startDevServer(t)
	md := dialMD(t, srv.url, srv.lay)
	shapes := []struct {
		name   string
		topic  string
		ticker func(i int) string
	}{
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
	const (
		n       = 500
		session = "500fed"
	)
	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			feeds := make([]string, n)
			for i := range feeds {
				feeds[i] = shape.topic + "." + shape.ticker(i)
			}
			// md-tw-1 is the instance the 4096 measurement used. This user
			// cannot be published to by the dev MD credential; connecting
			// is the check. The dev instance below also receives a message.
			wide := signSession(t, srv.lay, session, perms(t, session, specOf([]int64{7}, feeds), inst("md-tw-1")))
			t.Logf("%s: 500 feeds on md-tw-1, jwt %d bytes", shape.name, len(wide.jwt))
			wideNC, err := dialSession(t, srv.url, wide, "")
			if err != nil {
				t.Fatalf("connect md-tw-1: %v", err)
			}
			wideNC.Close()

			user := signSession(t, srv.lay, session, perms(t, session, specOf([]int64{7}, feeds), inst("dev")))
			t.Logf("%s: 500 feeds, jwt %d bytes", shape.name, len(user.jwt))
			sess, err := dialSession(t, srv.url, user, "")
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			defer sess.Close()
			ends := []string{feeds[0], feeds[n-1]}
			subs := make([]*nats.Subscription, len(ends))
			for i, feed := range ends {
				sub, err := sess.SubscribeSync("md.v1.f.dev." + feed)
				if err != nil {
					t.Fatal(err)
				}
				subs[i] = sub
			}
			if err := sess.FlushTimeout(opTimeout); err != nil {
				t.Fatal(err)
			}
			if err := sess.LastError(); err != nil {
				t.Fatal(err)
			}
			for i, feed := range ends {
				payload := []byte(fmt.Sprintf("tape-%d", i))
				subject := "md.v1.f.dev." + feed
				if err := md.Publish(subject, payload); err != nil {
					t.Fatal(err)
				}
				if err := md.FlushTimeout(opTimeout); err != nil {
					t.Fatal(err)
				}
				msg, err := subs[i].NextMsg(opTimeout)
				if err != nil {
					t.Fatal(err)
				}
				if string(msg.Data) != string(payload) {
					t.Fatalf("payload %q", msg.Data)
				}
			}
			// Five hundred exact subjects are not an instance wildcard.
			other, err := dialSession(t, srv.url, user, "")
			if err != nil {
				t.Fatal(err)
			}
			defer other.Close()
			expectDenied(t, other, func(nc *nats.Conn) error {
				_, err := nc.SubscribeSync("md.v1.f.dev.>")
				return err
			})
		})
	}
}
