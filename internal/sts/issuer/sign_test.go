package issuer

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	"github.com/BullionBear/mftiko/internal/platform/natsdev"
	"github.com/BullionBear/mftiko/internal/sts"
)

const opTimeout = 2 * time.Second

func TestSignSessionNilKey(t *testing.T) {
	_, _, err := SignSession("a1b2c3", sts.Permissions{}, "A", nil)
	if err == nil || !strings.Contains(err.Error(), "nil session signing key") {
		t.Fatal(err)
	}
}

// TestConnectLimit is D827 against a real nats-server: the CONNECT argument
// SignSession measures is the one the client sends, the generated config's
// max_control_line is what the server enforces, and the first feed count
// that SignSession rejects is the first the server refuses.
func TestConnectLimit(t *testing.T) {
	url, account, signer := startServer(t)
	t.Run("argument", func(t *testing.T) {
		token, seed := mustSign(t, "a1b2c3", optionFeeds(1), account, signer)
		arg := captureConnectArg(t, url, token, seed, "_INBOX_a1b2c3")
		var got struct {
			JWT string `json:"jwt"`
			Sig string `json:"sig"`
		}
		if err := json.Unmarshal([]byte(arg), &got); err != nil {
			t.Fatal(err)
		}
		if got.JWT != token {
			t.Fatal("CONNECT jwt is not the signed user jwt")
		}
		n, err := nonceSignatureLen()
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Sig) != n {
			t.Fatalf("nonce signature is %d bytes of base64, measured %d", len(got.Sig), n)
		}
		want, err := connectArg(token)
		if err != nil {
			t.Fatal(err)
		}
		swapped := strings.Replace(arg, `"sig":"`+got.Sig+`"`, `"sig":"`+strings.Repeat("A", len(got.Sig))+`"`, 1)
		if swapped != string(want) {
			t.Fatalf("CONNECT argument\n got %s\nwant %s", swapped, want)
		}
	})
	t.Run("boundary", func(t *testing.T) {
		const session = "b0b0b0"
		lo, hi := fitBounds(t, session, account, signer)
		okToken, okSeed := mustSign(t, session, optionFeeds(lo), account, signer)
		okArg, err := connectArg(okToken)
		if err != nil {
			t.Fatal(err)
		}
		if len(okArg) > natsdev.MaxControlLine {
			t.Fatalf("accepted %d feeds: CONNECT argument %d", lo, len(okArg))
		}
		nc, err := dial(t, url, okToken, okSeed, "_INBOX_"+session)
		if err != nil {
			t.Fatalf("%d feeds did not connect (jwt %d, CONNECT %d): %v", lo, len(okToken), len(okArg), err)
		}
		nc.Close()

		badToken, badSeed, err := encode(session, perms(t, session, optionFeeds(hi)), account, signer)
		if err != nil {
			t.Fatal(err)
		}
		badArg, err := connectArg(badToken)
		if err != nil {
			t.Fatal(err)
		}
		if len(badArg) <= natsdev.MaxControlLine {
			t.Fatalf("rejected %d feeds: CONNECT argument %d is within the limit", hi, len(badArg))
		}
		_, _, err = SignSession(session, perms(t, session, optionFeeds(hi)), account, signer)
		if !errors.Is(err, ErrTooManyFeeds) || !strings.Contains(err.Error(), "too many feeds") {
			t.Fatalf("size error: %v", err)
		}
		if _, err := dial(t, url, badToken, badSeed, "_INBOX_"+session); err == nil || !controlLineExceeded(err) {
			t.Fatalf("%d feeds: server refusal was %v (jwt %d, CONNECT %d)", hi, err, len(badToken), len(badArg))
		}
		t.Logf("option feeds: %d connect (jwt %d, CONNECT %d); %d rejected (jwt %d, CONNECT %d)",
			lo, len(okToken), len(okArg), hi, len(badToken), len(badArg))

		// The refusal does not stop the server.
		again, againSeed := mustSign(t, "c3d4e5", optionFeeds(1), account, signer)
		nc, err = dial(t, url, again, againSeed, "_INBOX_c3d4e5")
		if err != nil {
			t.Fatal(err)
		}
		nc.Close()
	})
}

func fitBounds(t *testing.T, session, account string, signer nkeys.KeyPair) (lo, hi int) {
	t.Helper()
	fits := func(n int) bool {
		t.Helper()
		_, _, err := SignSession(session, perms(t, session, optionFeeds(n)), account, signer)
		if err == nil {
			return true
		}
		if errors.Is(err, ErrTooManyFeeds) {
			return false
		}
		t.Fatal(err)
		return false
	}
	if !fits(1) {
		t.Fatal("one feed does not fit")
	}
	hi = 1
	for fits(hi) {
		if hi > 1<<20 {
			t.Fatal("did not reach max_control_line")
		}
		hi *= 2
	}
	lo = hi / 2
	for lo+1 < hi {
		mid := (lo + hi) / 2
		if fits(mid) {
			lo = mid
			continue
		}
		hi = mid
	}
	return lo, hi
}

func optionFeeds(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("orderbook_delta.BinanceUM_Option_BTCUSDT-260925-%06d-C", 100000+i)
	}
	return out
}

func perms(t *testing.T, session string, feeds []string) sts.Permissions {
	t.Helper()
	spec := &sessionv1.SessionSpec{
		Accounts: []*sessionv1.AccountDecl{{ApiId: 7, Name: "a"}},
	}
	for _, feed := range feeds {
		spec.Feeds = append(spec.Feeds, &sessionv1.FeedDecl{Feed: feed, MdInstance: "from-the-spec"})
	}
	got, err := sts.SessionPermissions(session, spec, func(string) string { return "md-tw-1" })
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func mustSign(t *testing.T, session string, feeds []string, account string, signer nkeys.KeyPair) (string, string) {
	t.Helper()
	token, seed, err := SignSession(session, perms(t, session, feeds), account, signer)
	if err != nil {
		t.Fatal(err)
	}
	return token, seed
}

func startServer(t *testing.T) (url, account string, signer nkeys.KeyPair) {
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
	want := fmt.Sprintf("max_control_line: %d\n", natsdev.MaxControlLine)
	if !strings.Contains(conf, want) {
		t.Fatalf("config missing %q\n%s", want, conf)
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
	raw, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(lay.Manifest.SigningKeyFile)))
	if err != nil {
		t.Fatal(err)
	}
	kp, err := nkeys.FromSeed(bytes.TrimSpace(raw))
	if err != nil {
		t.Fatal(err)
	}
	return ns.ClientURL(), lay.Manifest.Account, kp
}

func dial(t *testing.T, url, token, seed, inbox string, extra ...nats.Option) (*nats.Conn, error) {
	t.Helper()
	kp, err := nkeys.FromSeed([]byte(seed))
	if err != nil {
		t.Fatal(err)
	}
	opts := []nats.Option{
		nats.UserJWT(
			func() (string, error) { return token, nil },
			func(nonce []byte) ([]byte, error) { return kp.Sign(nonce) },
		),
		nats.CustomInboxPrefix(inbox),
		nats.NoReconnect(),
		nats.Timeout(opTimeout),
		nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {}),
	}
	opts = append(opts, extra...)
	return nats.Connect(url, opts...)
}

type tapConn struct {
	net.Conn
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *tapConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if n > 0 {
		c.mu.Lock()
		c.buf.Write(p[:n])
		c.mu.Unlock()
	}
	return n, err
}

func (c *tapConn) sent() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}

type recDialer struct {
	net.Dialer
	mu   sync.Mutex
	taps []*tapConn
}

func (d *recDialer) Dial(network, address string) (net.Conn, error) {
	c, err := d.Dialer.Dial(network, address)
	if err != nil {
		return nil, err
	}
	tap := &tapConn{Conn: c}
	d.mu.Lock()
	d.taps = append(d.taps, tap)
	d.mu.Unlock()
	return tap, nil
}

func captureConnectArg(t *testing.T, url, token, seed, inbox string) string {
	t.Helper()
	d := &recDialer{Dialer: net.Dialer{Timeout: opTimeout}}
	nc, err := dial(t, url, token, seed, inbox, nats.SetCustomDialer(d))
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.taps) != 1 {
		t.Fatalf("%d connections", len(d.taps))
	}
	stream := d.taps[0].sent()
	const prefix = "CONNECT "
	i := strings.Index(stream, prefix)
	if i < 0 {
		t.Fatalf("no CONNECT in %q", stream)
	}
	rest := stream[i+len(prefix):]
	j := strings.Index(rest, "\r\n")
	if j < 0 {
		t.Fatalf("CONNECT is not terminated: %q", rest)
	}
	return rest[:j]
}

func controlLineExceeded(err error) bool {
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "maximum control line") || strings.Contains(s, "max control line")
}
