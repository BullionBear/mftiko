package natsdev

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

const (
	opTimeout      = 2 * time.Second
	checkSessionID = "smoke"
)

// Check dials url with the credentials in dir and checks the three D417 cases:
// an MD publish of md.f is delivered, request/reply uses the caller's inbox prefix,
// and publish/subscribe outside the allow lists is denied. The legal caller is an
// ephemeral session user signed in memory with the account signing key. Its
// credentials are not written.
func Check(dir, url string) error {
	if url == "" {
		return errors.New("natsdev: empty url")
	}
	lay, err := Load(dir)
	if err != nil {
		return err
	}
	if err := rejectAnonymous(url); err != nil {
		return err
	}
	for _, u := range lay.Manifest.MD {
		if err := checkMD(url, lay, u); err != nil {
			return err
		}
	}
	for _, u := range lay.Manifest.Fetch {
		if err := checkFetch(url, lay, u); err != nil {
			return err
		}
	}
	return nil
}

func checkMD(url string, lay Layout, u User) error {
	caller, err := newCaller(lay, u.Inst)
	if err != nil {
		return err
	}
	md, err := dialCreds(url, filepath.Join(lay.Dir, filepath.FromSlash(u.Creds)), u.InboxPrefix)
	if err != nil {
		return fmt.Errorf("natsdev: md %s: %w", u.Inst, err)
	}
	defer md.Close()
	sess, err := dialCaller(url, caller)
	if err != nil {
		return err
	}
	defer sess.Close()

	if err := deliverFeed(sess, md, u.Inst); err != nil {
		return err
	}
	if err := allowSub(md, u.InboxPrefix+".>"); err != nil {
		return fmt.Errorf("natsdev: md %s own inbox: %w", u.Inst, err)
	}
	if err := denyOutside(url, lay, u, PlaneMD); err != nil {
		return err
	}
	other := foreignInst(u.Inst)
	if err := denyOnce(url, lay, u, "publish other md.f", func(nc *nats.Conn) error {
		return nc.Publish("md.f."+other+".trade.BTC", []byte("x"))
	}); err != nil {
		return err
	}
	sess, err = dialCaller(url, caller)
	if err != nil {
		return err
	}
	err = expectDenied(sess, func(nc *nats.Conn) error {
		_, err := nc.SubscribeSync("md.f." + other + ".>")
		return err
	})
	sess.Close()
	if err != nil {
		return fmt.Errorf("natsdev: session subscribe other md.f: %w", err)
	}
	sess, err = dialCaller(url, caller)
	if err != nil {
		return err
	}
	err = expectDenied(sess, func(nc *nats.Conn) error {
		_, err := nc.SubscribeSync(u.InboxPrefix + ".>")
		return err
	})
	sess.Close()
	if err != nil {
		return fmt.Errorf("natsdev: session subscribe md inbox: %w", err)
	}
	return roundTrip(url, caller, md, "md.view."+u.Inst+".trade.BTC", "")
}

func checkFetch(url string, lay Layout, u User) error {
	caller, err := newCaller(lay, u.Inst)
	if err != nil {
		return err
	}
	fetch, err := dialCreds(url, filepath.Join(lay.Dir, filepath.FromSlash(u.Creds)), u.InboxPrefix)
	if err != nil {
		return fmt.Errorf("natsdev: fetch %s: %w", u.Inst, err)
	}
	defer fetch.Close()
	if err := allowSub(fetch, u.InboxPrefix+".>"); err != nil {
		return fmt.Errorf("natsdev: fetch %s own inbox: %w", u.Inst, err)
	}
	if err := denyOutside(url, lay, u, PlaneFetch); err != nil {
		return err
	}
	// natsx Serve uses the subject as the queue name when the caller leaves it empty.
	return roundTrip(url, caller, fetch, "md.fetch", "md.fetch")
}

// denyOutside checks the inbox subjects a plane user must not touch: the other
// plane's prefix, this check's session inbox, and the default _INBOX.>.
func denyOutside(url string, lay Layout, u User, plane string) error {
	otherPlane := PlaneFetch
	if plane == PlaneFetch {
		otherPlane = PlaneMD
	}
	other, err := InboxPrefix(otherPlane)
	if err != nil {
		return err
	}
	session, err := sessionInbox(checkSessionID)
	if err != nil {
		return err
	}
	denies := []struct {
		what string
		do   func(*nats.Conn) error
	}{
		{"subscribe _INBOX.>", func(nc *nats.Conn) error { _, err := nc.SubscribeSync("_INBOX.>"); return err }},
		{"publish _INBOX", func(nc *nats.Conn) error { return nc.Publish("_INBOX.not-allowed", []byte("x")) }},
		{"subscribe other plane", func(nc *nats.Conn) error { _, err := nc.SubscribeSync(other + ".>"); return err }},
		{"publish other plane", func(nc *nats.Conn) error { return nc.Publish(other+".not-allowed", []byte("x")) }},
		{"subscribe session inbox", func(nc *nats.Conn) error { _, err := nc.SubscribeSync(session + ".>"); return err }},
		{"publish session inbox", func(nc *nats.Conn) error { return nc.Publish(session+".not-allowed", []byte("x")) }},
	}
	for _, d := range denies {
		if err := denyOnce(url, lay, u, d.what, d.do); err != nil {
			return err
		}
	}
	return nil
}

func denyOnce(url string, lay Layout, u User, what string, do func(*nats.Conn) error) error {
	nc, err := dialCreds(url, filepath.Join(lay.Dir, filepath.FromSlash(u.Creds)), u.InboxPrefix)
	if err != nil {
		return err
	}
	err = expectDenied(nc, do)
	nc.Close()
	if err != nil {
		return fmt.Errorf("natsdev: %s %s %s: %w", u.Name, u.Inst, what, err)
	}
	return nil
}

func deliverFeed(sess, md *nats.Conn, inst string) error {
	subject := "md.f." + inst + ".trade.BTC"
	sub, err := sess.SubscribeSync(subject)
	if err != nil {
		return err
	}
	if err := flushClean(sess); err != nil {
		return fmt.Errorf("natsdev: session subscribe %s: %w", subject, err)
	}
	payload := []byte("tape")
	if err := md.Publish(subject, payload); err != nil {
		return err
	}
	if err := flushClean(md); err != nil {
		return fmt.Errorf("natsdev: md publish %s: %w", subject, err)
	}
	msg, err := sub.NextMsg(opTimeout)
	if err != nil {
		return fmt.Errorf("natsdev: md.f was not delivered: %w", err)
	}
	if string(msg.Data) != string(payload) {
		return errors.New("natsdev: md.f payload mismatch")
	}
	return nil
}

func roundTrip(url string, caller *sessionCaller, svc *nats.Conn, subject, queue string) error {
	bad := make(chan string, 1)
	handler := func(m *nats.Msg) {
		if !strings.HasPrefix(m.Reply, caller.inbox+".") {
			select {
			case bad <- m.Reply:
			default:
			}
		}
		_ = m.Respond([]byte("ok"))
	}
	var err error
	if queue == "" {
		_, err = svc.Subscribe(subject, handler)
	} else {
		_, err = svc.QueueSubscribe(subject, queue, handler)
	}
	if err != nil {
		return err
	}
	if err := flushClean(svc); err != nil {
		return fmt.Errorf("natsdev: subscribe %s: %w", subject, err)
	}
	sess, err := dialCaller(url, caller)
	if err != nil {
		return err
	}
	defer sess.Close()
	msg, err := sess.Request(subject, []byte("q"), opTimeout)
	if err != nil {
		return fmt.Errorf("natsdev: request %s: %w", subject, err)
	}
	if string(msg.Data) != "ok" {
		return fmt.Errorf("natsdev: request %s: unexpected reply", subject)
	}
	select {
	case reply := <-bad:
		return fmt.Errorf("natsdev: request %s: reply subject %q, want prefix %s", subject, reply, caller.inbox)
	default:
	}
	return nil
}

func allowSub(nc *nats.Conn, subject string) error {
	if _, err := nc.SubscribeSync(subject); err != nil {
		return err
	}
	return flushClean(nc)
}

func expectDenied(nc *nats.Conn, do func(*nats.Conn) error) error {
	if err := do(nc); err != nil {
		if errors.Is(err, nats.ErrPermissionViolation) {
			return nil
		}
		return err
	}
	if err := nc.FlushTimeout(opTimeout); err != nil {
		return err
	}
	if !errors.Is(nc.LastError(), nats.ErrPermissionViolation) {
		return fmt.Errorf("expected a permission violation, got %v", nc.LastError())
	}
	return nil
}

func flushClean(nc *nats.Conn) error {
	if err := nc.FlushTimeout(opTimeout); err != nil {
		return err
	}
	if err := nc.LastError(); err != nil {
		return err
	}
	return nil
}

func rejectAnonymous(url string) error {
	nc, err := nats.Connect(url, connectOpts()...)
	if err != nil {
		return nil
	}
	nc.Close()
	return errors.New("natsdev: anonymous connect succeeded")
}

func dialCreds(url, creds, inbox string) (*nats.Conn, error) {
	opts := append(connectOpts(), nats.UserCredentials(creds), nats.CustomInboxPrefix(inbox))
	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, err
	}
	return nc, nil
}

func dialCaller(url string, c *sessionCaller) (*nats.Conn, error) {
	opts := append(connectOpts(),
		nats.UserJWT(
			func() (string, error) { return c.jwt, nil },
			func(nonce []byte) ([]byte, error) { return c.kp.Sign(nonce) },
		),
		nats.CustomInboxPrefix(c.inbox),
	)
	return nats.Connect(url, opts...)
}

func connectOpts() []nats.Option {
	return []nats.Option{
		nats.NoReconnect(),
		nats.Timeout(opTimeout),
		nats.Name("mftiko-nats-dev"),
		// Expected denials arrive as asynchronous errors. LastError still records them.
		// The default handler would print those denials on stderr.
		nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {}),
	}
}

// sessionCaller is the in-memory D906 user the checks use as a legal client.
type sessionCaller struct {
	jwt   string
	kp    nkeys.KeyPair
	inbox string
}

func newCaller(lay Layout, inst string) (*sessionCaller, error) {
	inbox, err := sessionInbox(checkSessionID)
	if err != nil {
		return nil, err
	}
	kp, err := nkeys.CreateUser()
	if err != nil {
		return nil, err
	}
	pub, err := kp.PublicKey()
	if err != nil {
		return nil, err
	}
	signPath := filepath.Join(lay.Dir, filepath.FromSlash(lay.Manifest.SigningKeyFile))
	signer, err := readKey(signPath, keyAccountKind)
	if err != nil {
		return nil, fmt.Errorf("natsdev: signing key: %w", err)
	}
	// Publish is only what this check calls. Subscribe is the feed plus this session's inbox.
	// No response permission: the caller does not answer requests.
	token, err := encodeUser(pub, "session-"+checkSessionID, []string{
		"md.view." + inst + ".>",
		"md.fetch",
	}, []string{
		"md.f." + inst + ".>",
		inbox + ".>",
	}, false, lay.Manifest.Account, signer)
	if err != nil {
		return nil, err
	}
	return &sessionCaller{jwt: token, kp: kp, inbox: inbox}, nil
}

func sessionInbox(sessionID string) (string, error) {
	if !tokenOK(sessionID) {
		return "", errors.New("natsdev: session id is not a single token")
	}
	return "_INBOX_" + sessionID, nil
}

func foreignInst(inst string) string {
	if inst == "other" {
		return "elsewhere"
	}
	return "other"
}
