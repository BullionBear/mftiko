package natsdev

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

const (
	opTimeout      = 2 * time.Second
	checkSessionID = "smoke"

	// checkAPIID is the account the session caller is bound to and whose account worker the
	// check signs. otherAPIID is an account it is not bound to.
	checkAPIID = 7
	otherAPIID = 8
)

// Check dials url with the credentials in dir and checks the three D417 cases:
// an MD publish of md.v1.f is delivered, request/reply uses the caller's inbox prefix,
// and publish/subscribe outside the allow lists is denied. The legal caller is an
// ephemeral session user signed in memory with the account signing key. The TD
// account workers are signed the same way (D611). None of these credentials are written.
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
	for _, u := range lay.Manifest.TD {
		if err := checkTD(url, lay, u); err != nil {
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

	if err := deliver(md, "md.v1.f."+u.Inst+".trade.BTC", sess); err != nil {
		return err
	}
	if err := allowSub(md, u.InboxPrefix+".>"); err != nil {
		return fmt.Errorf("natsdev: md %s own inbox: %w", u.Inst, err)
	}
	if err := allowSub(md, "sym.v1.changed.>"); err != nil {
		return fmt.Errorf("natsdev: md %s sym.v1.changed: %w", u.Inst, err)
	}
	if err := denyOutside(url, lay, u, PlaneMD); err != nil {
		return err
	}
	other := foreignInst(u.Inst)
	if err := denyOnce(url, lay, u, "publish other md.v1.f", func(nc *nats.Conn) error {
		return nc.Publish("md.v1.f."+other+".trade.BTC", []byte("x"))
	}); err != nil {
		return err
	}
	sess, err = dialCaller(url, caller)
	if err != nil {
		return err
	}
	err = expectDenied(sess, func(nc *nats.Conn) error {
		_, err := nc.SubscribeSync("md.v1.f." + other + ".>")
		return err
	})
	sess.Close()
	if err != nil {
		return fmt.Errorf("natsdev: session subscribe other md.v1.f: %w", err)
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
	return roundTrip(url, caller, md, "md.v1.view."+u.Inst+".trade.BTC", "")
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
	// The exact subject md.v1.fetch has no venue token, so the user may not subscribe to it (D418, D456).
	if err := denyOnce(url, lay, u, "subscribe md.v1.fetch", func(nc *nats.Conn) error {
		_, err := nc.SubscribeSync("md.v1.fetch")
		return err
	}); err != nil {
		return err
	}
	if err := allowSub(fetch, "sym.v1.changed.>"); err != nil {
		return fmt.Errorf("natsdev: fetch %s sym.v1.changed: %w", u.Inst, err)
	}
	// natsx Serve uses the subject as the queue name when the caller leaves it empty.
	// md.v1.fetch.Binance is one venue subject; the venue token is only an example.
	return roundTrip(url, caller, fetch, "md.v1.fetch.Binance", "md.v1.fetch.Binance")
}

// checkTD checks the TD agent's user and the account worker of checkAPIID (D611). The agent
// reads every account's td.evt and serves nothing. The account worker serves td.order and
// td.view of its own account, publishes only its own td.evt, and reaches nothing of another
// account: another worker serving the same account would send each order twice.
func checkTD(url string, lay Layout, u User) error {
	caller, err := newCaller(lay, u.Inst)
	if err != nil {
		return err
	}
	worker, err := newAccountWorker(lay, checkAPIID)
	if err != nil {
		return err
	}
	agent, err := dialCreds(url, filepath.Join(lay.Dir, filepath.FromSlash(u.Creds)), u.InboxPrefix)
	if err != nil {
		return fmt.Errorf("natsdev: td %s: %w", u.Inst, err)
	}
	defer agent.Close()
	if err := allowSub(agent, u.InboxPrefix+".>"); err != nil {
		return fmt.Errorf("natsdev: td %s own inbox: %w", u.Inst, err)
	}
	if err := allowSub(agent, "sym.v1.changed.>"); err != nil {
		return fmt.Errorf("natsdev: td %s sym.v1.changed: %w", u.Inst, err)
	}
	if err := denyOutside(url, lay, u, PlaneTD); err != nil {
		return err
	}
	own := strconv.Itoa(checkAPIID)
	for _, d := range []denial{
		{"publish td.evt", publishTo("td.evt." + own)},
		{"publish td.order", publishTo("td.order." + own)},
		{"subscribe td.order", subscribeTo("td.order." + own)},
		{"subscribe td.view", subscribeTo("td.view." + own)},
	} {
		if err := denyOnce(url, lay, u, d.what, d.do); err != nil {
			return err
		}
	}

	w, err := dialCaller(url, worker)
	if err != nil {
		return fmt.Errorf("natsdev: account worker %s: %w", own, err)
	}
	defer w.Close()
	if err := allowSub(w, worker.inbox+".>"); err != nil {
		return fmt.Errorf("natsdev: account worker %s own inbox: %w", own, err)
	}
	if err := allowSub(w, "sym.v1.changed.>"); err != nil {
		return fmt.Errorf("natsdev: account worker %s sym.v1.changed: %w", own, err)
	}
	sess, err := dialCaller(url, caller)
	if err != nil {
		return err
	}
	defer sess.Close()
	// The account's events reach the bound session and the agent, which stores fills (D601).
	if err := deliver(w, "td.evt."+own, sess, agent); err != nil {
		return err
	}
	if err := denyWorker(url, worker); err != nil {
		return err
	}
	if err := roundTrip(url, caller, w, "td.order."+own, ""); err != nil {
		return err
	}
	return roundTrip(url, caller, w, "td.view."+own, "")
}

// denyWorker checks that the account worker reaches nothing outside its own account: no
// other account's subjects, no account's orders as a caller, and no other reply prefix.
func denyWorker(url string, worker *memUser) error {
	other := strconv.Itoa(otherAPIID)
	otherInbox, err := TDAccountInbox(otherAPIID)
	if err != nil {
		return err
	}
	agentInbox, err := InboxPrefix(PlaneTD)
	if err != nil {
		return err
	}
	session, err := sessionInbox(checkSessionID)
	if err != nil {
		return err
	}
	for _, d := range []denial{
		{"publish other td.evt", publishTo("td.evt." + other)},
		{"subscribe other td.order", subscribeTo("td.order." + other)},
		{"subscribe other td.view", subscribeTo("td.view." + other)},
		{"subscribe td.order.>", subscribeTo("td.order.>")},
		{"subscribe td.evt.>", subscribeTo("td.evt.>")},
		{"publish other td.order", publishTo("td.order." + other)},
		{"subscribe other account inbox", subscribeTo(otherInbox + ".>")},
		{"subscribe td agent inbox", subscribeTo(agentInbox + ".>")},
		{"subscribe session inbox", subscribeTo(session + ".>")},
		{"subscribe _INBOX.>", subscribeTo("_INBOX.>")},
		{"publish _INBOX", publishTo("_INBOX.not-allowed")},
	} {
		nc, err := dialCaller(url, worker)
		if err != nil {
			return err
		}
		err = expectDenied(nc, d.do)
		nc.Close()
		if err != nil {
			return fmt.Errorf("natsdev: %s %s: %w", worker.name, d.what, err)
		}
	}
	return nil
}

// denial is one publish or subscribe the user must be refused.
type denial struct {
	what string
	do   func(*nats.Conn) error
}

func publishTo(subject string) func(*nats.Conn) error {
	return func(nc *nats.Conn) error { return nc.Publish(subject, []byte("x")) }
}

func subscribeTo(subject string) func(*nats.Conn) error {
	return func(nc *nats.Conn) error { _, err := nc.SubscribeSync(subject); return err }
}

// denyOutside checks the inbox subjects a plane user must not touch: the other
// planes' prefixes, an account worker's prefix, this check's session inbox, and
// the default _INBOX.>.
func denyOutside(url string, lay Layout, u User, plane string) error {
	session, err := sessionInbox(checkSessionID)
	if err != nil {
		return err
	}
	account, err := TDAccountInbox(checkAPIID)
	if err != nil {
		return err
	}
	denies := []denial{
		{"subscribe _INBOX.>", subscribeTo("_INBOX.>")},
		{"publish _INBOX", publishTo("_INBOX.not-allowed")},
		{"subscribe session inbox", subscribeTo(session + ".>")},
		{"publish session inbox", publishTo(session + ".not-allowed")},
		{"subscribe account worker inbox", subscribeTo(account + ".>")},
		{"publish account worker inbox", publishTo(account + ".not-allowed")},
	}
	for _, p := range []string{PlaneMD, PlaneFetch, PlaneTD} {
		if p == plane {
			continue
		}
		other, err := InboxPrefix(p)
		if err != nil {
			return err
		}
		denies = append(denies,
			denial{"subscribe " + p + " inbox", subscribeTo(other + ".>")},
			denial{"publish " + p + " inbox", publishTo(other + ".not-allowed")},
		)
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

// deliver publishes one message on subject from pub and checks that every subscriber gets it.
func deliver(pub *nats.Conn, subject string, subscribers ...*nats.Conn) error {
	subs := make([]*nats.Subscription, 0, len(subscribers))
	for _, nc := range subscribers {
		sub, err := nc.SubscribeSync(subject)
		if err != nil {
			return err
		}
		defer func() { _ = sub.Unsubscribe() }()
		if err := flushClean(nc); err != nil {
			return fmt.Errorf("natsdev: subscribe %s: %w", subject, err)
		}
		subs = append(subs, sub)
	}
	payload := []byte("tape")
	if err := pub.Publish(subject, payload); err != nil {
		return err
	}
	if err := flushClean(pub); err != nil {
		return fmt.Errorf("natsdev: publish %s: %w", subject, err)
	}
	for _, sub := range subs {
		msg, err := sub.NextMsg(opTimeout)
		if err != nil {
			return fmt.Errorf("natsdev: %s was not delivered: %w", subject, err)
		}
		if string(msg.Data) != string(payload) {
			return fmt.Errorf("natsdev: %s payload mismatch", subject)
		}
	}
	return nil
}

func roundTrip(url string, caller *memUser, svc *nats.Conn, subject, queue string) error {
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

func dialCaller(url string, c *memUser) (*nats.Conn, error) {
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

// memUser is a user signed in memory with the account signing key: the D906 session the checks
// use as a legal client, or a D611 account worker. Its credentials are not written.
type memUser struct {
	name  string
	jwt   string
	kp    nkeys.KeyPair
	inbox string
}

func newCaller(lay Layout, inst string) (*memUser, error) {
	inbox, err := sessionInbox(checkSessionID)
	if err != nil {
		return nil, err
	}
	account := strconv.Itoa(checkAPIID)
	// Publish is only what this check calls. Subscribe is the feed, the bound account's events,
	// and this session's inbox. No response permission: the caller does not answer requests.
	return signMem(lay, "session-"+checkSessionID, inbox, []string{
		"md.v1.view." + inst + ".>",
		"md.v1.fetch.>",
		"td.order." + account,
		"td.view." + account,
	}, []string{
		"md.v1.f." + inst + ".>",
		"td.evt." + account,
		inbox + ".>",
	}, false)
}

// newAccountWorker signs the account worker of apiID as the TD agent does at spawn (D611).
func newAccountWorker(lay Layout, apiID int64) (*memUser, error) {
	// The same Signer the TD agent uses (D611).
	signer, err := LoadSigner(lay.Manifest.Account, filepath.Join(lay.Dir, filepath.FromSlash(lay.Manifest.SigningKeyFile)))
	if err != nil {
		return nil, err
	}
	token, seed, inbox, err := signer.AccountWorker(apiID)
	if err != nil {
		return nil, err
	}
	kp, err := nkeys.FromSeed([]byte(seed))
	if err != nil {
		return nil, err
	}
	return &memUser{name: "td-account-" + strconv.FormatInt(apiID, 10), jwt: token, kp: kp, inbox: inbox}, nil
}

func signMem(lay Layout, name, inbox string, pub, sub []string, resp bool) (*memUser, error) {
	kp, err := nkeys.CreateUser()
	if err != nil {
		return nil, err
	}
	userPub, err := kp.PublicKey()
	if err != nil {
		return nil, err
	}
	signPath := filepath.Join(lay.Dir, filepath.FromSlash(lay.Manifest.SigningKeyFile))
	signer, err := readKey(signPath, keyAccountKind)
	if err != nil {
		return nil, fmt.Errorf("natsdev: signing key: %w", err)
	}
	token, err := encodeUser(userPub, name, pub, sub, resp, lay.Manifest.Account, signer)
	if err != nil {
		return nil, err
	}
	return &memUser{name: name, jwt: token, kp: kp, inbox: inbox}, nil
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
