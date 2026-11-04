package natsdev

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"

	"github.com/BullionBear/mftiko/contract/wire/tdsubject"
)

const (
	opTimeout      = 2 * time.Second
	checkSessionID = "smoke"

	// checkAPIID is the account the session caller is bound to and whose account worker the
	// check signs. otherAPIID is an account it is not bound to.
	// checkTDInst is that binding's td_instance. otherTDInst is a second instance of the
	// same api_id (E21-D045).
	checkAPIID  = 7
	otherAPIID  = 8
	checkTDInst = "td-a"
	otherTDInst = "td-b"
)

// Check dials url with the credentials in dir and checks the three E04-D017 cases:
// an MD publish of md.v1.f is delivered, request/reply uses the caller's inbox prefix,
// and publish/subscribe outside the allow lists is denied. The legal caller is an
// ephemeral session user signed in memory with the account signing key. The TD
// account workers are signed the same way (E06-D011). None of these credentials are written.
// The TD agent has no NATS user (E06-D039): a manifest entry, a td- creds file, a user-td-
// seed, or a user that subscribes to td.evt.> fails the check before any dial.
func Check(dir, url string) error {
	if url == "" {
		return errors.New("natsdev: empty url")
	}
	lay, err := Load(dir)
	if err != nil {
		return err
	}
	if err := checkNoTDAgent(lay); err != nil {
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
	for _, u := range lay.Manifest.Sym {
		if err := checkSym(url, lay, u); err != nil {
			return err
		}
	}
	if err := checkAccountWorker(url, lay); err != nil {
		return err
	}
	return checkTwoInstances(url, lay)
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
	// The exact subject md.v1.fetch has no venue token, so the user may not subscribe to it (E04-D018, E04-D056).
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

// checkNoTDAgent fails when dir still has a TD agent NATS user (E06-D039). Account-worker
// users are signed in memory and are not files; tdAccountPerms is not involved.
func checkNoTDAgent(lay Layout) error {
	if len(lay.Manifest.TD) != 0 {
		return errors.New("natsdev: TD agent has no NATS user")
	}
	keyEntries, err := os.ReadDir(filepath.Join(lay.Dir, dirKeys))
	if err != nil {
		return err
	}
	for _, e := range keyEntries {
		name := e.Name()
		if !e.IsDir() && strings.HasPrefix(name, "user-td-") && strings.HasSuffix(name, ".nk") {
			return fmt.Errorf("natsdev: TD agent has no NATS user (%s)", name)
		}
	}
	credsDir := filepath.Join(lay.Dir, dirCreds)
	credEntries, err := os.ReadDir(credsDir)
	if err != nil {
		return err
	}
	for _, e := range credEntries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".creds") {
			continue
		}
		if strings.HasPrefix(name, "td-") {
			return fmt.Errorf("natsdev: TD agent has no NATS user (%s)", name)
		}
		body, err := os.ReadFile(filepath.Join(credsDir, name))
		if err != nil {
			return err
		}
		token, err := jwt.ParseDecoratedJWT(body)
		if err != nil {
			return fmt.Errorf("natsdev: %s: %w", name, err)
		}
		uc, err := jwt.DecodeUserClaims(token)
		if err != nil {
			return fmt.Errorf("natsdev: %s: %w", name, err)
		}
		if tdAgentUserName(uc.Name) || subscribesAllTDEvents([]string(uc.Sub.Allow)) {
			return fmt.Errorf("natsdev: TD agent has no NATS user (%s)", uc.Name)
		}
	}
	return nil
}

func tdAgentUserName(name string) bool {
	// td-account-{td_instance}-{api_id} is an account worker. Those users are not files either (E06-D011).
	return strings.HasPrefix(name, "td-")
}

func subscribesAllTDEvents(allow []string) bool {
	for _, subject := range allow {
		if subject == "td.evt.>" {
			return true
		}
	}
	return false
}

// checkAccountWorker checks the account worker of checkAPIID (E06-D011). It serves td.order and
// td.view of its own account, publishes only its own td.evt, and reaches nothing of another
// account: another worker serving the same account would send each order twice. The TD agent
// is not a subscriber (E06-D038, E06-D039).
func checkAccountWorker(url string, lay Layout) error {
	caller, err := newCaller(lay, lay.Manifest.MD[0].Inst)
	if err != nil {
		return err
	}
	worker, err := newAccountWorker(lay, checkTDInst, checkAPIID)
	if err != nil {
		return err
	}
	evt, err := tdsubject.Evt(checkTDInst, checkAPIID)
	if err != nil {
		return err
	}
	order, err := tdsubject.Order(checkTDInst, checkAPIID)
	if err != nil {
		return err
	}
	view, err := tdsubject.View(checkTDInst, checkAPIID)
	if err != nil {
		return err
	}
	label := checkTDInst + "/" + strconv.FormatInt(checkAPIID, 10)
	w, err := dialCaller(url, worker)
	if err != nil {
		return fmt.Errorf("natsdev: account worker %s: %w", label, err)
	}
	defer w.Close()
	if err := allowSub(w, worker.inbox+".>"); err != nil {
		return fmt.Errorf("natsdev: account worker %s own inbox: %w", label, err)
	}
	if err := allowSub(w, "sym.v1.changed.>"); err != nil {
		return fmt.Errorf("natsdev: account worker %s sym.v1.changed: %w", label, err)
	}
	sess, err := dialCaller(url, caller)
	if err != nil {
		return err
	}
	defer sess.Close()
	// The account's events reach the bound session. Nobody stores them (E06-D038).
	if err := deliver(w, evt, sess); err != nil {
		return err
	}
	if err := denyWorker(url, worker); err != nil {
		return err
	}
	if err := roundTrip(url, caller, w, order, ""); err != nil {
		return err
	}
	return roundTrip(url, caller, w, view, "")
}

// checkTwoInstances is E21-D045: api_id checkAPIID on checkTDInst and otherTDInst.
// One request is received only by the worker of that subject. An event on the other
// instance is not delivered to a session subscribed only to the first. That session
// cannot publish or subscribe the other instance.
func checkTwoInstances(url string, lay Layout) error {
	aUser, err := newAccountWorker(lay, checkTDInst, checkAPIID)
	if err != nil {
		return err
	}
	bUser, err := newAccountWorker(lay, otherTDInst, checkAPIID)
	if err != nil {
		return err
	}
	orderA, err := tdsubject.Order(checkTDInst, checkAPIID)
	if err != nil {
		return err
	}
	orderB, err := tdsubject.Order(otherTDInst, checkAPIID)
	if err != nil {
		return err
	}
	evtA, err := tdsubject.Evt(checkTDInst, checkAPIID)
	if err != nil {
		return err
	}
	evtB, err := tdsubject.Evt(otherTDInst, checkAPIID)
	if err != nil {
		return err
	}
	wa, err := dialCaller(url, aUser)
	if err != nil {
		return err
	}
	defer wa.Close()
	wb, err := dialCaller(url, bUser)
	if err != nil {
		return err
	}
	defer wb.Close()
	var na, nb atomic.Int32
	if _, err := wa.Subscribe(orderA, func(m *nats.Msg) {
		na.Add(1)
		if m.Reply != "" {
			_ = m.Respond([]byte("a"))
		}
	}); err != nil {
		return err
	}
	if _, err := wb.Subscribe(orderB, func(m *nats.Msg) {
		nb.Add(1)
		if m.Reply != "" {
			_ = m.Respond([]byte("b"))
		}
	}); err != nil {
		return err
	}
	if err := flushClean(wa); err != nil {
		return fmt.Errorf("natsdev: two instances: %w", err)
	}
	if err := flushClean(wb); err != nil {
		return fmt.Errorf("natsdev: two instances: %w", err)
	}
	caller, err := newCaller(lay, lay.Manifest.MD[0].Inst)
	if err != nil {
		return err
	}
	sess, err := dialCaller(url, caller)
	if err != nil {
		return err
	}
	msg, err := sess.Request(orderA, []byte("q"), opTimeout)
	if err != nil {
		sess.Close()
		return fmt.Errorf("natsdev: two instances: %w", err)
	}
	if string(msg.Data) != "a" {
		sess.Close()
		return fmt.Errorf("natsdev: two instances: reply %q", msg.Data)
	}
	time.Sleep(200 * time.Millisecond)
	if na.Load() != 1 || nb.Load() != 0 {
		sess.Close()
		return fmt.Errorf("natsdev: two instances: order counts a=%d b=%d", na.Load(), nb.Load())
	}
	sub, err := sess.SubscribeSync(evtA)
	if err != nil {
		sess.Close()
		return err
	}
	if err := flushClean(sess); err != nil {
		sess.Close()
		return fmt.Errorf("natsdev: two instances: %w", err)
	}
	if err := wa.Publish(evtA, []byte("only-a")); err != nil {
		sess.Close()
		return err
	}
	if err := wb.Publish(evtB, []byte("only-b")); err != nil {
		sess.Close()
		return err
	}
	if err := flushClean(wa); err != nil || flushClean(wb) != nil {
		sess.Close()
		return errors.New("natsdev: two instances: flush td.evt")
	}
	got, err := sub.NextMsg(opTimeout)
	if err != nil {
		sess.Close()
		return fmt.Errorf("natsdev: two instances: td.evt: %w", err)
	}
	if string(got.Data) != "only-a" {
		sess.Close()
		return fmt.Errorf("natsdev: two instances: td.evt %q", got.Data)
	}
	if _, err := sub.NextMsg(300 * time.Millisecond); err == nil {
		sess.Close()
		return errors.New("natsdev: two instances: td.evt interleaved")
	}
	sess.Close()
	for _, d := range []denial{
		{"publish other instance td.order", publishTo(orderB)},
		{"subscribe other instance td.evt", subscribeTo(evtB)},
	} {
		nc, err := dialCaller(url, caller)
		if err != nil {
			return err
		}
		err = expectDenied(nc, d.do)
		nc.Close()
		if err != nil {
			return fmt.Errorf("natsdev: session %s: %w", d.what, err)
		}
	}
	return nil
}

// denyWorker checks that the account worker reaches nothing outside its own account: no
// other account's subjects, no account's orders as a caller, and no other reply prefix.
func denyWorker(url string, worker *memUser) error {
	otherEvt, err := tdsubject.Evt(checkTDInst, otherAPIID)
	if err != nil {
		return err
	}
	otherOrder, err := tdsubject.Order(checkTDInst, otherAPIID)
	if err != nil {
		return err
	}
	otherView, err := tdsubject.View(checkTDInst, otherAPIID)
	if err != nil {
		return err
	}
	instEvt, err := tdsubject.Evt(otherTDInst, checkAPIID)
	if err != nil {
		return err
	}
	instOrder, err := tdsubject.Order(otherTDInst, checkAPIID)
	if err != nil {
		return err
	}
	instView, err := tdsubject.View(otherTDInst, checkAPIID)
	if err != nil {
		return err
	}
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
	old := "td.order." + strconv.FormatInt(checkAPIID, 10)
	for _, d := range []denial{
		{"publish other td.evt", publishTo(otherEvt)},
		{"subscribe other td.order", subscribeTo(otherOrder)},
		{"subscribe other td.view", subscribeTo(otherView)},
		{"publish other instance td.evt", publishTo(instEvt)},
		{"subscribe other instance td.order", subscribeTo(instOrder)},
		{"subscribe other instance td.view", subscribeTo(instView)},
		{"subscribe other instance td.evt", subscribeTo(instEvt)},
		{"publish other instance td.order", publishTo(instOrder)},
		{"publish old td.order", publishTo(old)},
		{"subscribe td.order.>", subscribeTo("td.order.>")},
		{"subscribe td.evt.>", subscribeTo("td.evt.>")},
		{"publish other td.order", publishTo(otherOrder)},
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

// checkSym checks the SYM user (E12-D079): its change notice reaches a session, a session's
// sym request is answered in the sym queue group, and it may not send requests, read its own
// notices, publish a feed, or touch any inbox.
func checkSym(url string, lay Layout, u User) error {
	caller, err := newCaller(lay, u.Inst)
	if err != nil {
		return err
	}
	sym, err := dialCreds(url, filepath.Join(lay.Dir, filepath.FromSlash(u.Creds)), u.InboxPrefix)
	if err != nil {
		return fmt.Errorf("natsdev: sym %s: %w", u.Inst, err)
	}
	defer sym.Close()
	sess, err := dialCaller(url, caller)
	if err != nil {
		return err
	}
	defer sess.Close()

	// Binance.Perp is one venue and category; the tokens are only an example.
	if err := deliver(sym, "sym.v1.changed.Binance.Perp", sess); err != nil {
		return err
	}
	if err := denyOutside(url, lay, u, PlaneSym); err != nil {
		return err
	}
	for _, d := range []denial{
		{"publish sym", publishTo("sym")},
		{"subscribe sym.v1.changed", subscribeTo("sym.v1.changed.>")},
		{"publish md.v1.f", publishTo("md.v1.f." + u.Inst + ".trade.BTC")},
	} {
		if err := denyOnce(url, lay, u, d.what, d.do); err != nil {
			return err
		}
	}
	return roundTrip(url, caller, sym, "sym", "sym")
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

// dialCreds connects with a creds file. An empty inbox keeps the client default: the SYM
// user has no inbox and sends no requests (E12-D079).
func dialCreds(url, creds, inbox string) (*nats.Conn, error) {
	opts := append(connectOpts(), nats.UserCredentials(creds))
	if inbox != "" {
		opts = append(opts, nats.CustomInboxPrefix(inbox))
	}
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

// memUser is a user signed in memory with the account signing key: the E09-D006 session the checks
// use as a legal client, or a E06-D011 account worker. Its credentials are not written.
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
	order, err := tdsubject.Order(checkTDInst, checkAPIID)
	if err != nil {
		return nil, err
	}
	view, err := tdsubject.View(checkTDInst, checkAPIID)
	if err != nil {
		return nil, err
	}
	evt, err := tdsubject.Evt(checkTDInst, checkAPIID)
	if err != nil {
		return nil, err
	}
	// Publish is only what this check calls. Subscribe is the feed, the bound account's events,
	// SYM's change notices, and this session's inbox. No response permission: the caller does
	// not answer requests. The TD subjects are this session's (td_instance, api_id) only (E21-D045).
	return signMem(lay, "session-"+checkSessionID, inbox, []string{
		"md.v1.view." + inst + ".>",
		"md.v1.fetch.>",
		order,
		view,
		"sym",
	}, []string{
		"md.v1.f." + inst + ".>",
		evt,
		"sym.v1.changed.>",
		inbox + ".>",
	}, false)
}

// newAccountWorker signs the account worker of (tdInstance, apiID) as the TD agent does at spawn (E06-D011).
func newAccountWorker(lay Layout, tdInstance string, apiID int64) (*memUser, error) {
	// The same Signer the TD agent uses (E06-D011).
	signer, err := LoadSigner(lay.Manifest.Account, filepath.Join(lay.Dir, filepath.FromSlash(lay.Manifest.SigningKeyFile)))
	if err != nil {
		return nil, err
	}
	token, seed, inbox, err := signer.AccountWorker(tdInstance, apiID)
	if err != nil {
		return nil, err
	}
	kp, err := nkeys.FromSeed([]byte(seed))
	if err != nil {
		return nil, err
	}
	return &memUser{name: "td-account-" + tdInstance + "-" + strconv.FormatInt(apiID, 10), jwt: token, kp: kp, inbox: inbox}, nil
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
