package symclient

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	symv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
)

const (
	// DefaultCheckInterval is how often the client compares revisions when no
	// notification arrived (PROTOCOL §7.3, §9.4). A reconnect checks as well;
	// the session worker calls Reconnect from its NATS reconnect handler.
	DefaultCheckInterval = 5 * time.Minute

	// subjectSym is the sym request subject (PROTOCOL §7).
	subjectSym = "sym"

	// refreshTries is how many times one refresh repeats when the table's
	// revision moves while the rows are being read. The stored revision is the
	// one observed after a pass that saw the same revision at both ends.
	refreshTries = 8

	notifyCap = 64
)

// ErrNotInInit is Resolve called from a hook other than OnInit or OnStart (E08-D004).
// Nothing was sent to SYM.
var ErrNotInInit = errors.New("symclient: only OnInit and OnStart may call Resolve")

// ErrUnknownUID is Info of a UID this session has not resolved (E08-D024, E08-D025).
// Info does not ask SYM and does not retry.
var ErrUnknownUID = errors.New("symclient: the instrument is not known to this session")

// errRevisionMoved means the table changed while its rows were being refetched.
var errRevisionMoved = errors.New("symclient: revision moved during refresh")

// Bus is how the client reaches SYM. Tests pass a fake. The session worker passes
// NATS. Request sends one protobuf and unmarshals one reply. A business not_found
// is an error wrapping symbol.ErrNotFound. Subscribe returns after the subscription
// is active; fn may run on another goroutine and must not block. The subscription
// ends when ctx is cancelled.
type Bus interface {
	Request(ctx context.Context, subject string, req, resp proto.Message) error
	Subscribe(ctx context.Context, subject string, fn func(subject string, data []byte)) error
}

// Options tunes Open. The zero value checks every DefaultCheckInterval and discards logs.
type Options struct {
	// CheckInterval is the gap between revision comparisons. Zero selects
	// DefaultCheckInterval. Negative is rejected.
	CheckInterval time.Duration
	// Log receives refresh failures. It may be called from the revision loop and from
	// Resolve at the same time. Nil discards them.
	Log func(string)
}

// Client is one session's view of the instruments it has resolved.
//
// Safe for concurrent use. Info never calls SYM. Resolve may, and only while
// AllowResolve is set. Close stops the subscription and the revision loop. It
// does not close the Bus.
type Client struct {
	bus      Bus
	interval time.Duration
	log      func(string)

	mu    sync.Mutex
	allow bool
	rows  map[uint32]symbol.Info
	byTab map[tableKey]map[uint32]struct{}
	revs  map[tableKey]revState
	subs  map[tableKey]struct{}

	// loadMu serializes Resolve's read with a refresh, so a notification cannot
	// record a revision that does not include the row Resolve just stored.
	loadMu sync.Mutex

	notify chan note
	kick   chan struct{}

	runCtx context.Context
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

type tableKey struct {
	venue string
	cat   symbol.Category
}

type revState struct {
	rev int64
	ok  bool
}

type note struct {
	key tableKey
	rev int64
}

// Open starts the revision loop. ctx is its lifetime. Resolve is refused until
// AllowResolve(true), which the session sets for the duration of OnInit and OnStart.
func Open(ctx context.Context, bus Bus, opt Options) (*Client, error) {
	if bus == nil {
		return nil, errors.New("symclient: nil connection")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	interval := opt.CheckInterval
	if interval == 0 {
		interval = DefaultCheckInterval
	}
	if interval < 0 {
		return nil, errors.New("symclient: negative check interval")
	}
	runCtx, cancel := context.WithCancel(ctx)
	c := &Client{
		bus:      bus,
		interval: interval,
		log:      opt.Log,
		rows:     map[uint32]symbol.Info{},
		byTab:    map[tableKey]map[uint32]struct{}{},
		revs:     map[tableKey]revState{},
		subs:     map[tableKey]struct{}{},
		notify:   make(chan note, notifyCap),
		kick:     make(chan struct{}, 1),
		runCtx:   runCtx,
		cancel:   cancel,
		done:     make(chan struct{}),
	}
	if c.log == nil {
		c.log = func(string) {}
	}
	go c.loop()
	return c, nil
}

// Close stops the loop and the changed subscriptions. A second call does nothing.
// In-flight Resolve calls return when their context ends.
func (c *Client) Close() {
	if c == nil {
		return
	}
	c.once.Do(func() { c.cancel() })
}

// Exited is closed when the revision loop has returned.
func (c *Client) Exited() <-chan struct{} { return c.done }

// AllowResolve is true while OnInit or OnStart is the hook running (E08-D004).
// The session sets it false around every other hook. Resolve then returns
// ErrNotInInit without calling SYM.
func (c *Client) AllowResolve(on bool) {
	c.mu.Lock()
	c.allow = on
	c.mu.Unlock()
}

// Reconnect runs the revision comparison now. The session worker calls it from
// the NATS reconnect handler (PROTOCOL §7.3). It does not block.
func (c *Client) Reconnect() {
	if c == nil {
		return
	}
	select {
	case c.kick <- struct{}{}:
	default:
	}
}

// Resolve looks ticker up with GetRequest. Only OnInit and OnStart may call it.
// A ticker SYM does not list wraps symbol.ErrNotFound and is not tracked. The returned
// Info is a copy.
func (c *Client) Resolve(ctx context.Context, ticker string) (symbol.Info, error) {
	if err := ctx.Err(); err != nil {
		return symbol.Info{}, err
	}
	if !c.allowed() {
		return symbol.Info{}, ErrNotInInit
	}
	t, err := symbol.Parse(ticker)
	if err != nil {
		return symbol.Info{}, err
	}

	c.loadMu.Lock()
	defer c.loadMu.Unlock()
	if err := ctx.Err(); err != nil {
		return symbol.Info{}, err
	}
	if !c.allowed() {
		return symbol.Info{}, ErrNotInInit
	}

	info, err := c.rpcGet(ctx, t)
	if err != nil {
		return symbol.Info{}, err
	}
	key := tableKey{info.Ticker.Venue, info.Ticker.Category}
	if err := c.ensureSub(key); err != nil {
		return symbol.Info{}, err
	}
	c.mu.Lock()
	c.storeLocked(info)
	c.mu.Unlock()
	// A failed sync leaves the Get row in memory and the table's revision
	// unconfirmed, so the next check tries again. Resolve itself is the Get.
	if err := c.syncTable(ctx, key); err != nil && ctx.Err() == nil {
		c.log(fmt.Sprintf("symclient: sync %s %s: %v", key.venue, key.cat, err))
	}
	return c.copied(info.UID, info), nil
}

// Info is the current row of a resolved instrument. It reads memory (E08-D025).
// A UID that was not resolved returns ErrUnknownUID and does not call SYM (E08-D023, E08-D024).
// A resolved instrument that has since been delisted is still returned (E08-D026).
func (c *Client) Info(uid uint32) (symbol.Info, error) {
	c.mu.Lock()
	info, ok := c.rows[uid]
	c.mu.Unlock()
	if !ok {
		return symbol.Info{}, ErrUnknownUID
	}
	return cloneInfo(info), nil
}

// Seed stores info as a resolved row without asking SYM. A replay seeds the rows Resolve returned
// in the recording, so Info answers as it did (E20-D009).
func (c *Client) Seed(info symbol.Info) {
	c.mu.Lock()
	c.storeLocked(info)
	c.mu.Unlock()
}

func (c *Client) allowed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.allow
}

func (c *Client) copied(uid uint32, fallback symbol.Info) symbol.Info {
	c.mu.Lock()
	info, ok := c.rows[uid]
	c.mu.Unlock()
	if !ok {
		return cloneInfo(fallback)
	}
	return cloneInfo(info)
}

func (c *Client) loop() {
	defer close(c.done)
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.runCtx.Done():
			return
		case <-ticker.C:
			c.checkLog(c.runCtx)
		case <-c.kick:
			c.checkLog(c.runCtx)
		case n := <-c.notify:
			c.onNotify(c.runCtx, n)
		}
	}
}

func (c *Client) checkLog(ctx context.Context) {
	if err := c.check(ctx); err != nil && ctx.Err() == nil {
		c.log(fmt.Sprintf("symclient: revision check: %v", err))
	}
}

func (c *Client) onNotify(ctx context.Context, n note) {
	c.mu.Lock()
	st := c.revs[n.key]
	tracked := len(c.byTab[n.key]) > 0
	c.mu.Unlock()
	if !tracked || (st.ok && n.rev <= st.rev) {
		return
	}
	c.loadMu.Lock()
	defer c.loadMu.Unlock()
	c.mu.Lock()
	st = c.revs[n.key]
	tracked = len(c.byTab[n.key]) > 0
	c.mu.Unlock()
	if !tracked || (st.ok && n.rev <= st.rev) {
		return
	}
	if err := c.refresh(ctx, n.key); err != nil && ctx.Err() == nil {
		c.log(fmt.Sprintf("symclient: reload %s %s: %v", n.key.venue, n.key.cat, err))
	}
}

func (c *Client) onChanged(subject string, data []byte) {
	var msg symv1.Changed
	if err := proto.Unmarshal(data, &msg); err != nil {
		c.Reconnect()
		return
	}
	want, err := changedSubject(msg.GetVenue(), msg.GetCategory())
	if err != nil || want != subject || msg.GetRevision() < 1 {
		c.Reconnect()
		return
	}
	key := tableKey{msg.GetVenue(), symbol.Category(msg.GetCategory())}
	select {
	case c.notify <- note{key: key, rev: msg.GetRevision()}:
	default:
		c.Reconnect()
	}
}

// check refetches every tracked table whose revision differs from the one last
// loaded, and any table that has not confirmed a revision yet.
func (c *Client) check(ctx context.Context) error {
	c.mu.Lock()
	if len(c.byTab) == 0 {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()
	remote, err := c.revisionMap(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	var behind []tableKey
	for key := range c.byTab {
		st := c.revs[key]
		if !st.ok || remote[key] != st.rev {
			behind = append(behind, key)
		}
	}
	c.mu.Unlock()
	var errs []error
	for _, key := range behind {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.loadMu.Lock()
		err := c.refresh(ctx, key)
		c.loadMu.Unlock()
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// syncTable refetches key when this process has not confirmed its revision, or
// the remote revision has moved. Caller holds loadMu.
func (c *Client) syncTable(ctx context.Context, key tableKey) error {
	remote, err := c.revisionOf(ctx, key)
	if err != nil {
		return err
	}
	c.mu.Lock()
	st := c.revs[key]
	c.mu.Unlock()
	if st.ok && st.rev == remote {
		return nil
	}
	return c.refresh(ctx, key)
}

// refresh refetches the resolved rows of one table. Caller holds loadMu.
// The stored revision advances only after a pass whose revision did not move.
func (c *Client) refresh(ctx context.Context, key tableKey) error {
	var last error
	for range refreshTries {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := c.refreshOnce(ctx, key)
		if err == nil {
			return nil
		}
		if !errors.Is(err, errRevisionMoved) {
			return err
		}
		last = err
	}
	if last == nil {
		last = fmt.Errorf("symclient: %s %s: refresh did not settle", key.venue, key.cat)
	}
	return last
}

func (c *Client) refreshOnce(ctx context.Context, key tableKey) error {
	rev1, err := c.revisionOf(ctx, key)
	if err != nil {
		return err
	}
	for _, uid := range c.uids(key) {
		if err := c.refreshOne(ctx, key, uid); err != nil {
			return err
		}
	}
	rev2, err := c.revisionOf(ctx, key)
	if err != nil {
		return err
	}
	if rev1 != rev2 {
		return fmt.Errorf("%w: %s %s from %d to %d", errRevisionMoved, key.venue, key.cat, rev1, rev2)
	}
	c.mu.Lock()
	c.revs[key] = revState{rev: rev2, ok: true}
	c.mu.Unlock()
	return nil
}

// refreshOne refetches one resolved UID. Active not_found means the row moved
// to the terminal table: GetByUid with include_inactive keeps that last copy
// (E08-D026). Any other failure leaves the previous copy and does not advance the
// table revision, so the next check tries again.
func (c *Client) refreshOne(ctx context.Context, key tableKey, uid uint32) error {
	info, err := c.rpcByUID(ctx, uid, false)
	if errors.Is(err, symbol.ErrNotFound) {
		info, err = c.rpcByUID(ctx, uid, true)
	}
	if err != nil {
		return err
	}
	if info.Ticker.Venue != key.venue || info.Ticker.Category != key.cat {
		return fmt.Errorf("symclient: uid %d is %s, not %s %s", uid, info.Ticker, key.venue, key.cat)
	}
	c.mu.Lock()
	c.storeLocked(info)
	c.mu.Unlock()
	return nil
}

func (c *Client) uids(key tableKey) []uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	set := c.byTab[key]
	out := make([]uint32, 0, len(set))
	for uid := range set {
		out = append(out, uid)
	}
	slices.Sort(out)
	return out
}

func (c *Client) ensureSub(key tableKey) error {
	c.mu.Lock()
	_, ok := c.subs[key]
	c.mu.Unlock()
	if ok {
		return nil
	}
	subject, err := changedSubject(key.venue, string(key.cat))
	if err != nil {
		return err
	}
	// The subscription outlives the hook. It ends when Close cancels runCtx.
	if err := c.bus.Subscribe(c.runCtx, subject, c.onChanged); err != nil {
		return err
	}
	c.mu.Lock()
	c.subs[key] = struct{}{}
	c.mu.Unlock()
	return nil
}

func (c *Client) storeLocked(info symbol.Info) {
	info = cloneInfo(info)
	c.rows[info.UID] = info
	key := tableKey{info.Ticker.Venue, info.Ticker.Category}
	set := c.byTab[key]
	if set == nil {
		set = map[uint32]struct{}{}
		c.byTab[key] = set
	}
	set[info.UID] = struct{}{}
}

func cloneInfo(info symbol.Info) symbol.Info {
	info.Filters = maps.Clone(info.Filters)
	return info
}

func (c *Client) rpcGet(ctx context.Context, t symbol.UniversalTicker) (symbol.Info, error) {
	rep, err := c.rpc(ctx, &symv1.Request{Req: &symv1.Request_Get{Get: &symv1.GetRequest{
		UniversalTicker: t.String(),
	}}})
	if err != nil {
		return symbol.Info{}, err
	}
	info, err := InfoFromProto(rep.GetInfo())
	if err != nil {
		return symbol.Info{}, err
	}
	if info.Ticker != t {
		return symbol.Info{}, fmt.Errorf("symclient: get %s returned %s", t, info.Ticker)
	}
	return info, nil
}

func (c *Client) rpcByUID(ctx context.Context, uid uint32, inactive bool) (symbol.Info, error) {
	rep, err := c.rpc(ctx, &symv1.Request{Req: &symv1.Request_GetByUid{GetByUid: &symv1.GetByUidRequest{
		Uid:             uid,
		IncludeInactive: inactive,
	}}})
	if err != nil {
		return symbol.Info{}, err
	}
	info, err := InfoFromProto(rep.GetInfo())
	if err != nil {
		return symbol.Info{}, err
	}
	if info.UID != uid {
		return symbol.Info{}, fmt.Errorf("symclient: get uid %d returned %d", uid, info.UID)
	}
	return info, nil
}

func (c *Client) revisionOf(ctx context.Context, key tableKey) (int64, error) {
	all, err := c.revisionMap(ctx)
	if err != nil {
		return 0, err
	}
	return all[key], nil
}

func (c *Client) revisionMap(ctx context.Context) (map[tableKey]int64, error) {
	rep, err := c.rpc(ctx, &symv1.Request{Req: &symv1.Request_Revisions{Revisions: &symv1.RevisionsRequest{}}})
	if err != nil {
		return nil, err
	}
	list := rep.GetRevisions()
	if list == nil {
		return nil, errors.New("symclient: revisions: empty reply")
	}
	out := make(map[tableKey]int64, len(list.GetTables()))
	for _, row := range list.GetTables() {
		if row.GetRevision() < 0 {
			return nil, fmt.Errorf("symclient: revision %d is negative", row.GetRevision())
		}
		out[tableKey{row.GetVenue(), symbol.Category(row.GetCategory())}] = row.GetRevision()
	}
	return out, nil
}

func (c *Client) rpc(ctx context.Context, req *symv1.Request) (*symv1.Reply, error) {
	var rep symv1.Reply
	if err := c.bus.Request(ctx, subjectSym, req, &rep); err != nil {
		return nil, err
	}
	if rep.GetError() != "" || rep.GetReason() != "" {
		return nil, fmt.Errorf("symclient: reply error field %q", rep.GetError())
	}
	return &rep, nil
}

func changedSubject(venue, category string) (string, error) {
	if err := subjectToken("venue", venue); err != nil {
		return "", err
	}
	if err := subjectToken("category", category); err != nil {
		return "", err
	}
	return "sym.v1.changed." + venue + "." + category, nil
}

func subjectToken(field, value string) error {
	if value == "" || strings.ContainsAny(value, ".*> \t") {
		return fmt.Errorf("symclient: %s %q is not a subject token", field, value)
	}
	return nil
}
