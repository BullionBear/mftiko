package state

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Mirror defaults.
const (
	DefaultMirrorPageSize      = 500
	DefaultMirrorMinBackoff    = 100 * time.Millisecond
	DefaultMirrorMaxBackoff    = 5 * time.Second
	DefaultMirrorProbeInterval = 5 * time.Second

	// A watch silent for this many probe intervals, while the cluster answers the probe, is reopened.
	mirrorStallProbes = 3
	// Lists restarted this many times in a row by a mid-list compaction back off before the next try.
	mirrorCompactedLists = 3
)

// Entry is one key as the mirror last saw it.
type Entry struct {
	Key            string
	Value          []byte // shared with the mirror: do not modify
	CreateRevision int64
	ModRevision    int64
	Version        int64
	Lease          int64
}

// View is the prefix as of Rev: every key under it, and nothing else, as etcd held them at that revision. Entries
// is a copy, O(keys) to make; the values it holds are shared and must not be modified.
type View struct {
	Rev     int64
	Entries map[string]Entry
}

// MirrorOptions tune a Mirror. The zero value takes the defaults.
type MirrorOptions struct {
	PageSize      int64           // keys per list request; default 500
	MinBackoff    time.Duration   // first wait after a failure; default 100ms
	MaxBackoff    time.Duration   // longest wait; default 5s
	ProbeInterval time.Duration   // how often to check the cluster and the watch; default 5s
	Log           *zerolog.Logger // failures and relists; nil logs nothing
}

// MirrorStats counts what Run has done.
type MirrorStats struct {
	Lists   int // complete lists, the first one included; later ones follow a compaction or a replaced cluster
	Watches int // watches opened
	Errors  int // failed lists, broken watches, and stalled watches that were reopened
}

// Mirror keeps an in-memory copy of every key under one prefix of one cluster (PROTOCOL §3.2 W4).
//
// Run lists the prefix in pages, all at the first page's revision, then watches and applies each watch response
// whole, so a view never shows half of a txn. W4 says to watch from the list's revision + 1; Run watches from the
// revision itself and drops events at or below the copy's revision, which are already in it. The effect is the
// same, but Run's own watches never start beyond the cluster's current revision: etcd withholds progress
// notifications from every watch on a stream while one of them does, which on a quiet cluster would be
// indefinitely. (clientv3 still does so when it resumes a watch by itself after a progress notification; see
// Staleness.) The watch requires a leader (clientv3.WithRequireLeader): a member cut off from its leader cancels
// it rather than going quiet.
//
//   - Compaction: when the watch reports that a revision it needs is gone, Run lists again and replaces the copy,
//     which drops keys whose deletes were compacted away. A compaction at exactly the copy's revision + 1 costs a
//     relist that a watch from revision + 1 would not have needed.
//   - Interruption: when the watch ends for any other reason, Run reopens it from the last revision it applied.
//     clientv3 also resumes a watch by itself across reconnects; either way nothing is skipped.
//   - A replaced or rebuilt cluster: revisions start over, and a resumed watch would wait silently for a revision
//     the new cluster has not reached. Every ProbeInterval Run reads one key: another cluster ID, or a revision or
//     raft term below what the copy had seen before the read was sent, means the copy belongs to another history,
//     so Run lists again. Watch responses are checked for the cluster ID too. A restore that keeps the cluster ID
//     must not reuse revisions (etcdutl snapshot restore --bump-revision --mark-compacted): one that reused them
//     and overtook the copy's revision within a probe interval would not be noticed.
//   - Staleness: LastContact is the last time the copy was known to be current: a list, a progress notification,
//     a watch response that brought the copy up to the response's revision, or a probe that found nothing in the
//     cluster newer than the copy. The probe also asks the watch stream for a progress notification; a watch that
//     stays silent for 3 probe intervals while the cluster has moved past the copy is reopened.
//
// Readers act level-triggered: Notify says that something changed, Snapshot or Take says what is there now.
// Notify and Take are for one consumer: Take consumes the set of changed keys. Safe for concurrent use; Run is
// called once.
type Mirror struct {
	r      Reader
	prefix string
	end    string
	opt    MirrorOptions

	ran    atomic.Bool
	synced chan struct{}
	notify chan struct{}
	kick   chan struct{} // the probe asks the watch loop to stop; relistAsked and kickGen say why

	mu          sync.Mutex
	rev         int64 // the revision entries are current as of; 0 until the first list
	revCh       chan struct{}
	clusterID   uint64    // of the cluster the copy was listed from
	term        uint64    // highest raft term seen
	contact     time.Time // see LastContact
	gen         uint64    // the current watch's generation
	opened      time.Time // when it was opened
	kickGen     uint64    // the generation a stall kick is for
	relistAsked uint64    // relists the probe asked for, counted
	relistDone  uint64    // relistAsked as of the start of the last completed list
	entries     map[string]Entry
	dirty       map[string]struct{}
	stats       MirrorStats
}

// NewMirror mirrors every key that starts with prefix, read through r. prefix must not be empty; end it with "/"
// unless sibling keys that share its spelling should be included.
func NewMirror(r Reader, prefix string, opt MirrorOptions) (*Mirror, error) {
	switch {
	case r == nil:
		return nil, fmt.Errorf("%w: mirror without a reader", ErrBadOptions)
	case prefix == "":
		return nil, fmt.Errorf("%w: mirror of an empty prefix", ErrBadOptions)
	case opt.PageSize < 0 || opt.MinBackoff < 0 || opt.MaxBackoff < 0 || opt.ProbeInterval < 0:
		return nil, fmt.Errorf("%w: negative mirror option", ErrBadOptions)
	}
	if opt.PageSize == 0 {
		opt.PageSize = DefaultMirrorPageSize
	}
	if opt.MinBackoff == 0 {
		opt.MinBackoff = DefaultMirrorMinBackoff
	}
	if opt.MaxBackoff == 0 {
		opt.MaxBackoff = DefaultMirrorMaxBackoff
	}
	if opt.MaxBackoff < opt.MinBackoff {
		opt.MaxBackoff = opt.MinBackoff
	}
	if opt.ProbeInterval == 0 {
		opt.ProbeInterval = DefaultMirrorProbeInterval
	}
	return &Mirror{
		r:       r,
		prefix:  prefix,
		end:     clientv3.GetPrefixRangeEnd(prefix),
		opt:     opt,
		synced:  make(chan struct{}),
		notify:  make(chan struct{}, 1),
		kick:    make(chan struct{}, 1),
		revCh:   make(chan struct{}),
		entries: map[string]Entry{},
		dirty:   map[string]struct{}{},
	}, nil
}

// Prefix is the mirrored prefix.
func (m *Mirror) Prefix() string { return m.prefix }

// Synced is closed once the first list has been applied. Before that the mirror is empty, which says nothing about
// etcd: wait for it before acting on a view.
func (m *Mirror) Synced() <-chan struct{} { return m.synced }

// Notify receives a signal after the first list and after each later change. Signals coalesce: one pending signal
// covers every change before it.
func (m *Mirror) Notify() <-chan struct{} { return m.notify }

// Rev is the revision the copy is current as of; 0 before the first list. It only goes back when the cluster was
// replaced or rebuilt and the copy was listed again from it.
func (m *Mirror) Rev() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rev
}

// WaitRev waits until the copy is current as of rev, for example a write's revision, or ctx ends. A revision with
// no event under the prefix is reached at the next progress notification, within a ProbeInterval.
func (m *Mirror) WaitRev(ctx context.Context, rev int64) error {
	for {
		m.mu.Lock()
		cur, ch := m.rev, m.revCh
		m.mu.Unlock()
		if cur >= rev {
			return nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// LastContact is the last time the mirror knew it was current: a list completed, or the watch delivered events
// or a progress notification. A consumer that must not act on stale state compares it with its own tolerance;
// expect it to lag by up to a ProbeInterval while all is well. Zero before the first list.
func (m *Mirror) LastContact() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.contact
}

// Get is one key of the copy.
func (m *Mirror) Get(key string) (Entry, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.entries[key]
	return e, ok
}

// Snapshot copies the current view.
func (m *Mirror) Snapshot() View {
	m.mu.Lock()
	defer m.mu.Unlock()
	return View{Rev: m.rev, Entries: maps.Clone(m.entries)}
}

// Take is Snapshot plus the keys put or deleted since the previous Take, sorted; it starts the next set. The keys
// are a hint for which objects to look at first: the view, not the list, is what a reconciler acts on.
func (m *Mirror) Take() (View, []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	changed := slices.Sorted(maps.Keys(m.dirty))
	clear(m.dirty)
	return View{Rev: m.rev, Entries: maps.Clone(m.entries)}, changed
}

// Stats counts lists, watches and failures so far.
func (m *Mirror) Stats() MirrorStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stats
}

// Run keeps the copy current until ctx is done, then returns ctx's error. Failures are retried with exponential
// backoff and jitter, never returned, except one: once the reader's client is closed (Clusters.Close), Run returns
// an error matching ErrClustersClosed.
func (m *Mirror) Run(ctx context.Context) error {
	if !m.ran.CompareAndSwap(false, true) {
		return fmt.Errorf("state: mirror %s: Run called twice", m.prefix)
	}
	pctx, stopProbe := context.WithCancel(ctx)
	probing := make(chan struct{})
	go func() {
		defer close(probing)
		m.probe(pctx)
	}()
	defer func() {
		stopProbe()
		<-probing
	}()

	stopped := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case <-m.r.Done():
			return fmt.Errorf("%w: mirror %s", ErrClustersClosed, m.prefix)
		default:
			return nil
		}
	}
	wait := m.opt.MinBackoff
	backoff := func() {
		d := wait/2 + rand.N(wait/2+1) // jitter: [wait/2, wait]
		wait = min(2*wait, m.opt.MaxBackoff)
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-t.C:
		case <-ctx.Done():
		case <-m.r.Done():
		}
	}

	needList, compactedLists := true, 0
	for {
		if err := stopped(); err != nil {
			return err
		}
		if needList {
			err := m.list(ctx)
			if err == nil {
				needList, compactedLists = false, 0
				wait = m.opt.MinBackoff
				continue
			}
			if stopped() != nil {
				continue
			}
			m.failed("list", err)
			// A page's revision compacted mid-list: start over at once, unless that keeps happening.
			if errors.Is(err, rpctypes.ErrCompacted) {
				if compactedLists++; compactedLists < mirrorCompactedLists {
					continue
				}
			}
			backoff()
			continue
		}

		opened := time.Now()
		progressed, relist, err := m.watch(ctx)
		if stopped() != nil {
			continue
		}
		if progressed || time.Since(opened) > m.opt.MaxBackoff { // it worked for a while: start backoff over
			wait = m.opt.MinBackoff
		}
		if relist {
			needList = true
			continue
		}
		m.failed("watch", err)
		backoff()
	}
}

// list reads the whole prefix at one revision, in pages, and replaces the copy with it.
func (m *Mirror) list(ctx context.Context) error {
	m.mu.Lock()
	asked := m.relistAsked // a relist asked for after this point is not answered by this list
	m.mu.Unlock()
	entries := map[string]Entry{}
	var first *etcdserverpb.ResponseHeader
	sent := time.Now()
	key := m.prefix
	for {
		opts := []clientv3.OpOption{clientv3.WithRange(m.end), clientv3.WithLimit(m.opt.PageSize)}
		if first != nil {
			opts = append(opts, clientv3.WithRev(first.Revision)) // later pages at the first page's revision
		}
		resp, err := m.r.Get(ctx, key, opts...)
		if err != nil {
			return err
		}
		if first == nil {
			first = resp.Header
		}
		for _, kv := range resp.Kvs {
			entries[string(kv.Key)] = entryOf(kv)
		}
		if !resp.More || len(resp.Kvs) == 0 {
			break
		}
		key = string(resp.Kvs[len(resp.Kvs)-1].Key) + "\x00"
	}
	m.replace(first, entries, asked, sent)
	return nil
}

var errKicked = errors.New("watch silent while the cluster moved on")

// watch applies events after Rev() until the watch ends. progressed: at least one response was applied. relist:
// the copy must be listed again (compaction, or another cluster's history).
func (m *Mirror) watch(ctx context.Context) (progressed, relist bool, err error) {
	wctx, cancel := context.WithCancel(clientv3.WithRequireLeader(ctx))
	defer cancel()
	m.mu.Lock()
	select { // a kick sent before now was decided about an older watch, or asks for a relist that stays counted
	case <-m.kick:
	default:
	}
	if m.relistAsked > m.relistDone {
		m.mu.Unlock()
		return false, true, nil
	}
	from := m.rev // not +1: see the type's comment; apply drops what the copy already has
	m.gen++
	gen := m.gen
	m.opened = time.Now()
	m.stats.Watches++
	m.mu.Unlock()

	wch := m.r.Watch(wctx, m.prefix, clientv3.WithRange(m.end), clientv3.WithRev(from))
	for {
		select {
		case resp, ok := <-wch:
			if !ok {
				return progressed, false, errors.New("watch channel closed")
			}
			if resp.CompactRevision != 0 {
				m.logInfo("watch compacted, relisting")
				return progressed, true, nil
			}
			if err := resp.Err(); err != nil {
				return progressed, false, err
			}
			if m.otherCluster(resp.Header) {
				return progressed, true, nil
			}
			m.apply(resp)
			progressed = true
		case <-m.kick:
			m.mu.Lock()
			relist := m.relistAsked > m.relistDone
			stalled := m.kickGen == gen
			m.mu.Unlock()
			if relist {
				return progressed, true, nil
			}
			if stalled {
				return progressed, false, errKicked
			}
			// a stall kick for an older watch: ignore it
		}
	}
}

// probe checks the cluster and the watch every ProbeInterval until pctx ends. pctx derives from Run's ctx, so the
// progress request carries the same metadata as the watch's ctx and reaches the same stream.
func (m *Mirror) probe(pctx context.Context) {
	t := time.NewTicker(m.opt.ProbeInterval)
	defer t.Stop()
	for {
		select {
		case <-pctx.Done():
			return
		case <-m.r.Done():
			return
		case <-t.C:
		}
		select {
		case <-m.synced:
		default:
			continue // nothing to compare with before the first list
		}
		// What the copy had seen before the read was sent: a linearizable read is at least that, while events
		// applied during the read may already be past it.
		m.mu.Lock()
		revBefore, termBefore, gen := m.rev, m.term, m.gen
		m.mu.Unlock()
		sent := time.Now()
		ctx, cancel := context.WithTimeout(pctx, m.opt.ProbeInterval)
		resp, err := m.r.Get(ctx, m.prefix) // one key, only for the header
		if err == nil {
			_ = m.r.RequestProgress(clientv3.WithRequireLeader(ctx))
		}
		cancel()
		if err != nil {
			continue // the cluster does not answer: LastContact shows it, the watch resumes when it does
		}
		h := resp.Header
		switch {
		case m.otherCluster(h):
			m.askRelist()
			continue
		case h.RaftTerm < termBefore:
			m.logWarn(fmt.Sprintf("raft term went back from %d to %d, relisting", termBefore, h.RaftTerm))
			m.askRelist()
			continue
		case h.Revision < revBefore:
			m.logWarn(fmt.Sprintf("revision went back from %d to %d, relisting", revBefore, h.Revision))
			m.askRelist()
			continue
		}
		m.mu.Lock()
		m.term = max(m.term, h.RaftTerm)
		if h.Revision == revBefore { // nothing newer in the cluster than the copy: it was current when sent
			m.contact = latest(m.contact, sent)
			m.mu.Unlock()
			continue
		}
		stalled := m.gen == gen && time.Since(latest(m.contact, m.opened)) > mirrorStallProbes*m.opt.ProbeInterval
		m.mu.Unlock()
		if stalled {
			m.kickStall(gen)
		}
	}
}

// otherCluster reports a header from another cluster than the one the copy was listed from.
func (m *Mirror) otherCluster(h *etcdserverpb.ResponseHeader) bool {
	if h == nil {
		return false
	}
	m.mu.Lock()
	listed := m.clusterID
	m.mu.Unlock()
	if h.ClusterId == listed {
		return false
	}
	m.logWarn(fmt.Sprintf("cluster %x answered where %x was listed, relisting", h.ClusterId, listed))
	return true
}

func (m *Mirror) askRelist() {
	m.mu.Lock()
	m.relistAsked++
	m.mu.Unlock()
	m.sendKick()
}

func (m *Mirror) kickStall(gen uint64) {
	m.mu.Lock()
	m.kickGen = gen
	m.mu.Unlock()
	m.sendKick()
}

func (m *Mirror) sendKick() {
	select {
	case m.kick <- struct{}{}:
	default:
	}
}

// replace swaps in a fresh list and marks every key that differs from the old copy. Entries are compared whole:
// after a cluster was replaced, equal mod revisions say nothing. asked is relistAsked when the list started; sent is
// when its first page was sent.
func (m *Mirror) replace(h *etcdserverpb.ResponseHeader, entries map[string]Entry, asked uint64, sent time.Time) {
	m.mu.Lock()
	changed := m.stats.Lists == 0 // the first list signals even when the prefix is empty
	for k, e := range entries {
		if old, ok := m.entries[k]; !ok || !sameEntry(old, e) {
			m.dirty[k] = struct{}{}
			changed = true
		}
	}
	for k := range m.entries {
		if _, ok := entries[k]; !ok {
			m.dirty[k] = struct{}{}
			changed = true
		}
	}
	m.entries = entries
	m.setRev(h.Revision)
	m.clusterID = h.ClusterId
	m.term = h.RaftTerm
	m.contact = sent
	m.relistDone = max(m.relistDone, asked)
	m.stats.Lists++
	first := m.stats.Lists == 1
	m.mu.Unlock()

	if first {
		close(m.synced)
	}
	if changed {
		m.signal()
	}
}

// apply applies one watch response. Events at or below the copy's revision are already in it and are dropped. The
// resume point is the last event's revision, not the header's: the header can be ahead of events the server has
// not sent yet (a watch still catching up), so only a response that brings the copy up to its header, or a
// progress notification, counts as contact.
func (m *Mirror) apply(resp clientv3.WatchResponse) {
	m.mu.Lock()
	if resp.IsProgressNotify() {
		m.setRev(max(m.rev, resp.Header.Revision))
		m.contact = time.Now()
		m.mu.Unlock()
		return
	}
	rev, applied := m.rev, false
	for _, ev := range resp.Events {
		if ev.Kv.ModRevision <= m.rev {
			continue
		}
		applied = true
		k := string(ev.Kv.Key)
		switch ev.Type {
		case mvccpb.PUT:
			m.entries[k] = entryOf(ev.Kv)
		case mvccpb.DELETE:
			delete(m.entries, k)
		}
		m.dirty[k] = struct{}{}
		rev = max(rev, ev.Kv.ModRevision)
	}
	m.setRev(rev)
	if resp.Header != nil && m.rev >= resp.Header.Revision {
		m.contact = time.Now()
	}
	m.mu.Unlock()
	if applied {
		m.signal()
	}
}

// setRev moves the revision and wakes WaitRev. Called with m.mu held.
func (m *Mirror) setRev(rev int64) {
	if rev == m.rev {
		return
	}
	m.rev = rev
	close(m.revCh)
	m.revCh = make(chan struct{})
}

func (m *Mirror) signal() {
	select {
	case m.notify <- struct{}{}:
	default:
	}
}

func (m *Mirror) failed(what string, err error) {
	m.mu.Lock()
	m.stats.Errors++
	m.mu.Unlock()
	if m.opt.Log != nil {
		m.opt.Log.Warn().Err(err).Str("prefix", m.prefix).Str("op", what).Msg("mirror retrying")
	}
}

func (m *Mirror) logInfo(msg string) {
	if m.opt.Log != nil {
		m.opt.Log.Info().Str("prefix", m.prefix).Msg(msg)
	}
}

func (m *Mirror) logWarn(msg string) {
	if m.opt.Log != nil {
		m.opt.Log.Warn().Str("prefix", m.prefix).Msg(msg)
	}
}

func latest(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func sameEntry(a, b Entry) bool {
	return a.ModRevision == b.ModRevision && a.CreateRevision == b.CreateRevision && a.Version == b.Version &&
		a.Lease == b.Lease && bytes.Equal(a.Value, b.Value)
}

func entryOf(kv *mvccpb.KeyValue) Entry {
	return Entry{
		Key:            string(kv.Key),
		Value:          kv.Value,
		CreateRevision: kv.CreateRevision,
		ModRevision:    kv.ModRevision,
		Version:        kv.Version,
		Lease:          kv.Lease,
	}
}
