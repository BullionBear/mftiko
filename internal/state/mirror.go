package state

import (
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
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Mirror defaults.
const (
	DefaultMirrorPageSize   = 500
	DefaultMirrorMinBackoff = 100 * time.Millisecond
	DefaultMirrorMaxBackoff = 5 * time.Second
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

// View is the prefix as of Rev: every key under it, and nothing else, at that revision of its cluster. Entries is
// a copy; the values it holds are shared and must not be modified.
type View struct {
	Rev     int64
	Entries map[string]Entry
}

// MirrorOptions tune a Mirror. The zero value takes the defaults.
type MirrorOptions struct {
	PageSize   int64           // keys per list request; default 500
	MinBackoff time.Duration   // first wait after a failure; default 100ms
	MaxBackoff time.Duration   // longest wait; default 5s
	Log        *zerolog.Logger // failures and relists; nil logs nothing
}

// MirrorStats counts what Run has done.
type MirrorStats struct {
	Lists   int // complete lists, the first one included; more than one means a relist after compaction
	Watches int // watches opened
	Errors  int // failed lists and broken watches
}

// Mirror keeps an in-memory copy of every key under one prefix of one cluster (PROTOCOL §3.2 W4). Run lists the
// prefix at one revision, watches from that revision + 1, and applies each watch response as a whole, so a view
// never shows half of a txn. When the watch reports that the next revision it needs was compacted, Run lists
// again and replaces the copy, which drops keys whose deletes were compacted away; when the watch breaks for any
// other reason, Run watches again from the last revision it applied. Readers act level-triggered: Notify says
// that something changed, Snapshot or Take says what is there now.
//
// The watch requires a leader (clientv3.WithRequireLeader): a member cut off from its leader cancels it rather
// than going quiet, and Run moves on.
//
// Safe for concurrent use. Run is called once; Notify is for one consumer.
type Mirror struct {
	r      Reader
	prefix string
	end    string
	opt    MirrorOptions

	ran    atomic.Bool
	synced chan struct{}
	notify chan struct{}

	mu      sync.Mutex
	rev     int64 // the revision entries are current as of; 0 until the first list
	entries map[string]Entry
	dirty   map[string]struct{}
	stats   MirrorStats
}

// NewMirror mirrors every key that starts with prefix, read through r. prefix must not be empty; end it with "/"
// unless sibling keys that share its spelling should be included.
func NewMirror(r Reader, prefix string, opt MirrorOptions) (*Mirror, error) {
	switch {
	case r == nil:
		return nil, fmt.Errorf("%w: mirror without a reader", ErrBadOptions)
	case prefix == "":
		return nil, fmt.Errorf("%w: mirror of an empty prefix", ErrBadOptions)
	case opt.PageSize < 0 || opt.MinBackoff < 0 || opt.MaxBackoff < 0:
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
	return &Mirror{
		r:       r,
		prefix:  prefix,
		end:     clientv3.GetPrefixRangeEnd(prefix),
		opt:     opt,
		synced:  make(chan struct{}),
		notify:  make(chan struct{}, 1),
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

// Rev is the revision the copy is current as of; 0 before the first list.
func (m *Mirror) Rev() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rev
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

	needList := true
	for {
		if err := stopped(); err != nil {
			return err
		}
		if needList {
			err := m.list(ctx)
			if err == nil {
				needList = false
				wait = m.opt.MinBackoff
				continue
			}
			if stopped() != nil {
				continue
			}
			m.failed("list", err)
			if !errors.Is(err, rpctypes.ErrCompacted) { // a page's revision compacted mid-list: start over, no wait
				backoff()
			}
			continue
		}

		progressed, compacted, err := m.watch(ctx)
		if stopped() != nil {
			continue
		}
		if progressed {
			wait = m.opt.MinBackoff
		}
		if compacted {
			m.logInfo("watch compacted, relisting")
			needList = true
			continue
		}
		m.failed("watch", err)
		backoff()
	}
}

// list reads the whole prefix at one revision, in pages, and replaces the copy with it.
func (m *Mirror) list(ctx context.Context) error {
	entries := map[string]Entry{}
	var rev int64
	key := m.prefix
	for {
		opts := []clientv3.OpOption{clientv3.WithRange(m.end), clientv3.WithLimit(m.opt.PageSize)}
		if rev != 0 {
			opts = append(opts, clientv3.WithRev(rev)) // later pages at the first page's revision
		}
		resp, err := m.r.Get(ctx, key, opts...)
		if err != nil {
			return err
		}
		if rev == 0 {
			rev = resp.Header.Revision
		}
		for _, kv := range resp.Kvs {
			entries[string(kv.Key)] = entryOf(kv)
		}
		if !resp.More || len(resp.Kvs) == 0 {
			break
		}
		key = string(resp.Kvs[len(resp.Kvs)-1].Key) + "\x00"
	}
	m.replace(rev, entries)
	return nil
}

// watch applies events from Rev()+1 until the watch ends. progressed: at least one response was applied.
// compacted: the next revision needed is gone, so the caller must list again.
func (m *Mirror) watch(ctx context.Context) (progressed, compacted bool, err error) {
	wctx, cancel := context.WithCancel(clientv3.WithRequireLeader(ctx))
	defer cancel()
	m.mu.Lock()
	from := m.rev + 1
	m.stats.Watches++
	m.mu.Unlock()

	for resp := range m.r.Watch(wctx, m.prefix, clientv3.WithRange(m.end), clientv3.WithRev(from)) {
		if resp.CompactRevision != 0 {
			return progressed, true, nil
		}
		if err := resp.Err(); err != nil {
			return progressed, false, err
		}
		m.apply(resp)
		progressed = true
	}
	return progressed, false, errors.New("watch channel closed")
}

// replace swaps in a fresh list and marks every key that differs from the old copy.
func (m *Mirror) replace(rev int64, entries map[string]Entry) {
	m.mu.Lock()
	changed := m.rev == 0 // the first list signals even when the prefix is empty
	for k, e := range entries {
		if old, ok := m.entries[k]; !ok || old.ModRevision != e.ModRevision {
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
	m.rev = rev
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

// apply applies one watch response. The resume point is the last event's revision, not the header's: the header
// can be ahead of events the server has not sent yet. Only a progress notification vouches for its header.
func (m *Mirror) apply(resp clientv3.WatchResponse) {
	m.mu.Lock()
	if resp.IsProgressNotify() {
		m.rev = max(m.rev, resp.Header.Revision)
		m.mu.Unlock()
		return
	}
	for _, ev := range resp.Events {
		k := string(ev.Kv.Key)
		switch ev.Type {
		case mvccpb.PUT:
			m.entries[k] = entryOf(ev.Kv)
		case mvccpb.DELETE:
			delete(m.entries, k)
		}
		m.dirty[k] = struct{}{}
		m.rev = max(m.rev, ev.Kv.ModRevision)
	}
	m.mu.Unlock()
	if len(resp.Events) > 0 {
		m.signal()
	}
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
