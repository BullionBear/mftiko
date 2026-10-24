package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/metadata"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// sseQueueCapDefault is the per-connection send buffer (E10-D095). The decision
// says the buffer has a limit and does not pick the number, same as the audit
// queue (E10-D049). Tests set sseQueueCap before the handler starts.
const sseQueueCapDefault = 256

var sseQueueCap = sseQueueCapDefault

const sseHeartbeatDefault = 15 * time.Second

var sseHeartbeat = sseHeartbeatDefault

const sseListPage int64 = 500

var (
	errSSECompact = errors.New("sse compacted")
	errSSEFuture  = errors.New("sse future revision")
	sseWatchSeq   atomic.Uint64
)

func (s *Server) StreamEvents(w http.ResponseWriter, r *http.Request, params apiv1.StreamEventsParams) {
	if len(params.Topics) == 0 {
		writeProblem(w, http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, http.StatusText(http.StatusBadRequest), "missing parameter", []apiv1.FieldError{{
			Path:   "topics",
			Reason: "required",
		}})
		return
	}
	topics := map[apiv1.EventTopic]bool{}
	var topicList []apiv1.EventTopic
	for _, topic := range params.Topics {
		if topic != apiv1.Sessions && topic != apiv1.Instances {
			continue
		}
		if topics[topic] {
			continue
		}
		topics[topic] = true
		topicList = append(topicList, topic)
	}
	if len(topics) == 0 {
		writeProblem(w, http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, http.StatusText(http.StatusBadRequest), "invalid request", []apiv1.FieldError{{
			Path: "topics", Reason: "is not a topic",
		}})
		return
	}
	known := map[string]bool{}
	for _, c := range s.clusters {
		known[c.name] = true
	}
	var resume map[string]int64
	if params.LastEventID != nil && *params.LastEventID != "" {
		parsed, err := parseLastEventID(*params.LastEventID, known)
		if err != nil {
			writeProblem(w, http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, http.StatusText(http.StatusBadRequest), "invalid request", []apiv1.FieldError{{
				Path: "Last-Event-ID", Reason: "is not a resume id from this API",
			}})
			return
		}
		resume = parsed
	}
	sessionID := ""
	if params.SessionId != nil {
		sessionID = *params.SessionId
	}
	qcap := sseQueueCap
	if qcap < 1 {
		qcap = 1
	}
	hb := sseHeartbeat
	if hb <= 0 {
		hb = sseHeartbeatDefault
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	hub := newSSEHub(w, qcap, cancel)
	// net/http finishes the response on this goroutine when StreamEvents
	// returns. writeLoop has to be done first, or its Flush races with that.
	done := make(chan struct{})
	go func() {
		defer close(done)
		hub.writeLoop(ctx, hb)
	}()
	defer func() {
		cancel()
		if hub.rc != nil {
			_ = hub.rc.SetWriteDeadline(time.Now())
		}
		<-done
	}()

	conn := &sseConn{
		s: s, hub: hub, ctx: ctx, topics: topics, sessionID: sessionID, world: newSSEWorld(s),
	}
	for {
		if ctx.Err() != nil {
			return
		}
		if !conn.run(ctx, resume) {
			return
		}
		resume = nil
		body, err := json.Marshal(apiv1.ResetEvent{Topics: topicList})
		if err != nil {
			return
		}
		if !hub.send(sseMsg{event: "reset", data: body}) {
			return
		}
		conn.world = newSSEWorld(s)
	}
}

func parseLastEventID(raw string, known map[string]bool) (map[string]int64, error) {
	out := map[string]int64{}
	seen := map[string]bool{}
	for _, part := range strings.Split(raw, ";") {
		name, revStr, ok := strings.Cut(part, "=")
		if !ok || name == "" || revStr == "" || seen[name] {
			return nil, errSSEFuture
		}
		seen[name] = true
		rev, err := strconv.ParseInt(revStr, 10, 64)
		if err != nil || rev < 1 {
			return nil, errSSEFuture
		}
		if !known[name] {
			continue
		}
		out[name] = rev
	}
	if len(seen) == 0 {
		return nil, errSSEFuture
	}
	return out, nil
}

type sseMsg struct {
	id    string
	event string
	data  []byte
}

type sseHub struct {
	w      io.Writer
	rc     *http.ResponseController
	ch     chan sseMsg
	cancel context.CancelFunc
	once   sync.Once
}

func newSSEHub(w http.ResponseWriter, cap int, cancel context.CancelFunc) *sseHub {
	return &sseHub{
		w: w, rc: http.NewResponseController(w), ch: make(chan sseMsg, cap), cancel: cancel,
	}
}

func (h *sseHub) send(m sseMsg) bool {
	select {
	case h.ch <- m:
		return true
	default:
		h.fail()
		return false
	}
}

func (h *sseHub) fail() {
	h.once.Do(func() {
		if h.rc != nil {
			_ = h.rc.SetWriteDeadline(time.Now())
		}
		if h.cancel != nil {
			h.cancel()
		}
	})
}

func (h *sseHub) writeLoop(ctx context.Context, hb time.Duration) {
	defer h.cancel()
	ticker := time.NewTicker(hb)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-h.ch:
			if !ok {
				return
			}
			if err := writeSSE(h.w, msg); err != nil {
				h.fail()
				return
			}
			flushSSE(h.w)
			ticker.Reset(hb)
		case <-ticker.C:
			if _, err := io.WriteString(h.w, ": keepalive\n\n"); err != nil {
				h.fail()
				return
			}
			flushSSE(h.w)
		}
	}
}

func writeSSE(w io.Writer, m sseMsg) error {
	var b bytes.Buffer
	if m.id != "" {
		fmt.Fprintf(&b, "id: %s\n", m.id)
	}
	fmt.Fprintf(&b, "event: %s\n", m.event)
	for _, line := range strings.Split(string(m.data), "\n") {
		fmt.Fprintf(&b, "data: %s\n", line)
	}
	b.WriteByte('\n')
	_, err := w.Write(b.Bytes())
	return err
}

func flushSSE(w io.Writer) {
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

type sseKV struct {
	val         []byte
	mod, create int64
}

type sseRegionState struct {
	rev  int64
	down bool
	have bool
	keys map[string]sseKV
}

type sseWorld struct {
	mu   sync.Mutex
	gen  uint64
	reg  map[string]*sseRegionState
	sent map[string][]byte
}

func newSSEWorld(s *Server) *sseWorld {
	w := &sseWorld{reg: map[string]*sseRegionState{}, sent: map[string][]byte{}}
	for _, c := range s.clusters {
		w.reg[c.name] = &sseRegionState{down: true, keys: map[string]sseKV{}}
	}
	return w
}

func (w *sseWorld) eventID() string {
	parts := make([]string, 0, len(w.reg))
	for name, st := range w.reg {
		if st == nil || !st.have || st.down || st.rev < 1 {
			continue
		}
		parts = append(parts, name+"="+strconv.FormatInt(st.rev, 10))
	}
	slices.Sort(parts)
	return strings.Join(parts, ";")
}

func (w *sseWorld) copyViews() (map[string]state.View, []string) {
	views := make(map[string]state.View, len(w.reg))
	var unavail []string
	for name, st := range w.reg {
		if st == nil || !st.have || st.down {
			unavail = append(unavail, name)
			continue
		}
		entries := make(map[string]state.Entry, len(st.keys))
		for k, kv := range st.keys {
			entries[k] = state.Entry{Key: k, Value: kv.val, ModRevision: kv.mod, CreateRevision: kv.create}
		}
		views[name] = state.View{Rev: st.rev, Entries: entries}
	}
	slices.Sort(unavail)
	return views, unavail
}

func (w *sseWorld) replace(region string, rev int64, keys map[string]sseKV) {
	st := w.reg[region]
	if st == nil {
		st = &sseRegionState{}
		w.reg[region] = st
	}
	st.rev = rev
	st.down = false
	st.have = true
	if keys == nil {
		keys = map[string]sseKV{}
	}
	st.keys = keys
}

type sseConn struct {
	s         *Server
	hub       *sseHub
	ctx       context.Context
	topics    map[apiv1.EventTopic]bool
	sessionID string
	world     *sseWorld

	recMu sync.Mutex
	recs  map[string]*sessionRecord
}

func (c *sseConn) run(ctx context.Context, resume map[string]int64) bool {
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	resetCh := make(chan struct{}, 1)
	var wg sync.WaitGroup
	signal := func() {
		select {
		case resetCh <- struct{}{}:
		default:
		}
		cancel()
	}
	for _, cl := range c.s.clusters {
		region := cl.name
		rev := int64(0)
		resumed := false
		if resume != nil {
			if got, ok := resume[region]; ok {
				rev = got
				resumed = true
			}
		}
		wg.Add(1)
		go func(region string, rev int64, resumed bool) {
			defer wg.Done()
			c.follow(rctx, region, rev, resumed, signal)
		}(region, rev, resumed)
	}
	select {
	case <-ctx.Done():
		cancel()
		wg.Wait()
		return false
	case <-resetCh:
		cancel()
		wg.Wait()
		return ctx.Err() == nil
	}
}

func (c *sseConn) follow(ctx context.Context, region string, rev int64, resumed bool, signal func()) {
	bo := c.s.minBackoff
	for {
		if ctx.Err() != nil {
			return
		}
		if resumed {
			keys, got, err := c.s.listAt(ctx, region, rev)
			if ctx.Err() != nil {
				return
			}
			if isCompact(err) {
				signal()
				return
			}
			if isFuture(err) {
				resumed = false
				continue
			}
			if err != nil {
				c.markDown(region)
				if !sleepCtx(ctx, bo) {
					return
				}
				bo = min(bo*2, c.s.maxBackoff)
				continue
			}
			c.install(ctx, region, got, keys, false)
			rev = got
			resumed = false
			bo = c.s.minBackoff
		} else {
			keys, got, err := c.s.listAt(ctx, region, 0)
			if ctx.Err() != nil {
				return
			}
			if isCompact(err) {
				signal()
				return
			}
			if err != nil {
				c.markDown(region)
				if !sleepCtx(ctx, bo) {
					return
				}
				bo = min(bo*2, c.s.maxBackoff)
				continue
			}
			c.install(ctx, region, got, keys, true)
			rev = got
			bo = c.s.minBackoff
		}
		err := c.watch(ctx, region, rev+1)
		if ctx.Err() != nil {
			return
		}
		if isCompact(err) {
			signal()
			return
		}
		if isFuture(err) {
			continue
		}
		c.markDown(region)
		if !sleepCtx(ctx, bo) {
			return
		}
		bo = min(bo*2, c.s.maxBackoff)
	}
}

func (c *sseConn) install(ctx context.Context, region string, rev int64, keys map[string]sseKV, emit bool) {
	for k, kv := range keys {
		c.s.noteAgentValue(k, kv.val)
	}
	c.world.mu.Lock()
	c.world.replace(region, rev, keys)
	c.world.gen++
	gen := c.world.gen
	views, unavail := c.world.copyViews()
	c.world.mu.Unlock()
	items := c.build(ctx, views, unavail)
	c.world.mu.Lock()
	defer c.world.mu.Unlock()
	if c.world.gen != gen {
		return
	}
	if !emit {
		c.prime(region, items)
		return
	}
	c.emit(items)
}

func (c *sseConn) markDown(region string) {
	c.world.mu.Lock()
	st := c.world.reg[region]
	if st == nil || st.down {
		c.world.mu.Unlock()
		return
	}
	st.down = true
	st.keys = map[string]sseKV{}
	c.world.gen++
	gen := c.world.gen
	views, unavail := c.world.copyViews()
	c.world.mu.Unlock()
	items := c.build(c.ctx, views, unavail)
	c.world.mu.Lock()
	defer c.world.mu.Unlock()
	if c.world.gen != gen {
		return
	}
	c.emit(items)
}

func (c *sseConn) watch(ctx context.Context, region string, from int64) error {
	if from < 1 {
		from = 1
	}
	r, err := c.s.dialRequest(ctx, region)
	if err != nil {
		return err
	}
	id := sseWatchSeq.Add(1)
	wctx := metadata.AppendToOutgoingContext(ctx, "mftiko-watch", fmt.Sprintf("sse.%d", id))
	wctx = clientv3.WithRequireLeader(wctx)
	wch := r.Watch(wctx, keyPrefix, clientv3.WithPrefix(), clientv3.WithRev(from), clientv3.WithCreatedNotify())
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.Done():
			return errors.New("etcd client closed")
		case resp, ok := <-wch:
			if !ok {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return errors.New("watch closed")
			}
			if resp.CompactRevision != 0 {
				return errSSECompact
			}
			if err := resp.Err(); err != nil {
				if isCompact(err) {
					return errSSECompact
				}
				if isFuture(err) {
					return errSSEFuture
				}
				return err
			}
			if resp.Created || resp.IsProgressNotify() || len(resp.Events) == 0 {
				continue
			}
			if err := c.applyWatch(ctx, region, resp); err != nil {
				return err
			}
		}
	}
}

func (c *sseConn) applyWatch(ctx context.Context, region string, resp clientv3.WatchResponse) error {
	c.world.mu.Lock()
	st := c.world.reg[region]
	if st == nil || st.down {
		c.world.mu.Unlock()
		return nil
	}
	next := make(map[string]sseKV, len(st.keys)+len(resp.Events))
	for k, kv := range st.keys {
		next[k] = kv
	}
	for _, ev := range resp.Events {
		if ev == nil || ev.Kv == nil {
			continue
		}
		key := string(ev.Kv.Key)
		if ev.Type == clientv3.EventTypeDelete {
			delete(next, key)
			continue
		}
		val := append([]byte(nil), ev.Kv.Value...)
		next[key] = sseKV{val: val, mod: ev.Kv.ModRevision, create: ev.Kv.CreateRevision}
		c.s.noteAgentValue(key, val)
	}
	st.keys = next
	if resp.Header != nil && resp.Header.Revision > st.rev {
		st.rev = resp.Header.Revision
	}
	c.world.gen++
	gen := c.world.gen
	views, unavail := c.world.copyViews()
	c.world.mu.Unlock()
	items := c.build(ctx, views, unavail)
	c.world.mu.Lock()
	defer c.world.mu.Unlock()
	if c.world.gen != gen {
		return nil
	}
	c.emit(items)
	return nil
}

type sseItem struct {
	key, region, event string
	body               []byte
}

func (c *sseConn) build(ctx context.Context, views map[string]state.View, unavail []string) []sseItem {
	fresh := freshnessOf(c.s.cfg.Topology, views, unavail)
	var items []sseItem
	if c.topics[apiv1.Sessions] {
		for region, view := range views {
			for key, e := range view.Entries {
				plane, inst, err := state.Owner(key)
				if err != nil || plane != state.PlaneSTS {
					continue
				}
				home, err := c.s.cfg.Topology.RegionOf(inst)
				if err != nil || home != region {
					continue
				}
				id := sessionIDOfSpec(key, inst)
				if id == "" || (c.sessionID != "" && id != c.sessionID) {
					continue
				}
				spec := append([]byte(nil), e.Value...)
				rec := c.recordFor(ctx, id, e.CreateRevision, spec)
				sess, err := c.s.sessionIn(views, inst, id, spec, e.ModRevision, rec)
				if err != nil {
					continue
				}
				body, err := json.Marshal(detailFrom(sess, fresh))
				if err != nil {
					continue
				}
				items = append(items, sseItem{key: "session/" + id, region: region, event: "session", body: body})
			}
		}
	}
	if c.topics[apiv1.Instances] {
		for _, name := range slices.Sorted(maps.Keys(c.s.cfg.Planes)) {
			detail, ok := c.s.instanceDetail(views, name, c.s.cfg.Planes[name], fresh)
			if !ok {
				continue
			}
			body, err := json.Marshal(detail)
			if err != nil {
				continue
			}
			items = append(items, sseItem{key: "instance/" + name, region: detail.Region, event: "instance", body: body})
		}
	}
	slices.SortFunc(items, func(a, b sseItem) int {
		if c := rankEvent(a.event) - rankEvent(b.event); c != 0 {
			return c
		}
		return strings.Compare(a.key, b.key)
	})
	return items
}

func rankEvent(event string) int {
	if event == "session" {
		return 0
	}
	return 1
}

func (c *sseConn) recordFor(ctx context.Context, id string, createRev int64, spec []byte) *sessionRecord {
	key := id + "\x00" + strconv.FormatInt(createRev, 10)
	c.recMu.Lock()
	if rec, ok := c.recs[key]; ok {
		c.recMu.Unlock()
		return rec
	}
	c.recMu.Unlock()
	rec, err := c.s.rowForView(ctx, id, createRev, spec)
	if err != nil || rec == nil {
		return nil
	}
	c.recMu.Lock()
	if c.recs == nil {
		c.recs = map[string]*sessionRecord{}
	}
	if prev, ok := c.recs[key]; ok {
		c.recMu.Unlock()
		return prev
	}
	c.recs[key] = rec
	c.recMu.Unlock()
	return rec
}

func (c *sseConn) prime(region string, items []sseItem) {
	if c.world.sent == nil {
		c.world.sent = map[string][]byte{}
	}
	for _, it := range items {
		if it.region != region {
			continue
		}
		c.world.sent[it.key] = append([]byte(nil), it.body...)
	}
}

func (c *sseConn) emit(items []sseItem) {
	if c.world.sent == nil {
		c.world.sent = map[string][]byte{}
	}
	want := map[string]sseItem{}
	for _, it := range items {
		want[it.key] = it
	}
	var removed []string
	for key := range c.world.sent {
		if _, ok := want[key]; ok {
			continue
		}
		if id, ok := strings.CutPrefix(key, "session/"); ok {
			removed = append(removed, id)
		}
		delete(c.world.sent, key)
	}
	slices.Sort(removed)
	var msgs []sseMsg
	for _, id := range removed {
		body, err := json.Marshal(apiv1.SessionRemovedEvent{SessionId: id})
		if err != nil {
			continue
		}
		msgs = append(msgs, sseMsg{event: "session_removed", data: body})
	}
	for _, it := range items {
		if bytes.Equal(c.world.sent[it.key], it.body) {
			continue
		}
		c.world.sent[it.key] = append([]byte(nil), it.body...)
		msgs = append(msgs, sseMsg{event: it.event, data: it.body})
	}
	if len(msgs) == 0 {
		return
	}
	id := c.world.eventID()
	for i := range msgs {
		if i == len(msgs)-1 {
			msgs[i].id = id
		}
		if !c.hub.send(msgs[i]) {
			return
		}
	}
}

func (s *Server) listAt(ctx context.Context, region string, rev int64) (map[string]sseKV, int64, error) {
	r, err := s.dialRequest(ctx, region)
	if err != nil {
		return nil, 0, err
	}
	end := clientv3.GetPrefixRangeEnd(keyPrefix)
	out := map[string]sseKV{}
	var pinned int64
	key := keyPrefix
	for {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		opts := []clientv3.OpOption{clientv3.WithRange(end), clientv3.WithLimit(sseListPage)}
		switch {
		case pinned != 0:
			opts = append(opts, clientv3.WithRev(pinned))
		case rev > 0:
			opts = append(opts, clientv3.WithRev(rev))
		}
		resp, err := r.Get(ctx, key, opts...)
		if err != nil {
			return nil, 0, err
		}
		if pinned == 0 {
			// A historical read's header revision is the store's current revision, not the
			// revision the keys were read at. Watching past that header would skip every
			// event between the requested revision and now (E10-D094 starts at revision+1).
			if rev > 0 {
				pinned = rev
			} else {
				if resp.Header == nil {
					return nil, 0, errors.New("sse list: no header")
				}
				pinned = resp.Header.Revision
			}
		}
		for _, kv := range resp.Kvs {
			if kv == nil {
				continue
			}
			out[string(kv.Key)] = sseKV{
				val: append([]byte(nil), kv.Value...), mod: kv.ModRevision, create: kv.CreateRevision,
			}
		}
		if !resp.More || len(resp.Kvs) == 0 {
			break
		}
		key = string(resp.Kvs[len(resp.Kvs)-1].Key) + "\x00"
	}
	return out, pinned, nil
}

func isCompact(err error) bool {
	return errors.Is(err, rpctypes.ErrCompacted) || errors.Is(err, errSSECompact)
}

func isFuture(err error) bool {
	return errors.Is(err, rpctypes.ErrFutureRev) || errors.Is(err, errSSEFuture)
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d < 0 {
		d = 0
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
