package conn

import (
	"strings"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	"github.com/BullionBear/mftiko/contract/wire/envelope"
)

const (
	errUnsupported = "unsupported"
	errNotHeld     = "not_held"
	errInvalid     = "invalid"
	errNotSynced   = "not_synced"
)

// tapeDraft is one tape page copied under the lock. Encoding happens after the lock is dropped,
// so an md.f publish is not stuck behind a 900 KiB marshal (D445).
type tapeDraft struct {
	events     []tapeEvent
	more       bool
	next       *cursor
	continuous int64
	gaps       []tapeGap
	head       cursor
}

func viewSubject(inst, topic, ticker string) string {
	return "md.view." + inst + "." + topic + "." + ticker
}

func splitView(inst, subject string) (topic, ticker string, ok bool) {
	rest, found := strings.CutPrefix(subject, "md.view."+inst+".")
	if !found {
		return "", "", false
	}
	return splitFeed(rest)
}

func viewError(msg string) []byte {
	b, err := proto.Marshal(&mdv1.ViewReply{Error: msg})
	if err != nil {
		return nil
	}
	return b
}

// handleView answers one md.view request. The NATS callback is already off the websocket read
// goroutine (D445). Bad protobuf and a tape limit above 5000 are ViewReply.error, so the caller
// unmarshals a business error rather than a transport failure (D439).
func (c *Conn) handleView(subject string, req []byte) []byte {
	in := &mdv1.ViewRequest{}
	if err := proto.Unmarshal(req, in); err != nil {
		return viewError(errInvalid)
	}
	topic, ticker, ok := splitView(c.inst, subject)
	if !ok {
		return viewError(errNotHeld)
	}
	c.mu.Lock()
	draft, book, errStr := c.captureView(topic, ticker, in)
	c.mu.Unlock()
	if errStr != "" {
		return viewError(errStr)
	}
	if book != nil {
		b, err := proto.Marshal(&mdv1.ViewReply{Result: &mdv1.ViewReply_Book{Book: book}})
		if err != nil {
			return viewError(errInvalid)
		}
		return b
	}
	return encodeTapeReply(draft)
}

// captureView decides the reply from the subject topic (D442). The lock is held. Tape pages
// are copied, not encoded. Book levels are new slices, safe to marshal after unlock.
func (c *Conn) captureView(topic, ticker string, in *mdv1.ViewRequest) (tapeDraft, *mdv1.BookSnapshot, string) {
	key := topic + "." + ticker
	ow, ok := c.owners[key]
	if !ok {
		return tapeDraft{}, nil, errNotHeld
	}
	if bookTopic(topic) {
		snap, errStr := c.captureBook(ow, key)
		return tapeDraft{}, snap, errStr
	}
	if !tapeTopic(topic) || !ow.record {
		return tapeDraft{}, nil, errUnsupported
	}
	if in.GetLimit() > tapePageLimit {
		return tapeDraft{}, nil, errInvalid
	}
	c.trimAll(c.now().UnixNano())
	var since *int64
	if in.SinceNs != nil {
		v := in.GetSinceNs()
		since = &v
	}
	var before *cursor
	if b := in.GetBefore(); b != nil {
		before = &cursor{epoch: b.GetConnEpoch(), seq: b.GetSeq()}
	}
	return c.buildTape(c.rings[ow.atomID], key, int(in.GetLimit()), before, since), nil, ""
}

// captureBook is the current in-window book, not a historical seq and not the top-N (D430, D441).
// The cursor is this feed's last md.f publish. orderbook and orderbook_delta do not share a seq.
func (c *Conn) captureBook(ow feedOwner, key string) (*mdv1.BookSnapshot, string) {
	if c.live == nil {
		return nil, errNotSynced
	}
	sh := c.live.books[ow.ch]
	if sh == nil || sh.book == nil || sh.stalled || sh.holdResub {
		return nil, errNotSynced
	}
	n := sh.book.Window()
	epoch, seq, mdTs := c.epoch, uint64(0), int64(0)
	if m := c.marks[key]; m.set {
		epoch, seq, mdTs = m.epoch, m.seq, m.mdTs
	}
	return &mdv1.BookSnapshot{
		Meta:      meta(sh.exchTs),
		Bids:      protoLevels(sideLevels(sh.book, true, n)),
		Asks:      protoLevels(sideLevels(sh.book, false, n)),
		ConnEpoch: epoch,
		Seq:       seq,
		MdTsNs:    mdTs,
	}, ""
}

// buildTape walks one feed from newest to oldest. before is exclusive. since_ns stops the walk
// and forces more to false once that floor is the next event (D436). A page that would encode
// past the budget drops its oldest events until it fits, unless a single event is already over
// the budget: that one is returned so the cursor still advances (D439).
func (c *Conn) buildTape(ring *tapeRing, feed string, limit int, before *cursor, since *int64) tapeDraft {
	if limit <= 0 {
		limit = tapePageDefault
	}
	var eligible []tapeEvent
	if ring != nil {
		for i := len(ring.events) - 1; i >= 0; i-- {
			e := ring.events[i]
			if e.feed != feed {
				continue
			}
			if before != nil && !cursorLess(cursor{e.epoch, e.seq}, *before) {
				continue
			}
			if since != nil && e.mdTs < *since {
				break
			}
			eligible = append(eligible, e)
		}
	}
	chosen := eligible
	if len(chosen) > limit {
		chosen = chosen[:limit]
	}
	head := c.headOf(feed)
	continuous := continuousSince(ring, feed)
	for {
		more := len(chosen) < len(eligible)
		var nextOlder *tapeEvent
		if more {
			e := eligible[len(chosen)]
			nextOlder = &e
		}
		draft := tapeDraft{
			events:     chosen,
			more:       more,
			continuous: continuous,
			gaps:       gapsFor(ring, feed, chosen, before, since, more, nextOlder),
			head:       head,
		}
		if more && len(chosen) > 0 {
			oldest := chosen[len(chosen)-1]
			draft.next = &cursor{epoch: oldest.epoch, seq: oldest.seq}
		}
		if sizeTapeReply(draft) <= tapeReplyBudget || len(chosen) <= 1 {
			out := make([]tapeEvent, len(chosen))
			copy(out, chosen)
			draft.events = out
			return draft
		}
		chosen = chosen[:len(chosen)-1]
	}
}

// gapsFor reports the gaps whose place in the history is this page.
// A gap is on the page when its first event is, when the page is the newest and the gap is
// still open, when the page is an empty first page, when the page straddles the gap, or when
// the next older event (if any) is strictly older than the gap and this page is still newer.
func gapsFor(ring *tapeRing, feed string, chosen []tapeEvent, before *cursor, since *int64, more bool, nextOlder *tapeEvent) []tapeGap {
	if ring == nil || len(ring.gaps) == 0 {
		return nil
	}
	var out []tapeGap
	for _, g := range ring.gaps {
		if gapOnPage(g, feed, chosen, before, since, more, nextOlder) {
			out = append(out, g)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func gapOnPage(g tapeGap, feed string, chosen []tapeEvent, before *cursor, since *int64, more bool, nextOlder *tapeEvent) bool {
	if g.feed != feed {
		return false
	}
	first := cursor{epoch: g.firstEpoch, seq: g.firstSeq}
	onPage := false
	if g.firstSet {
		for _, e := range chosen {
			if e.epoch == g.firstEpoch && e.seq == g.firstSeq {
				onPage = true
				break
			}
		}
	}
	if since != nil && g.endNs < *since && !onPage {
		return false
	}
	if onPage {
		return true
	}
	if !g.firstSet {
		return before == nil
	}
	if len(chosen) == 0 {
		return before == nil && !more
	}
	newest := cursor{chosen[0].epoch, chosen[0].seq}
	oldest := cursor{chosen[len(chosen)-1].epoch, chosen[len(chosen)-1].seq}
	if cursorLess(oldest, first) && cursorLess(first, newest) {
		return true
	}
	if before == nil && cursorLess(newest, first) {
		return true
	}
	if cursorLess(first, oldest) {
		if nextOlder == nil {
			return true
		}
		if cursorLess(cursor{nextOlder.epoch, nextOlder.seq}, first) {
			return true
		}
	}
	return false
}

// planViewsLocked is the diff between feeds this worker holds and md.view subscriptions.
// Every held feed is subscribed, including record=false and topics that answer unsupported,
// so the client gets that error instead of no responder (D442, D443). Pending subscriptions
// (a nil unlisten) count as present so a second apply does not subscribe twice.
func (c *Conn) planViewsLocked() (add []string, remove []func()) {
	if c.viewListen == nil || c.viewsClosed {
		return nil, nil
	}
	want := make(map[string]struct{}, len(c.owners))
	for key := range c.owners {
		topic, ticker, ok := splitFeed(key)
		if !ok {
			continue
		}
		want[viewSubject(c.inst, topic, ticker)] = struct{}{}
	}
	for subj, un := range c.bound {
		if _, ok := want[subj]; ok {
			continue
		}
		if un != nil {
			remove = append(remove, un)
		}
		delete(c.bound, subj)
	}
	for subj := range want {
		if _, ok := c.bound[subj]; ok {
			continue
		}
		c.bound[subj] = nil
		add = append(add, subj)
	}
	return add, remove
}

// rebind subscribes and unsubscribes md.view outside the worker lock. ViewListen must not run
// while c.mu is held: the callback takes that lock.
func (c *Conn) rebind(add []string, remove []func()) {
	for _, un := range remove {
		if un != nil {
			un()
		}
	}
	if c.viewListen == nil {
		return
	}
	for _, subj := range add {
		un, err := c.viewListen(subj, func(req []byte) []byte {
			return c.handleView(subj, req)
		})
		c.mu.Lock()
		closed := c.viewsClosed
		held := c.holdsView(subj)
		if err != nil || closed || !held {
			delete(c.bound, subj)
			c.mu.Unlock()
			if un != nil {
				un()
			}
			if err != nil && !closed {
				c.log.Warn().Err(err).Str("subject", subj).Msg("md.view")
			}
			continue
		}
		c.bound[subj] = un
		c.mu.Unlock()
	}
}

func (c *Conn) holdsView(subject string) bool {
	topic, ticker, ok := splitView(c.inst, subject)
	if !ok {
		return false
	}
	_, ok = c.owners[topic+"."+ticker]
	return ok
}

// closeViews drops every md.view subscription. A listen that is still in flight sees
// viewsClosed and unsubscribes itself. A second call does nothing.
func (c *Conn) closeViews() {
	c.mu.Lock()
	c.viewsClosed = true
	uns := make([]func(), 0, len(c.bound))
	for _, un := range c.bound {
		if un != nil {
			uns = append(uns, un)
		}
	}
	c.bound = map[string]func(){}
	c.mu.Unlock()
	for _, un := range uns {
		un()
	}
}

func payloadOf(e tapeEvent) []byte {
	if len(e.raw) < envelope.Size {
		return nil
	}
	return e.raw[envelope.Size:]
}

func encodeTapeReply(d tapeDraft) []byte {
	page := encodeTapePage(d)
	dst := protowire.AppendTag(nil, 10, protowire.BytesType)
	return protowire.AppendBytes(dst, page)
}

func sizeTapeReply(d tapeDraft) int {
	return sizeBytes(10, sizeTapePage(d))
}

func encodeTapePage(d tapeDraft) []byte {
	var dst []byte
	for _, e := range d.events {
		dst = appendBytes(dst, 1, encodeTapeEvent(e))
	}
	if d.more {
		dst = appendVarint(dst, 2, 1)
	}
	if d.more && d.next != nil {
		dst = appendBytes(dst, 3, encodeCursor(d.next.epoch, d.next.seq))
	}
	if d.continuous != 0 {
		dst = appendVarint(dst, 4, uint64(d.continuous))
	}
	for _, g := range d.gaps {
		dst = appendBytes(dst, 5, encodeGap(g))
	}
	// Head is always present, including (epoch, 0) before the feed has published (D437).
	dst = appendBytes(dst, 6, encodeCursor(d.head.epoch, d.head.seq))
	return dst
}

func sizeTapePage(d tapeDraft) int {
	n := 0
	for _, e := range d.events {
		n += sizeBytes(1, sizeTapeEvent(e))
	}
	if d.more {
		n += sizeVarint(2, 1)
	}
	if d.more && d.next != nil {
		n += sizeBytes(3, sizeCursor(d.next.epoch, d.next.seq))
	}
	if d.continuous != 0 {
		n += sizeVarint(4, uint64(d.continuous))
	}
	for _, g := range d.gaps {
		n += sizeBytes(5, sizeGap(g))
	}
	n += sizeBytes(6, sizeCursor(d.head.epoch, d.head.seq))
	return n
}

func encodeTapeEvent(e tapeEvent) []byte {
	var dst []byte
	dst = appendVarint(dst, 1, e.epoch)
	dst = appendVarint(dst, 2, e.seq)
	dst = appendVarint(dst, 3, uint64(e.mdTs))
	// Field 4 is the payload bytes as stored. Empty is still written, so the field is present.
	dst = appendBytes(dst, 4, payloadOf(e))
	return dst
}

func sizeTapeEvent(e tapeEvent) int {
	return sizeVarint(1, e.epoch) + sizeVarint(2, e.seq) + sizeVarint(3, uint64(e.mdTs)) + sizeBytes(4, len(payloadOf(e)))
}

func encodeGap(g tapeGap) []byte {
	var dst []byte
	dst = appendVarint(dst, 1, uint64(g.startNs))
	dst = appendVarint(dst, 2, uint64(g.endNs))
	dst = appendBytes(dst, 3, encodeCursor(g.lastEpoch, g.lastSeq))
	if g.firstSet {
		dst = appendBytes(dst, 4, encodeCursor(g.firstEpoch, g.firstSeq))
	}
	return dst
}

func sizeGap(g tapeGap) int {
	n := sizeVarint(1, uint64(g.startNs)) + sizeVarint(2, uint64(g.endNs))
	n += sizeBytes(3, sizeCursor(g.lastEpoch, g.lastSeq))
	if g.firstSet {
		n += sizeBytes(4, sizeCursor(g.firstEpoch, g.firstSeq))
	}
	return n
}

func encodeCursor(epoch, seq uint64) []byte {
	var dst []byte
	dst = appendVarint(dst, 1, epoch)
	dst = appendVarint(dst, 2, seq)
	return dst
}

func sizeCursor(epoch, seq uint64) int {
	return sizeVarint(1, epoch) + sizeVarint(2, seq)
}

func appendVarint(dst []byte, num protowire.Number, v uint64) []byte {
	if v == 0 {
		return dst
	}
	dst = protowire.AppendTag(dst, num, protowire.VarintType)
	return protowire.AppendVarint(dst, v)
}

func appendBytes(dst []byte, num protowire.Number, b []byte) []byte {
	dst = protowire.AppendTag(dst, num, protowire.BytesType)
	return protowire.AppendBytes(dst, b)
}

func sizeVarint(num protowire.Number, v uint64) int {
	if v == 0 {
		return 0
	}
	return protowire.SizeVarint(protowire.EncodeTag(num, protowire.VarintType)) + protowire.SizeVarint(v)
}

func sizeBytes(num protowire.Number, n int) int {
	return protowire.SizeVarint(protowire.EncodeTag(num, protowire.BytesType)) + protowire.SizeBytes(n)
}
