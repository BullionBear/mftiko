package mdpath

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	"github.com/BullionBear/mftiko/sdk/eventlog"
	"github.com/BullionBear/mftiko/sdk/internal/dispatch"
	"github.com/BullionBear/mftiko/sdk/internal/evlog"
)

// ErrNoRead means a recorded read_reply has no read of this replay waiting for it: the strategy
// did not make that read this time.
var ErrNoRead = errors.New("mdpath: no replayed read waits for this reply")

// Tape asks md.view for one page of a feed's tape (E04-D036, E08-D047). The call returns when the
// request has been started. fn runs on the TD buffer with the page, or with an error when
// the wait times out, no responder is listening, or the reply reports an error (E08-D048).
// The SDK does not apply the page to the live stream; fn owns that.
func (p *Path) Tape(uid uint32, topic string, q TapeQuery, fn func(*mdv1.TapePage, error)) error {
	if fn == nil {
		return errors.New("mdpath: nil callback")
	}
	subject, err := p.viewOf(uid, topic)
	if err != nil {
		return err
	}
	req := &mdv1.ViewRequest{Limit: q.Limit, Before: q.Before}
	if q.SinceNs != 0 {
		since := q.SinceNs
		req.SinceNs = &since
	}
	body, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	id := p.readCall(eventlog.ReadTape, subject, body)
	if p.replay {
		p.keep(id, func(data []byte, err error) { fn(tapeFrom(data, err)) })
		return nil
	}
	p.track(func() {
		data, err := p.request(subject, body)
		in := p.replyIn(id, data, err)
		page, err := tapeFrom(data, err)
		p.enqueue(in, func() { fn(page, err) })
	})
	return nil
}

// BookSnapshot asks md.view for the windowed book of an orderbook_delta feed. fn runs on
// the TD buffer. A not_synced reply is an error; this call does not retry (E8-15 does).
func (p *Path) BookSnapshot(uid uint32, fn func(*mdv1.BookSnapshot, error)) error {
	if fn == nil {
		return errors.New("mdpath: nil callback")
	}
	subject, err := p.viewOf(uid, topicBook)
	if err != nil {
		return err
	}
	body, err := proto.Marshal(&mdv1.ViewRequest{})
	if err != nil {
		return err
	}
	id := p.readCall(eventlog.ReadBookSnapshot, subject, body)
	if p.replay {
		p.keep(id, func(data []byte, err error) { fn(bookFrom(data, err)) })
		return nil
	}
	p.track(func() {
		data, err := p.request(subject, body)
		in := p.replyIn(id, data, err)
		snap, err := bookFrom(data, err)
		p.enqueue(in, func() { fn(snap, err) })
	})
	return nil
}

// FetchKlines asks md.fetch for the latest limit closed klines (E04-D019, E04-D056). fn runs on
// the TD buffer. Timeout and a missing responder are errors there (E08-D048).
func (p *Path) FetchKlines(uid uint32, interval string, limit uint32, fn func(*mdv1.Klines, error)) error {
	if fn == nil {
		return errors.New("mdpath: nil callback")
	}
	subject, err := p.venueOf(uid)
	if err != nil {
		return err
	}
	body, err := proto.Marshal(&mdv1.FetchRequest{Req: &mdv1.FetchRequest_Klines{Klines: &mdv1.FetchKlines{
		Interval: interval,
		Limit:    limit,
		Uid:      uid,
	}}})
	if err != nil {
		return err
	}
	id := p.readCall(eventlog.ReadKlines, subject, body)
	if p.replay {
		p.keep(id, func(data []byte, err error) { fn(klinesFrom(data, err)) })
		return nil
	}
	p.track(func() {
		data, err := p.request(subject, body)
		in := p.replyIn(id, data, err)
		kl, err := klinesFrom(data, err)
		p.enqueue(in, func() { fn(kl, err) })
	})
	return nil
}

// FetchOpenInterest asks md.fetch for the open interest the exchange does not push.
func (p *Path) FetchOpenInterest(uid uint32, fn func(*mdv1.OpenInterest, error)) error {
	if fn == nil {
		return errors.New("mdpath: nil callback")
	}
	subject, err := p.venueOf(uid)
	if err != nil {
		return err
	}
	body, err := proto.Marshal(&mdv1.FetchRequest{Req: &mdv1.FetchRequest_OpenInterest{OpenInterest: &mdv1.FetchOpenInterest{
		Uid: uid,
	}}})
	if err != nil {
		return err
	}
	id := p.readCall(eventlog.ReadOpenInterest, subject, body)
	if p.replay {
		p.keep(id, func(data []byte, err error) { fn(oiFrom(data, err)) })
		return nil
	}
	p.track(func() {
		data, err := p.request(subject, body)
		in := p.replyIn(id, data, err)
		oi, err := oiFrom(data, err)
		p.enqueue(in, func() { fn(oi, err) })
	})
	return nil
}

// readCall writes the read_call of one request and returns its read id; the read_reply the
// callback's hook is given carries the same id (E20-D003). With the log off it writes nothing and
// returns 0.
func (p *Path) readCall(kind uint16, subject string, body []byte) uint64 {
	if p.rec == nil {
		return 0
	}
	id := p.reads.Add(1)
	var buf [128]byte
	p.rec.Append(eventlog.KindReadCall, kind, 0, p.nowFn().UnixNano(), id, eventlog.AppendReadCall(buf[:0], subject, nil), body)
	return id
}

// replyIn is the read_reply input of read id: the reply as it came, or how the request failed.
func (p *Path) replyIn(id uint64, data []byte, err error) dispatch.Input {
	if p.rec == nil {
		return dispatch.Input{}
	}
	in := dispatch.Input{Kind: eventlog.KindReadReply, RecvNs: p.nowFn().UnixNano(), Ref: id, Body: data}
	if err != nil {
		flags, text := evlog.Outcome(err)
		in.Flags, in.Body = flags, []byte(text)
	}
	return in
}

func (p *Path) keep(id uint64, done func(data []byte, err error)) {
	p.mu.Lock()
	p.waiting[id] = done
	p.mu.Unlock()
}

// ReplayReply runs the callback of the replayed read id with its recorded read_reply, on the
// replay's hook goroutine.
func (p *Path) ReplayReply(id uint64, flags uint16, body []byte) error {
	p.mu.Lock()
	done, ok := p.waiting[id]
	delete(p.waiting, id)
	p.mu.Unlock()
	if !ok {
		return fmt.Errorf("%w: read %d", ErrNoRead, id)
	}
	err := evlog.Err(flags, body)
	if err != nil {
		body = nil
	}
	done(body, err)
	return nil
}

func tapeFrom(data []byte, reqErr error) (*mdv1.TapePage, error) {
	if reqErr != nil {
		return nil, wrap("md.view", reqErr)
	}
	rep := &mdv1.ViewReply{}
	if err := proto.Unmarshal(data, rep); err != nil {
		return nil, fmt.Errorf("mdpath: md.view: %w", err)
	}
	if rep.GetError() != "" {
		return nil, fmt.Errorf("mdpath: md.view: %s", rep.GetError())
	}
	page := rep.GetTape()
	if page == nil {
		return nil, errors.New("mdpath: md.view: empty tape")
	}
	return page, nil
}

func bookFrom(data []byte, reqErr error) (*mdv1.BookSnapshot, error) {
	if reqErr != nil {
		return nil, wrap("md.view", reqErr)
	}
	rep := &mdv1.ViewReply{}
	if err := proto.Unmarshal(data, rep); err != nil {
		return nil, fmt.Errorf("mdpath: md.view: %w", err)
	}
	if rep.GetError() != "" {
		return nil, fmt.Errorf("mdpath: md.view: %s", rep.GetError())
	}
	snap := rep.GetBook()
	if snap == nil {
		return nil, errors.New("mdpath: md.view: empty book")
	}
	return snap, nil
}

func klinesFrom(data []byte, reqErr error) (*mdv1.Klines, error) {
	rep, err := fetchRep(data, reqErr)
	if err != nil {
		return nil, err
	}
	kl := rep.GetKlines()
	if kl == nil {
		return nil, errors.New("mdpath: md.fetch: empty klines")
	}
	return kl, nil
}

func oiFrom(data []byte, reqErr error) (*mdv1.OpenInterest, error) {
	rep, err := fetchRep(data, reqErr)
	if err != nil {
		return nil, err
	}
	oi := rep.GetOpenInterest()
	if oi == nil {
		return nil, errors.New("mdpath: md.fetch: empty open interest")
	}
	return oi, nil
}

func fetchRep(data []byte, reqErr error) (*mdv1.FetchReply, error) {
	if reqErr != nil {
		return nil, wrap("md.fetch", reqErr)
	}
	rep := &mdv1.FetchReply{}
	if err := proto.Unmarshal(data, rep); err != nil {
		return nil, fmt.Errorf("mdpath: md.fetch: %w", err)
	}
	if rep.GetError() != "" {
		if rep.GetReason() != "" {
			return nil, fmt.Errorf("mdpath: md.fetch: %s: %s", rep.GetError(), rep.GetReason())
		}
		return nil, fmt.Errorf("mdpath: md.fetch: %s", rep.GetError())
	}
	return rep, nil
}
