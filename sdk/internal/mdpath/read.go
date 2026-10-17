package mdpath

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
)

// Tape asks md.view for one page of a feed's tape (D436, D847). The call returns when the
// request has been started. fn runs on the TD buffer with the page, or with an error when
// the wait times out, no responder is listening, or the reply reports an error (D848).
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
	p.track(func() {
		data, err := p.request(subject, body)
		page, err := tapeFrom(data, err)
		p.enqueue(func() { fn(page, err) })
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
	p.track(func() {
		data, err := p.request(subject, body)
		snap, err := bookFrom(data, err)
		p.enqueue(func() { fn(snap, err) })
	})
	return nil
}

// FetchKlines asks md.fetch for the latest limit closed klines (D419, D456). fn runs on
// the TD buffer. Timeout and a missing responder are errors there (D848).
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
	p.track(func() {
		data, err := p.request(subject, body)
		kl, err := klinesFrom(data, err)
		p.enqueue(func() { fn(kl, err) })
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
	p.track(func() {
		data, err := p.request(subject, body)
		oi, err := oiFrom(data, err)
		p.enqueue(func() { fn(oi, err) })
	})
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
