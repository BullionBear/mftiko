package sdk

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"time"

	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/sdk/internal/mdpath"
	"github.com/BullionBear/mftiko/sdk/internal/symclient"
	"github.com/BullionBear/mftiko/sdk/internal/tdpath"
)

// sessCtx is the Context handed to every hook. Methods other than Post are for
// the hook goroutine. The local book is left to E8-15, so Book stays empty.
type sessCtx struct {
	p *plane
}

func (c *sessCtx) SessionID() string { return c.p.sessionID }

func (c *sessCtx) Params() []byte { return append([]byte(nil), c.p.params...) }

func (c *sessCtx) Resolve(ticker string) (symbol.Info, error) {
	if c.p.sym == nil {
		return symbol.Info{}, ErrStopping
	}
	info, err := c.p.sym.Resolve(c.p.resolveCtx(), ticker)
	return info, asSDK(err)
}

func (c *sessCtx) Info(uid uint32) (symbol.Info, error) {
	if c.p.sym == nil {
		return symbol.Info{}, ErrStopping
	}
	info, err := c.p.sym.Info(uid)
	return info, asSDK(err)
}

func (c *sessCtx) Submit(o Order) (string, error) {
	if c.p.td == nil {
		return "", ErrStopping
	}
	id, err := c.p.td.Submit(tdpath.Order{
		APIID:        o.APIID,
		AlgoID:       o.AlgoID,
		UID:          o.UID,
		Side:         o.Side,
		Type:         o.Type,
		TIF:          o.TIF,
		Qty:          o.Qty,
		QuoteQty:     o.QuoteQty,
		Price:        o.Price,
		RecvWindowMs: o.RecvWindowMs,
		ReduceOnly:   o.ReduceOnly,
		Params:       o.Params,
	})
	return id, asSDK(err)
}

func (c *sessCtx) Cancel(apiID int64, clientOrderID string) error {
	if c.p.td == nil {
		return ErrStopping
	}
	return asSDK(c.p.td.Cancel(apiID, clientOrderID))
}

func (c *sessCtx) BatchCancel(apiID int64, algoID uint32, uids ...uint32) error {
	if c.p.td == nil {
		return ErrStopping
	}
	return asSDK(c.p.td.BatchCancel(apiID, algoID, uids...))
}

func (c *sessCtx) RequestRecon(apiID int64) error {
	if c.p.td == nil {
		return ErrStopping
	}
	return asSDK(c.p.td.RequestRecon(apiID))
}

func (c *sessCtx) Order(apiID int64, clientOrderID string) (*tdv1.Order, bool) {
	if c.p.td == nil {
		return nil, false
	}
	return c.p.td.Order(apiID, clientOrderID)
}

func (c *sessCtx) OpenOrders(apiID int64, algoID uint32) []*tdv1.Order {
	if c.p.td == nil {
		return nil
	}
	return c.p.td.OpenOrders(apiID, algoID)
}

func (c *sessCtx) Position(apiID int64, uid uint32) (*tdv1.Position, bool) {
	if c.p.td == nil {
		return nil, false
	}
	return c.p.td.Position(apiID, uid)
}

func (c *sessCtx) Balance(apiID int64, asset string) (*tdv1.Balance, bool) {
	if c.p.td == nil {
		return nil, false
	}
	return c.p.td.Balance(apiID, asset)
}

func (c *sessCtx) Book(uint32) (BookView, bool) { return nil, false }

func (c *sessCtx) Tape(uid uint32, topic Topic, q TapeQuery, fn func(Context, *mdv1.TapePage, error)) error {
	if fn == nil {
		return errors.New("sdk: nil callback")
	}
	if c.p.md == nil {
		return ErrStopping
	}
	return asSDK(c.p.md.Tape(uid, string(topic), mdpath.TapeQuery{
		Limit: q.Limit, Before: q.Before, SinceNs: q.SinceNs,
	}, func(page *mdv1.TapePage, err error) {
		c.p.inHook("Tape", func() { fn(c, page, err) })
	}))
}

func (c *sessCtx) BookSnapshot(uid uint32, fn func(Context, *mdv1.BookSnapshot, error)) error {
	if fn == nil {
		return errors.New("sdk: nil callback")
	}
	if c.p.md == nil {
		return ErrStopping
	}
	return asSDK(c.p.md.BookSnapshot(uid, func(snap *mdv1.BookSnapshot, err error) {
		c.p.inHook("BookSnapshot", func() { fn(c, snap, err) })
	}))
}

func (c *sessCtx) FetchKlines(uid uint32, interval string, limit uint32, fn func(Context, *mdv1.Klines, error)) error {
	if fn == nil {
		return errors.New("sdk: nil callback")
	}
	if c.p.md == nil {
		return ErrStopping
	}
	return asSDK(c.p.md.FetchKlines(uid, interval, limit, func(k *mdv1.Klines, err error) {
		c.p.inHook("FetchKlines", func() { fn(c, k, err) })
	}))
}

func (c *sessCtx) FetchOpenInterest(uid uint32, fn func(Context, *mdv1.OpenInterest, error)) error {
	if fn == nil {
		return errors.New("sdk: nil callback")
	}
	if c.p.md == nil {
		return ErrStopping
	}
	return asSDK(c.p.md.FetchOpenInterest(uid, func(oi *mdv1.OpenInterest, err error) {
		c.p.inHook("FetchOpenInterest", func() { fn(c, oi, err) })
	}))
}

func (c *sessCtx) After(d time.Duration, fn func(Context)) {
	if fn == nil || c.p.disp == nil {
		return
	}
	// Each call is its own timer. The dispatcher coalesces extra fires of that one timer.
	c.p.disp.Timer(func() { c.p.inHook("After", func() { fn(c) }) }).After(d)
}

func (c *sessCtx) Post(fn func(Context)) {
	if fn == nil || c.p.disp == nil {
		return
	}
	_ = c.p.disp.PushTD(func() { c.p.inHook("Post", func() { fn(c) }) })
}

func (c *sessCtx) Exit() {
	c.p.asked.Store(true)
	if c.p.disp != nil {
		c.p.disp.Exit()
	}
}

func (c *sessCtx) Fail(reason string) {
	c.p.asked.Store(true)
	if c.p.disp != nil {
		c.p.disp.Fail(reason)
	}
}

func (c *sessCtx) VolumeDir() string { return c.p.volumeDir }

func (c *sessCtx) InputDir(name string) string {
	if c.p.inputDir == "" || !relativeName(name) {
		return ""
	}
	return filepath.Join(c.p.inputDir, name)
}

func relativeName(name string) bool {
	if name == "" || name == "." || filepath.IsAbs(name) || filepath.Clean(name) != name {
		return false
	}
	for _, p := range strings.Split(name, string(filepath.Separator)) {
		if p == "" || p == ".." {
			return false
		}
	}
	return true
}

func (p *plane) resolveCtx() context.Context {
	p.mu.Lock()
	ctx := p.hookCtx
	p.mu.Unlock()
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func asSDK(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, symclient.ErrNotInInit):
		return ErrNotInInit
	case errors.Is(err, symclient.ErrUnknownUID), errors.Is(err, mdpath.ErrUnknownUID):
		return ErrUnknownUID
	case errors.Is(err, tdpath.ErrUnknownAccount):
		return ErrUnknownAccount
	case errors.Is(err, tdpath.ErrAlgoNotDeclared):
		return ErrAlgoNotDeclared
	case errors.Is(err, mdpath.ErrNoFeed):
		return ErrNoFeed
	case errors.Is(err, tdpath.ErrStopping), errors.Is(err, mdpath.ErrStopping):
		return ErrStopping
	default:
		return err
	}
}
