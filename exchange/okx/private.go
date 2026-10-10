package okx

import (
	"errors"
	"fmt"
	"time"

	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/internal/codec"
)

// privateStream is one /ws/v5/private connection. Prepare is nil: login is a frame.
// Ready is the last subscribe acknowledgement, after login has succeeded.
type privateStream struct {
	ws   string
	key  string
	pass string
	sig  *codec.Signer

	login        bool
	gotOrders    bool
	gotPositions bool
	gotAccount   bool
	ready        bool
}

var _ exchange.PrivateStream = (*privateStream)(nil)

func (p *privateStream) Prepare(time.Time) (*exchange.Call[string], error) { return nil, nil }

func (p *privateStream) Refresh(time.Time) (*exchange.Call[struct{}], error) { return nil, nil }

func (p *privateStream) Dial(now time.Time, _ string) (*exchange.Dial, error) {
	login, err := loginFrame(p.key, p.pass, p.sig, now)
	if err != nil {
		return nil, err
	}
	sub, err := marshalBody(wsOp{Op: "subscribe", Args: []subArg{
		{Channel: "orders", InstType: "ANY"},
		{Channel: "positions", InstType: "ANY"},
		{Channel: "account"},
	}})
	if err != nil {
		return nil, err
	}
	// The worker sends Frames[0] and waits for the login answer before Frames[1].
	// Subscribing before that answer is refused with 60011.
	return &exchange.Dial{URL: p.ws, Frames: [][]byte{login, sub}}, nil
}

func (p *privateStream) Limits() exchange.StreamLimits {
	return exchange.StreamLimits{
		Keepalive: exchange.Keepalive{Interval: 20 * time.Second, Timeout: 60 * time.Second},
		ReadLimit: 1 << 20,
	}
}

func (p *privateStream) Ping() []byte { return []byte("ping") }

// loginFrame signs timestamp + "GET" + "/users/self/verify". timestamp is unix seconds.
// The passphrase travels in the frame: OKX checks it there.
func loginFrame(key, pass string, sig *codec.Signer, now time.Time) ([]byte, error) {
	ts := fmt.Sprintf("%d", now.Unix())
	mac := sig.SignBase64(nil, []byte(ts+"GET"+"/users/self/verify"))
	return marshalBody(wsOp{Op: "login", Args: []loginArg{{
		APIKey: key, Passphrase: pass, Timestamp: ts, Sign: string(mac),
	}}})
}

type wsOp struct {
	Op   string `json:"op"`
	Args any    `json:"args"`
}

type loginArg struct {
	APIKey     string `json:"apiKey"`
	Passphrase string `json:"passphrase"`
	Timestamp  string `json:"timestamp"`
	Sign       string `json:"sign"`
}

type subArg struct {
	Channel  string `json:"channel"`
	InstType string `json:"instType,omitempty"`
	InstID   string `json:"instId,omitempty"`
}

func (p *privateStream) Decode(frame []byte, sink exchange.AccountSink) error {
	switch string(frame) {
	case "ping", "pong":
		return nil
	}
	var head struct {
		Event string `json:"event"`
		Code  string `json:"code"`
		Msg   string `json:"msg"`
		Arg   struct {
			Channel string `json:"channel"`
		} `json:"arg"`
	}
	if err := codec.DecodeFrame(frame, &head); err != nil {
		return fmt.Errorf("okx: private frame: %w", err)
	}
	if head.Event != "" {
		return p.event(head.Event, head.Code, head.Msg, head.Arg.Channel, sink)
	}
	if head.Arg.Channel == "" {
		return errors.New("okx: private frame has no channel")
	}
	return p.channel(head.Arg.Channel, frame, sink)
}

func (p *privateStream) event(event, code, msg, channel string, sink exchange.AccountSink) error {
	switch event {
	case "login":
		if code != "0" {
			if msg == "" {
				msg = "login refused"
			}
			return fmt.Errorf("okx: %s: %w", msg, exchange.ErrAuth)
		}
		p.login = true
		p.maybeReady(sink)
		return nil
	case "subscribe":
		if code != "" && code != "0" {
			if msg == "" {
				msg = "subscribe refused"
			}
			return fmt.Errorf("okx: subscribe %s: %s (%s)", channel, msg, code)
		}
		switch channel {
		case "orders":
			p.gotOrders = true
		case "positions":
			p.gotPositions = true
		case "account":
			p.gotAccount = true
		}
		p.maybeReady(sink)
		return nil
	case "error":
		if msg == "" {
			msg = "refused"
		}
		if code == "60004" || code == "60005" || code == "60006" || code == "60007" || code == "60009" || code == "60011" {
			return fmt.Errorf("okx: %s: %w", msg, exchange.ErrAuth)
		}
		return fmt.Errorf("okx: %s (%s)", msg, code)
	case "unsubscribe", "notice", "channel-conn-count", "channel-conn-count-error":
		return nil
	default:
		return nil
	}
}

func (p *privateStream) maybeReady(sink exchange.AccountSink) {
	if p.ready || !p.login || !p.gotOrders || !p.gotPositions || !p.gotAccount {
		return
	}
	p.ready = true
	sink.Ready()
}

func (p *privateStream) channel(channel string, frame []byte, sink exchange.AccountSink) error {
	switch channel {
	case "orders":
		return p.orders(frame, sink)
	case "positions":
		return p.positions(frame, sink)
	case "account":
		return p.account(frame, sink)
	default:
		return nil
	}
}

func (p *privateStream) orders(frame []byte, sink exchange.AccountSink) error {
	var msg struct {
		Data []orderRow `json:"data"`
	}
	if err := codec.DecodeFrame(frame, &msg); err != nil {
		return fmt.Errorf("okx: orders: %w", err)
	}
	for i := range msg.Data {
		up := msg.Data[i].update()
		sink.Order(&up)
		if f, ok := msg.Data[i].fill(); ok {
			sink.Fill(&f)
		}
	}
	return nil
}

func (p *privateStream) positions(frame []byte, sink exchange.AccountSink) error {
	var msg struct {
		Data []posRow `json:"data"`
	}
	if err := codec.DecodeFrame(frame, &msg); err != nil {
		return fmt.Errorf("okx: positions: %w", err)
	}
	var pos []exchange.Position
	for i := range msg.Data {
		if row, ok := msg.Data[i].position(); ok {
			pos = append(pos, row)
		}
	}
	if len(pos) > 0 {
		sink.Positions(pos)
	}
	return nil
}

func (p *privateStream) account(frame []byte, sink exchange.AccountSink) error {
	var msg struct {
		Data []struct {
			Details []balRow `json:"details"`
		} `json:"data"`
	}
	if err := codec.DecodeFrame(frame, &msg); err != nil {
		return fmt.Errorf("okx: account: %w", err)
	}
	var bals []exchange.Balance
	for _, acct := range msg.Data {
		for _, row := range acct.Details {
			if b, ok := row.balance(); ok {
				bals = append(bals, b)
			}
		}
	}
	if len(bals) > 0 {
		sink.Balances(bals)
	}
	return nil
}
