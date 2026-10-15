package bybit

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/internal/codec"
)

// privateTopics is what Dial subscribes, in order. dcp carries no account event; the venue
// requires it before Disconnected Cancel Protection can fire.
var privateTopics = []string{"order", "execution", "position", "wallet", "dcp"}

// privateStream is one /v5/private connection. Prepare is nil: auth is a frame, not a REST
// call. Ready is the subscribe acknowledgement, after auth has succeeded.
type privateStream struct {
	ws    string
	key   string
	sig   *codec.Signer
	ready bool
}

var _ exchange.PrivateStream = (*privateStream)(nil)

func (p *privateStream) Prepare(time.Time) (*exchange.Call[string], error) { return nil, nil }

func (p *privateStream) Refresh(time.Time) (*exchange.Call[struct{}], error) { return nil, nil }

func (p *privateStream) Dial(now time.Time, _ string) (*exchange.Dial, error) {
	auth, err := authFrame(p.key, p.sig, now)
	if err != nil {
		return nil, err
	}
	sub, err := marshalBody(wsCall{Op: "subscribe", Args: privateTopics})
	if err != nil {
		return nil, err
	}
	return &exchange.Dial{URL: p.ws, Frames: [][]byte{auth, sub}}, nil
}

func (p *privateStream) Limits() exchange.StreamLimits {
	return exchange.StreamLimits{
		Keepalive: exchange.Keepalive{Interval: 20 * time.Second, Timeout: time.Minute},
		ReadLimit: 1 << 20,
	}
}

func (p *privateStream) Ping() []byte { return []byte(`{"op":"ping"}`) }

// authFrame is the private and trade login. expires is 10s ahead. The signature is HMAC-SHA256
// of "GET/realtime" + expires, the same bytes both sockets check.
func authFrame(key string, sig *codec.Signer, now time.Time) ([]byte, error) {
	exp := now.Add(authLead).UnixMilli()
	mac := sig.SignHex(nil, []byte("GET/realtime"+strconv.FormatInt(exp, 10)))
	return marshalBody(wsCall{Op: "auth", Args: []any{key, exp, string(mac)}})
}

type wsCall struct {
	Op   string `json:"op"`
	Args any    `json:"args,omitempty"`
}

func (p *privateStream) Decode(frame []byte, sink exchange.AccountSink) error {
	var head struct {
		Op      string `json:"op"`
		Topic   string `json:"topic"`
		Success *bool  `json:"success"`
		RetMsg  string `json:"ret_msg"`
	}
	if err := codec.DecodeFrame(frame, &head); err != nil {
		return fmt.Errorf("bybit: private frame: %w", err)
	}
	if head.Op != "" {
		return p.op(head.Op, head.Success, head.RetMsg, sink)
	}
	if head.Topic == "" {
		return errors.New("bybit: private frame has no topic")
	}
	return p.topic(head.Topic, frame, sink)
}

func (p *privateStream) op(op string, ok *bool, msg string, sink exchange.AccountSink) error {
	switch op {
	case "ping", "pong":
		return nil
	case "auth":
		if ok == nil || !*ok {
			if msg == "" {
				msg = "auth refused"
			}
			return fmt.Errorf("bybit: %s: %w", msg, exchange.ErrAuth)
		}
		return nil
	case "subscribe":
		if ok == nil || !*ok {
			if msg == "" {
				msg = "subscribe refused"
			}
			return fmt.Errorf("bybit: subscribe: %s", msg)
		}
		if !p.ready {
			p.ready = true
			sink.Ready()
		}
		return nil
	default:
		return fmt.Errorf("bybit: private op %q", op)
	}
}

func (p *privateStream) topic(topic string, frame []byte, sink exchange.AccountSink) error {
	switch topic {
	case "order", "order.spot", "order.linear", "order.inverse", "order.option":
		return p.orders(frame, sink)
	case "execution", "execution.spot", "execution.linear", "execution.inverse", "execution.option":
		return p.execs(frame, sink)
	case "position", "position.linear", "position.inverse", "position.option":
		return p.positions(frame, sink)
	case "wallet":
		return p.wallet(frame, sink)
	case "dcp":
		return nil
	default:
		// A topic this codec does not read. Dropping the connection over it would be worse.
		return nil
	}
}

func (p *privateStream) orders(frame []byte, sink exchange.AccountSink) error {
	var msg struct {
		CreationTime int64      `json:"creationTime"`
		Data         []orderRow `json:"data"`
	}
	if err := codec.DecodeFrame(frame, &msg); err != nil {
		return fmt.Errorf("bybit: order: %w", err)
	}
	for i := range msg.Data {
		up := msg.Data[i].update("", msg.CreationTime)
		sink.Order(&up)
	}
	return nil
}

func (p *privateStream) execs(frame []byte, sink exchange.AccountSink) error {
	var msg struct {
		Data []execRow `json:"data"`
	}
	if err := codec.DecodeFrame(frame, &msg); err != nil {
		return fmt.Errorf("bybit: execution: %w", err)
	}
	for i := range msg.Data {
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
		return fmt.Errorf("bybit: position: %w", err)
	}
	var pos []exchange.Position
	for i := range msg.Data {
		// A closed position comes with size 0 and is passed on: it is how the worker learns of it.
		if msg.Data[i].Symbol != "" {
			p, _ := msg.Data[i].position("")
			pos = append(pos, p)
		}
	}
	if len(pos) > 0 {
		sink.Positions(pos)
	}
	return nil
}

func (p *privateStream) wallet(frame []byte, sink exchange.AccountSink) error {
	var msg struct {
		Data []struct {
			Coin []coinRow `json:"coin"`
		} `json:"data"`
	}
	if err := codec.DecodeFrame(frame, &msg); err != nil {
		return fmt.Errorf("bybit: wallet: %w", err)
	}
	var bals []exchange.Balance
	for _, acct := range msg.Data {
		for _, c := range acct.Coin {
			if b, ok := c.balance(); ok {
				bals = append(bals, b)
			}
		}
	}
	if len(bals) > 0 {
		sink.Balances(bals)
	}
	return nil
}
