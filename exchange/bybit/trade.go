package bybit

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/internal/codec"
)

// tradeStream is one /v5/trade connection (D303). Auth is the same frame as the private
// socket. Orders are not signed again. The connection does not carry private events; those
// stay on /v5/private. Cancel-batch stays on REST.
type tradeStream struct {
	ws  string
	key string
	sig *codec.Signer
}

var _ exchange.TradeStream = (*tradeStream)(nil)

func (t *tradeStream) Limits() exchange.TradeLimits {
	return exchange.TradeLimits{
		Rate:      []exchange.RateLimit{{N: 10, Per: time.Second}},
		Keepalive: exchange.Keepalive{Interval: 20 * time.Second, Timeout: time.Minute},
		ReadLimit: 1 << 20,
	}
}

func (t *tradeStream) Ping() []byte { return []byte(`{"op":"ping"}`) }

func (t *tradeStream) Dial(now time.Time) (*exchange.Dial, error) {
	auth, err := authFrame(t.key, t.sig, now)
	if err != nil {
		return nil, err
	}
	return &exchange.Dial{URL: t.ws, Frames: [][]byte{auth}}, nil
}

func (t *tradeStream) Submit(now time.Time, id uint64, req *exchange.OrderRequest) ([]byte, error) {
	body, err := createBodyOf(req)
	if err != nil {
		return nil, err
	}
	return t.order(now, id, "order.create", body)
}

func (t *tradeStream) Cancel(now time.Time, id uint64, ref exchange.CancelRef) ([]byte, error) {
	body, err := cancelBodyOf(ref)
	if err != nil {
		return nil, err
	}
	return t.order(now, id, "order.cancel", body)
}

type tradeCall struct {
	ReqID  string      `json:"reqId"`
	Header tradeHeader `json:"header"`
	Op     string      `json:"op"`
	Args   []any       `json:"args"`
}

type tradeHeader struct {
	Timestamp  string `json:"X-BAPI-TIMESTAMP"`
	RecvWindow string `json:"X-BAPI-RECV-WINDOW"`
}

func (t *tradeStream) order(now time.Time, id uint64, op string, arg any) ([]byte, error) {
	if id == 0 {
		return nil, errors.New("bybit: request id is 0")
	}
	reqID := strconv.FormatUint(id, 10)
	if len(reqID) > cloidMax {
		return nil, fmt.Errorf("bybit: request id is %d characters, limit %d", len(reqID), cloidMax)
	}
	return marshalBody(tradeCall{
		ReqID:  reqID,
		Header: tradeHeader{Timestamp: strconv.FormatInt(now.UnixMilli(), 10), RecvWindow: recvWindow},
		Op:     op,
		Args:   []any{arg},
	})
}

// Decode reads one trade frame. Auth retCode 0 is Ready. order.create and order.cancel answer
// the worker's reqId. A ping answer (op pong) is ignored. frame is lent for the call.
func (t *tradeStream) Decode(frame []byte, sink exchange.TradeSink) error {
	if len(frame) == 0 || frame[0] != '{' || sink == nil {
		return errors.New("bybit: unrecognized frame")
	}
	var head tradeHead
	if err := codec.DecodeFrame(frame, &head); err != nil {
		return fmt.Errorf("bybit: trade frame: %w", err)
	}
	if head.Op == "" {
		return errors.New("bybit: trade frame has no op")
	}
	if u := usedHeader(head.Header.Limit, head.Header.Left, head.Header.Reset); len(u) > 0 {
		sink.Used(u)
	}
	switch head.Op {
	case "ping", "pong":
		return nil
	case "auth":
		if head.Code != 0 {
			return classify(http.StatusOK, head.Code, strings.Clone(head.Msg))
		}
		sink.Ready()
		return nil
	case "order.create", "order.cancel":
		return tradeAnswer(head, frame, sink)
	default:
		return fmt.Errorf("bybit: trade op %q", head.Op)
	}
}

type tradeHead struct {
	Op     string `json:"op"`
	ReqID  text   `json:"reqId"`
	Code   int    `json:"retCode"`
	Msg    string `json:"retMsg"`
	Header struct {
		Limit string `json:"X-Bapi-Limit"`
		Left  string `json:"X-Bapi-Limit-Status"`
		Reset string `json:"X-Bapi-Limit-Reset-Timestamp"`
		Now   string `json:"Timenow"`
	} `json:"header"`
}

func tradeAnswer(head tradeHead, frame []byte, sink exchange.TradeSink) error {
	if head.ReqID == "" {
		return fmt.Errorf("bybit: %s has no reqId", head.Op)
	}
	id, err := strconv.ParseUint(string(head.ReqID), 10, 64)
	if err != nil || id == 0 {
		return fmt.Errorf("bybit: %s reqId %q", head.Op, head.ReqID)
	}
	if head.Code != 0 {
		err := classify(http.StatusOK, head.Code, strings.Clone(head.Msg))
		if head.Op == "order.create" {
			sink.Submitted(id, nil, err)
		} else {
			sink.Canceled(id, err)
		}
		return nil
	}
	if head.Op == "order.cancel" {
		sink.Canceled(id, nil)
		return nil
	}
	var body struct {
		Data struct {
			OrderID string `json:"orderId"`
		} `json:"data"`
	}
	if err := codec.DecodeFrame(frame, &body); err != nil {
		return fmt.Errorf("bybit: order.create: %w", err)
	}
	if body.Data.OrderID == "" {
		return errors.New("bybit: empty order id")
	}
	sink.Submitted(id, &exchange.OrderResult{
		OrderID: strings.Clone(body.Data.OrderID), Status: exchange.OrderStatusPendingNew,
		ExchTsNs: msString(head.Header.Now),
	}, nil)
	return nil
}

func usedHeader(limit, left, reset string) []exchange.Used {
	if limit == "" || left == "" {
		return nil
	}
	lim, errL := strconv.Atoi(limit)
	rem, errS := strconv.Atoi(left)
	if errL != nil || errS != nil || rem > lim {
		return nil
	}
	return []exchange.Used{{Bucket: bucketEndpoint, N: lim - rem, ResetAt: resetMillis(reset)}}
}
