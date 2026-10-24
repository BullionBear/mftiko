package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	symv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	"github.com/BullionBear/mftiko/contract/wire/cloid"
	"github.com/BullionBear/mftiko/internal/sym"
)

const session = "accept" // Submit.session_id and Cancel.session_id: for the worker's log only

var apiID = flag.Int64("api-id", 1, "api_id")

// uidOf asks SYM for the ticker's uid, as the SDK does at the boundary (E02-D003).
func uidOf(ticker string) (uint32, error) {
	nc, err := dialx()
	if err != nil {
		return 0, err
	}
	defer nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rep := &symv1.Reply{}
	if err := nc.Request(ctx, sym.Subject, &symv1.Request{Req: &symv1.Request_Get{Get: &symv1.GetRequest{UniversalTicker: ticker}}}, rep); err != nil {
		return 0, err
	}
	if rep.GetInfo().GetUid() == 0 {
		return 0, fmt.Errorf("SYM has no %s", ticker)
	}
	return rep.GetInfo().GetUid(), nil
}

// newCloid is a platform client_order_id under algo: now, and a random seq.
func newCloid(algo uint32) (string, error) {
	var b [2]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return cloid.Encode(algo, time.Now().UnixMilli(), uint32(binary.BigEndian.Uint16(b[:]))&cloid.SeqMax)
}

// order sends one OrderRequest on td.order.{api_id} and prints the reply. It fails when TD did
// not accept it.
func order(req *tdv1.OrderRequest) error {
	nc, err := dial()
	if err != nil {
		return err
	}
	defer nc.Close()
	b, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	m, err := nc.Request("td.order."+strconv.FormatInt(*apiID, 10), b, 10*time.Second)
	if err != nil {
		return err // no responders is 101 at the SDK
	}
	r := &tdv1.OrderReply{}
	if err := proto.Unmarshal(m.Data, r); err != nil {
		return err
	}
	fmt.Printf("%s reply %s\n", stamp(), oneLine(r))
	if !r.GetAccepted() {
		return fmt.Errorf("rejected: %d %s", r.GetCode(), r.GetReason())
	}
	return nil
}

func submit() error {
	ticker := flag.String("ticker", "BinanceUM_Perp_BTCUSDT", "UniversalTicker")
	side := flag.String("side", "buy", "buy or sell")
	typ := flag.String("type", "limit", "limit or market")
	qty := flag.Float64("qty", 0, "quantity, in contract_value_ccy")
	price := flag.Float64("price", 0, "limit price")
	tif := flag.String("tif", "gtc", "gtc, ioc, fok or post_only; limit orders only")
	reduce := flag.Bool("reduce-only", false, "reduce_only")
	algo := flag.Uint("algo", 1, "algo_id, also in the client_order_id")
	cid := flag.String("cid", "", "client_order_id; empty makes one")
	flag.Parse()
	if *qty <= 0 {
		return errors.New("-qty is required")
	}
	uid, err := uidOf(*ticker)
	if err != nil {
		return err
	}
	if *cid == "" {
		if *cid, err = newCloid(uint32(*algo)); err != nil {
			return err
		}
	}
	s := &tdv1.Submit{SessionId: session, Uid: uid, ClientOrderId: *cid, Qty: qty, ReduceOnly: *reduce}
	switch *side {
	case "buy":
		s.Side = commonv1.Side_SIDE_BUY
	case "sell":
		s.Side = commonv1.Side_SIDE_SELL
	default:
		return fmt.Errorf("-side %q", *side)
	}
	switch *typ {
	case "limit":
		if *price <= 0 {
			return errors.New("a limit order needs -price")
		}
		s.OrderType, s.Price = commonv1.OrderType_ORDER_TYPE_LIMIT, price
		v, ok := commonv1.TimeInForce_value["TIME_IN_FORCE_"+strings.ToUpper(*tif)]
		if !ok {
			return fmt.Errorf("-tif %q", *tif)
		}
		s.Tif = commonv1.TimeInForce(v)
	case "market":
		s.OrderType = commonv1.OrderType_ORDER_TYPE_MARKET
	default:
		return fmt.Errorf("-type %q", *typ)
	}
	fmt.Printf("%s submit cid=%s uid=%d %s %s qty=%v price=%v\n", stamp(), *cid, uid, *side, *typ, *qty, *price)
	return order(&tdv1.OrderRequest{Req: &tdv1.OrderRequest_Submit{Submit: s}})
}

func cancel() error {
	cid := flag.String("cid", "", "client_order_id of the order")
	flag.Parse()
	if *cid == "" {
		return errors.New("-cid is required")
	}
	fmt.Printf("%s cancel cid=%s\n", stamp(), *cid)
	return order(&tdv1.OrderRequest{Req: &tdv1.OrderRequest_Cancel{Cancel: &tdv1.Cancel{
		SessionId: session, ClientOrderId: *cid,
	}}})
}

func batch() error {
	cids := flag.String("cid", "", "client_order_ids, comma separated")
	flag.Parse()
	if *cids == "" {
		return errors.New("-cid is required")
	}
	b := &tdv1.BatchCancel{}
	for c := range strings.SplitSeq(*cids, ",") {
		b.Orders = append(b.Orders, &tdv1.BatchCancelOrder{ClientOrderId: strings.TrimSpace(c)})
	}
	fmt.Printf("%s batch cancel %d orders\n", stamp(), len(b.Orders))
	return order(&tdv1.OrderRequest{Req: &tdv1.OrderRequest_BatchCancel{BatchCancel: b}})
}

// view asks td.view for one Recon and prints it: every open order, position and balance, with
// the state, incarnation and seq they share (E06-D033).
func view() error {
	flag.Parse()
	if flag.Arg(0) != "recon" {
		return errors.New("view recon")
	}
	req := &tdv1.ViewRequest{Req: &tdv1.ViewRequest_Recon{Recon: &tdv1.ReconRequest{}}}
	nc, err := dial()
	if err != nil {
		return err
	}
	defer nc.Close()
	b, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	m, err := nc.Request("td.view."+strconv.FormatInt(*apiID, 10), b, 10*time.Second)
	if err != nil {
		return err
	}
	r := &tdv1.ViewReply{}
	if err := proto.Unmarshal(m.Data, r); err != nil {
		return err
	}
	fmt.Printf("%s state=%s incarnation=%d seq=%d reconciled_at_ns=%d\n", stamp(), r.GetState(), r.GetIncarnation(), r.GetSeq(), r.GetReconciledAtNs())
	if r.GetError() != nil {
		return fmt.Errorf("td.view: %s", oneLine(r.GetError()))
	}
	rec := r.GetRecon()
	if rec.GetReason() != tdv1.ReconReason_RECON_REASON_REQUESTED {
		return fmt.Errorf("td.view recon reason %s", rec.GetReason())
	}
	for _, o := range rec.GetOpenOrders() {
		fmt.Printf("%s order %s\n", stamp(), oneLine(o))
	}
	for _, p := range rec.GetPositions() {
		fmt.Printf("%s position %s\n", stamp(), oneLine(p))
	}
	for _, bal := range rec.GetBalances() {
		if bal.GetFree() != 0 || bal.GetLocked() != 0 {
			fmt.Printf("%s balance %s\n", stamp(), oneLine(bal))
		}
	}
	return nil
}

func oneLine(m proto.Message) string {
	b, _ := protojson.MarshalOptions{UseEnumNumbers: false}.Marshal(m)
	return string(b)
}
