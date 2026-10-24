package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
)

// seqs follows td.evt's seq, which each incarnation numbers from 1 (E06-D006).
type seqs struct {
	inc        uint32
	seq        uint64
	n          int
	gaps, dups int
}

const (
	noteFirst = "first"
	noteInc   = "new incarnation"
	noteGap   = "GAP"
	noteDup   = "DUP"
)

// step takes one event's (incarnation, seq) and says what it was, "" for the next one in line.
func (s *seqs) step(inc uint32, seq uint64) string {
	s.n++
	prev := s.inc
	switch {
	case s.n == 1:
		s.inc, s.seq = inc, seq
		return noteFirst
	case inc != prev:
		s.inc, s.seq = inc, seq
		if inc < prev || seq != 1 {
			s.gaps++
			return noteGap
		}
		return noteInc
	case seq == s.seq+1:
		s.seq = seq
		return ""
	case seq <= s.seq:
		s.dups++
		return noteDup
	default:
		s.gaps++
		s.seq = seq
		return noteGap
	}
}

type record struct {
	Wall  string          `json:"wall"`
	Inc   uint32          `json:"incarnation"`
	Seq   uint64          `json:"seq"`
	Note  string          `json:"note,omitempty"`
	Event json.RawMessage `json:"event"`
}

// describe is one event in a line: its kind, and for an order its client_order_id, status and
// cause.
func describe(ev *tdv1.AccountEvent) string {
	switch e := ev.GetEvent().(type) {
	case *tdv1.AccountEvent_OrderUpdate:
		o := e.OrderUpdate.GetOrder()
		s := fmt.Sprintf("OrderUpdate cid=%s order_id=%s %s %s filled=%v", o.GetClientOrderId(), o.GetOrderId(), o.GetStatus(), e.OrderUpdate.GetCause(), o.GetFilledQty())
		if e.OrderUpdate.GetCode() != 0 {
			s += fmt.Sprintf(" code=%d reason=%q", e.OrderUpdate.GetCode(), e.OrderUpdate.GetReason())
		}
		return s
	case *tdv1.AccountEvent_Fill:
		f := e.Fill
		return fmt.Sprintf("Fill cid=%s fill_id=%s %s qty=%v price=%v fee=%v %s", f.GetClientOrderId(), f.GetFillId(), f.GetSide(), f.GetQty(), f.GetPrice(), f.GetFee(), f.GetFeeAsset())
	case *tdv1.AccountEvent_PositionUpdate:
		return "PositionUpdate " + oneLine(e.PositionUpdate)
	case *tdv1.AccountEvent_BalanceUpdate:
		return fmt.Sprintf("BalanceUpdate %d assets", len(e.BalanceUpdate.GetBalances()))
	case *tdv1.AccountEvent_Recon:
		r := e.Recon
		return fmt.Sprintf("Recon %s incarnation=%d open_orders=%d positions=%d", r.GetReason(), r.GetIncarnation(), len(r.GetOpenOrders()), len(r.GetPositions()))
	default:
		return fmt.Sprintf("%T", e)
	}
}

// watch follows td.evt.{api_id} until SIGINT or -for, checks seq with step, and writes every
// event to -out. It fails when there was a gap, a duplicate or an event that does not parse.
func watch() error {
	out := flag.String("out", "td-watch.jsonl", "record file, appended")
	dur := flag.Duration("for", 0, "stop after this long; 0 runs until SIGINT")
	flag.Parse()
	f, err := os.OpenFile(*out, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	nc, err := dial()
	if err != nil {
		return err
	}
	defer nc.Close()

	var mu sync.Mutex
	st := &seqs{}
	bad := 0
	subject := "td.evt." + strconv.FormatInt(*apiID, 10)
	sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
		ev := &tdv1.AccountEvent{}
		mu.Lock()
		defer mu.Unlock()
		if err := proto.Unmarshal(m.Data, ev); err != nil {
			bad++
			fmt.Printf("%s BAD EVENT %v\n", stamp(), err)
			return
		}
		inc := ev.GetSource().GetIncarnation()
		note := st.step(inc, ev.GetSeq())
		mark := ""
		if note != "" {
			mark = " [" + note + "]"
		}
		fmt.Printf("%s inc=%d seq=%d %s%s\n", stamp(), inc, ev.GetSeq(), describe(ev), mark)
		enc.Encode(record{ //nolint:errcheck // a short record file shows in the counts
			Wall: time.Now().UTC().Format(time.RFC3339Nano), Inc: inc, Seq: ev.GetSeq(), Note: note, Event: json.RawMessage(oneLine(ev)),
		})
	})
	if err != nil {
		return err
	}
	defer sub.Unsubscribe() //nolint:errcheck
	if err := nc.Flush(); err != nil {
		return err
	}
	fmt.Printf("%s watching %s into %s\n", stamp(), subject, *out)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if *dur > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *dur)
		defer cancel()
	}
	<-ctx.Done()
	mu.Lock()
	defer mu.Unlock()
	fmt.Printf("%s SUMMARY %s n=%d at=(%d,%d) gaps=%d dups=%d bad=%d\n", stamp(), subject, st.n, st.inc, st.seq, st.gaps, st.dups, bad)
	if st.gaps > 0 || st.dups > 0 || bad > 0 {
		return fmt.Errorf("gaps, duplicates or bad events; see %s", *out)
	}
	return nil
}
