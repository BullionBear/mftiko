package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"maps"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	"github.com/BullionBear/mftiko/contract/wire/envelope"
)

// record is one md.f message in the -out file.
type record struct {
	Wall    string  `json:"wall"`
	Subject string  `json:"subject"`
	Epoch   uint64  `json:"epoch"`
	Seq     uint64  `json:"seq"`
	MDTsNs  int64   `json:"md_ts_ns"`
	Kind    string  `json:"kind"`
	Reset   bool    `json:"reset,omitempty"`
	Levels  int     `json:"levels,omitempty"`
	Price   float64 `json:"price,omitempty"`
	Note    string  `json:"note,omitempty"`
}

type feedState struct {
	epoch, seq            uint64
	n, gaps, dups, resets int
	epochs                []uint64
	last                  time.Time
}

// Notes on a record. The upper-case ones fail the watch.
const (
	noteFirst     = "first"
	noteNewEpoch  = "new epoch"
	noteGap       = "GAP"
	noteDup       = "DUP"
	noteEpochBack = "EPOCH WENT BACK"
	noteEpochSeq  = "NEW EPOCH NOT AT SEQ 1"
)

// step is the seq and conn_epoch rule (D409, D425): +1 within one epoch, and a new, larger epoch
// starts at 1. It returns the note for the message and updates st.
func (st *feedState) step(epoch, seq uint64) string {
	var note string
	switch {
	case st.n == 0:
		note = noteFirst
		st.epochs = []uint64{epoch}
	case epoch < st.epoch:
		note = noteEpochBack
		st.gaps++
	case epoch > st.epoch:
		note = noteNewEpoch
		if seq != 1 {
			note = noteEpochSeq
			st.gaps++
		}
		st.epochs = append(st.epochs, epoch)
	case seq == st.seq+1:
	case seq <= st.seq:
		note = noteDup
		st.dups++
	default:
		note = noteGap
		st.gaps++
	}
	if note != noteDup && note != noteEpochBack {
		st.epoch, st.seq = epoch, seq
	}
	st.n++
	return note
}

func describe(payload []byte) (kind string, reset bool, levels int, price float64, err error) {
	ev := &mdv1.FeedEvent{}
	if err := proto.Unmarshal(payload, ev); err != nil {
		return "", false, 0, 0, err
	}
	switch e := ev.GetEvent().(type) {
	case *mdv1.FeedEvent_OrderBookDelta:
		d := e.OrderBookDelta
		return "orderbook_delta", d.GetReset_(), len(d.GetBids()) + len(d.GetAsks()), 0, nil
	case *mdv1.FeedEvent_AggTrade:
		return "aggtrade", false, 0, e.AggTrade.GetPrice(), nil
	case *mdv1.FeedEvent_Trade:
		return "trade", false, 0, e.Trade.GetPrice(), nil
	case *mdv1.FeedEvent_Liquidation:
		return "liquidation", false, 0, e.Liquidation.GetPrice(), nil
	default:
		return fmt.Sprintf("%T", e), false, 0, 0, nil
	}
}

// watch follows md.f.{inst}.> until SIGINT or -for, checks every subject with step, and writes
// every message to -out. It prints the epoch changes and resets as they happen and a summary
// per subject every -every. It fails when any subject had a gap, a duplicate or a bad message.
func watch() error {
	out := flag.String("out", "watch.jsonl", "record file, appended")
	every := flag.Duration("every", 10*time.Second, "summary interval")
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
	feeds := map[string]*feedState{}
	bad := 0
	sub, err := nc.Subscribe("md.f."+*instance+".>", func(m *nats.Msg) {
		now := time.Now()
		ts, epoch, seq, _, payload, err := envelope.Parse(m.Data)
		mu.Lock()
		defer mu.Unlock()
		if err != nil {
			bad++
			fmt.Printf("%s %s BAD ENVELOPE %v\n", stamp(), m.Subject, err)
			return
		}
		kind, reset, levels, price, err := describe(payload)
		if err != nil {
			bad++
			fmt.Printf("%s %s BAD PAYLOAD %v\n", stamp(), m.Subject, err)
			return
		}
		st := feeds[m.Subject]
		if st == nil {
			st = &feedState{}
			feeds[m.Subject] = st
		}
		prevEpoch, prevSeq := st.epoch, st.seq
		note := st.step(epoch, seq)
		st.last = now
		if reset {
			st.resets++
		}
		switch {
		case note == noteFirst:
			fmt.Printf("%s %s first epoch=%d seq=%d kind=%s reset=%v\n", stamp(), m.Subject, epoch, seq, kind, reset)
		case note != "":
			fmt.Printf("%s %s %s: (%d,%d) -> (%d,%d) reset=%v\n", stamp(), m.Subject, note, prevEpoch, prevSeq, epoch, seq, reset)
		case reset:
			fmt.Printf("%s %s reset at (%d,%d), %d levels\n", stamp(), m.Subject, epoch, seq, levels)
		}
		enc.Encode(record{ //nolint:errcheck // a short record file shows in the summary counts
			Wall: now.UTC().Format(time.RFC3339Nano), Subject: m.Subject, Epoch: epoch, Seq: seq, MDTsNs: ts,
			Kind: kind, Reset: reset, Levels: levels, Price: price, Note: note,
		})
	})
	if err != nil {
		return err
	}
	defer sub.Unsubscribe() //nolint:errcheck
	if err := nc.Flush(); err != nil {
		return err
	}
	fmt.Printf("%s watching md.f.%s.> into %s\n", stamp(), *instance, *out)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if *dur > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *dur)
		defer cancel()
	}
	tick := time.NewTicker(*every)
	defer tick.Stop()
	summary := func() (failed bool) {
		mu.Lock()
		defer mu.Unlock()
		for _, s := range slices.Sorted(maps.Keys(feeds)) {
			st := feeds[s]
			fmt.Printf("%s SUMMARY %s n=%d at=(%d,%d) epochs=%v gaps=%d dups=%d resets=%d last=%s ago\n",
				stamp(), s, st.n, st.epoch, st.seq, st.epochs, st.gaps, st.dups, st.resets, time.Since(st.last).Round(time.Millisecond))
			failed = failed || st.gaps > 0 || st.dups > 0
		}
		return failed || bad > 0
	}
	for {
		select {
		case <-ctx.Done():
			if summary() {
				return fmt.Errorf("gaps, duplicates or bad messages; see %s", *out)
			}
			return nil
		case <-tick.C:
			summary()
		}
	}
}
