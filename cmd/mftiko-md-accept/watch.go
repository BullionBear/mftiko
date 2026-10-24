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
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	"github.com/BullionBear/mftiko/contract/wire/envelope"
)

// record is one md.v1.f message in the -out file.
type record struct {
	Wall    string  `json:"wall"`
	Subject string  `json:"subject"`
	Epoch   uint64  `json:"epoch"`
	Seq     uint64  `json:"seq"`
	MDTsNs  int64   `json:"md_ts_ns"`
	Kind    string  `json:"kind"`
	UID     uint32  `json:"uid"`
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
	noteUID       = "UID"
)

// step is the seq and conn_epoch rule (E04-D009, E04-D025): +1 within one epoch, and a new, larger epoch
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

func describe(payload []byte) (kind string, reset bool, levels int, price float64, uid uint32, err error) {
	ev := &mdv1.FeedEvent{}
	if err := proto.Unmarshal(payload, ev); err != nil {
		return "", false, 0, 0, 0, err
	}
	uid = ev.GetUid()
	switch e := ev.GetEvent().(type) {
	case *mdv1.FeedEvent_OrderBookDelta:
		d := e.OrderBookDelta
		return "orderbook_delta", d.GetReset_(), len(d.GetBids()) + len(d.GetAsks()), 0, uid, nil
	case *mdv1.FeedEvent_AggTrade:
		return "aggtrade", false, 0, e.AggTrade.GetPrice(), uid, nil
	case *mdv1.FeedEvent_Trade:
		return "trade", false, 0, e.Trade.GetPrice(), uid, nil
	case *mdv1.FeedEvent_Liquidation:
		return "liquidation", false, 0, e.Liquidation.GetPrice(), uid, nil
	default:
		return fmt.Sprintf("%T", e), false, 0, 0, uid, nil
	}
}

// subjectTicker splits md.v1.f.{inst}.{topic}.{ticker}. inst and topic contain no dots;
// the ticker is the remainder, so a dotted ticker still parses.
func subjectTicker(subject, inst string) (topic, ticker string, ok bool) {
	rest, found := strings.CutPrefix(subject, "md.v1.f."+inst+".")
	if !found {
		return "", "", false
	}
	topic, ticker, ok = strings.Cut(rest, ".")
	return topic, ticker, ok && topic != "" && ticker != ""
}

// symUID is one SYM lookup, including a failure, so a watch does not ask again for the same ticker.
type symUID struct {
	uid uint32
	err error
}

// watch follows md.v1.f.{inst}.> until SIGINT or -for, checks every subject with step, and writes
// every message to -out. It prints the epoch changes and resets as they happen and a summary
// per subject every -every. The first message of a ticker asks SYM, with a deadline, and every
// payload uid must be that active-table uid (E08-D065). It fails when any subject had a gap, a
// duplicate, a bad message, or a uid that is 0 or not SYM's.
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
	known := map[string]symUID{}
	var knownMu sync.Mutex
	bad := 0
	// lookup asks SYM once per ticker. The deadline is the request's context, not a sleep.
	// A failure is cached: later messages of that ticker still count as bad, without asking again.
	lookup := func(ticker string) (uint32, error) {
		knownMu.Lock()
		if c, ok := known[ticker]; ok {
			knownMu.Unlock()
			return c.uid, c.err
		}
		knownMu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		info, err := symbolInfo(ctx, ticker)
		cancel()
		var uid uint32
		if err == nil {
			uid = info.GetUid()
			if uid == 0 {
				err = fmt.Errorf("%s uid is 0", ticker)
			}
		}
		knownMu.Lock()
		if c, ok := known[ticker]; ok {
			knownMu.Unlock()
			return c.uid, c.err
		}
		known[ticker] = symUID{uid: uid, err: err}
		knownMu.Unlock()
		return uid, err
	}
	sub, err := nc.Subscribe("md.v1.f."+*instance+".>", func(m *nats.Msg) {
		now := time.Now()
		ts, epoch, seq, _, payload, err := envelope.Parse(m.Data)
		if err != nil {
			mu.Lock()
			bad++
			mu.Unlock()
			fmt.Printf("%s %s BAD ENVELOPE %v\n", stamp(), m.Subject, err)
			return
		}
		kind, reset, levels, price, uid, err := describe(payload)
		if err != nil {
			mu.Lock()
			bad++
			mu.Unlock()
			fmt.Printf("%s %s BAD PAYLOAD %v\n", stamp(), m.Subject, err)
			return
		}
		_, ticker, ok := subjectTicker(m.Subject, *instance)
		var want uint32
		var uidErr error
		if !ok {
			uidErr = fmt.Errorf("subject %s has no ticker", m.Subject)
		} else {
			want, uidErr = lookup(ticker)
		}
		uidBad := uidErr != nil || uid == 0 || uid != want
		mu.Lock()
		defer mu.Unlock()
		if uidBad {
			bad++
			fmt.Printf("%s %s BAD UID payload=%d sym=%d err=%v\n", stamp(), m.Subject, uid, want, uidErr)
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
		if note == "" && uidBad {
			note = noteUID
		}
		switch {
		case note == noteFirst:
			fmt.Printf("%s %s first epoch=%d seq=%d kind=%s uid=%d reset=%v\n", stamp(), m.Subject, epoch, seq, kind, uid, reset)
		case note != "":
			fmt.Printf("%s %s %s: (%d,%d) -> (%d,%d) reset=%v\n", stamp(), m.Subject, note, prevEpoch, prevSeq, epoch, seq, reset)
		case reset:
			fmt.Printf("%s %s reset at (%d,%d), %d levels\n", stamp(), m.Subject, epoch, seq, levels)
		}
		enc.Encode(record{ //nolint:errcheck // a short record file shows in the summary counts
			Wall: now.UTC().Format(time.RFC3339Nano), Subject: m.Subject, Epoch: epoch, Seq: seq, MDTsNs: ts,
			Kind: kind, UID: uid, Reset: reset, Levels: levels, Price: price, Note: note,
		})
	})
	if err != nil {
		return err
	}
	defer sub.Unsubscribe() //nolint:errcheck
	if err := nc.Flush(); err != nil {
		return err
	}
	fmt.Printf("%s watching md.v1.f.%s.> into %s\n", stamp(), *instance, *out)

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
