package cloidgen

import (
	"cmp"
	"errors"
	"slices"
	"sync"
	"testing"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	"github.com/BullionBear/mftiko/contract/wire/cloid"
)

// The counter is one per process. Tests in this package run one at a time and put it
// back to zero; they do not call t.Parallel.

func reset(t *testing.T) {
	t.Helper()
	nextSlot.Store(0)
}

func acct(algos ...uint32) *sessionv1.AccountDecl {
	return &sessionv1.AccountDecl{AlgoIds: algos}
}

func mustNext(t *testing.T, account *sessionv1.AccountDecl, algo uint32, nowMs int64) cloid.ID {
	t.Helper()
	s, err := Next(account, algo, nowMs)
	if err != nil {
		t.Fatalf("Next(algo %d, %d): %v", algo, nowMs, err)
	}
	id, err := cloid.Decode(s)
	if err != nil {
		t.Fatalf("Decode(%q): %v", s, err)
	}
	want, err := cloid.Encode(algo, id.TsMs, id.Seq)
	if err != nil || want != s || id.Ver != cloid.Version || id.Algo != algo {
		t.Fatalf("Next = %q, decoded %+v, encode %q %v", s, id, want, err)
	}
	return id
}

func TestSharedCounterSameMillisecond(t *testing.T) {
	reset(t)
	now := cloid.EpochMs
	a := acct(1)
	b := acct(2, 3)
	seen := make(map[string]struct{}, 1024)
	for i := 0; i < 1024; i++ {
		account := a
		algo := uint32(1)
		if i%2 == 1 {
			account = b
			algo = 2
		}
		id := mustNext(t, account, algo, now)
		if id.TsMs != now || id.Seq != uint32(i) || id.Algo != algo {
			t.Fatalf("id %d: %+v", i, id)
		}
		s := id.String()
		if _, ok := seen[s]; ok {
			t.Fatalf("duplicate %s", s)
		}
		seen[s] = struct{}{}
	}
	// The 1025th id of this millisecond borrows the next one, on either account.
	id := mustNext(t, b, 3, now)
	if id.TsMs != now+1 || id.Seq != 0 || id.Algo != 3 {
		t.Fatalf("borrow: %+v", id)
	}
	id = mustNext(t, a, 1, now)
	if id.TsMs != now+1 || id.Seq != 1 {
		t.Fatalf("after borrow: %+v", id)
	}
}

func TestClock(t *testing.T) {
	reset(t)
	a := acct(1)
	now := cloid.EpochMs + 50_000

	id := mustNext(t, a, 1, now)
	if id.TsMs != now || id.Seq != 0 {
		t.Fatalf("first: %+v", id)
	}
	// Behind the counter, including before the epoch: keep the last ts_ms.
	id = mustNext(t, a, 1, now-10_000)
	if id.TsMs != now || id.Seq != 1 {
		t.Fatalf("back: %+v", id)
	}
	id = mustNext(t, a, 1, cloid.EpochMs-1)
	if id.TsMs != now || id.Seq != 2 {
		t.Fatalf("before epoch: %+v", id)
	}
	// Ahead of the counter: jump, seq starts again at 0.
	id = mustNext(t, a, 1, now+10)
	if id.TsMs != now+10 || id.Seq != 0 {
		t.Fatalf("forward: %+v", id)
	}
	id = mustNext(t, a, 1, now)
	if id.TsMs != now+10 || id.Seq != 1 {
		t.Fatalf("back after jump: %+v", id)
	}
}

func TestClockBeforeEpochOnAFreshCounter(t *testing.T) {
	reset(t)
	// Nothing has been issued. The counter's floor is the epoch, which is ahead of this
	// clock, and a ts before the epoch cannot be encoded (E08-D012).
	id := mustNext(t, acct(4), 4, cloid.EpochMs-1)
	if id.TsMs != cloid.EpochMs || id.Seq != 0 || id.Algo != 4 {
		t.Fatalf("%+v", id)
	}
}

func TestUndeclaredAlgo(t *testing.T) {
	reset(t)
	now := cloid.EpochMs + 10
	a := acct(1, 2)
	for _, algo := range []uint32{0, 3, 1023} {
		_, err := Next(a, algo, now)
		if !errors.Is(err, ErrAlgoNotDeclared) {
			t.Fatalf("algo %d: %v", algo, err)
		}
	}
	if _, err := Next(nil, 1, now); !errors.Is(err, ErrAlgoNotDeclared) {
		t.Fatalf("nil account: %v", err)
	}
	if _, err := Next(acct(), 1, now); !errors.Is(err, ErrAlgoNotDeclared) {
		t.Fatalf("empty account: %v", err)
	}
	// A listed algo that does not fit the layout fails in Encode and takes no number.
	if _, err := Next(acct(0), 0, now); err == nil || errors.Is(err, ErrAlgoNotDeclared) {
		t.Fatalf("algo 0 listed: %v", err)
	}
	if nextSlot.Load() != 0 {
		t.Fatalf("rejected calls took a number: %#x", nextSlot.Load())
	}
	id := mustNext(t, a, 1, now)
	if id.Seq != 0 || id.TsMs != now {
		t.Fatalf("after rejects: %+v", id)
	}
}

func TestOutsideWindowDoesNotConsume(t *testing.T) {
	reset(t)
	a := acct(1)
	bad := cloid.EpochMs + tsSpan
	_, err := Next(a, 1, bad)
	_, want := cloid.Encode(1, bad, 0)
	if err == nil || want == nil || err.Error() != want.Error() {
		t.Fatalf("past window: %v, encode %v", err, want)
	}
	if nextSlot.Load() != 0 {
		t.Fatalf("consumed: %#x", nextSlot.Load())
	}
	id := mustNext(t, a, 1, cloid.EpochMs)
	if id.TsMs != cloid.EpochMs || id.Seq != 0 {
		t.Fatalf("after refuse: %+v", id)
	}
}

func TestLastMillisecondOfTheWindow(t *testing.T) {
	reset(t)
	const last = tsSpan - 1
	nextSlot.Store(uint64(last)<<cloid.SeqBits | uint64(cloid.SeqMax))
	a := acct(1)
	id := mustNext(t, a, 1, cloid.EpochMs)
	if id.TsMs != cloid.EpochMs+last || id.Seq != cloid.SeqMax {
		t.Fatalf("last id: %+v", id)
	}
	if _, err := Next(a, 1, cloid.EpochMs); err == nil {
		t.Fatal("borrowed past the window")
	}
	if _, err := Next(a, 1, cloid.EpochMs+last); err == nil {
		t.Fatal("still past the window")
	}
}

func TestConcurrentSameNow(t *testing.T) {
	reset(t)
	now := cloid.EpochMs + 80_000
	const (
		workers = 16
		per     = 200
	)
	total := workers * per
	got := make([]string, total)
	var wg sync.WaitGroup
	start := make(chan struct{})
	a := acct(1, 2)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			algo := uint32(1)
			if w%2 == 1 {
				algo = 2
			}
			for i := 0; i < per; i++ {
				s, err := Next(a, algo, now)
				if err != nil {
					t.Errorf("worker %d: %v", w, err)
					return
				}
				got[w*per+i] = s
			}
		}(w)
	}
	close(start)
	wg.Wait()

	seen := make(map[cloid.ID]struct{}, total)
	for i, s := range got {
		if s == "" {
			t.Fatalf("missing id %d", i)
		}
		id, err := cloid.Decode(s)
		if err != nil {
			t.Fatalf("Decode(%q): %v", s, err)
		}
		if _, ok := seen[id]; ok {
			t.Fatalf("duplicate %+v", id)
		}
		seen[id] = struct{}{}
	}
	for i := 0; i < total; i++ {
		want := cloid.ID{Ver: cloid.Version, TsMs: now + int64(i/1024), Seq: uint32(i % 1024)}
		// Algo is either of the two; the (ts, seq) slot is shared.
		found := false
		for _, algo := range []uint32{1, 2} {
			want.Algo = algo
			if _, ok := seen[want]; ok {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing ts %d seq %d", want.TsMs, want.Seq)
		}
	}
	if len(seen) != total {
		t.Fatalf("got %d ids, want %d", len(seen), total)
	}
}

func TestConcurrentMixedClocks(t *testing.T) {
	reset(t)
	now := cloid.EpochMs + 90_000
	const (
		workers = 32
		per     = 128
	)
	a := acct(1, 2, 3)
	var wg sync.WaitGroup
	start := make(chan struct{})
	var mu sync.Mutex
	var got []string
	var bad int
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			switch w % 4 {
			case 3:
				for i := 0; i < per; i++ {
					_, err := Next(a, 9, now)
					if !errors.Is(err, ErrAlgoNotDeclared) {
						t.Errorf("undeclared: %v", err)
						return
					}
				}
				mu.Lock()
				bad += per
				mu.Unlock()
			default:
				when := now
				algo := uint32(1)
				switch w % 4 {
				case 1:
					when = now - 1_000_000 // behind, and before the epoch
					algo = 2
				case 2:
					when = now + int64(w%5) // a few milliseconds ahead
					algo = 3
				}
				local := make([]string, 0, per)
				for i := 0; i < per; i++ {
					s, err := Next(a, algo, when)
					if err != nil {
						t.Errorf("worker %d: %v", w, err)
						return
					}
					local = append(local, s)
				}
				mu.Lock()
				got = append(got, local...)
				mu.Unlock()
			}
		}(w)
	}
	close(start)
	wg.Wait()
	if bad != workers/4*per {
		t.Fatalf("undeclared calls recorded %d", bad)
	}
	wantN := workers / 4 * 3 * per
	if len(got) != wantN {
		t.Fatalf("got %d ids, want %d", len(got), wantN)
	}
	checkIssued(t, got)
}

// checkIssued fails when two ids share a (ts, seq), when ts moves backwards, or when a
// millisecond skips a seq. A forward jump and a borrowed millisecond both start at seq 0.
func checkIssued(t *testing.T, raw []string) {
	t.Helper()
	type pair struct {
		ts   int64
		seq  uint32
		algo uint32
	}
	ps := make([]pair, 0, len(raw))
	seen := make(map[string]struct{}, len(raw))
	for _, s := range raw {
		if _, ok := seen[s]; ok {
			t.Fatalf("duplicate %s", s)
		}
		seen[s] = struct{}{}
		id, err := cloid.Decode(s)
		if err != nil {
			t.Fatal(err)
		}
		if id.Ver != cloid.Version {
			t.Fatalf("ver %d", id.Ver)
		}
		ps = append(ps, pair{id.TsMs, id.Seq, id.Algo})
	}
	slices.SortFunc(ps, func(a, b pair) int {
		if a.ts != b.ts {
			return cmp.Compare(a.ts, b.ts)
		}
		return cmp.Compare(a.seq, b.seq)
	})
	if len(ps) == 0 || ps[0].seq != 0 {
		t.Fatalf("first %+v", ps)
	}
	for i := 1; i < len(ps); i++ {
		prev, cur := ps[i-1], ps[i]
		switch {
		case cur.ts == prev.ts && cur.seq == prev.seq+1:
		case cur.ts > prev.ts && cur.seq == 0:
		default:
			t.Fatalf("not monotonic: %+v then %+v", prev, cur)
		}
	}
}

func TestGenRepeatsTheProcessCounter(t *testing.T) {
	reset(t)
	a := acct(7)
	now := cloid.EpochMs + 1000
	mustNext(t, a, 7, now)
	slot := Slot()
	var live []string
	for _, ms := range []int64{now, now, now + 1, now} {
		s, err := Next(a, 7, ms)
		if err != nil {
			t.Fatal(err)
		}
		live = append(live, s)
	}
	after := Slot()
	g := NewGen(slot)
	for i, ms := range []int64{now, now, now + 1, now} {
		s, err := g.Next(a, 7, ms)
		if err != nil {
			t.Fatal(err)
		}
		if s != live[i] {
			t.Fatalf("id %d: replay %s, live %s", i, s, live[i])
		}
	}
	if Slot() != after {
		t.Fatal("a Gen of its own moved the process counter")
	}
	var none *Gen
	s, err := none.Next(a, 7, now)
	if err != nil {
		t.Fatal(err)
	}
	// The last live id borrowed now+1 seq 1, so the process counter's next is now+1 seq 2.
	if id, _ := cloid.Decode(s); id.TsMs != now+1 || id.Seq != 2 || Slot() == after {
		t.Fatalf("a nil Gen did not take the process counter: ts %d seq %d", id.TsMs-now, id.Seq)
	}
}
