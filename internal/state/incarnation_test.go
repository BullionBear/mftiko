package state

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func loadIncarnations(t *testing.T, ctx context.Context, term *Term) *Incarnations {
	t.Helper()
	inc, err := LoadIncarnations(ctx, term)
	if err != nil {
		t.Fatalf("LoadIncarnations: %v", err)
	}
	return inc
}

func next(t *testing.T, ctx context.Context, inc *Incarnations, workerKey string, want uint32) {
	t.Helper()
	n, err := inc.Next(ctx, workerKey)
	if err != nil || n != want {
		t.Fatalf("Next(%s) = %d, %v; want %d", workerKey, n, err, want)
	}
	if cur, ok := inc.Current(workerKey); !ok || cur != want {
		t.Fatalf("Current(%s) = %d, %v; want %d", workerKey, cur, ok, want)
	}
}

// stored is the raw value of workerKey's counter, or "" if absent.
func stored(t *testing.T, ctx context.Context, raw *clientv3.Client, p Plane, inst, workerKey string) string {
	t.Helper()
	kvs := mustGet(t, ctx, raw, IncarnationKey(p, inst, WorkerHash(workerKey))).Kvs
	if len(kvs) == 0 {
		return ""
	}
	if kvs[0].Lease != 0 {
		t.Fatalf("incarnation of %s bound to lease %x", workerKey, kvs[0].Lease)
	}
	return string(kvs[0].Value)
}

func TestIncarnationCounts(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "td-tw-1")
	raw := s.Client(t)
	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneTD, "td-tw-1", TermOptions{})
	inc := loadIncarnations(t, ctx, term)

	if _, ok := inc.Current("td/42"); ok {
		t.Fatal("Current before any Next")
	}
	for want := uint32(1); want <= 3; want++ {
		next(t, ctx, inc, "td/42", want)
	}
	if v := stored(t, ctx, raw, PlaneTD, "td-tw-1", "td/42"); v != "3" {
		t.Fatalf("stored %q, want 3", v)
	}
	next(t, ctx, inc, "td/43", 1) // each worker key counts on its own
}

func TestIncarnationSuperseded(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "td-tw-1")
	raw := s.Client(t)
	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneTD, "td-tw-1", TermOptions{})
	inc := loadIncarnations(t, ctx, term)
	next(t, ctx, inc, "td/42", 1)

	if _, err := raw.Put(ctx, WriterKey(PlaneTD, "td-tw-1"), formatEpoch(term.Epoch()+1)); err != nil {
		t.Fatal(err)
	}
	if n, err := inc.Next(ctx, "td/42"); !errors.Is(err, ErrSuperseded) {
		t.Fatalf("superseded agent got number %d, err %v", n, err)
	}
	if v := stored(t, ctx, raw, PlaneTD, "td-tw-1", "td/42"); v != "1" {
		t.Fatalf("stored %q, want 1", v)
	}
	if cur, _ := inc.Current("td/42"); cur != 1 {
		t.Fatalf("Current moved to %d", cur)
	}
	if _, err := LoadIncarnations(ctx, term); !errors.Is(err, ErrSuperseded) {
		t.Fatalf("LoadIncarnations on an ended term: %v", err)
	}
}

func TestIncarnationContinuesAcrossAgents(t *testing.T) {
	ctx := testCtx(t)
	_, topo := singleRegion(t, "td-tw-1")

	a := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneTD, "td-tw-1", TermOptions{ID: "a"})
	incA := loadIncarnations(t, ctx, a)
	for want := uint32(1); want <= 3; want++ {
		next(t, ctx, incA, "td/42", want)
	}
	if err := a.Close(ctx); err != nil {
		t.Fatal(err)
	}

	b := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneTD, "td-tw-1", TermOptions{ID: "b"})
	incB := loadIncarnations(t, ctx, b)
	if cur, ok := incB.Current("td/42"); !ok || cur != 3 {
		t.Fatalf("loaded %d, %v; want 3", cur, ok)
	}
	next(t, ctx, incB, "td/42", 4)
}

func TestIncarnationWorkerComesBack(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "md-tw-1")
	raw := s.Client(t)
	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneMD, "md-tw-1", TermOptions{})
	inc := loadIncarnations(t, ctx, term)
	const conn = "md/BinanceUM/default/1"

	next(t, ctx, inc, conn, 1)
	// The connection stops; nothing is deleted for MD and TD (W6.5). When it comes back it continues.
	next(t, ctx, inc, conn, 2)
	if err := inc.Forget(ctx, conn); !errors.Is(err, ErrNotSTS) {
		t.Fatalf("Forget on md: %v", err)
	}
	if v := stored(t, ctx, raw, PlaneMD, "md-tw-1", conn); v != "2" {
		t.Fatalf("stored %q, want 2", v)
	}
	next(t, ctx, inc, conn, 3)
}

func TestIncarnationForgetSTS(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "sts-tw-1")
	raw := s.Client(t)
	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneSTS, "sts-tw-1", TermOptions{})
	inc := loadIncarnations(t, ctx, term)

	next(t, ctx, inc, "sts/a1b2c3", 1)
	next(t, ctx, inc, "sts/d4e5f6", 1)
	// a1b2c3 is terminal and its last process is gone.
	if err := inc.Forget(ctx, "sts/a1b2c3"); err != nil {
		t.Fatal(err)
	}
	if v := stored(t, ctx, raw, PlaneSTS, "sts-tw-1", "sts/a1b2c3"); v != "" {
		t.Fatalf("forgotten key still stored: %q", v)
	}
	if _, ok := inc.Current("sts/a1b2c3"); ok {
		t.Fatal("forgotten key still cached")
	}
	if v := stored(t, ctx, raw, PlaneSTS, "sts-tw-1", "sts/d4e5f6"); v != "1" {
		t.Fatalf("other session's key: %q", v)
	}
}

// A wrong cache (here: a second Incarnations on the same Term) is refreshed through the retry path; no number is
// handed out twice.
func TestIncarnationStaleCache(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "td-tw-1")
	raw := s.Client(t)
	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneTD, "td-tw-1", TermOptions{})
	first := loadIncarnations(t, ctx, term)
	second := loadIncarnations(t, ctx, term)

	next(t, ctx, first, "td/42", 1)
	next(t, ctx, second, "td/42", 2) // second had no entry: CreateRevision == 0 failed
	next(t, ctx, first, "td/42", 3)  // first had 1: Value == 1 failed
	if v := stored(t, ctx, raw, PlaneTD, "td-tw-1", "td/42"); v != "3" {
		t.Fatalf("stored %q, want 3", v)
	}
	if !running(term) {
		t.Fatalf("a failed incarnation compare ended the term: %v", term.Err())
	}
}

func TestIncarnationBadValues(t *testing.T) {
	ctx := testCtx(t)
	s, topo := singleRegion(t, "td-tw-1")
	raw := s.Client(t)
	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneTD, "td-tw-1", TermOptions{})
	put := func(workerKey, v string) {
		t.Helper()
		if _, err := raw.Put(ctx, IncarnationKey(PlaneTD, "td-tw-1", WorkerHash(workerKey)), v); err != nil {
			t.Fatal(err)
		}
	}

	put("td/1", fmt.Sprint(uint32(math.MaxUint32)))
	inc := loadIncarnations(t, ctx, term)
	if n, err := inc.Next(ctx, "td/1"); !errors.Is(err, ErrIncarnationExhausted) {
		t.Fatalf("Next past MaxUint32 = %d, %v", n, err)
	}

	// Written after the load: found through the refresh.
	put("td/2", "abc")
	if n, err := inc.Next(ctx, "td/2"); !errors.Is(err, ErrBadIncarnation) {
		t.Fatalf("Next over a bad value = %d, %v", n, err)
	}
	// A value that never equals its own canonical form keeps the compare failing: retries are bounded.
	put("td/3", "07")
	if n, err := inc.Next(ctx, "td/3"); !errors.Is(err, ErrIncarnationConflict) {
		t.Fatalf("Next with a value that never matches = %d, %v", n, err)
	}
	if !running(term) {
		t.Fatalf("a bad stored value ended the term: %v", term.Err())
	}

	if _, err := LoadIncarnations(ctx, term); !errors.Is(err, ErrBadIncarnation) {
		t.Fatalf("LoadIncarnations over a bad value: %v", err)
	}
}

func TestIncarnationConcurrent(t *testing.T) {
	ctx := testCtx(t)
	_, topo := singleRegion(t, "td-tw-1")
	term := campaign(t, ctx, newClusters(t, topo, nil, nil), PlaneTD, "td-tw-1", TermOptions{})
	inc := loadIncarnations(t, ctx, term)

	const workers, each = 8, 5
	got := make(chan uint32, workers*each)
	errs := make(chan error, workers*each)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for range each {
				n, err := inc.Next(ctx, "td/42")
				if err != nil {
					errs <- err
					return
				}
				got <- n
			}
		})
	}
	wg.Wait()
	close(got)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	seen := map[uint32]bool{}
	for n := range got {
		if seen[n] || n < 1 || n > workers*each {
			t.Fatalf("number %d handed out twice or out of range", n)
		}
		seen[n] = true
	}
	if len(seen) != workers*each {
		t.Fatalf("%d numbers, want %d", len(seen), workers*each)
	}
}
