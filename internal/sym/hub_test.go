package sym

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"

	symv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/internal/platform/pgtest"
	"github.com/BullionBear/mftiko/migrations"
)

// startWatchDB is startSym plus 0013, the sym_changed trigger.
func startWatchDB(t *testing.T) (*pgtest.Server, *pgx.Conn) {
	t.Helper()
	srv, conn := startSym(t)
	b, err := migrations.FS.ReadFile("0013_symbol_changed.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if err := pgtest.ExecSQL(context.Background(), conn, string(b)); err != nil {
		t.Fatal(err)
	}
	return srv, conn
}

// runHub runs h until the test ends.
func runHub(t *testing.T, h *Hub) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = h.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return cancel
}

// waitListeners waits until n hub connections are listening: their last query is the catch-up
// read, which runs after LISTEN.
func waitListeners(t *testing.T, conn *pgx.Conn, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var got int
		if err := conn.QueryRow(context.Background(), `
			SELECT count(*) FROM pg_stat_activity
			WHERE application_name = 'mftiko-sym-hub' AND state = 'idle' AND query = $1`, catchUpSQL).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d hub listeners, want %d", got, n)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// bump commits one revision of (venue, category), as a refresh does, and returns it.
func bump(t *testing.T, conn *pgx.Conn, venue, category string) int64 {
	t.Helper()
	var rev int64
	if err := conn.QueryRow(context.Background(), `
		INSERT INTO symbol_revision (venue, category, revision, refreshed_at_ns) VALUES ($1, $2, 1, 1)
		ON CONFLICT (venue, category) DO UPDATE SET revision = symbol_revision.revision + 1
		RETURNING revision`, venue, category).Scan(&rev); err != nil {
		t.Fatal(err)
	}
	return rev
}

func watchService(t *testing.T, st *Store, h *Hub, f WatchFilter) *Watch {
	t.Helper()
	w, err := NewService(st, nil, zerolog.Nop()).WithHub(h).Watch(context.Background(), f)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Close)
	return w
}

func next(t *testing.T, w *Watch) *symv1.Changed {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ch, err := w.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return ch
}

func want(t *testing.T, got *symv1.Changed, venue, category string, rev int64) {
	t.Helper()
	if got.GetVenue() != venue || got.GetCategory() != category || got.GetRevision() != rev {
		t.Fatalf("got %s/%s/%d, want %s/%s/%d", got.GetVenue(), got.GetCategory(), got.GetRevision(), venue, category, rev)
	}
}

func okxSpot(base string) symbol.Listing {
	return symbol.Listing{
		Venue: "Okx", Category: symbol.Spot, Base: base, Quote: "USDT", ExchTicker: base + "-USDT", Status: Tradable,
		Filters: symbol.Filters{"price_tick": 0.1},
	}
}

// Two replicas refresh different feeds into one database. A watch on either hub sees both
// (E22-D009): the source is Postgres, not the replica that committed.
func TestHubTwoReplicas(t *testing.T) {
	srv, conn := startWatchDB(t)
	a := openStore(t, srv.DSN(), nil)
	b := openStore(t, srv.DSN(), nil)
	hubA, hubB := NewHub(a, HubOptions{}), NewHub(b, HubOptions{})
	runHub(t, hubA)
	runHub(t, hubB)
	waitListeners(t, conn, 2)
	onA := watchService(t, a, hubA, WatchFilter{})
	onB := watchService(t, b, hubB, WatchFilter{})

	ctx := context.Background()
	if _, err := a.Refresh(ctx, Feed{Venue: "Bybit", Category: symbol.Spot, REST: listREST{rows: []symbol.Listing{spotL("BTC", "USDT", "BTCUSDT")}}}, 1000, zerolog.Nop()); err != nil {
		t.Fatal(err)
	}
	for _, w := range []*Watch{onA, onB} {
		want(t, next(t, w), "Bybit", "Spot", 1)
	}
	if _, err := b.Refresh(ctx, Feed{Venue: "Okx", Category: symbol.Spot, REST: listREST{rows: []symbol.Listing{okxSpot("ETH")}}}, 2000, zerolog.Nop()); err != nil {
		t.Fatal(err)
	}
	for _, w := range []*Watch{onA, onB} {
		want(t, next(t, w), "Okx", "Spot", 1)
	}
}

// A rolled-back revision sends nothing; the next commit is the next thing a watch sees.
func TestHubRollback(t *testing.T) {
	srv, conn := startWatchDB(t)
	st := openStore(t, srv.DSN(), nil)
	h := NewHub(st, HubOptions{})
	runHub(t, h)
	waitListeners(t, conn, 1)
	bump(t, conn, "Bybit", "Spot")
	w := watchService(t, st, h, WatchFilter{Venue: "Bybit"})
	if len(w.Snapshot.GetTables()) != 1 || w.Snapshot.GetTables()[0].GetRevision() != 1 {
		t.Fatalf("snapshot %v", w.Snapshot)
	}
	ctx := context.Background()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE symbol_revision SET revision = revision + 1`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	rev := bump(t, conn, "Bybit", "Spot")
	want(t, next(t, w), "Bybit", "Spot", rev)
}

// A commit while the listener is down arrives after it reconnects, from the catch-up read.
func TestHubCatchesUpAfterReconnect(t *testing.T) {
	srv, conn := startWatchDB(t)
	st := openStore(t, srv.DSN(), nil)
	h := NewHub(st, HubOptions{MinBackoff: time.Second, MaxBackoff: time.Second})
	runHub(t, h)
	waitListeners(t, conn, 1)
	w := watchService(t, st, h, WatchFilter{})

	ctx := context.Background()
	if _, err := conn.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'mftiko-sym-hub'`); err != nil {
		t.Fatal(err)
	}
	waitListeners(t, conn, 0)
	rev := bump(t, conn, "Bybit", "Perp")
	want(t, next(t, w), "Bybit", "Perp", rev)
	waitListeners(t, conn, 1)
	rev = bump(t, conn, "Bybit", "Perp")
	want(t, next(t, w), "Bybit", "Perp", rev)
}

// A watch that does not read is closed once its buffer is full. The others get every change.
func TestHubClosesSlowWatch(t *testing.T) {
	srv, conn := startWatchDB(t)
	st := openStore(t, srv.DSN(), nil)
	h := NewHub(st, HubOptions{Buffer: 2})
	runHub(t, h)
	waitListeners(t, conn, 1)
	slow := watchService(t, st, h, WatchFilter{})
	fast := watchService(t, st, h, WatchFilter{})
	for i := 1; i <= 5; i++ {
		rev := bump(t, conn, "Bybit", "Spot")
		want(t, next(t, fast), "Bybit", "Spot", rev)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var err error
	for err == nil {
		_, err = slow.Next(ctx)
	}
	if !errors.Is(err, ErrSlowWatcher) {
		t.Fatalf("slow watch: %v", err)
	}
}

// The snapshot is the filtered revisions. Nothing at or below it comes from Next, even the
// hub's catch-up after a reconnect.
func TestWatchSnapshotAndFilter(t *testing.T) {
	srv, conn := startWatchDB(t)
	st := openStore(t, srv.DSN(), nil)
	bump(t, conn, "Bybit", "Spot")
	bump(t, conn, "Bybit", "Spot")
	bump(t, conn, "Bybit", "Perp")
	bump(t, conn, "Okx", "Spot")
	h := NewHub(st, HubOptions{MinBackoff: 10 * time.Millisecond})
	w := watchService(t, st, h, WatchFilter{Venue: "Bybit"})
	spot := watchService(t, st, h, WatchFilter{Venue: "Bybit", Category: "Spot"})
	var tables []string
	for _, r := range w.Snapshot.GetTables() {
		tables = append(tables, fmt.Sprintf("%s/%s/%d", r.GetVenue(), r.GetCategory(), r.GetRevision()))
	}
	if strings.Join(tables, ",") != "Bybit/Perp/1,Bybit/Spot/2" {
		t.Fatalf("snapshot %v", tables)
	}
	// The hub starts after the snapshot: its first read passes on every table, at the snapshot.
	runHub(t, h)
	waitListeners(t, conn, 1)
	bump(t, conn, "Okx", "Spot")
	spotRev := bump(t, conn, "Bybit", "Spot")
	want(t, next(t, w), "Bybit", "Spot", spotRev)
	want(t, next(t, spot), "Bybit", "Spot", spotRev)
	perpRev := bump(t, conn, "Bybit", "Perp")
	want(t, next(t, w), "Bybit", "Perp", perpRev)

	svc := NewService(st, nil, zerolog.Nop()).WithHub(h)
	cases := map[WatchFilter]string{
		{Category: "Spot"}:                 `invalid: category "Spot" needs a venue`,
		{Venue: "bybit"}:                   `invalid: venue "bybit" is not canonical, use "Bybit"`,
		{Venue: "Bybit", Category: "spot"}: `invalid: category "spot" is not canonical, use "Spot"`,
	}
	for f, text := range cases {
		if _, err := svc.Watch(context.Background(), f); err == nil || err.Error() != text {
			t.Fatalf("%+v: %v, want %s", f, err, text)
		}
	}
	if _, err := NewService(st, nil, zerolog.Nop()).Watch(context.Background(), WatchFilter{}); err == nil ||
		err.Error() != "unavailable: watch is not served by this process" {
		t.Fatalf("no hub: %v", err)
	}
}

// Stopping the hub ends every watch with unavailable and refuses new ones.
func TestHubStop(t *testing.T) {
	srv, conn := startWatchDB(t)
	st := openStore(t, srv.DSN(), nil)
	h := NewHub(st, HubOptions{})
	stop := runHub(t, h)
	waitListeners(t, conn, 1)
	w := watchService(t, st, h, WatchFilter{})
	stop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := w.Next(ctx); err == nil || err.Error() != "unavailable: change stream stopped" {
		t.Fatalf("after stop: %v", err)
	}
	if _, err := NewService(st, nil, zerolog.Nop()).WithHub(h).Watch(ctx, WatchFilter{}); err == nil ||
		err.Error() != "unavailable: change stream stopped" {
		t.Fatalf("new watch after stop: %v", err)
	}
}

func TestParseChanged(t *testing.T) {
	if v, c, r, ok := parseChanged("Bybit/Spot/12"); !ok || v != "Bybit" || c != "Spot" || r != 12 {
		t.Fatalf("parsed %s %s %d %v", v, c, r, ok)
	}
	for _, bad := range []string{"", "Bybit/Spot", "Bybit/Spot/x", "Bybit/Spot/0", "/Spot/1", "Bybit//1", "a/b/c/1"} {
		if _, _, _, ok := parseChanged(bad); ok {
			t.Fatalf("%q was accepted", bad)
		}
	}
}
