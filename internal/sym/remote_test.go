package sym

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	symv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/internal/platform/natsx"
	"github.com/BullionBear/mftiko/internal/platform/natsx/natsxtest"
)

func TestChangedSubject(t *testing.T) {
	got, err := ChangedSubject("Bybit", "Spot")
	if err != nil || got != "sym.v1.changed.Bybit.Spot" {
		t.Fatalf("subject %q %v", got, err)
	}
	if _, err := ChangedSubject("Bybit.Spot", "Spot"); err == nil {
		t.Fatal("dot in venue was accepted")
	}
	if _, err := ChangedSubject("", "Spot"); err == nil {
		t.Fatal("empty venue was accepted")
	}
	wild, err := ChangedVenueWildcard("Bybit")
	if err != nil || wild != "sym.v1.changed.Bybit.>" {
		t.Fatalf("venue %q %v", wild, err)
	}
	if ChangedWildcard != "sym.v1.changed.>" {
		t.Fatal(ChangedWildcard)
	}
	if _, err := ChangedVenueWildcard("Bybit.Spot"); err == nil {
		t.Fatal("dot in venue wildcard was accepted")
	}
}

func TestOpenRemoteRejectsTable(t *testing.T) {
	ns := natsxtest.Start(t)
	c := dialSym(t, ns.URL())
	_, err := OpenRemote(t.Context(), c, []Table{{Venue: "bybit", Category: symbol.Spot}}, RemoteOptions{})
	if err == nil || !strings.Contains(err.Error(), `venue "bybit"`) {
		t.Fatalf("got %v", err)
	}
	_, err = OpenRemote(t.Context(), c, []Table{
		{Venue: "Bybit", Category: symbol.Spot},
		{Venue: "Bybit", Category: symbol.Spot},
	}, RemoteOptions{})
	if err == nil || !strings.Contains(err.Error(), "listed twice") {
		t.Fatalf("duplicate: %v", err)
	}
	if _, err := OpenRemote(t.Context(), nil, nil, RemoteOptions{}); err == nil {
		t.Fatal("nil connection was accepted")
	}
}

// A single sym.v1.changed message replaces the cached table. The check interval
// is an hour, so the timer is not what makes the row visible.
func TestRemoteNotification(t *testing.T) {
	fx := newSymFixture(t)
	fx.store.SetPublisher(NATSChanged(fx.client, zerolog.Nop()))
	ctx := t.Context()
	remote := openRemote(t, fx, time.Hour)

	eth, err := remote.Get(ctx, mustTicker(t, "Bybit_Spot_ETHUSDT"), false)
	if err != nil || eth.ExchTicker != "ETHUSDT" || eth.Filters["price_tick"] != 0.5 {
		t.Fatalf("eth %+v %v", eth, err)
	}
	if _, err := remote.Get(ctx, mustTicker(t, "Bybit_Spot_BTCUSDT"), false); !errors.Is(err, symbol.ErrNotFound) {
		t.Fatalf("cached delisted btc: %v", err)
	}
	old, err := remote.Get(ctx, mustTicker(t, "Bybit_Spot_BTCUSDT"), true)
	if err != nil || old.UID != fx.btc || old.Status != Delisted {
		t.Fatalf("history %+v %v", old, err)
	}
	again, err := remote.Get(ctx, mustTicker(t, "Bybit_Spot_BTCUSDT"), false)
	if again != nil || !errors.Is(err, symbol.ErrNotFound) {
		t.Fatalf("history was cached: %+v %v", again, err)
	}

	notes := make(chan natsx.Msg, 4)
	if _, err := fx.client.Subscribe(ctx, ChangedWildcard, "", func(m natsx.Msg) {
		notes <- natsx.Msg{Subject: m.Subject, Data: append([]byte(nil), m.Data...)}
	}); err != nil {
		t.Fatal(err)
	}

	if err := fx.src.Add(spotL("ADA", "USDT", "ADAUSDT")); err != nil {
		t.Fatal(err)
	}
	if _, err := fx.run.Trigger(ctx, "Bybit"); err != nil {
		t.Fatal(err)
	}
	note := awaitMsg(t, notes)
	if note.Subject != "sym.v1.changed.Bybit.Spot" {
		t.Fatalf("subject %s", note.Subject)
	}
	var changed symv1.Changed
	if err := natsx.Unmarshal(note.Data, &changed); err != nil {
		t.Fatal(err)
	}
	if changed.GetVenue() != "Bybit" || changed.GetCategory() != "Spot" || changed.GetRevision() < 3 {
		t.Fatalf("changed %+v", &changed)
	}

	ada := waitInfo(t, func() (*symbol.Info, error) {
		return remote.Get(ctx, mustTicker(t, "Bybit_Spot_ADAUSDT"), false)
	})
	if ada.ExchTicker != "ADAUSDT" || ada.UID == 0 {
		t.Fatalf("ada %+v", ada)
	}
	byUID, err := remote.GetByUID(ctx, ada.UID, false)
	if err != nil || byUID.Ticker.String() != "Bybit_Spot_ADAUSDT" {
		t.Fatalf("uid %+v %v", byUID, err)
	}
	rev, err := remote.Reverse(ctx, symbol.ExchKey{Venue: "Bybit", Category: symbol.Spot, ExchTicker: "ADAUSDT"}, false)
	if err != nil || rev.UID != ada.UID {
		t.Fatalf("reverse %+v %v", rev, err)
	}
	if !listed(t, remote, "Bybit_Spot_ADAUSDT") || listed(t, remote, "Bybit_Spot_BTCUSDT") {
		t.Fatal("list does not match the active table")
	}

	// An unchanged refresh does not advance the revision and does not publish.
	if _, err := fx.run.Trigger(ctx, "Bybit"); err != nil {
		t.Fatal(err)
	}
	select {
	case extra := <-notes:
		t.Fatalf("unchanged refresh published %s", extra.Subject)
	case <-time.After(200 * time.Millisecond):
	}

	if err := fx.src.Remove(symbol.Spot, "ETHUSDT"); err != nil {
		t.Fatal(err)
	}
	if _, err := fx.run.Trigger(ctx, "Bybit"); err != nil {
		t.Fatal(err)
	}
	waitGone(t, func() error {
		_, err := remote.Get(ctx, mustTicker(t, "Bybit_Spot_ETHUSDT"), false)
		return err
	})
	if listed(t, remote, "Bybit_Spot_ETHUSDT") {
		t.Fatal("delisted row stayed in the cache")
	}
	hist, err := remote.Get(ctx, mustTicker(t, "Bybit_Spot_ETHUSDT"), true)
	if err != nil || hist.Status != Delisted || hist.UID == 0 {
		t.Fatalf("terminal eth %+v %v", hist, err)
	}
	if _, err := remote.Get(ctx, mustTicker(t, "Bybit_Spot_ETHUSDT"), false); !errors.Is(err, symbol.ErrNotFound) {
		t.Fatalf("terminal lookup filled the cache: %v", err)
	}
	perp, err := remote.Get(ctx, mustTicker(t, "Bybit_Perp_BTCUSDT"), false)
	if err != nil || perp.Ticker.String() != "Bybit_Perp_BTCUSDT" {
		t.Fatalf("perp %+v %v", perp, err)
	}
}

// A dropped notification stays invisible until the revision check runs.
func TestRemoteMissedNotification(t *testing.T) {
	fx := newSymFixture(t)
	gate := &dropPub{next: NATSChanged(fx.client, zerolog.Nop())}
	fx.store.SetPublisher(gate)
	ctx := t.Context()
	// An hour so the loop cannot hide a check that does not reload.
	remote := openRemote(t, fx, time.Hour)
	if _, err := remote.Get(ctx, mustTicker(t, "Bybit_Spot_ETHUSDT"), false); err != nil {
		t.Fatal(err)
	}

	gate.setDrop(true)
	if err := fx.src.Add(spotL("ADA", "USDT", "ADAUSDT")); err != nil {
		t.Fatal(err)
	}
	if _, err := fx.run.Trigger(ctx, "Bybit"); err != nil {
		t.Fatal(err)
	}
	if _, err := remote.Get(ctx, mustTicker(t, "Bybit_Spot_ADAUSDT"), false); !errors.Is(err, symbol.ErrNotFound) {
		t.Fatalf("dropped row was already visible: %v", err)
	}
	if err := remote.check(ctx); err != nil {
		t.Fatal(err)
	}
	ada, err := remote.Get(ctx, mustTicker(t, "Bybit_Spot_ADAUSDT"), false)
	if err != nil || ada.ExchTicker != "ADAUSDT" {
		t.Fatalf("ada %+v %v", ada, err)
	}
}

// The loop runs that same check on its timer, without a caller poking it.
func TestRemotePeriodicCheck(t *testing.T) {
	fx := newSymFixture(t)
	gate := &dropPub{next: NATSChanged(fx.client, zerolog.Nop())}
	fx.store.SetPublisher(gate)
	ctx := t.Context()
	remote := openRemote(t, fx, 20*time.Millisecond)
	gate.setDrop(true)
	if err := fx.src.Add(spotL("ADA", "USDT", "ADAUSDT")); err != nil {
		t.Fatal(err)
	}
	if _, err := fx.run.Trigger(ctx, "Bybit"); err != nil {
		t.Fatal(err)
	}
	ada := waitInfo(t, func() (*symbol.Info, error) {
		return remote.Get(ctx, mustTicker(t, "Bybit_Spot_ADAUSDT"), false)
	})
	if ada.ExchTicker != "ADAUSDT" {
		t.Fatalf("ada %+v", ada)
	}
}

func TestRemoteReconnect(t *testing.T) {
	fx := newSymFixture(t)
	gate := &dropPub{next: NATSChanged(fx.client, zerolog.Nop())}
	fx.store.SetPublisher(gate)
	ctx := t.Context()
	remote := openRemote(t, fx, time.Hour)
	gate.setDrop(true)
	if err := fx.src.Add(spotL("ADA", "USDT", "ADAUSDT")); err != nil {
		t.Fatal(err)
	}
	if _, err := fx.run.Trigger(ctx, "Bybit"); err != nil {
		t.Fatal(err)
	}
	if _, err := remote.Get(ctx, mustTicker(t, "Bybit_Spot_ADAUSDT"), false); !errors.Is(err, symbol.ErrNotFound) {
		t.Fatalf("row visible before reconnect: %v", err)
	}
	if err := fx.ns.Restart(); err != nil {
		t.Fatal(err)
	}
	ada := waitInfo(t, func() (*symbol.Info, error) {
		return remote.Get(ctx, mustTicker(t, "Bybit_Spot_ADAUSDT"), false)
	})
	if ada.ExchTicker != "ADAUSDT" {
		t.Fatalf("ada %+v", ada)
	}
}

type dropPub struct {
	mu   sync.Mutex
	drop bool
	next ChangedPublisher
}

func (d *dropPub) setDrop(v bool) {
	d.mu.Lock()
	d.drop = v
	d.mu.Unlock()
}

func (d *dropPub) Changed(ctx context.Context, venue, category string, revision int64) {
	d.mu.Lock()
	drop := d.drop
	d.mu.Unlock()
	if drop || d.next == nil {
		return
	}
	d.next.Changed(ctx, venue, category, revision)
}

func openRemote(t *testing.T, fx *symFixture, every time.Duration) *Remote {
	t.Helper()
	c := dialSym(t, fx.url)
	// The conn worker subscribes only its venue (E04-D057). These tables are both Bybit.
	notify, err := ChangedVenueWildcard("Bybit")
	if err != nil {
		t.Fatal(err)
	}
	r, err := OpenRemote(t.Context(), c, []Table{
		{Venue: "Bybit", Category: symbol.Spot},
		{Venue: "Bybit", Category: symbol.Perp},
	}, RemoteOptions{CheckInterval: every, Log: zerolog.Nop(), NotifySubject: notify})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	return r
}

func mustTicker(t *testing.T, s string) symbol.UniversalTicker {
	t.Helper()
	u, err := symbol.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func listed(t *testing.T, r *Remote, ticker string) bool {
	t.Helper()
	rows, err := r.List(t.Context(), "Bybit", symbol.Spot)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Ticker.String() == ticker {
			return true
		}
	}
	return false
}

func waitInfo(t *testing.T, get func() (*symbol.Info, error)) *symbol.Info {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		info, err := get()
		if err == nil {
			return info
		}
		last = err
		if !errors.Is(err, symbol.ErrNotFound) && !strings.Contains(err.Error(), "unavailable") {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out: %v", last)
	return nil
}

func waitGone(t *testing.T, get func() error) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		err := get()
		if errors.Is(err, symbol.ErrNotFound) {
			return
		}
		last = err
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("row stayed: %v", last)
}

func awaitMsg(t *testing.T, ch <-chan natsx.Msg) natsx.Msg {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(5 * time.Second):
		t.Fatal("no change notification")
		return natsx.Msg{}
	}
}
