package main

import (
	"testing"
	"time"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	"github.com/BullionBear/mftiko/contract/wire/cloid"
)

var t0 = time.Date(2026, 11, 5, 8, 0, 0, 0, time.UTC)

func at(min int) time.Time { return t0.Add(time.Duration(min) * time.Minute) }

func ptr(t time.Time) *time.Time { return &t }

func mustCloid(t *testing.T, algo uint32, ts time.Time) string {
	t.Helper()
	s, err := cloid.Encode(algo, ts.UnixMilli(), 0)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// TestSessionInterval is E21-D011's four cases, and E21-D083's retire.
func TestSessionInterval(t *testing.T) {
	for _, c := range []struct {
		name  string
		f     sessionFacts
		want  interval
		probe map[int]bool // minute → contained
	}{
		{"running", sessionFacts{State: "created", CreatedAt: at(0)},
			interval{Start: at(0), Open: true, Source: "none"}, map[int]bool{-1: false, 0: true, 999: true}},
		{"no status yet", sessionFacts{State: "created", CreatedAt: at(0), NoStatus: true},
			interval{Start: at(0), Open: true, Source: "none"}, map[int]bool{5: true}},
		{"ended", sessionFacts{State: "closed", CreatedAt: at(0), Terminal: true, LastExit: ptr(at(10))},
			interval{Start: at(0), End: at(10), Source: "last_exit"}, map[int]bool{10: true, 11: false}},
		{"never spawned", sessionFacts{State: "closed", CreatedAt: at(0), Terminal: true},
			interval{Start: at(0), Empty: true, Source: "none"}, map[int]bool{0: false, 1: false}},
		{"abandoned", sessionFacts{State: "abandoned", CreatedAt: at(0)},
			interval{Start: at(0), Empty: true, Source: "none"}, map[int]bool{0: false}},
		{"retired", sessionFacts{State: "created", CreatedAt: at(0), Retired: ptr(at(20))},
			interval{Start: at(0), End: at(20), Source: "sts_retired_at"}, map[int]bool{20: true, 21: false}},
		{"exited before retire", sessionFacts{State: "created", CreatedAt: at(0), Terminal: true, LastExit: ptr(at(5)), Retired: ptr(at(20))},
			interval{Start: at(0), End: at(5), Source: "last_exit"}, map[int]bool{6: false}},
		{"never spawned, then retired", sessionFacts{State: "created", CreatedAt: at(0), Terminal: true, Retired: ptr(at(20))},
			interval{Start: at(0), Empty: true, Source: "none"}, map[int]bool{1: false}},
	} {
		got := sessionInterval(c.f)
		if got != c.want {
			t.Errorf("%s: %+v, want %+v", c.name, got, c.want)
		}
		for m, want := range c.probe {
			if got.contains(at(m)) != want {
				t.Errorf("%s: contains(+%dm) = %v", c.name, m, !want)
			}
		}
	}
}

func TestAttribute(t *testing.T) {
	retired := owner{RecordID: "r-retired", SessionID: "aaaaaa", APIID: 1, Algos: []uint32{3},
		Interval: sessionInterval(sessionFacts{State: "created", CreatedAt: at(0), Retired: ptr(at(20))})}
	after := owner{RecordID: "r-after", SessionID: "bbbbbb", APIID: 1, Algos: []uint32{3},
		Interval: sessionInterval(sessionFacts{State: "created", CreatedAt: at(21)})}
	ended := owner{RecordID: "r-ended", SessionID: "cccccc", APIID: 2, Algos: []uint32{1},
		Interval: sessionInterval(sessionFacts{State: "closed", CreatedAt: at(0), Terminal: true, LastExit: ptr(at(1))})}
	never := owner{RecordID: "r-never", SessionID: "dddddd", APIID: 2, Algos: []uint32{4},
		Interval: sessionInterval(sessionFacts{State: "closed", CreatedAt: at(0), Terminal: true})}
	// The same session on two api_ids of one exchange account is one owner.
	twin := []owner{
		{RecordID: "r-twin", SessionID: "eeeeee", APIID: 1, Algos: []uint32{7}, Interval: interval{Start: at(0), Open: true}},
		{RecordID: "r-twin", SessionID: "eeeeee", APIID: 2, Algos: []uint32{7}, Interval: interval{Start: at(0), Open: true}},
	}
	owners := append([]owner{retired, after, ended, never}, twin...)

	for _, c := range []struct {
		name, cloid, kind, session string
	}{
		{"before retire", mustCloid(t, 3, at(10)), "session", "aaaaaa"},
		{"algo reused after retire", mustCloid(t, 3, at(30)), "session", "bbbbbb"},
		{"placed before exit, filled any time", mustCloid(t, 1, at(1)), "session", "cccccc"},
		{"after exit", mustCloid(t, 1, at(2)), "unattributed", ""},
		{"never spawned owns nothing", mustCloid(t, 4, at(0)), "unattributed", ""},
		{"by hand", "web_8sd7f6g", "manual", ""},
		{"twin", mustCloid(t, 7, at(5)), "session", "eeeeee"},
	} {
		got := attribute(c.cloid, owners)
		if got.Kind != c.kind || got.SessionID != c.session {
			t.Errorf("%s: %+v", c.name, got)
		}
	}

	// Without sts_retired_at the retired session stays open, and the reuse is ambiguous.
	open := retired
	open.Interval = sessionInterval(sessionFacts{State: "created", CreatedAt: at(0)})
	if got := attribute(mustCloid(t, 3, at(30)), []owner{open, after}); got.Kind != "ambiguous" || len(got.Candidates) != 2 {
		t.Fatalf("no retire time: %+v", got)
	}
}

func TestGroupAccounts(t *testing.T) {
	s := func(v string) *string { return &v }
	got := groupAccounts([]apiv1.Account{
		{ApiId: 9, Exchange: s("binance"), ExchAccountUid: s("u1")},
		{ApiId: 3, Exchange: s("binance"), ExchAccountUid: s("u1")},
		{ApiId: 5, Exchange: s("okx"), ExchAccountUid: s("u1")},
		{ApiId: 7},
	})
	if len(got) != 3 || got[0].uid != "u1" || len(got[0].apiIDs) != 2 || got[0].apiIDs[0] != 3 || got[1].exchange != "okx" || got[2].exchange != "unknown" {
		t.Fatalf("%+v", got)
	}
}
