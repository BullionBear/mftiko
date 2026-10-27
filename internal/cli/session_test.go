package cli

import (
	"bytes"
	"context"
	"errors"
	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	"github.com/BullionBear/mftiko/internal/cli/apitest"
	"github.com/google/uuid"
	openapi_types "github.com/oapi-codegen/runtime/types"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestShortDigest(t *testing.T) {
	if got := shortDigest(digA); got != "0000000" {
		t.Fatalf("prefixed %q", got)
	}
	if got := shortDigest("abcdef012345"); got != "abcdef0" {
		t.Fatalf("bare %q", got)
	}
	if got := shortDigest("sha256:abc"); got != "abc" {
		t.Fatalf("short %q", got)
	}
	if got := shortDigest(""); got != "" {
		t.Fatalf("empty %q", got)
	}
}

func TestParseWhen(t *testing.T) {
	midnight := time.Date(2026, 10, 24, 0, 0, 0, 0, time.Local)
	got, err := parseWhen("created-after", "2026-10-24")
	if err != nil || !got.Equal(midnight) {
		t.Fatalf("date %v %v", got, err)
	}
	zoned, err := parseWhen("created-after", "2026-10-24T00:00:00Z")
	if err != nil || !zoned.Equal(time.Date(2026, 10, 24, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("rfc %v %v", zoned, err)
	}
	for _, bad := range []string{"1h", "now", "yesterday", "2026-10-24T00:00:00", "2026-10-24 "} {
		if _, err := parseWhen("created-after", bad); err == nil || !strings.Contains(err.Error(), "RFC 3339") {
			t.Fatalf("%q err %v", bad, err)
		}
	}
	if _, err := parseWhen("created-before", ""); err == nil || !strings.Contains(err.Error(), "needs a time") {
		t.Fatalf("empty %v", err)
	}
}

func TestSessionList(t *testing.T) {
	created := time.Date(2026, 10, 24, 15, 4, 5, 0, time.FixedZone("test", 9*3600))
	by := "ada"
	running := &apiv1.SessionStatus{Phase: apiv1.PhaseRunning, Reason: "up", RestartCount: 1, UpdatedAt: created}
	done := &apiv1.SessionStatus{Phase: apiv1.PhaseDone, Reason: "stop", RestartCount: 0, UpdatedAt: created}
	mk := func(id, name, sts string, st *apiv1.SessionStatus) apiv1.SessionDetail {
		c := created
		who := by
		return apiv1.SessionDetail{
			Id: id, StsInstance: sts, Region: "tw", Revision: 1,
			CreatedAt: &c, CreatedBy: &who, Status: st,
			Spec: apiv1.SessionSpecView{
				Name: name, DesiredState: apiv1.DesiredStateRunning,
				Strategy: strategyRef("alpha", name+"-strat", digA),
			},
		}
	}
	sessions := []apiv1.SessionDetail{
		mk("cd34ef", "second", "sts-1", done),
		mk("ab12cd", "first", "sts-1", running),
		mk("ef56ab", "third", "sts-2", running),
	}
	srv, home := startSess(t, apitest.Option{Sessions: sessions, RequestID: "req-sess"})
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "session", "list")
	if got.code != ExitOK || got.err != "" {
		t.Fatalf("%+v", got)
	}
	if strings.Index(got.out, "ab12cd") < 0 || strings.Index(got.out, "ab12cd") > strings.Index(got.out, "cd34ef") || strings.Index(got.out, "cd34ef") > strings.Index(got.out, "ef56ab") {
		t.Fatalf("order %q", got.out)
	}
	if !strings.Contains(got.out, "first") || !strings.Contains(got.out, "running") || !strings.Contains(got.out, "sts-2") || !strings.Contains(got.out, localStamp(created)) {
		t.Fatalf("table %q", got.out)
	}
	if len(srv.Calls()) != 1 || srv.Calls()[0].Operation != "ListSessions" || !strings.Contains(srv.Calls()[0].RawQuery, "limit=1000") {
		t.Fatalf("call %+v", srv.Calls())
	}
	noLeak(t, tokenA, got.out+got.err)

	got = runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "-o", "json", "session", "list")
	if got.code != ExitOK || !strings.Contains(got.out, "ab12cd") || !strings.Contains(got.out, "ef56ab") || strings.Contains(got.out, "next_cursor") || !strings.Contains(got.out, "freshness") {
		t.Fatalf("json %q", got.out)
	}

	before := len(srv.Calls())
	got = runAt(context.Background(), home, nil, strings.NewReader(""),
		"--context", "dev", "session", "list", "-phase", "running", "-phase", "failed")
	if got.code != ExitOK || strings.Contains(got.out, "cd34ef") || !strings.Contains(got.out, "ab12cd") || !strings.Contains(got.out, "ef56ab") {
		t.Fatalf("phases %+v", got)
	}
	q, err := url.ParseQuery(srv.Calls()[len(srv.Calls())-1].RawQuery)
	if err != nil || len(srv.Calls()) != before+1 {
		t.Fatal(err)
	}
	phases := q["phase"]
	if len(phases) != 2 || phases[0] != "running" || phases[1] != "failed" {
		t.Fatalf("query %q", srv.Calls()[len(srv.Calls())-1].RawQuery)
	}

	got = runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "session", "list", "-active", "false")
	if got.code != ExitOK || !strings.Contains(got.out, "cd34ef") || strings.Contains(got.out, "ab12cd") {
		t.Fatalf("active false %+v", got)
	}
	if !strings.Contains(srv.Calls()[len(srv.Calls())-1].RawQuery, "active=false") {
		t.Fatalf("query %q", srv.Calls()[len(srv.Calls())-1].RawQuery)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "session", "list", "-active", "true", "-sts-instance", "sts-2")
	if got.code != ExitOK || strings.Contains(got.out, "ab12cd") || !strings.Contains(got.out, "ef56ab") || strings.Contains(got.out, "cd34ef") {
		t.Fatalf("sts %+v", got)
	}
	if !strings.Contains(srv.Calls()[len(srv.Calls())-1].RawQuery, "sts_instance=sts-2") || !strings.Contains(srv.Calls()[len(srv.Calls())-1].RawQuery, "active=true") {
		t.Fatalf("query %q", srv.Calls()[len(srv.Calls())-1].RawQuery)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "session", "list", "-phase", "running", "-active", "false")
	if got.code != ExitOK || strings.Contains(got.out, "ab12cd") || strings.Contains(got.out, "cd34ef") || strings.Contains(got.out, "ef56ab") {
		t.Fatalf("and %+v", got)
	}

	paged, homeP := startSess(t, apitest.Option{
		Sessions:      sessions[:2],
		PageSize:      1,
		RequestID:     "req-page",
		Freshness:     apiv1.Freshness{UnavailableRegions: []string{"b"}},
		NextFreshness: &apiv1.Freshness{UnavailableRegions: []string{"a"}, ControllerAbsent: true},
	})
	got = runAt(context.Background(), homeP, nil, strings.NewReader(""), "--context", "dev", "-o", "json", "session", "list")
	if got.code != ExitOK || len(paged.Calls()) != 2 {
		t.Fatalf("pages %d %+v", len(paged.Calls()), got)
	}
	if !strings.Contains(got.out, "ab12cd") || !strings.Contains(got.out, "cd34ef") || strings.Contains(got.out, "next_cursor") {
		t.Fatalf("merged %s", got.out)
	}
	if !strings.Contains(got.out, `"a"`) || !strings.Contains(got.out, `"b"`) || !strings.Contains(got.out, `"controller_absent":true`) {
		t.Fatalf("fresh %s", got.out)
	}
	if strings.Count(got.err, "unavailable regions") != 1 || strings.Count(got.err, "controller absent") != 1 || !strings.Contains(got.err, "a, b") {
		t.Fatalf("warn %q", got.err)
	}
	got = runAt(context.Background(), homeP, nil, strings.NewReader(""), "--context", "dev", "session", "list", "-limit", "1")
	if got.code != ExitOK || strings.Contains(got.out, "cd34ef") || !strings.Contains(got.out, "ab12cd") || len(paged.Calls()) != 3 {
		t.Fatalf("limit %+v calls %d", got, len(paged.Calls()))
	}
	got = runAt(context.Background(), homeP, nil, strings.NewReader(""), "--context", "dev", "-o", "json", "session", "list", "-cursor", "1")
	if got.code != ExitOK || !bytes.Equal([]byte(got.out), paged.LastBody()) || !strings.Contains(got.out, "cd34ef") || strings.Contains(got.out, "ab12cd") {
		t.Fatalf("cursor %s\n%s", got.out, paged.LastBody())
	}
	got = runAt(context.Background(), homeP, nil, strings.NewReader(""), "--context", "dev", "session", "list", "-cursor", "nope")
	if got.code != ExitValidationFailed || !strings.Contains(got.err, "/cursor") {
		t.Fatalf("bad cursor %+v", got)
	}
}

func TestSessionShow(t *testing.T) {
	created := time.Date(2026, 10, 24, 15, 4, 5, 0, time.FixedZone("test", 9*3600))
	by := "ada"
	feed := "orderbook_delta.X"
	other := "orderbook_delta.Y"
	rej := "unknown ticker"
	deadWhy := "account dead"
	apiID := int64(7)
	last := true
	d := apiv1.SessionDetail{
		Id: "ab12cd", StsInstance: "sts-1", Region: "tw", Revision: 4,
		CreatedAt: &created, CreatedBy: &by,
		Spec: apiv1.SessionSpecView{
			Name: "chase", DesiredState: apiv1.DesiredStateRunning,
			Strategy: strategyRef("alpha", "chase-man", digA),
		},
		Status: &apiv1.SessionStatus{
			Phase: apiv1.PhaseFailed, Reason: "deps_failed", RestartCount: 2, UpdatedAt: created,
			Conditions: []apiv1.Condition{
				{Type: "dependency", Status: false, Reason: "deps_failed", Message: "unknown ticker", At: created},
				{Type: "ready", Status: true, Reason: "ok", Message: "hidden-marker", At: created},
			},
			LastExit: &apiv1.Exit{Outcome: apiv1.OutcomeFailed, Code: 1, Signal: 9, Reason: "deps_failed", At: created},
		},
		Dependencies: []apiv1.Dependency{
			{Kind: apiv1.DependencyKindFeed, Feed: &feed, ProviderInstance: "md1", State: apiv1.EntryStateRejected, Reason: &rej},
			{Kind: apiv1.DependencyKindAccount, ApiId: &apiID, ProviderInstance: "td1", State: apiv1.EntryStateDead, Reason: &deadWhy},
			{Kind: apiv1.DependencyKindFeed, Feed: &other, ProviderInstance: "md2", State: apiv1.EntryStateSilent},
		},
		Logs: &apiv1.LogLocation{
			Host:   "host-a",
			Stdout: "/var/lib/mftiko/log/sts/ab12cd.stdout",
			Stderr: "/var/lib/mftiko/log/sts/ab12cd.stderr",
		},
	}
	srv, home := startSess(t, apitest.Option{
		RequestID: "req-show",
		Sessions:  []apiv1.SessionDetail{d},
		Agents: map[string]apiv1.AgentStatus{
			"sts-1": {Host: strp("host-a"), LastKnown: &last, Release: "test", StartedAt: created},
		},
	})
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "session", "show", "ab12cd")
	if got.code != ExitOK {
		t.Fatalf("%+v", got)
	}
	shortA := shortDigest(digA)
	if shortA != "0000000" {
		t.Fatalf("short digest %q", shortA)
	}
	for _, want := range []string{
		"alpha", "chase-man", shortA, "sts-1", localStamp(created), "ada",
		"failed", "deps_failed", "2",
		"rejected", "REJECTED", "unknown ticker",
		"dead", "DEAD", "account dead", "7",
		"silent",
		"dependency", "false",
		"failed", "1", "9",
		"host-a",
		"sts-1 host-a:/var/lib/mftiko/log/sts/ab12cd.stdout",
		"sts-1 host-a:/var/lib/mftiko/log/sts/ab12cd.stderr",
		"最後已知，agent 目前不在線",
	} {
		if !strings.Contains(got.out, want) {
			t.Fatalf("missing %q\n%s", want, got.out)
		}
	}
	if strings.Contains(got.out, "hidden-marker") || strings.Contains(got.out, "\nTAG") || strings.Contains(got.out, "TAG  ") || strings.Contains(got.out, digA) || strings.Contains(got.out, "sha256:") || strings.Contains(got.out, "00000000") {
		t.Fatalf("extra %s", got.out)
	}
	for _, c := range srv.Calls() {
		if strings.Contains(c.Path, "/strategies") {
			t.Fatalf("registry lookup %+v", c)
		}
	}
	if len(srv.Calls()) != 2 || srv.Calls()[0].Operation != "GetSession" || srv.Calls()[1].Operation != "GetInstance" {
		t.Fatalf("calls %+v", srv.Calls())
	}
	if srv.Calls()[0].Path != "/api/v1/sessions/ab12cd" || srv.Calls()[0].ContentType != "" {
		t.Fatalf("get %+v", srv.Calls()[0])
	}
	noLeak(t, tokenA, got.out+got.err)

	before := len(srv.Calls())
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "-o", "json", "-v", "session", "show", "ab12cd")
	if got.code != ExitOK || !bytes.Equal([]byte(got.out), srv.LastBody()) || len(srv.Calls()) != before+1 || srv.Calls()[before].Operation != "GetSession" {
		t.Fatalf("json calls %d %+v\n%s\n%s", len(srv.Calls())-before, got, got.out, srv.LastBody())
	}
	if strings.Contains(got.out, "最後已知") || strings.Count(got.err, "X-Request-Id:") != 1 || !strings.Contains(got.out, digA) {
		t.Fatalf("json note %s err %q", got.out, got.err)
	}

	plain := d
	plain.Id = "cd34ef"
	plain.Logs = nil
	plain.Status = &apiv1.SessionStatus{Phase: apiv1.PhaseRunning, Reason: "up", RestartCount: 0, UpdatedAt: created, LastExit: &apiv1.Exit{Outcome: apiv1.OutcomeDone, Code: 0, Signal: 0, At: created}}
	plain.Spec.Strategy = strategyRef("", "no-tag", digB)
	plain.Dependencies = nil
	srv2, home2 := startSess(t, apitest.Option{Sessions: []apiv1.SessionDetail{plain}, RequestID: "req-plain"})
	got = runAt(context.Background(), home2, nil, strings.NewReader(""), "--context", "dev", "session", "show", "cd34ef")
	if got.code != ExitOK || strings.Contains(got.out, "最後已知") || strings.Contains(got.out, "TAG") {
		t.Fatalf("%+v", got)
	}
	if !strings.Contains(got.out, "no-tag") || strings.Contains(got.out, digB) || !strings.Contains(got.out, shortDigest(digB)) || !strings.Contains(got.out, "done") || !strings.Contains(got.out, "SIGNAL") {
		t.Fatalf("plain %s", got.out)
	}
	exit := got.out[strings.Index(got.out, "EXIT\n"):]
	if !strings.Contains(exit, "done") || !strings.Contains(exit, "  0  ") {
		t.Fatalf("exit %s", exit)
	}
	if len(srv2.Calls()) != 1 || srv2.Calls()[0].Operation != "GetSession" {
		t.Fatalf("calls %+v", srv2.Calls())
	}

	missing := d
	missing.Id = "ef56ab"
	_, home3 := startSess(t, apitest.Option{Sessions: []apiv1.SessionDetail{missing}})
	got = runAt(context.Background(), home3, nil, strings.NewReader(""), "--context", "dev", "session", "show", "ef56ab")
	if got.code != ExitNotFound || got.out != "" || !strings.Contains(got.err, "not_found (12)") {
		t.Fatalf("%+v", got)
	}
}

func TestSessionEnd(t *testing.T) {
	d := baseSession("ab12cd", apiv1.PhaseRunning)
	d.Spec.DesiredState = apiv1.DesiredStateRunning
	srv, home := startSess(t, apitest.Option{Sessions: []apiv1.SessionDetail{d}})

	var in countReader
	in.r = strings.NewReader("y\n")
	got := runReader(context.Background(), home, nil, &in, "--context", "dev", "session", "end", "ab12cd")
	if got.code != ExitUsage || in.n != 0 || !strings.Contains(got.err, "not a terminal") || len(srv.Calls()) != 0 {
		t.Fatalf("off tty %+v reads %d calls %d", got, in.n, len(srv.Calls()))
	}

	app, _, errb := newApp(home, strings.NewReader("n\n"), options{context: "dev", contextSet: true, yes: true}, true)
	app.Args = []string{"ab12cd"}
	if code := runSessionEnd(app); code != ExitUsage || !strings.Contains(errb.String(), "[y/N]") || !strings.Contains(errb.String(), "not confirmed") || len(srv.Calls()) != 0 {
		t.Fatalf("tty %d %q calls %d", code, errb.String(), len(srv.Calls()))
	}

	got = runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "--yes", "-o", "json", "session", "end", "ab12cd")
	if got.code != ExitOK || !strings.Contains(got.out, `"desired_state":"ended"`) || !bytes.Equal([]byte(got.out), srv.LastBody()) {
		t.Fatalf("%+v\n%s", got, srv.LastBody())
	}
	call := srv.Calls()[0]
	if call.Operation != "EndSession" || call.Method != "POST" || call.Path != "/api/v1/sessions/ab12cd/end" || call.ContentType != "" || len(call.Body) != 0 {
		t.Fatalf("end %+v", call)
	}
	noLeak(t, tokenA, got.out+got.err)

	ended := d
	ended.Id = "cd34ef"
	ended.Spec.DesiredState = apiv1.DesiredStateEnded
	ended.Status = &apiv1.SessionStatus{Phase: apiv1.PhaseDone, Reason: "stop", UpdatedAt: time.Now()}
	srv2, home2 := startSess(t, apitest.Option{Sessions: []apiv1.SessionDetail{ended}})
	got = runAt(context.Background(), home2, nil, strings.NewReader(""), "--context", "dev", "--yes", "-o", "json", "session", "end", "cd34ef")
	if got.code != ExitOK || !strings.Contains(got.out, `"desired_state":"ended"`) || len(opCalls(srv2.Calls(), "EndSession")) != 1 {
		t.Fatalf("%+v", got)
	}
}

func TestSessionDelete(t *testing.T) {
	running := baseSession("ab12cd", apiv1.PhaseRunning)
	done := baseSession("cd34ef", apiv1.PhaseDone)
	srv, home := startSess(t, apitest.Option{Sessions: []apiv1.SessionDetail{running, done}})

	var in countReader
	in.r = strings.NewReader("y\n")
	got := runReader(context.Background(), home, nil, &in, "--context", "dev", "session", "delete", "cd34ef")
	if got.code != ExitUsage || in.n != 0 || !strings.Contains(got.err, "not a terminal") || len(srv.Calls()) != 0 {
		t.Fatalf("off tty %+v reads %d", got, in.n)
	}
	app, _, errb := newApp(home, strings.NewReader("n\n"), options{context: "dev", contextSet: true, yes: true}, true)
	app.Args = []string{"cd34ef"}
	if code := runSessionDelete(app); code != ExitUsage || !strings.Contains(errb.String(), "[y/N]") || len(srv.Calls()) != 0 {
		t.Fatalf("tty %d %q", code, errb.String())
	}

	got = runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "--yes", "session", "delete", "ab12cd")
	if got.code != ExitNotTerminal || got.out != "" || !strings.Contains(got.err, "not_terminal (14)") || !strings.Contains(got.err, "mftiko: run session end ab12cd first") {
		t.Fatalf("%+v", got)
	}
	call := opCalls(srv.Calls(), "DeleteSession")
	if len(call) != 1 || call[0].ContentType != "" || len(call[0].Body) != 0 || call[0].Path != "/api/v1/sessions/ab12cd" {
		t.Fatalf("delete %+v", call)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "session", "show", "ab12cd")
	if got.code != ExitOK || !strings.Contains(got.out, "running") || !strings.Contains(got.out, "because") {
		t.Fatalf("still there %+v", got)
	}

	got = runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "--yes", "-o", "json", "session", "delete", "cd34ef")
	if got.code != ExitOK || got.out != "" {
		t.Fatalf("terminal %+v", got)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "session", "show", "cd34ef")
	if got.code != ExitNotFound {
		t.Fatalf("gone %+v", got)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "--yes", "session", "delete", "ef56ab")
	if got.code != ExitNotFound || strings.Contains(got.err, "session end") {
		t.Fatalf("missing %+v", got)
	}
	noLeak(t, tokenA, got.out+got.err)
}

func TestRecordList(t *testing.T) {
	midnight := time.Date(2026, 10, 24, 0, 0, 0, 0, time.Local)
	early := midnight.Add(-time.Second)
	late := midnight.Add(time.Second)
	ada := "ada"
	bob := "bob"
	recs := []apiv1.SessionRecord{
		testRecord("00000000-0000-4000-8000-000000000001", "ab12cd", "early-row", ada, early),
		testRecord("00000000-0000-4000-8000-000000000002", "ab12cd", "midnight-row", ada, midnight),
		testRecord("00000000-0000-4000-8000-000000000003", "cd34ef", "late-row", bob, late),
	}
	recs[2].Spec.Strategy = strategyRef("beta", "late-row", digB)
	srv, home := startSess(t, apitest.Option{Records: recs, RequestID: "req-rec"})

	got := runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "record", "list", "-created-after", "2026-10-24")
	if got.code != ExitOK || !strings.Contains(got.out, "late-row") || strings.Contains(got.out, "early-row") || strings.Contains(got.out, "midnight-row") {
		t.Fatalf("after date %+v", got)
	}
	q, err := url.ParseQuery(srv.Calls()[0].RawQuery)
	if err != nil {
		t.Fatal(err)
	}
	sent, err := time.Parse(time.RFC3339Nano, q.Get("created_after"))
	if err != nil || !sent.Equal(midnight) {
		t.Fatalf("sent %q %v %v", q.Get("created_after"), sent, err)
	}

	got = runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "record", "list", "-created-before", "2026-10-24")
	if got.code != ExitOK || !strings.Contains(got.out, "early-row") || strings.Contains(got.out, "late-row") || strings.Contains(got.out, "midnight-row") {
		t.Fatalf("before date %+v", got)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "record", "list", "-created-after", midnight.Format(time.RFC3339))
	if got.code != ExitOK || !strings.Contains(got.out, "late-row") || strings.Contains(got.out, "midnight-row") {
		t.Fatalf("rfc %+v", got)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""),
		"--context", "dev", "record", "list", "-session-id", "cd34ef", "-strategy-id", "beta", "-created-by", "bob")
	if got.code != ExitOK || !strings.Contains(got.out, "late-row") || strings.Contains(got.out, "early-row") {
		t.Fatalf("filters %+v", got)
	}
	last := srv.Calls()[len(srv.Calls())-1].RawQuery
	if !strings.Contains(last, "session_id=cd34ef") || !strings.Contains(last, "strategy_id=beta") || !strings.Contains(last, "created_by=bob") {
		t.Fatalf("query %s", last)
	}

	before := len(srv.Calls())
	for _, bad := range []string{"1h", "yesterday", "2026-10-24T00:00:00"} {
		got = runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "record", "list", "-created-after", bad)
		if got.code != ExitUsage || !strings.Contains(got.err, "RFC 3339") || len(srv.Calls()) != before {
			t.Fatalf("%s %+v calls %d", bad, got, len(srv.Calls()))
		}
	}

	paged, homeP := startSess(t, apitest.Option{Records: recs[:2], PageSize: 1})
	got = runAt(context.Background(), homeP, nil, strings.NewReader(""), "--context", "dev", "-o", "json", "record", "list")
	if got.code != ExitOK || len(paged.Calls()) != 2 || strings.Contains(got.out, "next_cursor") || strings.Contains(got.out, "freshness") {
		t.Fatalf("page %+v calls %d", got, len(paged.Calls()))
	}
	if !strings.Contains(got.out, "00000000-0000-4000-8000-000000000001") || !strings.Contains(got.out, "00000000-0000-4000-8000-000000000002") {
		t.Fatalf("ids %s", got.out)
	}
	got = runAt(context.Background(), homeP, nil, strings.NewReader(""), "--context", "dev", "-o", "json", "record", "list", "-cursor", "1")
	if got.code != ExitOK || !bytes.Equal([]byte(got.out), paged.LastBody()) {
		t.Fatalf("cursor %s\n%s", got.out, paged.LastBody())
	}
}

func TestRecordShow(t *testing.T) {
	created := time.Date(2026, 6, 1, 3, 4, 5, 0, time.UTC)
	rec := testRecord("00000000-0000-4000-8000-000000000009", "ab12cd", "kept", "ada", created)
	rec.State = apiv1.SessionRecordStateClosed
	closed := created.Add(time.Minute)
	rec.ClosedAt = &closed
	rec.Final = &apiv1.SessionStatus{
		Phase: apiv1.PhaseFailed, Reason: "deps_failed", UpdatedAt: closed,
		LastExit: &apiv1.Exit{Outcome: apiv1.OutcomeFailed, Code: 2, Signal: 0, At: closed},
	}
	rec.Spec.Strategy = strategyRef("alpha", "kept", digA)
	sess := baseSession("ab12cd", apiv1.PhaseDone)
	srv, home := startSess(t, apitest.Option{Sessions: []apiv1.SessionDetail{sess}, Records: []apiv1.SessionRecord{rec}})

	got := runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "--yes", "session", "delete", "ab12cd")
	if got.code != ExitOK {
		t.Fatalf("delete %+v", got)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "record", "show", "00000000-0000-4000-8000-000000000009")
	if got.code != ExitOK {
		t.Fatalf("%+v", got)
	}
	for _, want := range []string{"kept", digA, "alpha", "closed", "deps_failed", "failed", "2", localStamp(created), "ab12cd"} {
		if !strings.Contains(got.out, want) {
			t.Fatalf("missing %q\n%s", want, got.out)
		}
	}
	var shows int
	for _, c := range srv.Calls() {
		if c.Operation == "GetSessionRecord" {
			shows++
			if c.Path != "/api/v1/session-records/00000000-0000-4000-8000-000000000009" {
				t.Fatalf("path %s", c.Path)
			}
		}
		if c.Operation == "GetSession" {
			t.Fatalf("record show called session %+v", c)
		}
	}
	if shows != 1 {
		t.Fatalf("shows %d", shows)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "-o", "json", "record", "show", "00000000-0000-4000-8000-000000000009")
	if got.code != ExitOK || !bytes.Equal([]byte(got.out), srv.LastBody()) {
		t.Fatalf("json %s\n%s", got.out, srv.LastBody())
	}
	noLeak(t, tokenA, got.out+got.err)
}

func TestSessionUsage(t *testing.T) {
	got := runAt(context.Background(), t.TempDir(), nil, strings.NewReader(""), "help")
	if got.code != ExitOK || !strings.Contains(got.out, "session") || !strings.Contains(got.out, "record") {
		t.Fatalf("help %+v", got)
	}
	got = runAt(context.Background(), t.TempDir(), nil, strings.NewReader(""), "help", "session")
	if got.code != ExitOK || !strings.Contains(got.out, "最後已知，agent 目前不在線") || !strings.Contains(got.out, "-phase") || !strings.Contains(got.out, "first 7") || !strings.Contains(got.out, "session watch") || !strings.Contains(got.out, "等待逾時 (38)") || !strings.Contains(got.out, "deps_failed (35)") || !strings.Contains(got.out, "some output was rotated out") || !strings.Contains(got.out, "API 版本太舊") || !strings.Contains(got.out, "manifest.json") || !strings.Contains(got.out, "--latest") {
		t.Fatalf("session help %+v", got)
	}
	got = runAt(context.Background(), t.TempDir(), nil, strings.NewReader(""), "help", "record")
	if got.code != ExitOK || !strings.Contains(got.out, "YYYY-MM-DD") || !strings.Contains(got.out, "record show") {
		t.Fatalf("record help %+v", got)
	}
	got = runAt(context.Background(), t.TempDir(), nil, strings.NewReader(""), "session")
	if got.code != ExitUsage || !strings.Contains(got.err, "session list|show|end|delete|watch|logs|export-logs") {
		t.Fatalf("bare %+v", got)
	}
	got = runAt(context.Background(), t.TempDir(), nil, strings.NewReader(""), "session", "show", "ABC123")
	if got.code != ExitUsage || !strings.Contains(got.err, "6 lowercase hex") {
		t.Fatalf("id %+v", got)
	}
	got = runAt(context.Background(), t.TempDir(), nil, strings.NewReader(""), "session", "list", "-phase", "nope")
	if got.code != ExitUsage || !strings.Contains(got.err, "not a session phase") {
		t.Fatalf("phase %+v", got)
	}
	got = runAt(context.Background(), t.TempDir(), nil, strings.NewReader(""), "session", "list", "-active", "maybe")
	if got.code != ExitUsage || !strings.Contains(got.err, "invalid value for flag") || strings.Contains(got.err, "maybe") {
		t.Fatalf("active %+v", got)
	}
	got = runAt(context.Background(), t.TempDir(), nil, strings.NewReader(""), "session", "list", "-limit", "1", "-cursor", "1")
	if got.code != ExitUsage || !strings.Contains(got.err, "-limit and -cursor cannot both be set") {
		t.Fatalf("both %+v", got)
	}
	got = runAt(context.Background(), t.TempDir(), nil, strings.NewReader(""), "account", "list", "-phase", "running")
	if got.code != ExitUsage || !strings.Contains(got.err, "flag not accepted: -phase") {
		t.Fatalf("account phase %+v", got)
	}
	got = runAt(context.Background(), t.TempDir(), nil, strings.NewReader(""), "record", "show", "not-a-uuid")
	if got.code != ExitUsage || !strings.Contains(got.err, "record id must be a UUID") {
		t.Fatalf("uuid %+v", got)
	}
	got = runAt(context.Background(), t.TempDir(), nil, strings.NewReader(""), "record", "list", "-created-after", "")
	if got.code != ExitUsage || !strings.Contains(got.err, "needs a time") {
		t.Fatalf("empty time %+v", got)
	}
	home := t.TempDir()
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "whoami", "-phase", "running")
	if got.code != ExitUsage || !strings.Contains(got.err, "flag not accepted: -phase") {
		t.Fatalf("whoami %+v", got)
	}
}

func startSess(t *testing.T, opt apitest.Option) (*apitest.Server, string) {
	t.Helper()
	if opt.Token == "" {
		opt.Token = tokenA
	}
	if opt.Name == "" {
		opt.Name = "ada"
	}
	srv := apitest.Start(opt)
	t.Cleanup(srv.Close)
	home := t.TempDir()
	writeConfig(t, home, configYAML(srv.URL(), true, tokenA))
	return srv, home
}

func strategyRef(id, name, digest string) apiv1.StrategyRef {
	ref := apiv1.StrategyRef{Name: name, Digest: digest, Repository: "example.invalid/alpha"}
	if id != "" {
		ref.Id = &id
	}
	return ref
}

func baseSession(id string, phase apiv1.Phase) apiv1.SessionDetail {
	by := "ada"
	created := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	return apiv1.SessionDetail{
		Id: id, StsInstance: "sts-1", Region: "tw", Revision: 1,
		CreatedAt: &created, CreatedBy: &by,
		Spec: apiv1.SessionSpecView{
			Name: "chase", DesiredState: apiv1.DesiredStateRunning,
			Strategy: strategyRef("alpha", "chase", digA),
		},
		Status: &apiv1.SessionStatus{Phase: phase, Reason: "because", UpdatedAt: created},
	}
}

func testRecord(id, sessionID, name, by string, at time.Time) apiv1.SessionRecord {
	who := by
	return apiv1.SessionRecord{
		RecordId:    openapi_types.UUID(uuid.MustParse(id)),
		SessionId:   sessionID,
		State:       apiv1.SessionRecordStateCreated,
		StsInstance: "sts-1",
		CreatedAt:   at,
		CreatedBy:   &who,
		Spec: apiv1.SessionSpecView{
			Name: name, DesiredState: apiv1.DesiredStateRunning,
			Strategy: strategyRef("alpha", name, digA),
		},
	}
}

func strp(s string) *string { return &s }

const idemOnes = "11111111-1111-1111-1111-111111111111"

var uuidRe = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func TestRetryableNet(t *testing.T) {
	ctx := context.Background()
	if !retryableNet(ctx, io.EOF) || !retryableNet(ctx, io.ErrUnexpectedEOF) || !retryableNet(ctx, context.DeadlineExceeded) || !retryableNet(ctx, net.ErrClosed) {
		t.Fatal("connection errors must be retried")
	}
	if retryableNet(ctx, context.Canceled) || retryableNet(ctx, errors.New("nope")) {
		t.Fatal("unrelated errors must not be retried")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if retryableNet(canceled, context.DeadlineExceeded) || retryableNet(canceled, io.EOF) {
		t.Fatal("a canceled parent must not be retried")
	}
	reset := &url.Error{Op: "POST", URL: "http://127.0.0.1/api/v1/sessions", Err: &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}}
	if !retryableNet(ctx, reset) {
		t.Fatal("connection reset must be retried")
	}
	timed := &url.Error{Op: "POST", URL: "http://127.0.0.1/api/v1/sessions", Err: context.DeadlineExceeded}
	if !retryableNet(ctx, timed) {
		t.Fatal("client timeout must be retried")
	}
	other := &url.Error{Op: "POST", URL: "http://127.0.0.1/api/v1/sessions", Err: errors.New("stopped")}
	if retryableNet(ctx, other) {
		t.Fatal("an unrelated client error must not be retried")
	}
}

func TestSessionCreatePreservesNumbers(t *testing.T) {
	srv, home := startSession(t, apitest.Option{RequestID: "req-sess"})
	path := writeBody(t, "session.yaml", sessionYAML)
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", path, "--context", "dev")
	if got.code != ExitOK {
		t.Fatalf("exit %d stderr %q", got.code, got.err)
	}
	noLeak(t, tokenA, got.out+got.err)
	calls := srv.Calls()
	if len(calls) != 1 {
		t.Fatalf("calls %d", len(calls))
	}
	assertSessionCall(t, calls[0], calls[0].IdempotencyKey)
	if !uuidRe.MatchString(calls[0].IdempotencyKey) {
		t.Fatalf("key %q", calls[0].IdempotencyKey)
	}
	if string(calls[0].Body) != sessionJSON {
		t.Fatalf("body %s", calls[0].Body)
	}
	if strings.Contains(got.err, "Idempotency-Key:") {
		t.Fatalf("success printed the key: %q", got.err)
	}
	digest := "sha256:" + strings.Repeat("ab", 32)
	if !strings.Contains(got.out, "SESSION_ID") || !strings.Contains(got.out, "DIGEST") || !strings.Contains(got.out, "000001") || !strings.Contains(got.out, digest) {
		t.Fatalf("stdout %q", got.out)
	}
	if ids := srv.SessionIDs(); len(ids) != 1 || ids[0] != "000001" {
		t.Fatalf("ids %v", ids)
	}

	jsonPath := writeBody(t, "session.json", sessionJSON)
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", jsonPath, "--context", "dev", "-o", "json")
	if got.code != ExitOK || string(srv.Calls()[1].Body) != sessionJSON {
		t.Fatalf("json exit %d body %s stderr %q", got.code, srv.Calls()[1].Body, got.err)
	}
	if got.out != string(srv.LastBody()) {
		t.Fatalf("stdout is not the API body\n%s\n%s", got.out, srv.LastBody())
	}

	got = runAt(context.Background(), home, nil, strings.NewReader(sessionYAML), "session", "create", "-f", "-", "--context", "dev", "-v")
	if got.code != ExitOK || string(srv.Calls()[2].Body) != sessionJSON {
		t.Fatalf("stdin exit %d body %s stderr %q", got.code, srv.Calls()[2].Body, got.err)
	}
	if !strings.Contains(got.err, "X-Request-Id: req-sess\n") || strings.Contains(got.err, "Idempotency-Key:") {
		t.Fatalf("verbose stderr %q", got.err)
	}
}

func TestSessionCreateIdempotencyKey(t *testing.T) {
	srv, home := startSession(t, apitest.Option{})
	path := writeBody(t, "session.yaml", sessionYAML)
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", path, "--idempotency-key", idemOnes, "--context", "dev")
	if got.code != ExitOK {
		t.Fatalf("exit %d stderr %q", got.code, got.err)
	}
	if srv.Calls()[0].IdempotencyKey != idemOnes {
		t.Fatalf("key %q", srv.Calls()[0].IdempotencyKey)
	}
	if strings.Contains(got.err, "Idempotency-Key:") {
		t.Fatalf("stderr %q", got.err)
	}
	again := runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", path, "--idempotency-key", idemOnes, "--context", "dev")
	if again.code != ExitOK || srv.Calls()[1].IdempotencyKey != idemOnes {
		t.Fatalf("replay exit %d key %q stderr %q", again.code, srv.Calls()[1].IdempotencyKey, again.err)
	}
	if ids := srv.SessionIDs(); len(ids) != 1 || !strings.Contains(again.out, ids[0]) {
		t.Fatalf("ids %v stdout %q", ids, again.out)
	}
}

func TestSessionCreateRetry(t *testing.T) {
	waits := installWaits(t)
	srv, home := startSession(t, apitest.Option{DropCreates: 1, RequestID: "req-sess"})
	path := writeBody(t, "session.yaml", sessionYAML)
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", path, "--context", "dev")
	if got.code != ExitOK {
		t.Fatalf("exit %d stderr %q", got.code, got.err)
	}
	calls := srv.Calls()
	if len(calls) != 2 || calls[0].IdempotencyKey == "" || calls[0].IdempotencyKey != calls[1].IdempotencyKey {
		t.Fatalf("calls %+v", calls)
	}
	if string(calls[0].Body) != sessionJSON || string(calls[1].Body) != sessionJSON {
		t.Fatal("retry changed the body")
	}
	if len(*waits) != 1 || (*waits)[0] != time.Second {
		t.Fatalf("waits %v", *waits)
	}
	if ids := srv.SessionIDs(); len(ids) != 1 || !strings.Contains(got.out, ids[0]) {
		t.Fatalf("ids %v stdout %q", ids, got.out)
	}
	if strings.Contains(got.err, "Idempotency-Key:") {
		t.Fatalf("stderr %q", got.err)
	}
}

func TestSessionCreateRetryExhausted(t *testing.T) {
	waits := installWaits(t)
	srv, home := startSession(t, apitest.Option{DropCreates: 4})
	path := writeBody(t, "session.yaml", sessionYAML)
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", path, "--context", "dev")
	if got.code != ExitNetwork {
		t.Fatalf("exit %d stderr %q", got.code, got.err)
	}
	calls := srv.Calls()
	if len(calls) != 4 {
		t.Fatalf("calls %d", len(calls))
	}
	key := calls[0].IdempotencyKey
	for _, c := range calls {
		if c.IdempotencyKey != key || string(c.Body) != sessionJSON {
			t.Fatalf("call %+v", c)
		}
	}
	if len(*waits) != len(sessionBackoffs) {
		t.Fatalf("waits %v", *waits)
	}
	for i, d := range sessionBackoffs {
		if (*waits)[i] != d {
			t.Fatalf("waits %v", *waits)
		}
	}
	if len(srv.SessionIDs()) != 0 {
		t.Fatalf("stored %v", srv.SessionIDs())
	}
	if !strings.Contains(got.err, "network (2):") || strings.Count(got.err, "Idempotency-Key: "+key+"\n") != 1 {
		t.Fatalf("stderr %q", got.err)
	}
	if strings.Contains(got.err, "此 key 已用於不同內容") {
		t.Fatalf("stderr %q", got.err)
	}
}

func TestSessionCreateDoesNotRetryHTTP(t *testing.T) {
	waits := installWaits(t)
	srv, home := startSession(t, apitest.Option{StorageUnavailable: true, RequestID: "req-sess"})
	path := writeBody(t, "session.yaml", sessionYAML)
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", path, "--idempotency-key", idemOnes, "--context", "dev")
	if got.code != ExitStorageUnavailable || len(srv.Calls()) != 1 || len(*waits) != 0 || len(srv.SessionIDs()) != 0 {
		t.Fatalf("exit %d calls %d waits %v ids %v stderr %q", got.code, len(srv.Calls()), *waits, srv.SessionIDs(), got.err)
	}
	if !strings.Contains(got.err, "storage_unavailable (18):") || strings.Contains(got.err, "此 key 已用於不同內容") || !strings.Contains(got.err, "Idempotency-Key: "+idemOnes+"\n") {
		t.Fatalf("stderr %q", got.err)
	}

	over, home := startSession(t, apitest.Option{AlgoOverlap: true})
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", path, "--idempotency-key", idemOnes, "--context", "dev")
	if got.code != ExitAlgoOverlap || len(over.Calls()) != 1 || strings.Contains(got.err, "此 key 已用於不同內容") {
		t.Fatalf("overlap exit %d calls %d stderr %q", got.code, len(over.Calls()), got.err)
	}
	if !strings.Contains(got.err, "Idempotency-Key: "+idemOnes+"\n") {
		t.Fatalf("stderr %q", got.err)
	}
}

func TestSessionCreateConflict(t *testing.T) {
	srv, home := startSession(t, apitest.Option{RequestID: "req-sess"})
	first := writeBody(t, "a.yaml", sessionYAML)
	second := writeBody(t, "b.yaml", strings.Replace(sessionYAML, "name: demo", "name: other", 1))
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", first, "--idempotency-key", idemOnes, "--context", "dev")
	if got.code != ExitOK {
		t.Fatalf("first exit %d stderr %q", got.code, got.err)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", second, "--idempotency-key", idemOnes, "--context", "dev")
	if got.code != ExitConflict {
		t.Fatalf("exit %d stderr %q", got.code, got.err)
	}
	if !strings.Contains(got.err, "conflict (13):") || !strings.Contains(got.err, "此 key 已用於不同內容\n") || !strings.Contains(got.err, "Idempotency-Key: "+idemOnes+"\n") || !strings.Contains(got.err, "X-Request-Id: req-sess\n") {
		t.Fatalf("stderr %q", got.err)
	}
	if len(srv.Calls()) != 2 || len(srv.SessionIDs()) != 1 {
		t.Fatalf("calls %d ids %v", len(srv.Calls()), srv.SessionIDs())
	}
}

func TestSessionCreateFieldErrors(t *testing.T) {
	const src = "name: demo\nsts_instance: sts1\nstrategy:\n  repository: example.invalid/chase\n  ref: v1\nparams:\n  big: 123\n"
	_, locs, err := convertSession("ignored", []byte(src))
	if err != nil {
		t.Fatal(err)
	}
	pos := locs["/params/big"]
	srv, home := startSession(t, apitest.Option{
		RequestID: "req-sess",
		CreateFields: []apiv1.FieldError{
			{Path: "/params/big", Reason: "nope"},
			{Path: "/no/such", Reason: "gone"},
			{Path: "", Reason: "skip-me"},
		},
	})
	path := writeBody(t, "session.yaml", src)
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", path, "--idempotency-key", idemOnes, "--context", "dev")
	if got.code != ExitValidationFailed || len(srv.Calls()) != 1 || len(srv.SessionIDs()) != 0 {
		t.Fatalf("exit %d calls %d ids %v stderr %q", got.code, len(srv.Calls()), srv.SessionIDs(), got.err)
	}
	want := path + ":7:8 /params/big nope"
	if pos.Line != 7 || pos.Col != 8 || !strings.Contains(got.err, want+"\n") {
		t.Fatalf("pos %+v stderr %q", pos, got.err)
	}
	if !strings.Contains(got.err, "/no/such\n") || strings.Contains(got.err, "gone") || strings.Contains(got.err, "skip-me") {
		t.Fatalf("stderr %q", got.err)
	}
	if !strings.Contains(got.err, "validation_failed (10):") || !strings.Contains(got.err, "X-Request-Id: req-sess\n") || !strings.Contains(got.err, "Idempotency-Key: "+idemOnes+"\n") {
		t.Fatalf("stderr %q", got.err)
	}
}

func TestSessionCreateRejectsFileLocally(t *testing.T) {
	srv, home := startSession(t, apitest.Option{})
	path := writeBody(t, "bad.yaml", "mode: 0755\n")
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", path, "--context", "dev")
	if got.code != ExitUsage || len(srv.Calls()) != 0 {
		t.Fatalf("exit %d calls %d stderr %q", got.code, len(srv.Calls()), got.err)
	}
	if !strings.Contains(got.err, "quote it") || !strings.Contains(got.err, "0755") || !strings.Contains(got.err, path+":1:") {
		t.Fatalf("stderr %q", got.err)
	}
	if !regexp.MustCompile(`Idempotency-Key: [0-9a-f-]{36}\n`).MatchString(got.err) {
		t.Fatalf("stderr %q", got.err)
	}

	dup := writeBody(t, "dup.yaml", "name: a\nname: b\n")
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", dup, "--idempotency-key", idemOnes, "--context", "dev")
	if got.code != ExitUsage || len(srv.Calls()) != 0 || !strings.Contains(got.err, `duplicate key "name"`) || !strings.Contains(got.err, "Idempotency-Key: "+idemOnes+"\n") {
		t.Fatalf("dup exit %d calls %d stderr %q", got.code, len(srv.Calls()), got.err)
	}

	got = runAt(context.Background(), home, nil, strings.NewReader("mode: 0x1F\n"), "session", "create", "-f", "-", "--context", "dev")
	if got.code != ExitUsage || len(srv.Calls()) != 0 || !strings.Contains(got.err, "-:1:") || !strings.Contains(got.err, "0x1F") {
		t.Fatalf("stdin exit %d calls %d stderr %q", got.code, len(srv.Calls()), got.err)
	}
}

func TestSessionCreateSendsUncheckedJSON(t *testing.T) {
	srv, home := startSession(t, apitest.Option{})
	path := writeBody(t, "extra.yaml", "extra: 1\n")
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", path, "--context", "dev")
	if got.code != ExitOK || len(srv.Calls()) != 1 || string(srv.Calls()[0].Body) != `{"extra":1}` {
		t.Fatalf("exit %d body %s stderr %q", got.code, srv.Calls()[0].Body, got.err)
	}
}

func TestSessionCreateUsage(t *testing.T) {
	home := t.TempDir()
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "--context", "dev")
	if got.code != ExitUsage || !strings.Contains(got.err, "session create requires -f") || !regexp.MustCompile(`Idempotency-Key: [0-9a-f-]{36}\n`).MatchString(got.err) {
		t.Fatalf("missing -f: %d %q", got.code, got.err)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "--idempotency-key", idemOnes)
	if got.code != ExitUsage || !strings.Contains(got.err, "Idempotency-Key: "+idemOnes+"\n") {
		t.Fatalf("explicit missing -f: %d %q", got.code, got.err)
	}
	path := writeBody(t, "session.yaml", sessionYAML)
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", path, "extra", "--idempotency-key", idemOnes)
	if got.code != ExitUsage || !strings.Contains(got.err, "takes no arguments") || !strings.Contains(got.err, "Idempotency-Key: "+idemOnes+"\n") {
		t.Fatalf("args: %d %q", got.code, got.err)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", path, "-name", "demo", "--idempotency-key", idemOnes)
	if got.code != ExitUsage || !strings.Contains(got.err, "flag not accepted: -name") || !strings.Contains(got.err, "Idempotency-Key: "+idemOnes+"\n") {
		t.Fatalf("name: %d %q", got.code, got.err)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", path, "-limit", "1", "--idempotency-key", idemOnes)
	if got.code != ExitUsage || !strings.Contains(got.err, "-limit and -cursor are only for strategy list and account list") || !strings.Contains(got.err, "Idempotency-Key: "+idemOnes+"\n") {
		t.Fatalf("limit: %d %q", got.code, got.err)
	}

	got = runAt(context.Background(), home, nil, strings.NewReader(""), "whoami", "-f", "x", "--idempotency-key", idemOnes)
	if got.code != ExitUsage || !strings.Contains(got.err, "flag not accepted: -f -idempotency-key") || strings.Contains(got.err, "Idempotency-Key:") {
		t.Fatalf("whoami: %d %q", got.code, got.err)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "whoami", "-f", "x")
	if got.code != ExitUsage || !strings.Contains(got.err, "flag not accepted: -f") || strings.Contains(got.err, "Idempotency-Key:") {
		t.Fatalf("whoami: %d %q", got.code, got.err)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "account", "create", "-f", "x")
	if got.code != ExitUsage || !strings.Contains(got.err, "flag not accepted: -f") || strings.Contains(got.err, "Idempotency-Key:") {
		t.Fatalf("account: %d %q", got.code, got.err)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "help", "session")
	if got.code != ExitOK || !strings.Contains(got.out, "session create -f") || !strings.Contains(got.out, "此 key 已用於不同內容") {
		t.Fatalf("help %d %q", got.code, got.out)
	}

	big := filepath.Join(t.TempDir(), "big.yaml")
	f, err := os.Create(big)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(int64(maxBody) + 1); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", big, "--idempotency-key", idemOnes)
	if got.code != ExitUsage || !strings.Contains(got.err, "session file is too large") || !strings.Contains(got.err, "Idempotency-Key: "+idemOnes+"\n") {
		t.Fatalf("large: %d %q", got.code, got.err)
	}
}

func TestSessionCreateEmptyKey(t *testing.T) {
	srv, home := startSession(t, apitest.Option{})
	path := writeBody(t, "session.yaml", sessionYAML)
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "-f", path, "--idempotency-key=", "--context", "dev")
	if got.code != ExitOK {
		t.Fatalf("exit %d stderr %q", got.code, got.err)
	}
	if len(srv.Calls()) != 1 || srv.Calls()[0].IdempotencyKey != "" {
		t.Fatalf("calls %+v", srv.Calls())
	}
	if strings.Contains(got.err, "Idempotency-Key:") {
		t.Fatalf("stderr %q", got.err)
	}
}

func TestSessionCreateCanceled(t *testing.T) {
	waits := installWaits(t)
	_, home := startSession(t, apitest.Option{})
	path := writeBody(t, "session.yaml", sessionYAML)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got := runAt(ctx, home, nil, strings.NewReader(""), "session", "create", "-f", path, "--context", "dev")
	if got.code != ExitNetwork || len(*waits) != 0 {
		t.Fatalf("exit %d waits %v stderr %q", got.code, *waits, got.err)
	}
	if !strings.Contains(got.err, "network (2):") || !regexp.MustCompile(`Idempotency-Key: [0-9a-f-]{36}\n`).MatchString(got.err) {
		t.Fatalf("stderr %q", got.err)
	}
}

func startSession(t *testing.T, opt apitest.Option) (*apitest.Server, string) {
	t.Helper()
	if opt.Token == "" {
		opt.Token = tokenA
	}
	if opt.Name == "" {
		opt.Name = "ada"
	}
	srv := apitest.Start(opt)
	t.Cleanup(srv.Close)
	home := t.TempDir()
	writeConfig(t, home, configYAML(srv.URL(), true, tokenA))
	return srv, home
}

func writeBody(t *testing.T, name, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func installWaits(t *testing.T) *[]time.Duration {
	t.Helper()
	var waits []time.Duration
	prev := sessionRetryWait
	sessionRetryWait = func(ctx context.Context, d time.Duration) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		waits = append(waits, d)
		return nil
	}
	t.Cleanup(func() { sessionRetryWait = prev })
	return &waits
}

func assertSessionCall(t *testing.T, c apitest.Call, key string) {
	t.Helper()
	if c.Operation != "CreateSession" || c.Method != "POST" || c.Path != "/api/v1/sessions" {
		t.Fatalf("call %+v", c)
	}
	if c.Authorization != "Bearer "+tokenA || c.ContentType != "application/json" || c.Cookie != "" {
		t.Fatalf("headers %+v", c)
	}
	if !strings.Contains(c.UserAgent, "mftiko-cli/") {
		t.Fatalf("ua %q", c.UserAgent)
	}
	if c.IdempotencyKey != key {
		t.Fatalf("key %q want %q", c.IdempotencyKey, key)
	}
}
