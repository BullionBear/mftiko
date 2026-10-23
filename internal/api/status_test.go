package api

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
)

func TestStatusReads(t *testing.T) {
	se := newSessionEnv(t, sessionOpt{})
	code, hdr, body := se.post(t, minBody("sts-tw-1"), keyHeader("status-a"))
	first := mustSession(t, code, hdr, body)
	code, hdr, body = se.post(t, minBody("sts-tw-1"), keyHeader("status-b"))
	second := mustSession(t, code, hdr, body)

	page := se.sessions(t, "/api/v1/sessions?limit=1")
	if len(page.Items) != 1 || page.NextCursor == nil {
		t.Fatalf("first page %+v", page)
	}
	rest := se.sessions(t, "/api/v1/sessions?limit=1&cursor="+url.QueryEscape(*page.NextCursor))
	if len(rest.Items) != 1 || rest.NextCursor != nil || rest.Items[0].Id == page.Items[0].Id {
		t.Fatalf("second page %+v", rest)
	}
	gotIDs := map[string]bool{page.Items[0].Id: true, rest.Items[0].Id: true}
	if !gotIDs[first.Id] || !gotIDs[second.Id] {
		t.Fatalf("pages %v want %s %s", gotIDs, first.Id, second.Id)
	}

	if items := se.sessions(t, "/api/v1/sessions?phase=running").Items; len(items) != 0 {
		t.Fatalf("phase filter kept a session with no status: %d", len(items))
	}
	active := se.sessions(t, "/api/v1/sessions?active=true")
	if len(active.Items) != 2 {
		t.Fatalf("active %d", len(active.Items))
	}
	if items := se.sessions(t, "/api/v1/sessions?active=false").Items; len(items) != 0 {
		t.Fatalf("active=false %d", len(items))
	}
	if items := se.sessions(t, "/api/v1/sessions?sts_instance=sts-tw-1").Items; len(items) != 2 {
		t.Fatalf("sts filter %d", len(items))
	}
	if items := se.sessions(t, "/api/v1/sessions?sts_instance=sts-jp-1").Items; len(items) != 0 {
		t.Fatalf("other sts %d", len(items))
	}
	if items := se.sessions(t, "/api/v1/sessions?sts_instance=not-an-instance").Items; len(items) != 0 {
		t.Fatalf("unknown sts %d", len(items))
	}
	code, _, body = se.call(t, http.MethodGet, "/api/v1/sessions/abc123", "", nil)
	if code != http.StatusNotFound {
		t.Fatalf("missing session %d %s", code, body)
	}
	mustProblem(t, body, http.StatusNotFound, "not_found")

	tw := se.e.servers["tw"]
	putProto(t, tw, state.AgentKey(state.PlaneMD, "md-tw-1"), &statev1.AgentRecord{
		Plane: string(state.PlaneMD), Instance: "md-tw-1", Epoch: 3, Release: "rel",
		StartedAtNs: 1_700_000_000_000_000_000, Host: "md-host", WorkDir: "/md",
	})
	putProto(t, tw, state.AgentKey(state.PlaneSTS, "sts-tw-1"), &statev1.AgentRecord{
		Plane: string(state.PlaneSTS), Instance: "sts-tw-1", Epoch: 4, Release: "rel",
		StartedAtNs: 1_700_000_000_000_000_000, Host: "test-host", WorkDir: "/work",
	})
	putProto(t, tw, state.WorkerStatusKey(state.PlaneSTS, "sts-tw-1", state.WorkerHash("sts/"+first.Id)), &statev1.WorkerStatus{
		Worker: &commonv1.WorkerRef{Key: "sts/" + first.Id, Incarnation: 2}, Phase: "RUNNING",
	})
	putProto(t, tw, state.MDDemandKey("md-tw-1", first.Id, "feed/book.X"), &statev1.DemandEntry{})
	putProto(t, tw, state.MDAckKey("md-tw-1", first.Id, "feed/book.X"), &statev1.EntryStatus{
		State: statev1.EntryState_ENTRY_STATE_REJECTED, Reason: "no_feed", Subject: "subj", UpdatedAtNs: 11,
	})
	putProto(t, tw, state.MDDemandKey("md-tw-1", first.Id, "select/main"), &statev1.DemandEntry{})
	putProto(t, tw, state.SessionStatusKey("sts-tw-1", first.Id), &statev1.SessionStatus{
		Phase: statev1.Phase_PHASE_RUNNING, UpdatedAtNs: 9,
	})
	putProto(t, tw, state.SessionDemandKey("sts-tw-1", first.Id), &statev1.SessionDemand{
		Desired: sessionv1.DesiredState_DESIRED_STATE_RUNNING, Start: true, IssuedAtNs: 100,
		Bindings: []*statev1.Binding{
			{Target: &statev1.Binding_Feed{Feed: "book X"}, Instance: "md-tw-1",
				Status: &statev1.EntryStatus{State: statev1.EntryState_ENTRY_STATE_REJECTED, Reason: "no_feed", UpdatedAtNs: 11}},
			{Target: &statev1.Binding_Feed{Feed: "book.Y"}, Instance: "md-tw-1",
				Status: &statev1.EntryStatus{State: statev1.EntryState_ENTRY_STATE_READY, Reason: "up", UpdatedAtNs: 12}},
		},
	})

	var stsDetail apiv1.InstanceDetail
	until(t, 10*time.Second, "sts instance shows its agent and worker", func() error {
		stsDetail = se.instance(t, "sts-tw-1")
		if stsDetail.Agent == nil || stsDetail.Agent.Host == nil || *stsDetail.Agent.Host != "test-host" ||
			stsDetail.Agent.WorkDir == nil || *stsDetail.Agent.WorkDir != "/work" || stsDetail.Agent.LastKnown != nil {
			return fmt.Errorf("agent %+v", stsDetail.Agent)
		}
		if len(stsDetail.Workers) != 1 || stsDetail.Workers[0].Key != "sts/"+first.Id || stsDetail.Workers[0].Phase != apiv1.WorkerPhaseRunning {
			return fmt.Errorf("workers %+v", stsDetail.Workers)
		}
		if stsDetail.Workers == nil || stsDetail.Sessions == nil || stsDetail.Demands != nil || !stsDetail.RegionAvailable {
			return fmt.Errorf("shape workers %v sessions %v demands %v available %v", stsDetail.Workers, stsDetail.Sessions, stsDetail.Demands, stsDetail.RegionAvailable)
		}
		return nil
	})
	var mdDetail apiv1.InstanceDetail
	until(t, 10*time.Second, "md demand keeps the ack", func() error {
		mdDetail = se.instance(t, "md-tw-1")
		if mdDetail.Demands == nil {
			return errors.New("no demands")
		}
		var feed *apiv1.DemandStatus
		for i := range *mdDetail.Demands {
			item := &(*mdDetail.Demands)[i]
			if item.Kind != apiv1.DemandStatusKindFeed {
				return fmt.Errorf("unexpected demand %+v", item)
			}
			feed = item
		}
		if feed == nil || feed.State != apiv1.EntryStateRejected || feed.Reason == nil || *feed.Reason != "no_feed" {
			return fmt.Errorf("demands %+v", mdDetail.Demands)
		}
		return nil
	})

	listed := se.instances(t)
	if len(listed.Items) != 6 || !listed.Freshness.ControllerAbsent || listed.Controller.Present || listed.Controller.HomeRegion != "tw" {
		t.Fatalf("instances %+v controller %+v fresh %+v", listed.Items, listed.Controller, listed.Freshness)
	}
	for _, item := range listed.Items {
		if item.Name == state.ControllerInstance {
			t.Fatal("controller is an instance")
		}
	}
	if items := se.instancesQuery(t, "/api/v1/instances?plane=sts").Items; len(items) != 2 {
		t.Fatalf("plane sts %d", len(items))
	}
	code, _, body = se.call(t, http.MethodGet, "/api/v1/instances?plane=nope", "", nil)
	if code != http.StatusBadRequest {
		t.Fatalf("plane %d %s", code, body)
	}
	hasPath(t, body, "/plane")
	for _, name := range []string{state.ControllerInstance, "nope"} {
		code, _, body = se.call(t, http.MethodGet, "/api/v1/instances/"+name, "", nil)
		if code != http.StatusNotFound {
			t.Fatalf("instance %s %d %s", name, code, body)
		}
		mustProblem(t, body, http.StatusNotFound, "not_found")
	}

	var detail apiv1.SessionDetail
	until(t, 10*time.Second, "rejected dependency while the provider is up", func() error {
		detail = se.detail(t, first.Id)
		rej := depByFeed(detail.Dependencies, "book X")
		ready := depByFeed(detail.Dependencies, "book.Y")
		if rej == nil || rej.State != apiv1.EntryStateRejected || rej.Reason == nil || *rej.Reason != "no_feed" {
			return fmt.Errorf("rejected %+v", rej)
		}
		if ready == nil || ready.State != apiv1.EntryStateReady {
			return fmt.Errorf("ready %+v", ready)
		}
		return nil
	})
	stdout := filepath.Join("/work", "log", "sts", state.WorkerHash("sts/"+first.Id)+".stdout")
	if detail.Logs == nil || detail.Logs.Host != "test-host" || detail.Logs.Stdout != stdout {
		t.Fatalf("logs %+v", detail.Logs)
	}
	if items := se.sessions(t, "/api/v1/sessions?phase=running").Items; len(items) != 1 || items[0].Id != first.Id {
		t.Fatalf("phase running %+v", items)
	}

	delKey(t, tw, state.AgentKey(state.PlaneMD, "md-tw-1"))
	until(t, 10*time.Second, "absent provider is silent and keeps the reason", func() error {
		detail = se.detail(t, first.Id)
		rej := depByFeed(detail.Dependencies, "book X")
		if rej == nil || rej.State != apiv1.EntryStateSilent || rej.Reason == nil || *rej.Reason != "no_feed" {
			return fmt.Errorf("silent %+v", rej)
		}
		return nil
	})
	putProto(t, tw, state.AgentKey(state.PlaneMD, "md-tw-1"), &statev1.AgentRecord{
		Plane: string(state.PlaneMD), Instance: "md-tw-1", Epoch: 5, Release: "rel",
		StartedAtNs: 1_700_000_000_000_000_000, Host: "md-host", WorkDir: "/md",
	})
	until(t, 10*time.Second, "provider is back", func() error {
		detail = se.detail(t, first.Id)
		rej := depByFeed(detail.Dependencies, "book X")
		if rej == nil || rej.State != apiv1.EntryStateRejected {
			return fmt.Errorf("state %+v", rej)
		}
		return nil
	})

	// updated_at has to be inside the retention window. A terminal status stamped
	// at the epoch is deleted on the next reconcile (D1017).
	failedAt := time.Now().UnixNano()
	putProto(t, tw, state.SessionStatusKey("sts-tw-1", first.Id), &statev1.SessionStatus{
		Phase: statev1.Phase_PHASE_FAILED, Reason: "deps_failed", UpdatedAtNs: failedAt,
		Conditions: []*commonv1.Condition{
			{Type: "dependency", Reason: "no_feed", Message: "feed book X md-tw-1 rejected", AtNs: failedAt},
			{Type: "dependency", Reason: "empty", Message: "selector main md-tw-1 rejected", AtNs: failedAt},
		},
	})
	delKey(t, tw, state.SessionDemandKey("sts-tw-1", first.Id))
	delKey(t, tw, state.AgentKey(state.PlaneMD, "md-tw-1"))
	until(t, 10*time.Second, "terminal dependency survives the demand and the agent", func() error {
		detail = se.detail(t, first.Id)
		if detail.Status == nil || detail.Status.Phase != apiv1.PhaseFailed || detail.Assignment != nil {
			return fmt.Errorf("phase %v assignment %v", detail.Status, detail.Assignment)
		}
		if len(detail.Dependencies) != 1 {
			return fmt.Errorf("deps %+v", detail.Dependencies)
		}
		dep := detail.Dependencies[0]
		if dep.State != apiv1.EntryStateRejected || dep.Feed == nil || *dep.Feed != "book X" || dep.Reason == nil || *dep.Reason != "no_feed" {
			return fmt.Errorf("dep %+v", dep)
		}
		if detail.Status == nil || len(detail.Status.Conditions) != 2 {
			return fmt.Errorf("conditions %v", detail.Status)
		}
		return nil
	})

	delKey(t, tw, state.AgentKey(state.PlaneSTS, "sts-tw-1"))
	until(t, 10*time.Second, "sts agent is last known", func() error {
		stsDetail = se.instance(t, "sts-tw-1")
		ag := stsDetail.Agent
		if ag == nil || ag.LastKnown == nil || !*ag.LastKnown || ag.Host == nil || *ag.Host != "test-host" || ag.WorkDir == nil || *ag.WorkDir != "/work" {
			return fmt.Errorf("agent %+v", ag)
		}
		return nil
	})
	detail = se.detail(t, first.Id)
	if detail.Logs == nil || detail.Logs.Stdout != stdout {
		t.Fatalf("last known logs %+v", detail.Logs)
	}

	other := startRunning(t, se.e.cfg, se.e.log)
	until(t, 30*time.Second, "second replica ready", func() error {
		if other.stopped() {
			return errors.New("exited")
		}
		body, _ := other.s.readiness(time.Now())
		if len(body.UnavailableRegions) != 0 {
			return fmt.Errorf("unavailable %v", body.UnavailableRegions)
		}
		return nil
	})
	until(t, 10*time.Second, "restarted replica has no memory of the agent", func() error {
		d := instanceAt(t, other.s, "sts-tw-1")
		if !d.RegionAvailable {
			return errors.New("region down")
		}
		if d.Agent != nil {
			return fmt.Errorf("agent %+v", d.Agent)
		}
		sess := detailAt(t, other.s, first.Id)
		if sess.Logs != nil {
			return fmt.Errorf("logs %+v", sess.Logs)
		}
		return nil
	})
}

func TestSessionRecordPages(t *testing.T) {
	se := newSessionEnv(t, sessionOpt{})
	code, hdr, body := se.post(t, minBody("sts-tw-1"), keyHeader("rec-a"))
	older := mustSession(t, code, hdr, body)
	code, hdr, body = se.post(t, minBody("sts-tw-1"), keyHeader("rec-b"))
	newer := mustSession(t, code, hdr, body)

	page := se.records(t, "/api/v1/session-records?limit=1")
	if len(page.Items) != 1 || page.NextCursor == nil {
		t.Fatalf("newest page %+v", page.Items)
	}
	if page.Items[0].Final != nil || page.Items[0].CreatedBy == nil || *page.Items[0].CreatedBy != "ada" || page.Items[0].State != apiv1.SessionRecordStateCreated {
		t.Fatalf("row %+v", page.Items[0])
	}
	next := se.records(t, "/api/v1/session-records?limit=1&cursor="+url.QueryEscape(*page.NextCursor))
	if len(next.Items) != 1 || next.NextCursor != nil || next.Items[0].SessionId == page.Items[0].SessionId {
		t.Fatalf("older page %+v", next.Items)
	}
	if next.Items[0].CreatedAt.After(page.Items[0].CreatedAt) {
		t.Fatalf("order %s then %s", page.Items[0].CreatedAt, next.Items[0].CreatedAt)
	}
	ids := map[string]bool{page.Items[0].SessionId: true, next.Items[0].SessionId: true}
	if !ids[older.Id] || !ids[newer.Id] {
		t.Fatalf("pages %v", ids)
	}
	if page.Items[0].CreatedAt.After(next.Items[0].CreatedAt) && page.Items[0].SessionId != newer.Id {
		t.Fatalf("newest session %s, first page %s", newer.Id, page.Items[0].SessionId)
	}

	matched := se.records(t, "/api/v1/session-records?strategy_id=strategies")
	if len(matched.Items) != 2 {
		t.Fatalf("strategy %d", len(matched.Items))
	}
	if items := se.records(t, "/api/v1/session-records?strategy_id=nope").Items; len(items) != 0 {
		t.Fatalf("strategy nope %d", len(items))
	}
	if items := se.records(t, "/api/v1/session-records?created_by=ada").Items; len(items) != 2 {
		t.Fatalf("created_by %d", len(items))
	}
	if items := se.records(t, "/api/v1/session-records?created_by=nobody").Items; len(items) != 0 {
		t.Fatalf("nobody %d", len(items))
	}
	bySession := se.records(t, "/api/v1/session-records?session_id="+url.QueryEscape(older.Id))
	if len(bySession.Items) != 1 || bySession.Items[0].SessionId != older.Id {
		t.Fatalf("session filter %+v", bySession.Items)
	}

	bound := page.Items[0].CreatedAt
	q := url.Values{}
	q.Set("created_before", bound.Format(time.RFC3339Nano))
	before := se.records(t, "/api/v1/session-records?"+q.Encode())
	for _, item := range before.Items {
		if !item.CreatedAt.Before(bound) {
			t.Fatalf("created_before kept %s", item.CreatedAt)
		}
	}
	if bound.After(next.Items[0].CreatedAt) {
		found := false
		for _, item := range before.Items {
			if item.SessionId == older.Id {
				found = true
			}
		}
		if !found {
			t.Fatal("created_before dropped the older row")
		}
	}
	q = url.Values{}
	q.Set("created_after", next.Items[0].CreatedAt.Format(time.RFC3339Nano))
	after := se.records(t, "/api/v1/session-records?"+q.Encode())
	for _, item := range after.Items {
		if !item.CreatedAt.After(next.Items[0].CreatedAt) {
			t.Fatalf("created_after kept %s", item.CreatedAt)
		}
	}

	code, _, body = se.call(t, http.MethodGet, "/api/v1/session-records/"+page.Items[0].RecordId.String(), "", nil)
	if code != http.StatusOK {
		t.Fatalf("get record %d %s", code, body)
	}
	var one apiv1.SessionRecord
	if err := json.Unmarshal(body, &one); err != nil {
		t.Fatal(err)
	}
	if one.RecordId != page.Items[0].RecordId || one.Final != nil {
		t.Fatalf("one %+v", one)
	}
	code, _, body = se.call(t, http.MethodGet, "/api/v1/session-records/00000000-0000-4000-8000-000000000001", "", nil)
	if code != http.StatusNotFound {
		t.Fatalf("missing record %d %s", code, body)
	}
	mustProblem(t, body, http.StatusNotFound, "not_found")

	bad := url.Values{}
	bad.Set("cursor", base64.RawURLEncoding.EncodeToString([]byte("not-a-cursor")))
	code, _, body = se.call(t, http.MethodGet, "/api/v1/session-records?"+bad.Encode(), "", nil)
	if code != http.StatusBadRequest {
		t.Fatalf("cursor %d %s", code, body)
	}
	hasPath(t, body, "/cursor")
}

func TestSSEResume(t *testing.T) {
	prevHB := sseHeartbeat
	t.Cleanup(func() { sseHeartbeat = prevHB })
	sseHeartbeat = 50 * time.Millisecond

	se := newSessionEnv(t, sessionOpt{})
	code, hdr, body := se.post(t, minBody("sts-tw-1"), keyHeader("sse"))
	sess := mustSession(t, code, hdr, body)

	for _, id := range []string{"tw=0", "tw", "tw=1;tw=2", "tw=1;", "=1", "tw=x"} {
		h := make(http.Header)
		h.Set("Last-Event-ID", id)
		code, _, body = se.call(t, http.MethodGet, "/api/v1/events?topics=sessions", "", h)
		if code != http.StatusBadRequest {
			t.Fatalf("id %q: %d %s", id, code, body)
		}
		hasPath(t, body, "Last-Event-ID")
	}

	live := openSSE(t, se.s.Addr(), "topics=sessions", "")
	var id string
	until(t, 15*time.Second, "snapshot id", func() error {
		id = resumeID(live.copy())
		if id == "" {
			return errors.New("no id yet")
		}
		if !frameHasName(live.copy(), sess.Id, "n") {
			return errors.New("snapshot has not named the session")
		}
		return nil
	})
	live.close()

	renameSession(t, se, sess.Id, "renamed")
	resumed := openSSE(t, se.s.Addr(), "topics=sessions", id)
	var nextID string
	until(t, 15*time.Second, "watch delivers the change made while disconnected", func() error {
		frames := resumed.copy()
		for _, f := range frames {
			if f.Event == "reset" {
				return errors.New("reset")
			}
		}
		if !frameHasName(frames, sess.Id, "renamed") {
			return errors.New("renamed session not in")
		}
		nextID = resumeID(frames)
		if nextID == "" {
			return errors.New("change has no resume id")
		}
		return nil
	})
	resumed.close()

	quiet := openSSE(t, se.s.Addr(), "topics=sessions", nextID)
	until(t, 5*time.Second, "resume does not replay the current session", func() error {
		for _, f := range quiet.copy() {
			if f.Event == "session" || f.Event == "reset" {
				t.Fatalf("resume emitted %s before a new change", f.Event)
			}
		}
		if !frameHasComment(quiet.copy(), "keepalive") {
			return errors.New("no keepalive")
		}
		return nil
	})
	renameSession(t, se, sess.Id, "again")
	until(t, 10*time.Second, "watch after resume", func() error {
		if !frameHasName(quiet.copy(), sess.Id, "again") {
			return errors.New("again not in")
		}
		return nil
	})
	caught := resumeID(quiet.copy())
	quiet.close()
	if caught == "" {
		t.Fatal("no id after the second change")
	}

	twRev := revOf(caught, "tw")
	if twRev < 1 {
		t.Fatalf("id %q", caught)
	}
	advanced := putKey(t, t.Context(), se.e.servers["tw"], state.Root+"/compact-marker", "1")
	if advanced <= twRev {
		t.Fatalf("compact point %d not after %d", advanced, twRev)
	}
	if _, err := se.e.servers["tw"].Client(t).Compact(t.Context(), advanced); err != nil {
		t.Fatal(err)
	}
	reset := openSSE(t, se.s.Addr(), "topics=sessions", caught)
	until(t, 10*time.Second, "compacted resume resets", func() error {
		for _, f := range reset.copy() {
			if f.Event == "reset" {
				return nil
			}
		}
		return errors.New("no reset")
	})
	reset.close()

	future := openSSE(t, se.s.Addr(), "topics=sessions", "tw=9000000000000000000")
	until(t, 10*time.Second, "future revision snapshots", func() error {
		if !frameHasName(future.copy(), sess.Id, "again") {
			return errors.New("no snapshot")
		}
		return nil
	})
	future.close()
}

func TestSSEBufferDisconnects(t *testing.T) {
	prevCap, prevHB := sseQueueCap, sseHeartbeat
	t.Cleanup(func() {
		sseQueueCap = prevCap
		sseHeartbeat = prevHB
	})
	sseQueueCap = 1
	sseHeartbeat = time.Hour

	se := newSessionEnv(t, sessionOpt{})
	db, err := pgx.Connect(t.Context(), se.pg.DSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close(context.Background()) })
	before := countEventAudits(t, db)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/events?topics=instances", nil)
	req.Header.Set("Authorization", "Bearer "+tokenAda)
	req = req.WithContext(ctx)
	w := &gateWriter{h: make(http.Header), gate: make(chan struct{}), stop: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(w.gate) }) }
	t.Cleanup(release)
	go se.s.serveHTTP(w, req)

	until(t, 15*time.Second, "full send buffer closes the handler", func() error {
		if countEventAudits(t, db) > before {
			return nil
		}
		return errors.New("handler still open")
	})
	release()
	cancel()
}

type gateWriter struct {
	h    http.Header
	gate chan struct{}
	stop chan struct{}
	once sync.Once
}

func (g *gateWriter) Header() http.Header { return g.h }
func (g *gateWriter) WriteHeader(int)     {}
func (g *gateWriter) Flush()              {}

// SetWriteDeadline is what a full buffer uses to unblock a stuck write.
func (g *gateWriter) SetWriteDeadline(deadline time.Time) error {
	if deadline.IsZero() || deadline.After(time.Now()) {
		return nil
	}
	g.once.Do(func() { close(g.stop) })
	return nil
}

func (g *gateWriter) Write(p []byte) (int, error) {
	select {
	case <-g.gate:
		return len(p), nil
	case <-g.stop:
		return 0, errors.New("write deadline")
	}
}

func (se *sessionEnv) sessions(t *testing.T, path string) apiv1.SessionList {
	t.Helper()
	code, _, body := se.call(t, http.MethodGet, path, "", nil)
	if code != http.StatusOK {
		t.Fatalf("%s %d %s", path, code, body)
	}
	var list apiv1.SessionList
	if err := json.Unmarshal(body, &list); err != nil {
		t.Fatal(err)
	}
	return list
}

func (se *sessionEnv) records(t *testing.T, path string) apiv1.SessionRecordList {
	t.Helper()
	code, _, body := se.call(t, http.MethodGet, path, "", nil)
	if code != http.StatusOK {
		t.Fatalf("%s %d %s", path, code, body)
	}
	var list apiv1.SessionRecordList
	if err := json.Unmarshal(body, &list); err != nil {
		t.Fatal(err)
	}
	return list
}

func (se *sessionEnv) instances(t *testing.T) apiv1.InstanceList {
	t.Helper()
	return se.instancesQuery(t, "/api/v1/instances")
}

func (se *sessionEnv) instancesQuery(t *testing.T, path string) apiv1.InstanceList {
	t.Helper()
	code, _, body := se.call(t, http.MethodGet, path, "", nil)
	if code != http.StatusOK {
		t.Fatalf("%s %d %s", path, code, body)
	}
	var list apiv1.InstanceList
	if err := json.Unmarshal(body, &list); err != nil {
		t.Fatal(err)
	}
	return list
}

func (se *sessionEnv) instance(t *testing.T, name string) apiv1.InstanceDetail {
	t.Helper()
	return instanceAt(t, se.s, name)
}

func (se *sessionEnv) detail(t *testing.T, id string) apiv1.SessionDetail {
	t.Helper()
	return detailAt(t, se.s, id)
}

func instanceAt(t *testing.T, s *Server, name string) apiv1.InstanceDetail {
	t.Helper()
	code, _, body := callServer(t, s, http.MethodGet, "/api/v1/instances/"+name)
	if code != http.StatusOK {
		t.Fatalf("instance %s %d %s", name, code, body)
	}
	var detail apiv1.InstanceDetail
	if err := json.Unmarshal(body, &detail); err != nil {
		t.Fatal(err)
	}
	return detail
}

func detailAt(t *testing.T, s *Server, id string) apiv1.SessionDetail {
	t.Helper()
	code, _, body := callServer(t, s, http.MethodGet, "/api/v1/sessions/"+id)
	if code != http.StatusOK {
		t.Fatalf("session %s %d %s", id, code, body)
	}
	var detail apiv1.SessionDetail
	if err := json.Unmarshal(body, &detail); err != nil {
		t.Fatal(err)
	}
	return detail
}

func callServer(t *testing.T, s *Server, method, path string) (int, http.Header, []byte) {
	t.Helper()
	c := apiCall{method: method, path: path, bearer: tokenAda}
	return c.do(t, &http.Client{Timeout: 20 * time.Second}, "http://"+s.Addr())
}

func putProto(t *testing.T, srv *etcdtest.Server, key string, msg proto.Message) {
	t.Helper()
	b, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	putKey(t, t.Context(), srv, key, string(b))
}

func delKey(t *testing.T, srv *etcdtest.Server, key string) {
	t.Helper()
	if _, err := srv.Client(t).Delete(t.Context(), key); err != nil {
		t.Fatalf("delete %s: %v", key, err)
	}
}

func depByFeed(deps []apiv1.Dependency, feed string) *apiv1.Dependency {
	for i := range deps {
		if deps[i].Feed != nil && *deps[i].Feed == feed {
			return &deps[i]
		}
	}
	return nil
}

func renameSession(t *testing.T, se *sessionEnv, id, name string) {
	t.Helper()
	key := state.SessionSpecKey("sts-tw-1", id)
	kv := se.e.get(t, "tw", key)
	if kv == nil {
		t.Fatal("spec missing")
	}
	spec := &sessionv1.SessionSpec{}
	if err := proto.Unmarshal(kv.Value, spec); err != nil {
		t.Fatal(err)
	}
	spec.Name = name
	putProto(t, se.e.servers["tw"], key, spec)
}

func countEventAudits(t *testing.T, conn *pgx.Conn) int {
	t.Helper()
	n := 0
	for _, row := range queryAudits(t, conn) {
		if row.Method == http.MethodGet && row.Path == "/api/v1/events" && row.Status == http.StatusOK {
			n++
		}
	}
	return n
}

type sseFrame struct {
	ID, Event, Data, Comment string
}

type sseStream struct {
	cancel context.CancelFunc
	body   io.ReadCloser
	mu     sync.Mutex
	frames []sseFrame
	once   sync.Once
}

func openSSE(t *testing.T, addr, query, lastID string) *sseStream {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/api/v1/events?"+query, nil)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+tokenAda)
	if lastID != "" {
		req.Header.Set("Last-Event-ID", lastID)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		cancel()
		t.Fatalf("sse %d %s", resp.StatusCode, b)
	}
	st := &sseStream{cancel: cancel, body: resp.Body}
	t.Cleanup(st.close)
	go st.read()
	return st
}

func (st *sseStream) close() {
	st.once.Do(func() {
		st.cancel()
		_ = st.body.Close()
	})
}

func (st *sseStream) read() {
	r := bufio.NewReader(st.body)
	for {
		f, err := readFrame(r)
		if err != nil {
			return
		}
		st.mu.Lock()
		st.frames = append(st.frames, f)
		st.mu.Unlock()
	}
}

func (st *sseStream) copy() []sseFrame {
	st.mu.Lock()
	defer st.mu.Unlock()
	return append([]sseFrame(nil), st.frames...)
}

func readFrame(r *bufio.Reader) (sseFrame, error) {
	var f sseFrame
	var data []string
	got := false
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return sseFrame{}, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if !got {
				continue
			}
			f.Data = strings.Join(data, "\n")
			return f, nil
		}
		got = true
		if strings.HasPrefix(line, ":") {
			f.Comment = strings.TrimSpace(strings.TrimPrefix(line, ":"))
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "id":
			f.ID = value
		case "event":
			f.Event = value
		case "data":
			data = append(data, value)
		}
	}
}

func resumeID(frames []sseFrame) string {
	id := ""
	for _, f := range frames {
		if strings.Contains(f.ID, "tw=") && strings.Contains(f.ID, "jp=") {
			id = f.ID
		}
	}
	return id
}

func frameHasName(frames []sseFrame, id, name string) bool {
	for _, f := range frames {
		if f.Event != "session" {
			continue
		}
		var detail apiv1.SessionDetail
		if json.Unmarshal([]byte(f.Data), &detail) != nil {
			continue
		}
		if detail.Id == id && detail.Spec.Name == name {
			return true
		}
	}
	return false
}

func frameHasComment(frames []sseFrame, comment string) bool {
	for _, f := range frames {
		if f.Comment == comment {
			return true
		}
	}
	return false
}

func revOf(id, region string) int64 {
	for _, part := range strings.Split(id, ";") {
		name, revStr, ok := strings.Cut(part, "=")
		if !ok || name != region {
			continue
		}
		n, err := strconv.ParseInt(revStr, 10, 64)
		if err != nil {
			return 0
		}
		return n
	}
	return 0
}
