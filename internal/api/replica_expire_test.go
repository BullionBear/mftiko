package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

func TestWarnShortReplicaGrace(t *testing.T) {
	var buf lockedBuf
	log := zerolog.New(&buf).Level(zerolog.InfoLevel)
	warnShortReplicaGrace(log, 4*time.Second)
	warnShortReplicaGrace(log, tdDefaultLinger)
	warnShortReplicaGrace(log, DefaultTDReplicaGrace)
	got := buf.String()
	if strings.Count(got, "td_replica_grace is below the TD default linger") != 1 {
		t.Fatalf("log %s", got)
	}
	if !strings.Contains(got, `"td_replica_grace":"4s"`) || !strings.Contains(got, `"td_default_linger":"5s"`) {
		t.Fatalf("log %s", got)
	}
}

func TestReplicaDueBoundary(t *testing.T) {
	start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	clock := &testClock{t: start}
	s := &Server{cfg: Config{TDReplicaGrace: time.Second}, now: clock.Now}
	row := replicaView{Inst: "td-tw-1", replicaCreated: start}
	row.ID = 1
	key := replicaKey{inst: "td-tw-1", id: 1}
	uses := map[replicaKey]replicaUse{}
	if s.replicaDue(row, uses) {
		t.Fatal("due at creation")
	}
	clock.Set(start.Add(time.Second))
	if s.replicaDue(row, uses) {
		t.Fatal("due at exactly grace")
	}
	clock.Set(start.Add(time.Second + time.Millisecond))
	if !s.replicaDue(row, uses) {
		t.Fatal("kept past grace")
	}
	uses[key] = replicaUse{live: true, ended: start, hasEnded: true}
	if s.replicaDue(row, uses) {
		t.Fatal("live reference did not block")
	}
	ended := start.Add(time.Hour)
	uses[key] = replicaUse{ended: ended, hasEnded: true}
	clock.Set(ended.Add(time.Second))
	if s.replicaDue(row, uses) {
		t.Fatal("due at exactly ended_at plus grace")
	}
	clock.Set(ended.Add(time.Second + time.Millisecond))
	if !s.replicaDue(row, uses) {
		t.Fatal("kept past ended_at plus grace")
	}
	uses[replicaKey{inst: "td-jp-1", id: 1}] = replicaUse{live: true}
	if !s.replicaDue(row, uses) {
		t.Fatal("a live session on another instance kept this replica")
	}
	uses[key] = replicaUse{ended: start.Add(-time.Hour), hasEnded: true}
	clock.Set(start.Add(time.Second))
	if !s.replicaDue(row, uses) {
		t.Fatal("an earlier ended_at was ignored in favor of created_at")
	}
}

func TestReplicaReferenceRules(t *testing.T) {
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if !blocksReplica(recPending, nil, false) || !blocksReplica(recCreated, nil, false) {
		t.Fatal("pending and created without ended_at must block")
	}
	if blocksReplica(recCreated, &at, false) || !blocksReplica(recCreated, &at, true) {
		t.Fatal("created with ended_at follows whether the spec is still running")
	}
	if blocksReplica(recAbandoned, nil, true) || blocksReplica(recClosed, &at, true) {
		t.Fatal("abandoned and closed must not block")
	}

	s := &Server{cfg: Config{Topology: state.Topology{Instances: map[string]string{"sts-tw-1": "tw"}}}}
	if !s.sessionRunning(nil, nil, "sts-missing", "sess-1") {
		t.Fatal("unknown sts instance did not block")
	}
	down := map[string]struct{}{"tw": {}}
	if !s.sessionRunning(nil, down, "sts-tw-1", "sess-1") {
		t.Fatal("down region did not block")
	}
	raw, err := proto.Marshal(&sessionv1.SessionSpec{
		Accounts: []*sessionv1.AccountDecl{{Name: "acct-1", ApiId: 1, TdInstance: "td-tw-1"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	specKey := state.SessionSpecKey("sts-tw-1", "sess-1")
	views := map[string]state.View{"tw": {Entries: map[string]state.Entry{}}}
	if s.sessionRunning(views, nil, "sts-tw-1", "sess-1") {
		t.Fatal("missing spec blocked")
	}
	views["tw"].Entries[specKey] = state.Entry{Value: raw}
	if !s.sessionRunning(views, nil, "sts-tw-1", "sess-1") {
		t.Fatal("missing status was treated as terminal")
	}
	live := liveEtcdReplicas(views)
	if _, ok := live[replicaKey{inst: "td-tw-1", id: 1}]; !ok {
		t.Fatal("non-terminal spec was not a reference")
	}
	if _, ok := live[replicaKey{inst: "td-jp-1", id: 1}]; ok {
		t.Fatal("spec referenced an instance it does not name")
	}
	views["tw"].Entries[specKey] = state.Entry{Value: []byte("zz")}
	if len(liveEtcdReplicas(views)) != 0 {
		t.Fatal("unreadable spec was not skipped")
	}
	views["tw"].Entries[specKey] = state.Entry{Value: raw}
	status, err := proto.Marshal(&statev1.SessionStatus{Phase: statev1.Phase_PHASE_DONE})
	if err != nil {
		t.Fatal(err)
	}
	views["tw"].Entries[state.SessionStatusKey("sts-tw-1", "sess-1")] = state.Entry{Value: status}
	if s.sessionRunning(views, nil, "sts-tw-1", "sess-1") || len(liveEtcdReplicas(views)) != 0 {
		t.Fatal("terminal spec still referenced the replica")
	}
	views["tw"].Entries[state.SessionStatusKey("sts-tw-1", "sess-1")] = state.Entry{Value: []byte("zz")}
	if !s.sessionRunning(views, nil, "sts-tw-1", "sess-1") || len(liveEtcdReplicas(views)) != 1 {
		t.Fatal("unreadable status was treated as terminal")
	}
}

func newGraceFix(t *testing.T, clock *testClock, grace time.Duration) *accountFix {
	t.Helper()
	env := newWriteEnv(t)
	env.cfg.Now = clock.Now
	env.cfg.TDReplicaGrace = grace
	env.cfg.UnavailableAfter = 48 * time.Hour
	return startAccountFix(t, startAccountsDB(t), env)
}

func (f *accountFix) holdReconcile(fn func()) {
	f.srv.reconcileMu.Lock()
	defer f.srv.reconcileMu.Unlock()
	fn()
}

func (f *accountFix) createNamed(t *testing.T, name string) apiv1.Account {
	t.Helper()
	code, _, body := f.call(t, http.MethodPost, "/api/v1/accounts", accountBody(name, "Binance", "td-tw-1", "hmac", "k", "s", "", nil))
	if code != http.StatusCreated {
		t.Fatalf("create %s: %d %s", name, code, body)
	}
	var created apiv1.Account
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	return created
}

func acctSpec(name string, id int64, inst string) *sessionv1.SessionSpec {
	return &sessionv1.SessionSpec{
		Accounts: []*sessionv1.AccountDecl{{Name: name, ApiId: id, TdInstance: inst}},
	}
}

func recID(n int) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", n)
}

func (f *accountFix) insertRecord(t *testing.T, id, sessionID, state string, ended *time.Time, spec *sessionv1.SessionSpec) {
	t.Helper()
	raw, err := proto.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	desired := desRunning
	if state == recClosed {
		desired = desEnded
	}
	pool, err := f.srv.db()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), `
INSERT INTO session_records (
    id, session_id, sts_instance, spec_bytes, spec_sha256,
    state, desired_state, created_at, ended_at
) VALUES ($1::uuid, $2, 'sts-tw-1', $3, $4, $5, $6, $7, $8)`,
		id, sessionID, raw, sum[:], state, desired, f.srv.now().UTC(), ended); err != nil {
		t.Fatal(err)
	}
}

func (f *accountFix) closeRecord(t *testing.T, id string, ended time.Time) {
	t.Helper()
	pool, err := f.srv.db()
	if err != nil {
		t.Fatal(err)
	}
	tag, err := pool.Exec(t.Context(), `
UPDATE session_records
SET state = 'closed', desired_state = 'ended', ended_at = $2
WHERE id = $1::uuid`, id, ended.UTC())
	if err != nil {
		t.Fatal(err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("close %s affected %d", id, tag.RowsAffected())
	}
}

func (f *accountFix) replicaSealed(t *testing.T, inst string, id int64) []byte {
	t.Helper()
	pool, err := f.srv.db()
	if err != nil {
		t.Fatal(err)
	}
	var b []byte
	if err := pool.QueryRow(t.Context(), `
SELECT sealed_credentials FROM account_replicas
WHERE td_instance = $1 AND api_id = $2`, inst, id).Scan(&b); err != nil {
		t.Fatal(err)
	}
	return append([]byte(nil), b...)
}

func (f *accountFix) replicaCreated(t *testing.T, inst string, id int64) time.Time {
	t.Helper()
	pool, err := f.srv.db()
	if err != nil {
		t.Fatal(err)
	}
	var at time.Time
	if err := pool.QueryRow(t.Context(), `
SELECT created_at FROM account_replicas
WHERE td_instance = $1 AND api_id = $2`, inst, id).Scan(&at); err != nil {
		t.Fatal(err)
	}
	return at.UTC()
}

func (f *accountFix) hasReplicaRow(t *testing.T, inst string, id int64) bool {
	t.Helper()
	pool, err := f.srv.db()
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if err := pool.QueryRow(t.Context(), `
SELECT count(*) FROM account_replicas
WHERE td_instance = $1 AND api_id = $2`, inst, id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n == 1
}

func (f *accountFix) hasEtcdAccount(t *testing.T, inst string, id int64) bool {
	t.Helper()
	key := state.AccountKey(inst, id)
	return f.env.get(t, f.env.region(t, key), key) != nil
}

func (f *accountFix) wantReplica(t *testing.T, inst string, id int64, present bool) {
	t.Helper()
	etcd := f.hasEtcdAccount(t, inst, id)
	row := f.hasReplicaRow(t, inst, id)
	if etcd != present || row != present {
		t.Fatalf("%s api %d etcd %v row %v, want present %v", inst, id, etcd, row, present)
	}
}

func (f *accountFix) putSpec(t *testing.T, sessionID string, spec *sessionv1.SessionSpec) string {
	t.Helper()
	raw, err := proto.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	key := state.SessionSpecKey("sts-tw-1", sessionID)
	if _, err := f.env.servers["tw"].Client(t).Put(t.Context(), key, string(raw)); err != nil {
		t.Fatal(err)
	}
	seen(t, f.srv, "tw", key)
	return key
}

func (f *accountFix) dropSpec(t *testing.T, key string) {
	t.Helper()
	if _, err := f.env.servers["tw"].Client(t).Delete(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	until(t, 10*time.Second, "spec gone "+key, func() error {
		if v, _ := f.srv.snapshot("tw"); hasKey(v, key) {
			return errors.New("still there")
		}
		return nil
	})
}

// TestReplicaGrace is E21-D061: a fake clock, no delete before grace, delete after,
// a new session during grace, and two instances sharing one api_id.
func TestReplicaGrace(t *testing.T) {
	start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	clock := &testClock{t: start}
	const grace = time.Second
	f := newGraceFix(t, clock, grace)
	f.srv.ids = apiSeq(1, 2, 3, 4, 5)
	if !strings.Contains(f.log.String(), "td_replica_grace is below the TD default linger") ||
		!strings.Contains(f.log.String(), `"td_replica_grace":"1s"`) {
		t.Fatalf("startup log %s", f.log.String())
	}

	// E21-D060: deleting the account removes every replica without waiting for grace.
	a1 := f.createNamed(t, "acct-1")
	if a1.ApiId != 1 {
		t.Fatalf("api id %d", a1.ApiId)
	}
	var code int
	var body []byte
	var twLeft, jpLeft, twRow, jpRow bool
	f.holdReconcile(func() {
		f.project(t, a1.ApiId, "td-tw-1")
		f.project(t, a1.ApiId, "td-jp-1")
		code, _, body = f.call(t, http.MethodDelete, fmt.Sprintf("/api/v1/accounts/%d?revision=%d", a1.ApiId, a1.Revision), "")
		twLeft = f.hasEtcdAccount(t, "td-tw-1", a1.ApiId)
		jpLeft = f.hasEtcdAccount(t, "td-jp-1", a1.ApiId)
		twRow = f.hasReplicaRow(t, "td-tw-1", a1.ApiId)
		jpRow = f.hasReplicaRow(t, "td-jp-1", a1.ApiId)
	})
	if code != http.StatusNoContent {
		t.Fatalf("delete %d %s", code, body)
	}
	if twLeft || jpLeft || !twRow || !jpRow {
		t.Fatalf("after delete etcd tw %v jp %v rows tw %v jp %v", twLeft, jpLeft, twRow, jpRow)
	}
	f.srv.reconcileOnce(context.Background())
	if f.hasReplicaRow(t, "td-tw-1", a1.ApiId) || f.hasReplicaRow(t, "td-jp-1", a1.ApiId) {
		t.Fatal("deleted account rows waited for grace")
	}
	if f.hasEtcdAccount(t, "td-tw-1", a1.ApiId) || f.hasEtcdAccount(t, "td-jp-1", a1.ApiId) {
		t.Fatal("deleted account etcd key came back")
	}

	// Exactly grace keeps the replica. A non-terminal spec keeps it past grace.
	// The delete then fails between etcd and the row, and the next pass finishes it (E21-D057).
	a2 := f.createNamed(t, "acct-2")
	if a2.ApiId != 2 {
		t.Fatalf("api id %d", a2.ApiId)
	}
	f.project(t, a2.ApiId, "td-tw-1")
	if created := f.replicaCreated(t, "td-tw-1", a2.ApiId); !created.Equal(start) {
		t.Fatalf("created_at %s want %s", created, start)
	}
	f.holdReconcile(func() { clock.Set(start.Add(grace)) })
	f.srv.reconcileOnce(context.Background())
	f.wantReplica(t, "td-tw-1", a2.ApiId, true)

	specKey := f.putSpec(t, "sess-1", acctSpec("acct-2", a2.ApiId, "td-tw-1"))
	f.holdReconcile(func() { clock.Set(start.Add(grace + time.Millisecond)) })
	f.srv.reconcileOnce(context.Background())
	f.wantReplica(t, "td-tw-1", a2.ApiId, true)

	f.holdReconcile(func() {
		f.srv.hook = func(step string) error {
			if step == "replica-after-etcd-delete" {
				return errors.New("stop after etcd delete")
			}
			return nil
		}
	})
	f.dropSpec(t, specKey)
	f.srv.reconcileOnce(context.Background())
	var etcdGone, rowKept bool
	f.holdReconcile(func() {
		etcdGone = !f.hasEtcdAccount(t, "td-tw-1", a2.ApiId)
		rowKept = f.hasReplicaRow(t, "td-tw-1", a2.ApiId)
		f.srv.hook = nil
	})
	if !etcdGone || !rowKept {
		t.Fatalf("after etcd delete etcd gone %v row kept %v", etcdGone, rowKept)
	}
	f.srv.reconcileOnce(context.Background())
	f.wantReplica(t, "td-tw-1", a2.ApiId, false)

	// The clock is the latest ended_at, not the replica's created_at (E21-D056).
	a3 := f.createNamed(t, "acct-3")
	if a3.ApiId != 3 {
		t.Fatalf("api id %d", a3.ApiId)
	}
	f.project(t, a3.ApiId, "td-tw-1")
	base := f.replicaCreated(t, "td-tw-1", a3.ApiId)
	ended := base.Add(time.Hour)
	f.insertRecord(t, recID(1), "sess-1", recClosed, &ended, acctSpec("acct-3", a3.ApiId, "td-tw-1"))
	f.holdReconcile(func() { clock.Set(ended.Add(grace)) })
	f.srv.reconcileOnce(context.Background())
	f.wantReplica(t, "td-tw-1", a3.ApiId, true)
	f.holdReconcile(func() { clock.Set(ended.Add(grace + time.Millisecond)) })
	f.srv.reconcileOnce(context.Background())
	f.wantReplica(t, "td-tw-1", a3.ApiId, false)

	// A new session during grace reuses the sealbox and cancels the clock (E21-D059).
	a4 := f.createNamed(t, "acct-4")
	if a4.ApiId != 4 {
		t.Fatalf("api id %d", a4.ApiId)
	}
	f.project(t, a4.ApiId, "td-tw-1")
	base = f.replicaCreated(t, "td-tw-1", a4.ApiId)
	f.insertRecord(t, recID(2), "sess-2", recClosed, &base, acctSpec("acct-4", a4.ApiId, "td-tw-1"))
	f.insertRecord(t, recID(3), "sess-3", recCreated, nil, acctSpec("acct-4", a4.ApiId, "td-tw-1"))
	before := f.replicaSealed(t, "td-tw-1", a4.ApiId)
	f.project(t, a4.ApiId, "td-tw-1")
	after := f.replicaSealed(t, "td-tw-1", a4.ApiId)
	if !bytes.Equal(before, after) {
		t.Fatal("grace session resealed the replica")
	}
	if !bytes.Equal(f.stored(t, "td-tw-1", a4.ApiId).GetSealedCredentials(), after) {
		t.Fatal("etcd sealbox changed")
	}
	f.holdReconcile(func() { clock.Set(base.Add(grace + time.Millisecond)) })
	f.srv.reconcileOnce(context.Background())
	f.wantReplica(t, "td-tw-1", a4.ApiId, true)
	liveEnd := clock.Now()
	f.closeRecord(t, recID(3), liveEnd)
	f.holdReconcile(func() { clock.Set(liveEnd.Add(grace + time.Millisecond)) })
	f.srv.reconcileOnce(context.Background())
	f.wantReplica(t, "td-tw-1", a4.ApiId, false)

	// Two instances share one api_id and expire on their own clocks (E21-D061).
	a5 := f.createNamed(t, "acct-5")
	if a5.ApiId != 5 {
		t.Fatalf("api id %d", a5.ApiId)
	}
	f.project(t, a5.ApiId, "td-tw-1")
	f.project(t, a5.ApiId, "td-jp-1")
	twAt := f.replicaCreated(t, "td-tw-1", a5.ApiId)
	jpAt := f.replicaCreated(t, "td-jp-1", a5.ApiId)
	if !twAt.Equal(jpAt) {
		t.Fatalf("replica created_at tw %s jp %s", twAt, jpAt)
	}
	f.insertRecord(t, recID(4), "sess-4", recClosed, &twAt, acctSpec("acct-5", a5.ApiId, "td-tw-1"))
	f.insertRecord(t, recID(5), "sess-5", recCreated, nil, acctSpec("acct-5", a5.ApiId, "td-jp-1"))
	f.holdReconcile(func() { clock.Set(twAt.Add(grace + time.Millisecond)) })
	f.srv.reconcileOnce(context.Background())
	f.wantReplica(t, "td-tw-1", a5.ApiId, false)
	f.wantReplica(t, "td-jp-1", a5.ApiId, true)
	jpEnd := clock.Now()
	f.closeRecord(t, recID(5), jpEnd)
	f.holdReconcile(func() { clock.Set(jpEnd.Add(grace + time.Millisecond)) })
	f.srv.reconcileOnce(context.Background())
	f.wantReplica(t, "td-jp-1", a5.ApiId, false)
}
