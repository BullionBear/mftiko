package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

func TestRetentionKeepsLiveAndFresh(t *testing.T) {
	se := newRetentionEnv(t)
	live := se.mustCreate(t)
	se.putStatus(t, live.Id, &statev1.SessionStatus{
		Phase: statev1.Phase_PHASE_RUNNING, Reason: "up", UpdatedAtNs: se.oldNS(),
	})
	se.s.reconcileOnce(t.Context())
	if se.spec(t, "sts-tw-1", live.Id) == nil {
		t.Fatal("non-terminal session was deleted")
	}
	fresh := se.mustCreate(t)
	se.putStatus(t, fresh.Id, doneStatus(se.clock.Now().UnixNano(), ""))
	se.s.reconcileOnce(t.Context())
	if se.spec(t, "sts-tw-1", fresh.Id) == nil || se.status(t, fresh.Id) == nil {
		t.Fatal("session inside the retention window was deleted")
	}
	if se.row(t, fresh.Id).State == recClosed {
		t.Fatal("fresh session was closed")
	}
}

func TestRetentionExpiresAndReplaysDelete(t *testing.T) {
	se := newRetentionEnv(t)
	sess := se.mustCreate(t)
	before := append([]byte(nil), se.spec(t, "sts-tw-1", sess.Id)...)
	se.s.haltBeforeDelete.Store(true)
	se.putStatus(t, sess.Id, doneStatus(se.oldNS(), dataDig))
	se.s.reconcileOnce(t.Context())
	if se.spec(t, "sts-tw-1", sess.Id) == nil || se.status(t, sess.Id) == nil {
		t.Fatal("delete ran before the row was durable")
	}
	row := se.row(t, sess.Id)
	if row.State != recClosed || row.ClosedAt == nil || row.EndedAt == nil || row.EndReason == nil || *row.EndReason != "ended_by_spec" {
		t.Fatalf("closed row %+v", row)
	}
	if row.StateOut == nil || *row.StateOut != dataDig || !row.EndedAt.Equal(time.Unix(0, se.oldNS()).UTC()) {
		t.Fatalf("terminal columns %+v", row)
	}
	if !bytes.Equal(row.Spec, before) {
		t.Fatal("closing the row replaced the spec")
	}
	mustFinal(t, row, dataDig)
	se.s.haltBeforeDelete.Store(false)
	se.s.reconcileOnce(t.Context())
	if se.spec(t, "sts-tw-1", sess.Id) != nil || se.status(t, sess.Id) != nil {
		t.Fatal("reconcile did not finish the etcd delete")
	}
	// The spec key is what E10-02 waits on: phase 2 runs only once the spec is absent (E10-D016, E10-D019).
	se.s.reconcileOnce(t.Context())
	if se.row(t, sess.Id).State != recClosed {
		t.Fatal("row did not stay closed")
	}

	// STS pushes state_out after the spec is gone (E10-D082). The closed row stays; the status goes.
	// Hold deletes until the mirror has shown the key, or the pass removes it before seen returns.
	se.s.haltBeforeDelete.Store(true)
	se.putStatus(t, sess.Id, doneStatus(se.oldNS(), dataDig))
	se.s.haltBeforeDelete.Store(false)
	se.s.reconcileOnce(t.Context())
	if se.status(t, sess.Id) != nil || se.spec(t, "sts-tw-1", sess.Id) != nil {
		t.Fatal("status without a spec survived retention")
	}
	mustFinal(t, se.row(t, sess.Id), dataDig)

	// A later spec that reuses the id is a different generation (E10-D042, E10-D007).
	se.putSpec(t, "sts-tw-1", sess.Id, &sessionv1.SessionSpec{Name: "again"})
	miss := se.s.missingRecord.Load()
	se.s.haltBeforeDelete.Store(true)
	se.putStatus(t, sess.Id, doneStatus(se.oldNS(), ""))
	se.s.haltBeforeDelete.Store(false)
	se.s.reconcileOnce(t.Context())
	if se.spec(t, "sts-tw-1", sess.Id) == nil {
		t.Fatal("reused id was deleted with the previous generation")
	}
	if se.s.missingRecord.Load() <= miss || !strings.Contains(se.e.log.String(), "terminal session has no record; not deleted") {
		t.Fatalf("missing %d log %s", se.s.missingRecord.Load(), se.e.log.String())
	}
}

func TestRetentionStatusOnlyClosesTheRow(t *testing.T) {
	se := newRetentionEnv(t)
	sess := se.mustCreate(t)
	se.putStatus(t, sess.Id, doneStatus(se.clock.Now().UnixNano(), ""))
	se.dropSpec(t, "sts-tw-1", sess.Id)
	if se.row(t, sess.Id).State == recClosed {
		t.Fatal("dropping the spec closed the row")
	}
	se.s.haltBeforeDelete.Store(true)
	se.putStatus(t, sess.Id, doneStatus(se.oldNS(), dataDig))
	se.s.haltBeforeDelete.Store(false)
	se.s.reconcileOnce(t.Context())
	if se.status(t, sess.Id) != nil {
		t.Fatal("orphan status was not deleted")
	}
	row := se.row(t, sess.Id)
	if row.State != recClosed || row.StateOut == nil || *row.StateOut != dataDig {
		t.Fatalf("status-only close %+v", row)
	}
	mustFinal(t, row, dataDig)
}

func TestRetentionMissingRow(t *testing.T) {
	se := newRetentionEnv(t)
	const id = "abc123"
	se.s.reconcileMu.Lock()
	se.putSpec(t, "sts-tw-1", id, &sessionv1.SessionSpec{Name: "direct"})
	se.putStatus(t, id, doneStatus(se.oldNS(), dataDig))
	miss := se.s.missingRecord.Load()
	se.s.reconcileMu.Unlock()
	se.s.reconcileOnce(t.Context())
	if se.spec(t, "sts-tw-1", id) == nil || se.status(t, id) == nil {
		t.Fatal("session without a record was deleted")
	}
	if se.s.missingRecord.Load() <= miss || !strings.Contains(se.e.log.String(), "terminal session has no record; not deleted") {
		t.Fatalf("missing %d", se.s.missingRecord.Load())
	}
	for _, row := range se.rows(t) {
		if row.SessionID == id {
			t.Fatal("retention inserted a row")
		}
	}
}

func TestRetentionPostgresDown(t *testing.T) {
	se := newRetentionEnv(t)
	sess := se.mustCreate(t)
	se.s.haltBeforeDelete.Store(true)
	se.putStatus(t, sess.Id, doneStatus(se.oldNS(), ""))
	se.s.audit.pool.Close()
	se.s.retain(t.Context())
	if se.spec(t, "sts-tw-1", sess.Id) == nil || se.status(t, sess.Id) == nil {
		t.Fatal("retention deleted while postgres was down")
	}
	code, _, body := se.call(t, http.MethodDelete, "/api/v1/sessions/"+sess.Id, "{}", nil)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("delete %d %s", code, body)
	}
	mustProblem(t, body, http.StatusServiceUnavailable, "storage_unavailable")
	if se.spec(t, "sts-tw-1", sess.Id) == nil {
		t.Fatal("DELETE removed the spec while postgres was down")
	}
}

func TestRetentionTwoReplicas(t *testing.T) {
	se := newRetentionEnv(t)
	other := se.e.replica(t)
	other.catalog = se.cat
	sess := se.mustCreate(t)
	se.s.haltBeforeDelete.Store(true)
	other.haltBeforeDelete.Store(true)
	se.putStatus(t, sess.Id, doneStatus(se.oldNS(), dataDig))
	seen(t, other, "tw", state.SessionStatusKey("sts-tw-1", sess.Id))
	se.s.haltBeforeDelete.Store(false)
	other.haltBeforeDelete.Store(false)

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		se.s.reconcileOnce(t.Context())
	}()
	go func() {
		defer wg.Done()
		<-start
		other.reconcileOnce(t.Context())
	}()
	close(start)
	wg.Wait()
	if se.spec(t, "sts-tw-1", sess.Id) != nil || se.status(t, sess.Id) != nil {
		t.Fatal("concurrent retention left the session in etcd")
	}
	closed := 0
	for _, row := range se.rows(t) {
		if row.SessionID == sess.Id && row.State == recClosed {
			closed++
		}
	}
	if closed != 1 {
		t.Fatalf("closed rows %d", closed)
	}
}

func TestDeleteSession(t *testing.T) {
	se := newRetentionEnv(t)
	code, _, body := se.call(t, http.MethodDelete, "/api/v1/sessions/abc123", "{}", nil)
	if code != http.StatusNotFound {
		t.Fatalf("missing %d %s", code, body)
	}
	mustProblem(t, body, http.StatusNotFound, "not_found")

	open := se.mustCreate(t)
	code, _, body = se.call(t, http.MethodDelete, "/api/v1/sessions/"+open.Id, "{}", nil)
	if code != http.StatusConflict {
		t.Fatalf("no status %d %s", code, body)
	}
	mustProblem(t, body, http.StatusConflict, "not_terminal")
	if se.spec(t, "sts-tw-1", open.Id) == nil {
		t.Fatal("non-terminal delete removed the spec")
	}

	const orphan = "def456"
	se.putSpec(t, "sts-tw-1", orphan, &sessionv1.SessionSpec{Name: "direct"})
	se.putStatus(t, orphan, doneStatus(se.oldNS(), dataDig))
	miss := se.s.missingRecord.Load()
	code, _, body = se.call(t, http.MethodDelete, "/api/v1/sessions/"+orphan, "{}", nil)
	if code != http.StatusInternalServerError {
		t.Fatalf("no record %d %s", code, body)
	}
	mustProblem(t, body, http.StatusInternalServerError, "internal")
	if se.spec(t, "sts-tw-1", orphan) == nil || se.s.missingRecord.Load() <= miss {
		t.Fatalf("orphan deleted or not counted: present %v count %d", se.spec(t, "sts-tw-1", orphan) != nil, se.s.missingRecord.Load())
	}

	term := se.mustCreate(t)
	se.putStatus(t, term.Id, doneStatus(se.clock.Now().UnixNano(), dataDig))
	code, _, body = se.call(t, http.MethodDelete, "/api/v1/sessions/"+term.Id, "{}", nil)
	if code != http.StatusNoContent {
		t.Fatalf("delete %d %s", code, body)
	}
	if len(body) != 0 {
		t.Fatalf("body %s", body)
	}
	if se.spec(t, "sts-tw-1", term.Id) != nil || se.status(t, term.Id) != nil {
		t.Fatal("DELETE left etcd keys")
	}
	row := se.row(t, term.Id)
	if row.State != recClosed || row.StateOut == nil || *row.StateOut != dataDig || row.ClosedAt == nil {
		t.Fatalf("deleted row %+v", row)
	}
	mustFinal(t, row, dataDig)
	code, _, body = se.call(t, http.MethodGet, "/api/v1/session-records/"+row.ID, "", nil)
	if code != http.StatusOK {
		t.Fatalf("record %d %s", code, body)
	}
	var rec apiv1.SessionRecord
	if err := json.Unmarshal(body, &rec); err != nil {
		t.Fatal(err)
	}
	if rec.State != apiv1.SessionRecordStateClosed || rec.Final == nil || rec.Final.Phase != apiv1.PhaseDone ||
		rec.Final.Reason != "ended_by_spec" || rec.Final.StateOut == nil || rec.Final.StateOut.Digest != dataDig {
		t.Fatalf("record final %+v", rec.Final)
	}
	se.waitAudit(t, term.Id)
	code, _, body = se.call(t, http.MethodDelete, "/api/v1/sessions/"+term.Id, "{}", nil)
	if code != http.StatusNoContent {
		t.Fatalf("repeat %d %s", code, body)
	}
}

// TestRecordFinalUndecodable: bytes that are not a status leave final out and keep the record readable.
func TestRecordFinalUndecodable(t *testing.T) {
	se := newRetentionEnv(t)
	sess := se.mustCreate(t)
	se.putStatus(t, sess.Id, doneStatus(se.clock.Now().UnixNano(), ""))
	if code, _, body := se.call(t, http.MethodDelete, "/api/v1/sessions/"+sess.Id, "{}", nil); code != http.StatusNoContent {
		t.Fatalf("delete %d %s", code, body)
	}
	row := se.row(t, sess.Id)
	pool, err := se.s.db()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), `UPDATE session_records SET final_status = '\xff' WHERE id = $1::uuid`, row.ID); err != nil {
		t.Fatal(err)
	}
	code, _, body := se.call(t, http.MethodGet, "/api/v1/session-records/"+row.ID, "", nil)
	if code != http.StatusOK {
		t.Fatalf("record %d %s", code, body)
	}
	var rec apiv1.SessionRecord
	if err := json.Unmarshal(body, &rec); err != nil {
		t.Fatal(err)
	}
	if rec.Final != nil || rec.State != apiv1.SessionRecordStateClosed {
		t.Fatalf("record %+v", rec)
	}
	if !strings.Contains(se.e.log.String(), "session record final status decode failed") {
		t.Fatalf("log %s", se.e.log.String())
	}
}

// mustFinal: the closed row keeps the done status it was closed with (E10-D117).
func mustFinal(t *testing.T, row sessionRecord, digest string) {
	t.Helper()
	st := &statev1.SessionStatus{}
	if len(row.Final) == 0 || proto.Unmarshal(row.Final, st) != nil {
		t.Fatalf("final_status %x", row.Final)
	}
	if st.GetPhase() != statev1.Phase_PHASE_DONE || st.GetReason() != "ended_by_spec" || st.GetStateOut().GetDigest() != digest {
		t.Fatalf("final %v", st)
	}
}

type retentionEnv struct {
	*sessionEnv
	clock *testClock
}

func newRetentionEnv(t *testing.T) *retentionEnv {
	t.Helper()
	clock := &testClock{}
	clock.Set(time.Now().UTC())
	// The clock does not advance with LastContact. A short unavailable threshold would
	// mark every region down for the rest of the test.
	return &retentionEnv{sessionEnv: newSessionEnv(t, sessionOpt{clock: clock, unavailable: 48 * time.Hour}), clock: clock}
}

func (se *retentionEnv) oldNS() int64 {
	return se.clock.Now().Add(-8 * 24 * time.Hour).Truncate(time.Microsecond).UnixNano()
}

func (se *retentionEnv) mustCreate(t *testing.T) apiv1.Session {
	t.Helper()
	code, hdr, body := se.post(t, minBody("sts-tw-1"), nil)
	return mustSession(t, code, hdr, body)
}

func doneStatus(updated int64, digest string) *statev1.SessionStatus {
	st := &statev1.SessionStatus{Phase: statev1.Phase_PHASE_DONE, Reason: "ended_by_spec", UpdatedAtNs: updated}
	if digest != "" {
		st.StateOut = &statev1.StateSnapshot{Digest: digest}
	}
	return st
}

func (se *retentionEnv) putStatus(t *testing.T, id string, st *statev1.SessionStatus) {
	t.Helper()
	raw, err := proto.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	key := state.SessionStatusKey("sts-tw-1", id)
	putKey(t, t.Context(), se.e.servers["tw"], key, string(raw))
	seen(t, se.s, "tw", key)
}

func (se *retentionEnv) status(t *testing.T, id string) []byte {
	t.Helper()
	kv := se.e.get(t, "tw", state.SessionStatusKey("sts-tw-1", id))
	if kv == nil {
		return nil
	}
	return kv.Value
}

func (se *retentionEnv) row(t *testing.T, id string) sessionRecord {
	t.Helper()
	var found *sessionRecord
	for _, row := range se.rows(t) {
		if row.SessionID != id {
			continue
		}
		if found == nil || row.CreatedAt.After(found.CreatedAt) {
			copied := row
			found = &copied
		}
	}
	if found == nil {
		t.Fatalf("no row for %s", id)
	}
	return *found
}

func (se *retentionEnv) waitAudit(t *testing.T, id string) {
	t.Helper()
	path := "/api/v1/sessions/" + id
	until(t, 5*time.Second, "audit session.delete", func() error {
		var n int
		err := se.s.audit.pool.QueryRow(t.Context(), `
SELECT count(*) FROM audit
WHERE method = 'DELETE' AND path = $1 AND status = 204 AND session_id = $2`, path, id).Scan(&n)
		if err != nil {
			return err
		}
		if n == 0 {
			return errors.New("waiting")
		}
		return nil
	})
}
