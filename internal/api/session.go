package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// endAttempts is how many times one end re-reads and CAS-es the spec (D1037). D1037 says the server
// retries and does not name a count. This is the same bound as one id allocation, not a new limit.
const endAttempts = idAttempts

// CreateSession validates the body, writes session_records, then create-only writes the spec (D1031, D1068).
func (s *Server) CreateSession(w http.ResponseWriter, r *http.Request, params apiv1.CreateSessionParams) {
	key := ""
	if params.IdempotencyKey != nil {
		key = *params.IdempotencyKey
		if !validIdempotencyKey(key) {
			writeFields(w, []apiv1.FieldError{{Path: "Idempotency-Key", Reason: "must be 1 to 255 visible ASCII characters"}})
			return
		}
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, "Bad Request", "invalid request", nil)
		return
	}
	req, fields := decodeCreate(body)
	if len(fields) > 0 {
		writeFields(w, fields)
		return
	}
	spec, fields, err := s.buildSessionSpec(r.Context(), req)
	if len(fields) > 0 {
		writeFields(w, fields)
		return
	}
	if err != nil {
		s.writeSessionProblem(w, err)
		return
	}
	raw, err := proto.Marshal(spec)
	if err != nil {
		s.writeSessionProblem(w, err)
		return
	}
	if key != "" {
		row, err := s.idempotencyRow(r.Context(), key)
		if err != nil {
			s.writeSessionProblem(w, err)
			return
		}
		if row != nil {
			if !specContentEqual(row.Spec, raw) {
				s.writeSessionProblem(w, errIdempotencyConflict)
				return
			}
			// The replay is this session, so its own algos are not an overlap (D1012, D1068).
			rev, stored, err := s.finishReplay(r.Context(), row)
			if err != nil {
				s.writeSessionProblem(w, err)
				return
			}
			s.writeCreated(w, row.STS, row.SessionID, stored, rev, row)
			return
		}
	}
	views, err := s.availableViews(req.StsInstance)
	if err != nil {
		s.writeSessionProblem(w, err)
		return
	}
	if err := algoOverlap(views, spec, ""); err != nil {
		s.writeSessionProblem(w, err)
		return
	}
	id, rev, stored, rec, err := s.allocateSession(r.Context(), req.StsInstance, raw, actorName(r), key)
	if err != nil {
		s.writeSessionProblem(w, err)
		return
	}
	s.writeCreated(w, req.StsInstance, id, stored, rev, rec)
}

func (s *Server) writeCreated(w http.ResponseWriter, sts, id string, spec []byte, rev int64, rec *sessionRecord) {
	sess, err := s.sessionView(sts, id, spec, rev, rec)
	if err != nil {
		s.writeSessionProblem(w, err)
		return
	}
	w.Header().Set("Location", "/api/v1/sessions/"+id)
	writeJSON(w, http.StatusCreated, sess)
}

func actorName(r *http.Request) string {
	st := accessFrom(r.Context())
	if st == nil || st.actor == nil {
		return ""
	}
	return st.actor.name
}

// allocateSession inserts the pending row before the etcd create (D1031). A lost etcd call leaves the row.
// A create that lands and then fails to mark the row still returns 201: the row exists, and reconcile promotes it.
func (s *Server) allocateSession(ctx context.Context, sts string, spec []byte, actor, key string) (string, int64, []byte, *sessionRecord, error) {
	views, err := s.availableViews(sts)
	if err != nil {
		return "", 0, nil, nil, err
	}
	open, err := s.openSessionIDs(ctx)
	if err != nil {
		return "", 0, nil, nil, err
	}
	taken := sessionIDs(views)
	for id := range open {
		taken[id] = struct{}{}
	}
	sha := digestSpec(spec)
	src := s.ids()
	w := s.cl.APIWriter()
	var rev int64
	var stored []byte
	var rec sessionRecord
	id, err := allocate(idAttempts,
		func() (string, error) { return newSessionID(src) },
		func(id string) bool {
			_, ok := taken[id]
			return ok
		},
		func(id string) error {
			row, ierr := s.insertPending(ctx, id, sts, spec, sha, actor, key)
			if ierr != nil {
				rec = row
				return ierr
			}
			rec = row
			if err := s.fireHook("before-etcd"); err != nil {
				return err
			}
			got, cerr := create(ctx, w, state.SessionSpecKey(sts, id), spec)
			if errors.Is(cerr, errConflict) {
				kv, gerr := getKV(ctx, w, state.SessionSpecKey(sts, id))
				if gerr != nil {
					return gerr
				}
				if kv != nil && specContentEqual(kv.Value, spec) {
					if merr := s.markCreated(ctx, &rec, kv.CreateRevision, kv.Value); merr != nil {
						s.logDB(merr, id, sts, "mark session created")
					}
					rev = kv.ModRevision
					stored = append([]byte(nil), kv.Value...)
					return nil
				}
				if derr := s.deletePending(ctx, rec.ID); derr != nil {
					s.logDB(derr, id, sts, "delete pending session")
				}
				taken[id] = struct{}{}
				return errConflict
			}
			if cerr != nil {
				return cerr
			}
			if err := s.fireHook("after-etcd"); err != nil {
				return err
			}
			if merr := s.markCreated(ctx, &rec, got, spec); merr != nil {
				s.logDB(merr, id, sts, "mark session created")
			}
			rev = got
			stored = append([]byte(nil), spec...)
			return nil
		})
	if errors.Is(err, errIdempotentReplay) {
		rev, stored, err = s.finishReplay(ctx, &rec)
		if err != nil {
			return "", 0, nil, nil, err
		}
		return rec.SessionID, rev, stored, &rec, nil
	}
	if err != nil {
		return "", 0, nil, nil, err
	}
	return id, rev, stored, &rec, nil
}

// finishReplay completes the row an Idempotency-Key already names (D1068). A closed row, or a created
// row whose spec is gone, is 409: create has no 404, and the key must not open a second session.
func (s *Server) finishReplay(ctx context.Context, row *sessionRecord) (int64, []byte, error) {
	if row.State == recClosed {
		return 0, nil, errIdempotencyConflict
	}
	if _, err := s.availableViews(row.STS); err != nil {
		return 0, nil, err
	}
	w := s.cl.APIWriter()
	key := state.SessionSpecKey(row.STS, row.SessionID)
	kv, err := getKV(ctx, w, key)
	if err != nil {
		return 0, nil, err
	}
	if kv != nil {
		if !specContentEqual(kv.Value, row.Spec) {
			return 0, nil, errIdempotencyConflict
		}
		if merr := s.markCreated(ctx, row, kv.CreateRevision, kv.Value); merr != nil {
			s.logDB(merr, row.SessionID, row.STS, "mark session created")
		}
		return kv.ModRevision, append([]byte(nil), kv.Value...), nil
	}
	if row.State == recCreated {
		return 0, nil, errIdempotencyConflict
	}
	if err := s.fireHook("before-etcd"); err != nil {
		return 0, nil, err
	}
	rev, err := create(ctx, w, key, row.Spec)
	if errors.Is(err, errConflict) {
		kv, gerr := getKV(ctx, w, key)
		if gerr != nil {
			return 0, nil, gerr
		}
		if kv == nil || !specContentEqual(kv.Value, row.Spec) {
			return 0, nil, errIdempotencyConflict
		}
		if merr := s.markCreated(ctx, row, kv.CreateRevision, kv.Value); merr != nil {
			s.logDB(merr, row.SessionID, row.STS, "mark session created")
		}
		return kv.ModRevision, append([]byte(nil), kv.Value...), nil
	}
	if err != nil {
		return 0, nil, err
	}
	if err := s.fireHook("after-etcd"); err != nil {
		return 0, nil, err
	}
	if merr := s.markCreated(ctx, row, rev, row.Spec); merr != nil {
		s.logDB(merr, row.SessionID, row.STS, "mark session created")
	}
	return rev, append([]byte(nil), row.Spec...), nil
}

func (s *Server) fireHook(step string) error {
	if s.hook == nil {
		return nil
	}
	return s.hook(step)
}

// EndSession CAS-es desired_state to ended, then updates the row (D1031, D1303). The etcd write is the
// success: a later Postgres error is logged and the response is still 200. A spec with no row is the
// same, and reconcile does not insert one (D1072).
func (s *Server) EndSession(w http.ResponseWriter, r *http.Request, id apiv1.SessionID) {
	ctx := r.Context()
	sessionID := string(id)
	rows, err := s.recordsForSession(ctx, sessionID)
	if err != nil && !errors.Is(err, errStorage) {
		s.writeSessionProblem(w, err)
		return
	}
	sts := ""
	if err == nil {
		if row := preferredRow(rows); row != nil {
			sts = row.STS
		}
	}
	if sts == "" {
		found, ferr := s.findSpec(sessionID)
		if ferr != nil {
			s.writeSessionProblem(w, ferr)
			return
		}
		sts = found
	}
	var createRev int64
	var ended []byte
	key := state.SessionSpecKey(sts, sessionID)
	rev, err := update(ctx, s.cl.APIWriter(), key, endAttempts, func(cur *mvccpb.KeyValue) ([]byte, bool, error) {
		if cur == nil {
			return nil, false, errNotFound
		}
		createRev = cur.CreateRevision
		var spec sessionv1.SessionSpec
		if uerr := proto.Unmarshal(cur.Value, &spec); uerr != nil {
			return nil, false, uerr
		}
		if spec.DesiredState == sessionv1.DesiredState_DESIRED_STATE_ENDED {
			ended = append([]byte(nil), cur.Value...)
			return nil, false, nil
		}
		spec.DesiredState = sessionv1.DesiredState_DESIRED_STATE_ENDED
		next, merr := proto.Marshal(&spec)
		if merr != nil {
			return nil, false, merr
		}
		ended = next
		return next, true, nil
	})
	if err != nil {
		s.writeSessionProblem(w, err)
		return
	}
	s.recordEnd(ctx, sessionID, sts, createRev, ended)
	rec, _ := s.rowForView(ctx, sessionID, createRev, ended)
	sess, err := s.sessionView(sts, sessionID, ended, rev, rec)
	if err != nil {
		s.writeSessionProblem(w, err)
		return
	}
	views, unavail := s.regionViews()
	writeJSON(w, http.StatusOK, detailFrom(sess, s.freshness(views, unavail)))
}

func (s *Server) findSpec(id string) (string, error) {
	now := s.now()
	var hits []string
	down := false
	for _, c := range s.clusters {
		view, contact, synced := c.observe()
		if !(regionSnap{synced: synced, contact: contact}).available(now, s.cfg.UnavailableAfter) {
			down = true
			continue
		}
		for key := range view.Entries {
			plane, inst, err := state.Owner(key)
			if err != nil || plane != state.PlaneSTS || key != state.SessionSpecKey(inst, id) {
				continue
			}
			hits = append(hits, inst)
		}
	}
	switch len(hits) {
	case 1:
		return hits[0], nil
	case 0:
		if down {
			return "", fmt.Errorf("%w: session %s", errRegionUnavailable, id)
		}
		return "", errNotFound
	default:
		return "", fmt.Errorf("api: session %s is in more than one STS instance", id)
	}
}

func preferredRow(rows []sessionRecord) *sessionRecord {
	best := -1
	rank := func(state string) int {
		switch state {
		case recCreated:
			return 3
		case recPending:
			return 2
		case recAbandoned:
			return 1
		default:
			return 0
		}
	}
	for i := range rows {
		r := rank(rows[i].State)
		if r == 0 {
			continue
		}
		if best < 0 || r > rank(rows[best].State) || (r == rank(rows[best].State) && rows[i].CreatedAt.After(rows[best].CreatedAt)) {
			best = i
		}
	}
	if best < 0 {
		return nil
	}
	return &rows[best]
}

func (s *Server) recordEnd(ctx context.Context, sessionID, sts string, createRev int64, spec []byte) {
	rows, err := s.recordsForSession(ctx, sessionID)
	if err != nil {
		s.logDB(err, sessionID, sts, "session end could not update session_records")
		return
	}
	row := pairRecord(rows, sessionID, createRev, spec)
	if row == nil {
		s.warnMissing(sessionID, sts)
		return
	}
	if specContentEqual(row.Spec, spec) {
		if err := s.markCreated(ctx, row, createRev, spec); err != nil {
			s.logDB(err, sessionID, sts, "session end could not update session_records")
		}
		return
	}
	s.warnHash(sessionID, sts)
	ended := s.now().UTC()
	if err := s.syncDesired(ctx, *row, desEnded, endReasonSpec, &ended); err != nil {
		s.logDB(err, sessionID, sts, "session end could not update session_records")
	}
}

func pairRecord(rows []sessionRecord, sessionID string, createRev int64, spec []byte) *sessionRecord {
	for i := range rows {
		r := &rows[i]
		if r.SessionID == sessionID && r.CreateRevision != nil && *r.CreateRevision == createRev {
			return r
		}
	}
	best := -1
	for i := range rows {
		r := &rows[i]
		if r.SessionID != sessionID || r.CreateRevision != nil || !specContentEqual(r.Spec, spec) {
			continue
		}
		if best < 0 || adoptBetter(r, &rows[best]) {
			best = i
		}
	}
	if best < 0 {
		return nil
	}
	return &rows[best]
}

func (s *Server) rowForView(ctx context.Context, sessionID string, createRev int64, spec []byte) (*sessionRecord, error) {
	rows, err := s.recordsForSession(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	return pairRecord(rows, sessionID, createRev, spec), nil
}

func getKV(ctx context.Context, w txner, key string) (*mvccpb.KeyValue, error) {
	resp, err := txn(ctx, w, nil, []clientv3.Op{clientv3.OpGet(key)}, nil)
	if err != nil {
		return nil, err
	}
	kv := firstKV(resp, 0)
	if kv == nil {
		return nil, nil
	}
	// The txn response owns these bytes. Clone keeps a copy the caller can use after it returns.
	return proto.Clone(kv).(*mvccpb.KeyValue), nil
}

func (s *Server) writeSessionProblem(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errStorage):
		writeProblem(w, http.StatusServiceUnavailable, apiv1.ErrorCodeStorageUnavailable, "Storage Unavailable", "storage is unavailable", nil)
	case errors.Is(err, errRegistry):
		writeProblem(w, http.StatusServiceUnavailable, apiv1.ErrorCodeRegistryUnavailable, "Registry Unavailable", "registry is unavailable", nil)
	case errors.Is(err, errRegionUnavailable):
		writeProblem(w, http.StatusServiceUnavailable, apiv1.ErrorCodeRegionUnavailable, "Region Unavailable", "region is unavailable", nil)
	case errors.Is(err, errNotFound):
		writeProblem(w, http.StatusNotFound, apiv1.ErrorCodeNotFound, "Not Found", "not found", nil)
	case errors.Is(err, errNotTerminal):
		writeProblem(w, http.StatusConflict, apiv1.ErrorCodeNotTerminal, "Conflict", "session is not terminal", nil)
	case errors.Is(err, errNoRecord):
		writeProblem(w, http.StatusInternalServerError, apiv1.ErrorCodeInternal, "Internal Server Error", "internal error", nil)
	case errors.Is(err, errAlgoOverlap):
		writeProblem(w, http.StatusConflict, apiv1.ErrorCodeAlgoOverlap, "Conflict", "algo_id overlaps another session", nil)
	case errors.Is(err, errIdempotencyConflict), errors.Is(err, errConflict):
		writeProblem(w, http.StatusConflict, apiv1.ErrorCodeConflict, "Conflict", "the request conflicts with existing state", nil)
	default:
		s.log.Error().Err(err).Msg("session")
		writeProblem(w, http.StatusInternalServerError, apiv1.ErrorCodeInternal, "Internal Server Error", "internal error", nil)
	}
}
