package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/sealbox"
)

// replicaRow is one account_replicas row (E21-D048). Sealed is the sealbox for that
// TD instance. It is not a response field and must not be logged.
type replicaRow struct {
	Inst    string
	Sealed  []byte
	Created time.Time
	Updated time.Time
}

// replicaView is a Postgres account plus one replica, the unit reconcile projects.
// replicaCreated is account_replicas.created_at, the clock when no session has ended (E21-D056).
type replicaView struct {
	accountPublic
	Inst           string
	replicaCreated time.Time
	sealed         []byte
}

// rejectTDInstance reports a request that still carries td_instance (E21-D052).
// A present key is rejected even when the value is null or empty. Decode errors
// are left to the caller. The check does not look inside nested strings.
func rejectTDInstance(body []byte) []apiv1.FieldError {
	if len(body) == 0 {
		return nil
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil
	}
	if _, ok := raw["td_instance"]; ok {
		return []apiv1.FieldError{{Path: "/td_instance", Reason: "is not accepted"}}
	}
	return nil
}

func requestCredentials(cred *apiv1.Credentials) sealbox.Credentials {
	pass := ""
	if cred != nil && cred.Passphrase != nil {
		pass = *cred.Passphrase
	}
	out := sealbox.Credentials{}
	if cred != nil {
		out.KeyType = string(cred.KeyType)
		out.Key = cred.Key
		out.Secret = cred.Secret
		out.Passphrase = pass
	}
	return out
}

func (s *Server) listReplicas(ctx context.Context, id int64) ([]replicaRow, error) {
	pool, err := s.db()
	if err != nil {
		return nil, err
	}
	rows, err := pool.Query(ctx, `
SELECT td_instance, sealed_credentials, created_at, updated_at
FROM account_replicas
WHERE api_id = $1
ORDER BY td_instance`, id)
	if err != nil {
		return nil, storageErr(err)
	}
	defer rows.Close()
	return collectReplicas(rows)
}

func collectReplicas(rows pgx.Rows) ([]replicaRow, error) {
	var out []replicaRow
	for rows.Next() {
		var row replicaRow
		if err := rows.Scan(&row.Inst, &row.Sealed, &row.Created, &row.Updated); err != nil {
			return nil, storageErr(err)
		}
		row.Sealed = append([]byte(nil), row.Sealed...)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, storageErr(err)
	}
	return out, nil
}

// replicasReachable is E21-D029 for every replica. No replicas is success:
// an account that has never been placed does not depend on a TD region.
func (s *Server) replicasReachable(reps []replicaRow) error {
	for _, rep := range reps {
		region, err := s.cfg.Topology.RegionOf(rep.Inst)
		if err != nil || !s.regionAvailable(region) {
			return fmt.Errorf("%w: %s", errRegionUnavailable, rep.Inst)
		}
	}
	return nil
}

// loadReplicaViews reads every replica, including those of a soft-deleted account (E21-D022).
func (s *Server) loadReplicaViews(ctx context.Context) ([]replicaView, error) {
	pool, err := s.db()
	if err != nil {
		return nil, err
	}
	rows, err := pool.Query(ctx, `
SELECT a.api_id, a.kind, a.name, a.settings, a.enabled, a.exch_account_uid,
       a.created_at, a.updated_at, a.deleted_at, a.revision,
       r.td_instance, r.sealed_credentials, r.created_at
FROM account_replicas r
JOIN accounts a ON a.api_id = r.api_id
ORDER BY r.td_instance, r.api_id`)
	if err != nil {
		return nil, storageErr(err)
	}
	defer rows.Close()
	var out []replicaView
	for rows.Next() {
		var (
			view     replicaView
			settings []byte
		)
		if err := rows.Scan(
			&view.ID, &view.Kind, &view.Name, &settings, &view.Enabled, &view.UID,
			&view.Created, &view.Updated, &view.Deleted, &view.Revision,
			&view.Inst, &view.sealed, &view.replicaCreated,
		); err != nil {
			return nil, storageErr(err)
		}
		cancel, err := cancelOf(settings)
		if err != nil {
			return nil, err
		}
		view.Cancel = cancel
		view.sealed = append([]byte(nil), view.sealed...)
		s.decorate(&view.accountPublic)
		out = append(out, view)
	}
	if err := rows.Err(); err != nil {
		return nil, storageErr(err)
	}
	return out, nil
}

// ensureSessionReplicas seals each spec account onto its td_instance and projects it
// before the caller writes the session spec (E21-D005, E21-D051). A missing public
// key or a down region returns before any replica or spec write. An existing sealbox
// that already matches the adopted key is reused, so the worker is not restarted.
func (s *Server) ensureSessionReplicas(ctx context.Context, spec *sessionv1.SessionSpec) error {
	accts := spec.GetAccounts()
	if len(accts) == 0 {
		return nil
	}
	type item struct {
		inst string
		id   int64
		pub  sealbox.PublicKey
	}
	items := make([]item, 0, len(accts))
	for _, a := range accts {
		inst := a.GetTdInstance()
		region, err := s.cfg.Topology.RegionOf(inst)
		if err != nil || !s.regionAvailable(region) {
			return fmt.Errorf("%w: %s", errRegionUnavailable, inst)
		}
		pub, err := s.pubkeyFor(ctx, inst)
		if err != nil {
			return err
		}
		items = append(items, item{inst: inst, id: a.GetApiId(), pub: pub})
	}
	planned := make([]replicaView, 0, len(items))
	for _, it := range items {
		view, err := s.syncReplica(ctx, it.inst, it.id, it.pub)
		if err != nil {
			return err
		}
		planned = append(planned, view)
	}
	if err := s.fireHook("account-before-etcd"); err != nil {
		return err
	}
	for _, view := range planned {
		val, err := projectionBytes(view.accountPublic, view.sealed)
		if err != nil {
			return err
		}
		if err := s.putProjection(ctx, view.Inst, view.ID, val); err != nil {
			return err
		}
	}
	return nil
}

// syncReplica locks the account, reuses a matching sealbox, or seals the stored
// AEAD and upserts the replica. Reuse sets updated_at to now in that same
// transaction (E21-D063). It does not write etcd and does not change the
// account row.
func (s *Server) syncReplica(ctx context.Context, inst string, id int64, pub sealbox.PublicKey) (replicaView, error) {
	pool, err := s.db()
	if err != nil {
		return replicaView{}, err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return replicaView{}, storageErr(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	row, cred, err := s.lockAccountCipher(ctx, tx, id)
	if err != nil {
		return replicaView{}, err
	}
	defer clear(cred)
	var cur []byte
	err = tx.QueryRow(ctx, `
SELECT sealed_credentials FROM account_replicas
WHERE td_instance = $1 AND api_id = $2
FOR UPDATE`, inst, id).Scan(&cur)
	missing := errors.Is(err, pgx.ErrNoRows)
	if err != nil && !missing {
		return replicaView{}, storageErr(err)
	}
	if !missing && sealbox.SealedTo(pub, cur) {
		// The account row is already locked. Touch updated_at before commit so a
		// concurrent expiry, which takes the same lock, sees the reuse (E21-D063).
		if _, err := tx.Exec(ctx, `
UPDATE account_replicas SET updated_at = $3
WHERE td_instance = $1 AND api_id = $2`, inst, id, s.now().UTC()); err != nil {
			return replicaView{}, storageErr(err)
		}
		if err := tx.Commit(ctx); err != nil {
			return replicaView{}, storageErr(err)
		}
		return replicaView{accountPublic: row, Inst: inst, sealed: append([]byte(nil), cur...)}, nil
	}
	sealed, err := s.sealFromStored(pub, id, row.Kind, cred)
	if err != nil {
		s.log.Error().Int64("api_id", id).Str("td_instance", inst).Msg("account was not sealed")
		return replicaView{}, err
	}
	now := s.now().UTC()
	if missing {
		_, err = tx.Exec(ctx, `
INSERT INTO account_replicas (td_instance, api_id, sealed_credentials, created_at, updated_at)
VALUES ($1, $2, $3, $4, $4)`, inst, id, sealed, now)
	} else {
		_, err = tx.Exec(ctx, `
UPDATE account_replicas
SET sealed_credentials = $1, updated_at = $2
WHERE td_instance = $3 AND api_id = $4 AND sealed_credentials = $5`,
			sealed, now, inst, id, cur)
	}
	if err != nil {
		clear(sealed)
		return replicaView{}, storageErr(err)
	}
	if err := tx.Commit(ctx); err != nil {
		clear(sealed)
		return replicaView{}, storageErr(err)
	}
	return replicaView{accountPublic: row, Inst: inst, sealed: sealed}, nil
}

func (s *Server) lockAccountCipher(ctx context.Context, tx pgx.Tx, id int64) (accountPublic, []byte, error) {
	var (
		row      accountPublic
		settings []byte
		cred     []byte
	)
	err := tx.QueryRow(ctx, `
SELECT api_id, kind, name, settings, enabled, exch_account_uid, created_at, updated_at, deleted_at, revision, credentials
FROM accounts WHERE api_id = $1 FOR UPDATE`, id).Scan(
		&row.ID, &row.Kind, &row.Name, &settings, &row.Enabled, &row.UID,
		&row.Created, &row.Updated, &row.Deleted, &row.Revision, &cred,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return accountPublic{}, nil, errNotFound
	}
	if err != nil {
		return accountPublic{}, nil, storageErr(err)
	}
	if row.Deleted != nil {
		clear(cred)
		return accountPublic{}, nil, errNotFound
	}
	cancel, err := cancelOf(settings)
	if err != nil {
		clear(cred)
		return accountPublic{}, nil, err
	}
	row.Cancel = cancel
	s.decorate(&row)
	return row, cred, nil
}

func (s *Server) sealFromStored(pub sealbox.PublicKey, id int64, kind string, cred []byte) ([]byte, error) {
	plain, err := s.cfg.AccountKeys.Open(id, kind, cred)
	if err != nil {
		return nil, errUnreadable
	}
	defer clear(plain)
	var msg statev1.AccountCredentials
	if err := proto.Unmarshal(plain, &msg); err != nil {
		return nil, errUnreadable
	}
	sealed, err := sealbox.SealCredentials(pub, id, kind, sealbox.Credentials{
		KeyType: msg.KeyType, Key: msg.Key, Secret: msg.Secret, Passphrase: msg.Passphrase,
	})
	if err != nil {
		return nil, err
	}
	if len(sealed) < minSealedCredentials {
		clear(sealed)
		return nil, errUnreadable
	}
	return sealed, nil
}

// publishReplicas writes every replica after Postgres has committed (E21-D022).
// deleting removes the etcd keys and leaves the replica rows. Reconcile deletes
// those rows after the etcd delete succeeds, without waiting for grace (E21-D060).
// The hook "account-before-etcd" runs once before the first etcd call.
func (s *Server) publishReplicas(ctx context.Context, row accountPublic, reps []replicaRow, deleting bool) error {
	if len(reps) == 0 {
		return nil
	}
	if err := s.fireHook("account-before-etcd"); err != nil {
		return err
	}
	for _, rep := range reps {
		if deleting || row.Deleted != nil {
			if err := s.deleteProjection(ctx, rep.Inst, row.ID); err != nil {
				return err
			}
			continue
		}
		val, err := projectionBytes(row, rep.Sealed)
		if err != nil {
			return err
		}
		if err := s.putProjection(ctx, rep.Inst, row.ID, val); err != nil {
			return err
		}
	}
	return nil
}

// loadReplicaSealed reads one live replica and its account for a projection (E21-D041).
func (s *Server) loadReplicaSealed(ctx context.Context, id int64, inst string) (accountPublic, []byte, error) {
	pool, err := s.db()
	if err != nil {
		return accountPublic{}, nil, err
	}
	var (
		row      accountPublic
		settings []byte
		sealed   []byte
	)
	err = pool.QueryRow(ctx, `
SELECT a.api_id, a.kind, a.name, a.settings, a.enabled, a.exch_account_uid,
       a.created_at, a.updated_at, a.deleted_at, a.revision, r.sealed_credentials
FROM accounts a
JOIN account_replicas r ON r.api_id = a.api_id
WHERE a.api_id = $1 AND r.td_instance = $2`, id, inst).Scan(
		&row.ID, &row.Kind, &row.Name, &settings, &row.Enabled, &row.UID,
		&row.Created, &row.Updated, &row.Deleted, &row.Revision, &sealed,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return accountPublic{}, nil, errNotFound
	}
	if err != nil {
		return accountPublic{}, nil, storageErr(err)
	}
	cancel, err := cancelOf(settings)
	if err != nil {
		return accountPublic{}, nil, err
	}
	row.Cancel = cancel
	s.decorate(&row)
	return row, append([]byte(nil), sealed...), nil
}
