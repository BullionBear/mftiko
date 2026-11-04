package api

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/accountkey"
	"github.com/BullionBear/mftiko/internal/platform/sealbox"
)

// minSealedCredentials is the shortest sealbox: 9-byte header plus NaCl's anonymous overhead.
// migration 0009 checks the same length (E21-D028).
const minSealedCredentials = 57

const accountCols = `api_id, kind, name, settings, enabled, exch_account_uid, created_at, updated_at, deleted_at, revision`

// accountPublic is one accounts row without credential material (E10-D063).
// Exchange is filled from the registry, not from the table (E21-D031).
// The row has no td_instance and no region (E21-D052, E21-D054).
type accountPublic struct {
	ID       int64
	Kind     string
	Name     string
	Cancel   uint32
	Enabled  bool
	UID      string
	Exchange string
	Created  time.Time
	Updated  time.Time
	Deleted  *time.Time
	Revision int64
}

type accountInsert struct {
	ID          int64
	Kind        string
	Name        string
	Credentials []byte
	KeyVersion  int64
	Cancel      uint32
	UID         string
	At          time.Time
}

// accountCipher is a replacement of the AEAD column. Nil on an update keeps it.
// The sealbox lives on account_replicas and is rewritten there (E21-D048).
type accountCipher struct {
	AEAD    []byte
	Version int64
}

// accountChange is one committed update. Plain is set only when credentials change,
// so the transaction can reseal every replica it locks (E21-D047).
type accountChange struct {
	Name   string
	Cancel uint32
	Creds  *accountCipher
	Plain  *apiv1.Credentials
	At     time.Time
}

func (s *Server) decorate(row *accountPublic) {
	if k, ok := s.kindByName(row.Kind); ok {
		row.Exchange = k.Exchange
	}
}

func settingsJSON(cancel uint32) ([]byte, error) {
	return json.Marshal(struct {
		Cancel uint32 `json:"cancel_on_disconnect_s"`
	}{Cancel: cancel})
}

func cancelOf(b []byte) (uint32, error) {
	var doc struct {
		Cancel uint32 `json:"cancel_on_disconnect_s"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		return 0, errUnreadable
	}
	return doc.Cancel, nil
}

func scanPublic(row pgx.Row) (accountPublic, error) {
	var (
		p        accountPublic
		settings []byte
	)
	err := row.Scan(
		&p.ID, &p.Kind, &p.Name, &settings, &p.Enabled, &p.UID,
		&p.Created, &p.Updated, &p.Deleted, &p.Revision,
	)
	if err != nil {
		return accountPublic{}, err
	}
	cancel, err := cancelOf(settings)
	if err != nil {
		return accountPublic{}, err
	}
	p.Cancel = cancel
	return p, nil
}

// sealAEAD encrypts cred for Postgres (E21-D003, E21-D019). The plaintext is cleared
// before return. The blob is not logged. The sealbox is written later, per replica.
func (s *Server) sealAEAD(apiID int64, kind string, cred *apiv1.Credentials) (ct []byte, ver int64, err error) {
	if cred == nil {
		return nil, 0, errors.New("api: credentials are required")
	}
	box := requestCredentials(cred)
	plain, err := proto.Marshal(&statev1.AccountCredentials{
		ApiId: apiID, Kind: kind, KeyType: box.KeyType,
		Key: box.Key, Secret: box.Secret, Passphrase: box.Passphrase,
	})
	if err != nil {
		return nil, 0, err
	}
	defer clear(plain)
	ct, err = s.cfg.AccountKeys.Seal(apiID, kind, plain)
	if err != nil {
		return nil, 0, err
	}
	header, err := accountkey.HeaderVersion(ct)
	if err != nil {
		clear(ct)
		return nil, 0, err
	}
	if len(ct) < accountkey.MinCiphertext {
		clear(ct)
		return nil, 0, errUnreadable
	}
	return ct, int64(header), nil
}

func (s *Server) liveNameTaken(ctx context.Context, name string, except int64) (bool, error) {
	pool, err := s.db()
	if err != nil {
		return false, err
	}
	var taken bool
	err = pool.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM accounts
    WHERE name = $1 AND deleted_at IS NULL AND api_id <> $2
)`, name, except).Scan(&taken)
	if err != nil {
		return false, storageErr(err)
	}
	return taken, nil
}

func (s *Server) insertAccount(ctx context.Context, in accountInsert) (accountPublic, error) {
	pool, err := s.db()
	if err != nil {
		return accountPublic{}, err
	}
	settings, err := settingsJSON(in.Cancel)
	if err != nil {
		return accountPublic{}, err
	}
	var rev int64
	var created, updated time.Time
	err = pool.QueryRow(ctx, `
INSERT INTO accounts (
    api_id, kind, name, credentials, key_version, settings, enabled,
    exch_account_uid, created_at, updated_at, revision
) VALUES (
    $1, $2, $3, $4, $5, $6, true,
    $7, $8, $8, 1
)
RETURNING revision, created_at, updated_at`,
		in.ID, in.Kind, in.Name, in.Credentials, in.KeyVersion, settings,
		in.UID, in.At,
	).Scan(&rev, &created, &updated)
	if err != nil {
		if name, ok := pgConstraint(err); ok {
			switch name {
			case "accounts_pkey":
				clear(in.Credentials)
				return accountPublic{}, errConflict
			case "accounts_name_live":
				clear(in.Credentials)
				return accountPublic{}, errNameTaken
			}
		}
		clear(in.Credentials)
		return accountPublic{}, storageErr(err)
	}
	return accountPublic{
		ID: in.ID, Kind: in.Kind, Name: in.Name, Cancel: in.Cancel, Enabled: true,
		UID: in.UID, Created: created, Updated: updated, Revision: rev,
	}, nil
}

func (s *Server) getAccount(ctx context.Context, id int64) (accountPublic, error) {
	pool, err := s.db()
	if err != nil {
		return accountPublic{}, err
	}
	row, err := scanPublic(pool.QueryRow(ctx, `SELECT `+accountCols+` FROM accounts WHERE api_id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return accountPublic{}, errNotFound
	}
	if err != nil {
		if errors.Is(err, errUnreadable) {
			return accountPublic{}, err
		}
		return accountPublic{}, storageErr(err)
	}
	s.decorate(&row)
	return row, nil
}

func (s *Server) listAccounts(ctx context.Context, include bool, after int64, limit int) ([]accountPublic, error) {
	pool, err := s.db()
	if err != nil {
		return nil, err
	}
	rows, err := pool.Query(ctx, `
SELECT `+accountCols+`
FROM accounts
WHERE ($1::bool OR deleted_at IS NULL)
  AND ($2::bigint = 0 OR api_id > $2)
ORDER BY api_id
LIMIT $3`, include, after, limit)
	if err != nil {
		return nil, storageErr(err)
	}
	defer rows.Close()
	var out []accountPublic
	for rows.Next() {
		row, err := scanPublic(rows)
		if err != nil {
			if errors.Is(err, errUnreadable) {
				return nil, err
			}
			return nil, storageErr(err)
		}
		s.decorate(&row)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, storageErr(err)
	}
	return out, nil
}

// mutateAccount applies next when rev still matches. changed is false when the stored
// account is already in that state: revision stays and the caller must not rewrite etcd (E10-D034, E21-D027).
// A credential change reseals every replica locked in this transaction (E21-D047).
func (s *Server) mutateAccount(ctx context.Context, id, rev int64, next accountChange) (accountPublic, []replicaRow, bool, error) {
	pool, err := s.db()
	if err != nil {
		return accountPublic{}, nil, false, err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return accountPublic{}, nil, false, storageErr(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	cur, err := scanPublic(tx.QueryRow(ctx, `
SELECT `+accountCols+`
FROM accounts WHERE api_id = $1 FOR UPDATE`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return accountPublic{}, nil, false, errNotFound
	}
	if err != nil {
		if errors.Is(err, errUnreadable) {
			return accountPublic{}, nil, false, err
		}
		return accountPublic{}, nil, false, storageErr(err)
	}
	if cur.Deleted != nil {
		return accountPublic{}, nil, false, errNotFound
	}
	if cur.Revision != rev {
		return accountPublic{}, nil, false, errConflict
	}
	rrows, err := tx.Query(ctx, `
SELECT td_instance, sealed_credentials, created_at, updated_at
FROM account_replicas
WHERE api_id = $1
ORDER BY td_instance
FOR UPDATE`, id)
	if err != nil {
		return accountPublic{}, nil, false, storageErr(err)
	}
	reps, err := collectReplicas(rrows)
	rrows.Close()
	if err != nil {
		return accountPublic{}, nil, false, err
	}
	changed := cur.Name != next.Name || cur.Cancel != next.Cancel || !cur.Enabled || next.Creds != nil
	if !changed {
		if err := tx.Commit(ctx); err != nil {
			return accountPublic{}, nil, false, storageErr(err)
		}
		return cur, reps, false, nil
	}
	if err := s.replicasReachable(reps); err != nil {
		return accountPublic{}, nil, false, err
	}
	if next.Plain != nil {
		box := requestCredentials(next.Plain)
		for i := range reps {
			pub, perr := s.pubkeyFor(ctx, reps[i].Inst)
			if perr != nil {
				return accountPublic{}, nil, false, perr
			}
			sealed, serr := sealbox.SealCredentials(pub, id, cur.Kind, box)
			if serr != nil {
				return accountPublic{}, nil, false, serr
			}
			if len(sealed) < minSealedCredentials {
				clear(sealed)
				return accountPublic{}, nil, false, errUnreadable
			}
			if _, err := tx.Exec(ctx, `
UPDATE account_replicas
SET sealed_credentials = $1, updated_at = $2
WHERE td_instance = $3 AND api_id = $4`, sealed, next.At, reps[i].Inst, id); err != nil {
				clear(sealed)
				return accountPublic{}, nil, false, storageErr(err)
			}
			reps[i].Sealed = sealed
			reps[i].Updated = next.At
		}
	}
	settings, err := settingsJSON(next.Cancel)
	if err != nil {
		return accountPublic{}, nil, false, err
	}
	var newRev int64
	if next.Creds == nil {
		err = tx.QueryRow(ctx, `
UPDATE accounts SET
    name = $1, settings = $2, enabled = true, revision = revision + 1, updated_at = $3
WHERE api_id = $4 AND revision = $5 AND deleted_at IS NULL
RETURNING revision`, next.Name, settings, next.At, id, rev).Scan(&newRev)
	} else {
		err = tx.QueryRow(ctx, `
UPDATE accounts SET
    name = $1, settings = $2, enabled = true,
    credentials = $3, key_version = $4,
    revision = revision + 1, updated_at = $5
WHERE api_id = $6 AND revision = $7 AND deleted_at IS NULL
RETURNING revision`,
			next.Name, settings, next.Creds.AEAD, next.Creds.Version,
			next.At, id, rev).Scan(&newRev)
	}
	if err != nil {
		if name, ok := pgConstraint(err); ok && name == "accounts_name_live" {
			return accountPublic{}, nil, false, errNameTaken
		}
		if errors.Is(err, pgx.ErrNoRows) {
			return accountPublic{}, nil, false, errConflict
		}
		return accountPublic{}, nil, false, storageErr(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return accountPublic{}, nil, false, storageErr(err)
	}
	cur.Name = next.Name
	cur.Cancel = next.Cancel
	cur.Enabled = true
	cur.Revision = newRev
	cur.Updated = next.At
	return cur, reps, true, nil
}

// softDeleteAccount sets deleted_at when rev still matches a live row (E21-D012, E21-D024).
// Replica rows stay until the etcd delete succeeds. Reconcile then removes them without
// waiting for td_replica_grace (E21-D060). A failed etcd delete leaves the row for the next pass.
func (s *Server) softDeleteAccount(ctx context.Context, id, rev int64, at time.Time) (accountPublic, error) {
	pool, err := s.db()
	if err != nil {
		return accountPublic{}, err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return accountPublic{}, storageErr(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	cur, err := scanPublic(tx.QueryRow(ctx, `
SELECT `+accountCols+`
FROM accounts WHERE api_id = $1 FOR UPDATE`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return accountPublic{}, errNotFound
	}
	if err != nil {
		if errors.Is(err, errUnreadable) {
			return accountPublic{}, err
		}
		return accountPublic{}, storageErr(err)
	}
	if cur.Deleted != nil {
		return accountPublic{}, errNotFound
	}
	if cur.Revision != rev {
		return accountPublic{}, errConflict
	}
	var newRev int64
	err = tx.QueryRow(ctx, `
UPDATE accounts SET deleted_at = $1, updated_at = $1, revision = revision + 1
WHERE api_id = $2 AND revision = $3 AND deleted_at IS NULL
RETURNING revision`, at, id, rev).Scan(&newRev)
	if errors.Is(err, pgx.ErrNoRows) {
		return accountPublic{}, errConflict
	}
	if err != nil {
		return accountPublic{}, storageErr(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return accountPublic{}, storageErr(err)
	}
	cur.Deleted = &at
	cur.Updated = at
	cur.Revision = newRev
	return cur, nil
}
