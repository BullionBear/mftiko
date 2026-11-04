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

const accountCols = `api_id, kind, name, settings, enabled, exch_account_uid, td_instance, created_at, updated_at, deleted_at, revision`

// accountPublic is one accounts row without credential material (E10-D063).
// Exchange and Region are filled from the registry and the topology, not from the table (E21-D031).
type accountPublic struct {
	ID       int64
	Kind     string
	Name     string
	Cancel   uint32
	Enabled  bool
	UID      string
	Inst     string
	Region   string
	Exchange string
	Created  time.Time
	Updated  time.Time
	Deleted  *time.Time
	Revision int64
}

// accountStored is a row plus the sealbox blob that etcd must carry (E21-D028).
// The blob is not a response field and must not be logged.
type accountStored struct {
	accountPublic
	sealed []byte
}

type accountInsert struct {
	ID          int64
	Kind        string
	Name        string
	Credentials []byte
	KeyVersion  int64
	Cancel      uint32
	UID         string
	Inst        string
	Sealed      []byte
	At          time.Time
}

// accountCipher is a replacement of both ciphertext columns. Nil on an update keeps them.
type accountCipher struct {
	AEAD    []byte
	Version int64
	Sealed  []byte
}

type accountChange struct {
	Name   string
	Cancel uint32
	Creds  *accountCipher
	At     time.Time
}

func (s *Server) decorate(row *accountPublic) {
	if k, ok := s.kindByName(row.Kind); ok {
		row.Exchange = k.Exchange
	}
	if region, err := s.cfg.Topology.RegionOf(row.Inst); err == nil {
		row.Region = region
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
		&p.ID, &p.Kind, &p.Name, &settings, &p.Enabled, &p.UID, &p.Inst,
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

func scanStored(row pgx.Row) (accountStored, error) {
	var (
		st       accountStored
		settings []byte
	)
	err := row.Scan(
		&st.ID, &st.Kind, &st.Name, &settings, &st.Enabled, &st.UID, &st.Inst,
		&st.Created, &st.Updated, &st.Deleted, &st.Revision, &st.sealed,
	)
	if err != nil {
		return accountStored{}, err
	}
	cancel, err := cancelOf(settings)
	if err != nil {
		return accountStored{}, err
	}
	st.Cancel = cancel
	st.sealed = append([]byte(nil), st.sealed...)
	return st, nil
}

// sealAccount encrypts cred for Postgres and seals a copy for the TD instance (E21-D003, E21-D019, E06-D013).
// The plaintext is cleared before return. Neither blob is logged.
func (s *Server) sealAccount(pub sealbox.PublicKey, apiID int64, kind string, cred *apiv1.Credentials) (ct, sealed []byte, ver int64, err error) {
	if cred == nil {
		return nil, nil, 0, errors.New("api: credentials are required")
	}
	pass := ""
	if cred.Passphrase != nil {
		pass = *cred.Passphrase
	}
	plain, err := proto.Marshal(&statev1.AccountCredentials{
		ApiId: apiID, Kind: kind, KeyType: string(cred.KeyType),
		Key: cred.Key, Secret: cred.Secret, Passphrase: pass,
	})
	if err != nil {
		return nil, nil, 0, err
	}
	defer clear(plain)
	ct, err = s.cfg.AccountKeys.Seal(apiID, kind, plain)
	if err != nil {
		return nil, nil, 0, err
	}
	header, err := accountkey.HeaderVersion(ct)
	if err != nil {
		return nil, nil, 0, err
	}
	sealed, err = sealbox.SealCredentials(pub, apiID, kind, sealbox.Credentials{
		KeyType: string(cred.KeyType), Key: cred.Key, Secret: cred.Secret, Passphrase: pass,
	})
	if err != nil {
		clear(ct)
		return nil, nil, 0, err
	}
	if len(ct) < accountkey.MinCiphertext || len(sealed) < minSealedCredentials {
		clear(ct)
		clear(sealed)
		return nil, nil, 0, errUnreadable
	}
	return ct, sealed, int64(header), nil
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

func (s *Server) insertAccount(ctx context.Context, in accountInsert) (accountStored, error) {
	pool, err := s.db()
	if err != nil {
		return accountStored{}, err
	}
	settings, err := settingsJSON(in.Cancel)
	if err != nil {
		return accountStored{}, err
	}
	var rev int64
	var created, updated time.Time
	err = pool.QueryRow(ctx, `
INSERT INTO accounts (
    api_id, kind, name, credentials, key_version, settings, enabled,
    exch_account_uid, td_instance, created_at, updated_at, revision, sealed_credentials
) VALUES (
    $1, $2, $3, $4, $5, $6, true,
    $7, $8, $9, $9, 1, $10
)
RETURNING revision, created_at, updated_at`,
		in.ID, in.Kind, in.Name, in.Credentials, in.KeyVersion, settings,
		in.UID, in.Inst, in.At, in.Sealed,
	).Scan(&rev, &created, &updated)
	if err != nil {
		if name, ok := pgConstraint(err); ok {
			switch name {
			case "accounts_pkey":
				clear(in.Credentials)
				clear(in.Sealed)
				return accountStored{}, errConflict
			case "accounts_name_live":
				clear(in.Credentials)
				clear(in.Sealed)
				return accountStored{}, errNameTaken
			}
		}
		clear(in.Credentials)
		clear(in.Sealed)
		return accountStored{}, storageErr(err)
	}
	return accountStored{
		accountPublic: accountPublic{
			ID: in.ID, Kind: in.Kind, Name: in.Name, Cancel: in.Cancel, Enabled: true,
			UID: in.UID, Inst: in.Inst, Created: created, Updated: updated, Revision: rev,
		},
		sealed: append([]byte(nil), in.Sealed...),
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

func (s *Server) listAccounts(ctx context.Context, include bool, inst string, after int64, limit int) ([]accountPublic, error) {
	pool, err := s.db()
	if err != nil {
		return nil, err
	}
	rows, err := pool.Query(ctx, `
SELECT `+accountCols+`
FROM accounts
WHERE ($1::bool OR deleted_at IS NULL)
  AND ($2::text = '' OR td_instance = $2)
  AND ($3::bigint = 0 OR api_id > $3)
ORDER BY api_id
LIMIT $4`, include, inst, after, limit)
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

// loadAccountProjection reads every row, including soft-deleted ones, with the sealbox blob (E21-D022).
func (s *Server) loadAccountProjection(ctx context.Context) ([]accountStored, error) {
	pool, err := s.db()
	if err != nil {
		return nil, err
	}
	rows, err := pool.Query(ctx, `
SELECT `+accountCols+`, sealed_credentials
FROM accounts
ORDER BY api_id`)
	if err != nil {
		return nil, storageErr(err)
	}
	defer rows.Close()
	var out []accountStored
	for rows.Next() {
		row, err := scanStored(rows)
		if err != nil {
			if errors.Is(err, errUnreadable) {
				return nil, err
			}
			return nil, storageErr(err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, storageErr(err)
	}
	return out, nil
}

// mutateAccount applies next when rev still matches. changed is false when the stored
// account is already in that state: revision stays and the caller must not rewrite etcd (E10-D034, E21-D027).
func (s *Server) mutateAccount(ctx context.Context, id, rev int64, next accountChange) (accountStored, bool, error) {
	pool, err := s.db()
	if err != nil {
		return accountStored{}, false, err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return accountStored{}, false, storageErr(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	cur, err := scanStored(tx.QueryRow(ctx, `
SELECT `+accountCols+`, sealed_credentials
FROM accounts WHERE api_id = $1 FOR UPDATE`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return accountStored{}, false, errNotFound
	}
	if err != nil {
		if errors.Is(err, errUnreadable) {
			return accountStored{}, false, err
		}
		return accountStored{}, false, storageErr(err)
	}
	if cur.Deleted != nil {
		return accountStored{}, false, errNotFound
	}
	if cur.Revision != rev {
		return accountStored{}, false, errConflict
	}
	changed := cur.Name != next.Name || cur.Cancel != next.Cancel || !cur.Enabled || next.Creds != nil
	if !changed {
		if err := tx.Commit(ctx); err != nil {
			return accountStored{}, false, storageErr(err)
		}
		return cur, false, nil
	}
	settings, err := settingsJSON(next.Cancel)
	if err != nil {
		return accountStored{}, false, err
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
    credentials = $3, key_version = $4, sealed_credentials = $5,
    revision = revision + 1, updated_at = $6
WHERE api_id = $7 AND revision = $8 AND deleted_at IS NULL
RETURNING revision`,
			next.Name, settings, next.Creds.AEAD, next.Creds.Version, next.Creds.Sealed,
			next.At, id, rev).Scan(&newRev)
	}
	if err != nil {
		if name, ok := pgConstraint(err); ok && name == "accounts_name_live" {
			return accountStored{}, false, errNameTaken
		}
		if errors.Is(err, pgx.ErrNoRows) {
			return accountStored{}, false, errConflict
		}
		return accountStored{}, false, storageErr(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return accountStored{}, false, storageErr(err)
	}
	cur.Name = next.Name
	cur.Cancel = next.Cancel
	cur.Enabled = true
	cur.Revision = newRev
	cur.Updated = next.At
	if next.Creds != nil {
		cur.sealed = append([]byte(nil), next.Creds.Sealed...)
	}
	return cur, true, nil
}

// softDeleteAccount sets deleted_at when rev still matches a live row (E21-D012, E21-D024).
func (s *Server) softDeleteAccount(ctx context.Context, id, rev int64, at time.Time) (accountStored, error) {
	pool, err := s.db()
	if err != nil {
		return accountStored{}, err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return accountStored{}, storageErr(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	cur, err := scanStored(tx.QueryRow(ctx, `
SELECT `+accountCols+`, sealed_credentials
FROM accounts WHERE api_id = $1 FOR UPDATE`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return accountStored{}, errNotFound
	}
	if err != nil {
		if errors.Is(err, errUnreadable) {
			return accountStored{}, err
		}
		return accountStored{}, storageErr(err)
	}
	if cur.Deleted != nil {
		return accountStored{}, errNotFound
	}
	if cur.Revision != rev {
		return accountStored{}, errConflict
	}
	var newRev int64
	err = tx.QueryRow(ctx, `
UPDATE accounts SET deleted_at = $1, updated_at = $1, revision = revision + 1
WHERE api_id = $2 AND revision = $3 AND deleted_at IS NULL
RETURNING revision`, at, id, rev).Scan(&newRev)
	if errors.Is(err, pgx.ErrNoRows) {
		return accountStored{}, errConflict
	}
	if err != nil {
		return accountStored{}, storageErr(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return accountStored{}, storageErr(err)
	}
	cur.Deleted = &at
	cur.Updated = at
	cur.Revision = newRev
	return cur, nil
}
