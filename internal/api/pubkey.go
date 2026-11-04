package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"slices"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/sealbox"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// A TD public key in the process cache. unknown is "not looked up yet" and is the only
// state that causes a direct etcd read (E21-D037). missing falls back to td_keys.
// bad is an unreadable value and does not fall back (E21-D040).
const (
	pubUnknown = iota
	pubMissing
	pubBad
	pubOK
)

// pubkeySlot is one TD instance's cached etcd value. recorded is the fingerprint this
// process has already written, so a later watch of the same key does not take the SQL lock.
type pubkeySlot struct {
	state    int
	key      sealbox.PublicKey
	raw      string
	recorded []byte
	warned   bool
	warnEtcd [sha256.Size]byte
	warnCfg  [sha256.Size]byte
}

// pubkeyUnavailable is a seal with no usable public key (E21-D039).
type pubkeyUnavailable struct{ inst string }

func (e *pubkeyUnavailable) Error() string { return e.inst + " has no public key" }

func (s *Server) slotLocked(inst string) *pubkeySlot {
	if s.pubs == nil {
		s.pubs = map[string]*pubkeySlot{}
	}
	sl := s.pubs[inst]
	if sl == nil {
		sl = &pubkeySlot{}
		s.pubs[inst] = sl
	}
	return sl
}

// adopted is the key this process would seal to, from the cache only. unknown, bad, and a
// missing key with no td_keys entry are not adopted.
func (s *Server) adopted(inst string) (sealbox.PublicKey, bool) {
	s.pubMu.Lock()
	defer s.pubMu.Unlock()
	return s.adoptedLocked(inst)
}

func (s *Server) adoptedLocked(inst string) (sealbox.PublicKey, bool) {
	sl := s.pubs[inst]
	if sl == nil {
		return sealbox.PublicKey{}, false
	}
	switch sl.state {
	case pubOK:
		return sl.key, true
	case pubMissing:
		if k, ok := s.cfg.TDKeys[inst]; ok {
			return k, true
		}
	}
	return sealbox.PublicKey{}, false
}

func (s *Server) adoptedIs(inst string, pub sealbox.PublicKey) bool {
	got, ok := s.adoptedLocked(inst)
	return ok && got == pub
}

// observeValueLocked caches raw. A value that does not parse is pubBad and is not a key.
// The raw bytes are not logged.
func (s *Server) observeValueLocked(inst string, raw []byte) (sealbox.PublicKey, bool) {
	sl := s.slotLocked(inst)
	text := string(raw)
	pub, err := sealbox.ParsePublicKey(text)
	if err != nil {
		if sl.state != pubBad || sl.raw != text {
			s.log.Error().Str("td_instance", inst).Int("len", len(raw)).Msg("td pubkey is unreadable")
		}
		sl.state = pubBad
		sl.key = sealbox.PublicKey{}
		sl.raw = text
		return sealbox.PublicKey{}, false
	}
	same := sl.state == pubOK && sl.key == pub && sl.raw == text
	sl.state = pubOK
	sl.key = pub
	sl.raw = text
	if !same {
		s.noteMismatchLocked(inst, pub)
	}
	return pub, true
}

func (s *Server) observeAbsentLocked(inst string) (sealbox.PublicKey, bool) {
	sl := s.slotLocked(inst)
	sl.state = pubMissing
	sl.key = sealbox.PublicKey{}
	sl.raw = ""
	if k, ok := s.cfg.TDKeys[inst]; ok {
		return k, true
	}
	return sealbox.PublicKey{}, false
}

// noteMismatchLocked warns once per (etcd fingerprint, td_keys fingerprint). etcd still wins (E21-D035).
func (s *Server) noteMismatchLocked(inst string, etcd sealbox.PublicKey) {
	cfg, ok := s.cfg.TDKeys[inst]
	if !ok || cfg == etcd {
		return
	}
	efp := etcd.SHA256()
	cfp := cfg.SHA256()
	sl := s.slotLocked(inst)
	if sl.warned && sl.warnEtcd == efp && sl.warnCfg == cfp {
		return
	}
	sl.warned = true
	sl.warnEtcd = efp
	sl.warnCfg = cfp
	s.log.Warn().Str("td_instance", inst).
		Str("etcd_fingerprint", hex.EncodeToString(efp[:])).
		Str("td_keys_fingerprint", hex.EncodeToString(cfp[:])).
		Msg("td pubkey differs from td_keys; using etcd")
}

// pubkeyFor resolves the key create and update seal to. A known cache entry does not dial.
// An adopted key is recorded before it is returned, so a failed insert is retried on the next seal.
func (s *Server) pubkeyFor(ctx context.Context, inst string) (sealbox.PublicKey, error) {
	s.pubMu.Lock()
	sl := s.pubs[inst]
	known := sl != nil && sl.state != pubUnknown
	var pub sealbox.PublicKey
	var adopted bool
	if known {
		pub, adopted = s.adoptedLocked(inst)
	}
	s.pubMu.Unlock()
	if !known {
		raw, present, err := s.readPubkey(ctx, inst)
		if err != nil {
			return sealbox.PublicKey{}, err
		}
		s.pubMu.Lock()
		sl = s.slotLocked(inst)
		if sl.state == pubUnknown {
			if present {
				pub, adopted = s.observeValueLocked(inst, raw)
			} else {
				pub, adopted = s.observeAbsentLocked(inst)
			}
		} else {
			pub, adopted = s.adoptedLocked(inst)
		}
		s.pubMu.Unlock()
	}
	if !adopted {
		return sealbox.PublicKey{}, &pubkeyUnavailable{inst: inst}
	}
	if err := s.recordFingerprint(ctx, inst, pub); err != nil {
		return sealbox.PublicKey{}, err
	}
	return pub, nil
}

func (s *Server) readPubkey(ctx context.Context, inst string) ([]byte, bool, error) {
	region, err := s.cfg.Topology.RegionOf(inst)
	if err != nil {
		return nil, false, err
	}
	r, err := s.dialRequest(ctx, region)
	if err != nil {
		return nil, false, classify(err)
	}
	cctx, cancel := context.WithTimeout(ctx, state.DialTimeout)
	defer cancel()
	resp, err := r.Get(cctx, state.PubkeyKey(inst))
	if err != nil {
		return nil, false, classify(err)
	}
	if len(resp.Kvs) != 1 {
		return nil, false, nil
	}
	return append([]byte(nil), resp.Kvs[0].Value...), true, nil
}

// recordFingerprint inserts the adopted key's SHA-256, or updates it when it differs.
// Only the replica whose UPDATE matches a row logs and counts (E21-D038, E21-D041).
// The first insert does not. pubMu is never held while waiting for pubRecMu.
func (s *Server) recordFingerprint(ctx context.Context, inst string, pub sealbox.PublicKey) error {
	sum := pub.SHA256()
	s.pubMu.Lock()
	sl := s.slotLocked(inst)
	if bytes.Equal(sl.recorded, sum[:]) {
		s.pubMu.Unlock()
		return nil
	}
	s.pubMu.Unlock()

	s.pubRecMu.Lock()
	defer s.pubRecMu.Unlock()

	s.pubMu.Lock()
	sl = s.slotLocked(inst)
	if bytes.Equal(sl.recorded, sum[:]) || !s.adoptedIs(inst, pub) {
		s.pubMu.Unlock()
		return nil
	}
	s.pubMu.Unlock()

	inserted, changed, err := s.writeFingerprint(ctx, inst, sum[:])
	if err != nil {
		return err
	}
	s.pubMu.Lock()
	if !s.adoptedIs(inst, pub) {
		s.pubMu.Unlock()
		return nil
	}
	sl = s.slotLocked(inst)
	sl.recorded = append([]byte(nil), sum[:]...)
	if changed {
		if s.pubChanged == nil {
			s.pubChanged = map[string]uint64{}
		}
		s.pubChanged[inst]++
		s.log.Error().Str("td_instance", inst).Str("fingerprint", hex.EncodeToString(sum[:])).Msg("td pubkey changed")
	}
	s.pubMu.Unlock()
	if inserted || changed {
		s.resealInstance(ctx, inst, pub)
	}
	return nil
}

func (s *Server) writeFingerprint(ctx context.Context, inst string, sum []byte) (inserted, changed bool, err error) {
	pool, err := s.db()
	if err != nil {
		return false, false, err
	}
	var name string
	err = pool.QueryRow(ctx, `
INSERT INTO td_pubkeys (td_instance, fingerprint, first_seen_at, updated_at)
VALUES ($1, $2, now(), now())
ON CONFLICT DO NOTHING
RETURNING td_instance`, inst, sum).Scan(&name)
	if err == nil {
		return true, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return false, false, storageErr(err)
	}
	tag, err := pool.Exec(ctx, `
UPDATE td_pubkeys
SET fingerprint = $2, updated_at = now()
WHERE td_instance = $1 AND fingerprint <> $2`, inst, sum)
	if err != nil {
		return false, false, storageErr(err)
	}
	return false, tag.RowsAffected() == 1, nil
}

// recordAdopted retries a fingerprint write the watch did not finish. It does not dial.
func (s *Server) recordAdopted(ctx context.Context) {
	for inst, plane := range s.cfg.Planes {
		if plane != state.PlaneTD {
			continue
		}
		pub, ok := s.adopted(inst)
		if !ok {
			continue
		}
		if err := s.recordFingerprint(ctx, inst, pub); err != nil && !s.quiet(err) {
			s.log.Error().Err(err).Str("td_instance", inst).Msg("record td pubkey")
		}
	}
}

// resealInstance rewrites live sealboxes for inst onto pub and projects the winners.
// A put that fails is left for reconcile. Soft-deleted rows are skipped.
func (s *Server) resealInstance(ctx context.Context, inst string, pub sealbox.PublicKey) {
	pool, err := s.db()
	if err != nil {
		if !s.quiet(err) {
			s.log.Error().Err(err).Str("td_instance", inst).Msg("record td pubkey")
		}
		return
	}
	rows, err := pool.Query(ctx, `
SELECT api_id FROM accounts
WHERE td_instance = $1 AND deleted_at IS NULL
ORDER BY api_id`, inst)
	if err != nil {
		if !s.quiet(err) {
			s.log.Error().Err(storageErr(err)).Str("td_instance", inst).Msg("record td pubkey")
		}
		return
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			if !s.quiet(err) {
				s.log.Error().Err(storageErr(err)).Str("td_instance", inst).Msg("record td pubkey")
			}
			return
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		if !s.quiet(err) {
			s.log.Error().Err(storageErr(err)).Str("td_instance", inst).Msg("record td pubkey")
		}
		return
	}
	for _, id := range ids {
		sealed, publish, rerr := s.resealAccount(ctx, id, inst, pub)
		if rerr != nil {
			if !s.quiet(rerr) {
				s.log.Error().Err(rerr).Int64("api_id", id).Str("td_instance", inst).Msg("account was not resealed")
			}
			s.kickReconcile()
			continue
		}
		if !publish {
			continue
		}
		row, lerr := s.loadStoredAccount(ctx, id)
		if lerr != nil || row.Deleted != nil || !bytes.Equal(row.sealed, sealed) {
			s.kickReconcile()
			continue
		}
		val, merr := projectionBytes(row.accountPublic, row.sealed)
		if merr != nil {
			s.log.Error().Int64("api_id", id).Str("td_instance", inst).Msg("account projection is unreadable")
			continue
		}
		if perr := s.putProjection(ctx, inst, id, val); perr != nil {
			if !s.quiet(perr) {
				s.log.Error().Err(perr).Int64("api_id", id).Str("td_instance", inst).Msg("project account")
			}
			s.kickReconcile()
		}
	}
}

// resealAccount copies one live row's sealbox onto pub. revision, updated_at, credentials,
// and key_version stay put (E21-D041). publish is false when the header already matches,
// the CAS lost, or the credentials do not open. Open failures are logged without the error
// text and return the stored blob so a caller can still project it.
func (s *Server) resealAccount(ctx context.Context, id int64, inst string, pub sealbox.PublicKey) (sealed []byte, publish bool, err error) {
	pool, err := s.db()
	if err != nil {
		return nil, false, err
	}
	var kind string
	var cred, cur []byte
	err = pool.QueryRow(ctx, `
SELECT kind, credentials, sealed_credentials
FROM accounts
WHERE api_id = $1 AND td_instance = $2 AND deleted_at IS NULL`, id, inst).Scan(&kind, &cred, &cur)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, storageErr(err)
	}
	defer clear(cred)
	cur = append([]byte(nil), cur...)
	if sealbox.SealedTo(pub, cur) {
		return cur, false, nil
	}
	plain, oerr := s.cfg.AccountKeys.Open(id, kind, cred)
	if oerr != nil {
		s.log.Error().Int64("api_id", id).Str("td_instance", inst).Msg("account was not resealed")
		return cur, false, nil
	}
	defer clear(plain)
	var msg statev1.AccountCredentials
	if uerr := proto.Unmarshal(plain, &msg); uerr != nil {
		s.log.Error().Int64("api_id", id).Str("td_instance", inst).Msg("account was not resealed")
		return cur, false, nil
	}
	next, serr := sealbox.SealCredentials(pub, id, kind, sealbox.Credentials{
		KeyType: msg.KeyType, Key: msg.Key, Secret: msg.Secret, Passphrase: msg.Passphrase,
	})
	if serr != nil {
		return nil, false, serr
	}
	tag, err := pool.Exec(ctx, `
UPDATE accounts SET sealed_credentials = $1
WHERE api_id = $2 AND td_instance = $3 AND sealed_credentials = $4 AND deleted_at IS NULL`,
		next, id, inst, cur)
	if err != nil {
		return nil, false, storageErr(err)
	}
	if tag.RowsAffected() == 0 {
		got, rerr := s.currentSealed(ctx, id, inst)
		return got, false, rerr
	}
	got, rerr := s.currentSealed(ctx, id, inst)
	if rerr != nil {
		return nil, false, rerr
	}
	if !bytes.Equal(got, next) {
		return got, false, nil
	}
	return next, true, nil
}

func (s *Server) currentSealed(ctx context.Context, id int64, inst string) ([]byte, error) {
	pool, err := s.db()
	if err != nil {
		return nil, err
	}
	var cur []byte
	err = pool.QueryRow(ctx, `
SELECT sealed_credentials FROM accounts
WHERE api_id = $1 AND td_instance = $2 AND deleted_at IS NULL`, id, inst).Scan(&cur)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, storageErr(err)
	}
	return append([]byte(nil), cur...), nil
}

func (s *Server) loadStoredAccount(ctx context.Context, id int64) (accountStored, error) {
	pool, err := s.db()
	if err != nil {
		return accountStored{}, err
	}
	row, err := scanStored(pool.QueryRow(ctx, `
SELECT `+accountCols+`, sealed_credentials FROM accounts WHERE api_id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return accountStored{}, errNotFound
	}
	if err != nil {
		if errors.Is(err, errUnreadable) {
			return accountStored{}, err
		}
		return accountStored{}, storageErr(err)
	}
	return row, nil
}

func (s *Server) runPubkeyMirror(c *cluster) {
	defer s.wg.Done()
	bo := s.minBackoff
	for {
		if s.stopped() {
			return
		}
		r, err := s.dial(c.name)
		if s.stopped() {
			return
		}
		if err != nil {
			s.log.Warn().Err(err).Str("region", c.name).Msg("pubkey mirror dial")
			if !s.sleep(bo) {
				return
			}
			bo = min(bo*2, s.maxBackoff)
			continue
		}
		opt := s.cfg.Mirror
		l := s.log.With().Str("region", c.name).Logger()
		opt.Log = &l
		m, err := state.NewMirror(r, state.TDPrefix, opt)
		if err != nil {
			s.log.Error().Err(err).Str("region", c.name).Msg("pubkey mirror")
			return
		}
		watchCtx, watchCancel := context.WithCancel(s.ctx)
		s.wg.Add(1)
		go s.watchPubkeys(watchCtx, c.name, m)
		err = m.Run(s.ctx)
		watchCancel()
		if s.stopped() {
			return
		}
		s.log.Warn().Err(err).Str("region", c.name).Msg("pubkey mirror stopped")
		if !s.sleep(bo) {
			return
		}
		bo = min(bo*2, s.maxBackoff)
	}
}

func (s *Server) watchPubkeys(ctx context.Context, region string, m *state.Mirror) {
	defer s.wg.Done()
	notify := m.Notify()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.ctx.Done():
			return
		case <-notify:
			select {
			case <-m.Synced():
			default:
				continue
			}
			s.applyPubkeys(region, m.Snapshot())
		}
	}
}

// applyPubkeys keeps the cache for TD instances whose home is region. Other keys under /td/ are ignored.
func (s *Server) applyPubkeys(region string, view state.View) {
	for inst, home := range s.cfg.Topology.Instances {
		if home != region || s.cfg.Planes[inst] != state.PlaneTD {
			continue
		}
		e, ok := view.Entries[state.PubkeyKey(inst)]
		s.pubMu.Lock()
		var pub sealbox.PublicKey
		var adopted bool
		if ok {
			pub, adopted = s.observeValueLocked(inst, e.Value)
		} else {
			pub, adopted = s.observeAbsentLocked(inst)
		}
		s.pubMu.Unlock()
		if !adopted {
			continue
		}
		if err := s.recordFingerprint(s.ctx, inst, pub); err != nil && !s.quiet(err) {
			s.log.Error().Err(err).Str("td_instance", inst).Msg("record td pubkey")
		}
	}
}

func (s *Server) pubkeyChangedLines() []string {
	s.pubMu.Lock()
	defer s.pubMu.Unlock()
	if len(s.pubChanged) == 0 {
		return nil
	}
	names := make([]string, 0, len(s.pubChanged))
	for name := range s.pubChanged {
		names = append(names, name)
	}
	slices.Sort(names)
	out := make([]string, 0, len(names))
	for _, name := range names {
		out = append(out, counterLine("mftiko_td_pubkey_changed_total", [][2]string{{"td_instance", name}}, s.pubChanged[name]))
	}
	return out
}

func counterLine(name string, labels [][2]string, value uint64) string {
	var b strings.Builder
	b.WriteString(name)
	if len(labels) > 0 {
		b.WriteByte('{')
		for i, l := range labels {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(l[0])
			b.WriteString(`="`)
			b.WriteString(escapeLabel(l[1]))
			b.WriteByte('"')
		}
		b.WriteByte('}')
	}
	b.WriteByte(' ')
	b.WriteString(strconv.FormatUint(value, 10))
	return b.String()
}
