package api

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/sealbox"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// projectionBytes is the etcd value for row. sealed is stored as-is so a later reconcile
// does not draw a new nonce (E21-D028).
func projectionBytes(row accountPublic, sealed []byte) ([]byte, error) {
	if len(sealed) < minSealedCredentials {
		return nil, errUnreadable
	}
	rec := &statev1.Account{
		Name: row.Name, Kind: row.Kind,
		SealedCredentials: append([]byte(nil), sealed...),
		Settings:          &statev1.AccountSettings{CancelOnDisconnectS: row.Cancel},
		Enabled:           true,
	}
	b, err := proto.Marshal(rec)
	if err != nil {
		return nil, errUnreadable
	}
	return b, nil
}

// publishAccount writes the projection after Postgres has committed (E21-D022).
// The hook "account-before-etcd" runs first so a test can fail the etcd write.
func (s *Server) publishAccount(ctx context.Context, row accountStored) error {
	if err := s.fireHook("account-before-etcd"); err != nil {
		return err
	}
	if row.Deleted != nil {
		return s.deleteProjection(ctx, row.Inst, row.ID)
	}
	val, err := projectionBytes(row.accountPublic, row.sealed)
	if err != nil {
		return err
	}
	return s.putProjection(ctx, row.Inst, row.ID, val)
}

// putProjection makes key equal val. A key that already holds val is left alone.
// A different value is replaced: the Postgres row is the authority (E21-D030).
func (s *Server) putProjection(ctx context.Context, inst string, id int64, val []byte) error {
	key := state.AccountKey(inst, id)
	w := s.cl.APIWriter()
	for range idAttempts {
		resp, err := txn(ctx, w, nil, []clientv3.Op{clientv3.OpGet(key)}, nil)
		if err != nil {
			return err
		}
		kv := firstKV(resp, 0)
		if kv == nil {
			if _, err := create(ctx, w, key, val); err != nil {
				if errors.Is(err, errConflict) {
					continue
				}
				return err
			}
			return nil
		}
		if bytes.Equal(kv.Value, val) {
			return nil
		}
		if _, err := casPut(ctx, w, key, val, kv.ModRevision); err != nil {
			if errors.Is(err, errConflict) {
				continue
			}
			return err
		}
		return nil
	}
	return fmt.Errorf("%w: projection of %s kept changing", errRegionUnavailable, key)
}

// deleteProjection removes the etcd copy. A key that is already gone is success (E21-D024).
func (s *Server) deleteProjection(ctx context.Context, inst string, id int64) error {
	key := state.AccountKey(inst, id)
	w := s.cl.APIWriter()
	for range idAttempts {
		resp, err := txn(ctx, w, nil, []clientv3.Op{clientv3.OpGet(key)}, nil)
		if err != nil {
			return err
		}
		kv := firstKV(resp, 0)
		if kv == nil {
			return nil
		}
		if _, err := deleteIf(ctx, w, cond{key: key, rev: kv.ModRevision}); err != nil {
			if errors.Is(err, errConflict) {
				continue
			}
			return err
		}
		return nil
	}
	return fmt.Errorf("%w: delete of %s kept changing", errRegionUnavailable, key)
}

// reconcileAccounts projects every Postgres account into its TD instance and drops the etcd
// copy of a soft-deleted row (E21-D022, E21-D024). A region that is down is skipped until a
// later pass. An etcd account with no row is logged and left in place (E21-D021, E21-D030).
func (s *Server) reconcileAccounts(ctx context.Context) error {
	// A fingerprint write that the watch lost is retried here, and a sealbox whose header
	// does not match the adopted key is rewritten before it is projected (E21-D038, E21-D041).
	s.recordAdopted(ctx)
	rows, err := s.loadAccountProjection(ctx)
	if err != nil {
		return err
	}
	known := make(map[int64]struct{}, len(rows))
	for i := range rows {
		row := rows[i]
		known[row.ID] = struct{}{}
		region, err := s.cfg.Topology.RegionOf(row.Inst)
		if err != nil || !s.regionAvailable(region) {
			continue
		}
		var perr error
		if row.Deleted != nil {
			perr = s.deleteProjection(ctx, row.Inst, row.ID)
		} else {
			if pub, ok := s.adopted(row.Inst); ok && !sealbox.SealedTo(pub, row.sealed) {
				sealed, _, rerr := s.resealAccount(ctx, row.ID, row.Inst, pub)
				if rerr != nil {
					if !s.quiet(rerr) {
						s.log.Error().Err(rerr).Int64("api_id", row.ID).Str("td_instance", row.Inst).Msg("account was not resealed")
					}
					continue
				}
				if sealed != nil {
					row.sealed = sealed
				}
			}
			val, merr := projectionBytes(row.accountPublic, row.sealed)
			if merr != nil {
				s.log.Error().Int64("api_id", row.ID).Str("td_instance", row.Inst).Msg("account projection is unreadable")
				continue
			}
			perr = s.putProjection(ctx, row.Inst, row.ID, val)
		}
		if perr != nil && !s.quiet(perr) {
			s.log.Error().Err(perr).Int64("api_id", row.ID).Str("td_instance", row.Inst).Msg("project account")
		}
	}
	s.warnOrphanAccounts(known)
	return nil
}

// warnOrphanAccounts logs etcd account keys whose api_id has no Postgres row (E21-D021).
// The value is not logged. The key is not deleted.
func (s *Server) warnOrphanAccounts(known map[int64]struct{}) {
	views, _ := s.regionViews()
	for region, view := range views {
		for key := range view.Entries {
			inst, id, ok := accountOf(key)
			if !ok || key != state.AccountKey(inst, id) {
				continue
			}
			if _, ok := known[id]; ok {
				continue
			}
			n := s.orphanAccount.Add(1)
			s.log.Warn().Int64("api_id", id).Str("td_instance", inst).Str("region", region).Int64("count", n).Msg("etcd account has no postgres row")
		}
	}
}
