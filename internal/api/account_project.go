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

type replicaKey struct {
	inst string
	id   int64
}

// reconcileAccounts projects every replica and drops the etcd copy of a soft-deleted
// account (E21-D022, E21-D024). Replica rows of a soft-deleted account stay until a
// later change removes them. A region that is down is skipped until a later pass.
// An etcd account with no replica is logged and left in place (E21-D055).
func (s *Server) reconcileAccounts(ctx context.Context) error {
	// A fingerprint write that the watch lost is retried here, and a sealbox whose header
	// does not match the adopted key is rewritten before it is projected (E21-D038, E21-D041).
	s.recordAdopted(ctx)
	rows, err := s.loadReplicaViews(ctx)
	if err != nil {
		return err
	}
	known := make(map[replicaKey]struct{}, len(rows))
	for i := range rows {
		row := rows[i]
		known[replicaKey{inst: row.Inst, id: row.ID}] = struct{}{}
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

// warnOrphanAccounts logs etcd account keys that have no account_replicas row (E21-D055).
// The value is not logged. The key is not deleted.
func (s *Server) warnOrphanAccounts(known map[replicaKey]struct{}) {
	views, _ := s.regionViews()
	for region, view := range views {
		for key := range view.Entries {
			inst, id, ok := accountOf(key)
			if !ok || key != state.AccountKey(inst, id) {
				continue
			}
			if _, ok := known[replicaKey{inst: inst, id: id}]; ok {
				continue
			}
			n := s.orphanAccount.Add(1)
			s.log.Warn().Int64("api_id", id).Str("td_instance", inst).Str("region", region).Int64("count", n).Msg("etcd account has no replica")
		}
	}
}
