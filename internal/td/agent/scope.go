package agent

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	tdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/tdctl/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/td/account"
)

// maxAPIID is 2^53 − 1, the API's api_id ceiling (E10-D008).
const maxAPIID int64 = 1<<53 - 1

// scopeAttempts is how many compare-and-set tries one scope key gets before sync fails
// and the next reconcile tries again (E21-D067).
const scopeAttempts = 8

// recordScopes writes Report.first_submit_ns into /td/{inst}/scope/{api_id}/{uid} (E21-D009, E21-D066).
// It runs on every sync, including the one that follows Adopt, so a report the shim kept while this
// agent was down is written when the worker is adopted. A stale demand skips sync; the report stays
// in the shim and the next successful sync writes it. Keys are never deleted.
func (p *plane) recordScopes(ctx context.Context, observed map[agent.WorkerKey]agent.Observed) error {
	for _, wk := range slices.Sorted(maps.Keys(observed)) {
		id, ok := apiIDOf(wk)
		if !ok {
			continue
		}
		rep := reportOf(observed[wk].Report)
		if rep == nil {
			continue
		}
		for _, uid := range slices.Sorted(maps.Keys(rep.GetFirstSubmitNs())) {
			if uid == 0 {
				continue
			}
			ns := rep.GetFirstSubmitNs()[uid]
			if ns <= 0 {
				continue
			}
			key := state.ScopeKey(p.inst, id, uid)
			if cur, ok := p.applied[key]; ok && cur <= ns {
				continue
			}
			stored, replaced, err := ensureSince(ctx, p.get, p.w, key, ns)
			if err != nil {
				return err
			}
			if replaced {
				p.warnReplaced(key)
			}
			if p.applied == nil {
				p.applied = map[string]int64{}
			}
			p.applied[key] = stored
		}
	}
	return nil
}

func (p *plane) warnReplaced(key string) {
	if p.replaced == nil {
		p.replaced = map[string]struct{}{}
	}
	if _, ok := p.replaced[key]; ok {
		return
	}
	p.replaced[key] = struct{}{}
	p.log.Warn().Str("key", key).Msg("scope value replaced")
}

// ensureSince stores since when the key is missing, holds a larger since_ns, or does not parse.
// stored is the value known to be in etcd afterwards. replaced is set when an unreadable value was overwritten.
// The compare is numeric: etcd's byte compare orders "9" after "10" (E21-D067).
func ensureSince(ctx context.Context, r reader, w writer, key string, since int64) (stored int64, replaced bool, err error) {
	want, ok := state.FormatSince(since)
	if !ok {
		return 0, false, fmt.Errorf("td: scope %s: since_ns must be positive", key)
	}
	for range scopeAttempts {
		if err := ctx.Err(); err != nil {
			return 0, false, err
		}
		resp, err := r.Get(ctx, key)
		if err != nil {
			return 0, false, fmt.Errorf("td: scope %s: %w", key, err)
		}
		var cmp clientv3.Cmp
		overwrite := false
		switch {
		case resp == nil || len(resp.Kvs) == 0:
			cmp = clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
		case len(resp.Kvs) != 1:
			return 0, false, fmt.Errorf("td: scope %s: %d values", key, len(resp.Kvs))
		default:
			kv := resp.Kvs[0]
			got, parsed := state.ParseSince(kv.Value)
			if parsed && got <= since {
				return got, false, nil
			}
			overwrite = !parsed
			cmp = clientv3.Compare(clientv3.ModRevision(key), "=", kv.ModRevision)
		}
		txn, err := w.Txn(ctx, []clientv3.Cmp{cmp}, []clientv3.Op{clientv3.OpPut(key, want)}, nil)
		if err != nil {
			return 0, false, fmt.Errorf("td: scope %s: %w", key, err)
		}
		if txn != nil && txn.Succeeded {
			return since, overwrite, nil
		}
	}
	return 0, false, fmt.Errorf("td: scope %s: compare failed", key)
}

// apiIDOf is the api_id in an account worker key, account/{api_id}.
func apiIDOf(k agent.WorkerKey) (int64, bool) {
	rest, ok := strings.CutPrefix(string(k), "account/")
	if !ok || rest == "" || strings.Contains(rest, "/") {
		return 0, false
	}
	id, err := strconv.ParseInt(rest, 10, 64)
	if err != nil || id < 1 || id > maxAPIID || strconv.FormatInt(id, 10) != rest {
		return 0, false
	}
	return id, true
}

// reportOf is the tdctl.Report inside a worker report, or nil when it is absent or unreadable.
func reportOf(rep *workerv1.Report) *tdctlv1.Report {
	if rep == nil || rep.GetContract() != account.Contract || len(rep.GetPayload()) == 0 {
		return nil
	}
	r := &tdctlv1.Report{}
	if err := proto.Unmarshal(rep.GetPayload(), r); err != nil {
		return nil
	}
	return r
}
