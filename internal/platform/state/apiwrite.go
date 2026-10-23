package state

import (
	"context"
	"fmt"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// APIWriter is the API server's write path (PROTOCOL §3.2 W10, DECISIONS D1006, D1042). The API does not campaign:
// several replicas write at once, so there is no writer key and no fencing. What keeps two replicas from
// overwriting each other is the caller's own compares in the same txn: create_revision == 0 to create, the
// mod_revision it read to update or delete.
//
// W1 holds by scope, checked before any I/O as Term checks its own:
//   - put: the session specs of STS instances and the accounts of TD instances (the API rows of §3.1);
//   - delete: those, and the session statuses of STS instances (W5). etcd RBAC cannot keep the api user from
//     putting a status, since one prefix grant covers both (D1040); this scope is what does.
//
// Every key of a txn must route to one cluster (ErrCrossCluster): there are no cross-cluster writes.
type APIWriter struct{ cl *Clusters }

// APIWriter is c's write path for the API server. c should be dialled as the api user (AuthDial(UserAPI, …)), so
// that etcd enforces the same rows the scope does.
func (c *Clusters) APIWriter() *APIWriter { return &APIWriter{cl: c} }

// Txn runs If(cmps) Then(then) Else(els) in the one cluster every key routes to, and returns etcd's response:
// Succeeded says whether cmps held. A key outside the API's scope is ErrNotWriter, keys in two clusters are
// ErrCrossCluster, an unknown instance is ErrUnknownInstance; all before any I/O. Otherwise the error is the dial's
// or etcd's, wrapped.
func (w *APIWriter) Txn(ctx context.Context, cmps []clientv3.Cmp, then, els []clientv3.Op) (*clientv3.TxnResponse, error) {
	region, err := routeTxn(w.cl.topo, apiScope{}, cmps, then, els)
	if err != nil {
		return nil, err
	}
	cli, err := w.cl.client(ctx, region)
	if err != nil {
		return nil, err
	}
	resp, err := cli.Txn(ctx).If(cmps...).Then(then...).Else(els...).Commit()
	if err != nil {
		return nil, fmt.Errorf("state: txn in %s: %w", region, err)
	}
	return resp, nil
}

// apiScope is what the API may put and delete (PROTOCOL §3.1, W10).
type apiScope struct{}

func (apiScope) check(key, end []byte, del bool) error {
	k := string(key)
	p, inst, err := Owner(k)
	if err != nil {
		return err
	}
	var prefixes []string
	switch p {
	case PlaneSTS:
		prefixes = []string{SessionSpecPrefix(inst)}
		if del {
			prefixes = append(prefixes, SessionStatusPrefix(inst))
		}
	case PlaneTD:
		prefixes = []string{AccountPrefix(inst)}
	}
	for _, pre := range prefixes {
		if !strings.HasPrefix(k, pre) {
			continue
		}
		if len(end) == 0 || (string(end) != "\x00" && string(end) <= clientv3.GetPrefixRangeEnd(pre)) {
			return nil
		}
	}
	verb := "write"
	if del {
		verb = "delete"
	}
	if len(end) > 0 {
		return fmt.Errorf("%w: api may not %s [%q, %q)", ErrNotWriter, verb, k, end)
	}
	return fmt.Errorf("%w: api may not %s %q", ErrNotWriter, verb, k)
}
