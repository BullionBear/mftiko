package agent

import (
	"context"
	"testing"
	"time"

	"github.com/BullionBear/mftiko/internal/platform/sealbox"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
)

func TestPublishPubkey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	srv := etcdtest.Start(t)
	const inst = "td-tw-1"
	topo := state.Topology{
		Regions:   map[string]state.Region{"tw": {Endpoints: srv.Endpoints()}},
		Instances: map[string]string{state.ControllerInstance: "tw", inst: "tw"},
	}
	tables, err := state.AuthTables(topo, map[string]state.Plane{inst: state.PlaneTD})
	if err != nil {
		t.Fatal(err)
	}
	if err := state.EnsureAuth(ctx, srv.Client(t), tables["tw"], etcdtest.Password); err != nil {
		t.Fatal(err)
	}
	cl, err := state.NewClusters(topo, nil, state.AuthDial(inst, etcdtest.Password(inst)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cl.Close() })
	term, err := state.Campaign(ctx, cl, state.PlaneTD, inst, state.TermOptions{ID: "pubkey-test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { term.Close(context.Background()) })
	reader, err := cl.Reader("tw")
	if err != nil {
		t.Fatal(err)
	}
	key, err := sealbox.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	pub := key.Public()
	keyName := state.PubkeyKey(inst)
	if err := publishPubkey(ctx, reader, term, inst, pub); err != nil {
		t.Fatal(err)
	}
	kv := pubkeyKV(t, ctx, srv, keyName)
	if string(kv.Value) != pub.String() || kv.Lease != 0 {
		t.Fatalf("value %q lease %d", kv.Value, kv.Lease)
	}
	rev := kv.ModRevision
	if err := publishPubkey(ctx, reader, term, inst, pub); err != nil {
		t.Fatal(err)
	}
	kv = pubkeyKV(t, ctx, srv, keyName)
	if kv.ModRevision != rev {
		t.Fatalf("unchanged key rewritten: rev %d -> %d", rev, kv.ModRevision)
	}
	if _, err := srv.Client(t).Put(ctx, keyName, pub.String()+"\n"); err != nil {
		t.Fatal(err)
	}
	kv = pubkeyKV(t, ctx, srv, keyName)
	nlRev := kv.ModRevision
	if err := publishPubkey(ctx, reader, term, inst, pub); err != nil {
		t.Fatal(err)
	}
	kv = pubkeyKV(t, ctx, srv, keyName)
	if kv.ModRevision != nlRev || string(kv.Value) != pub.String()+"\n" {
		t.Fatalf("newline rewritten: rev %d -> %d value %q", nlRev, kv.ModRevision, kv.Value)
	}
	other, err := sealbox.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	if err := publishPubkey(ctx, reader, term, inst, other.Public()); err != nil {
		t.Fatal(err)
	}
	kv = pubkeyKV(t, ctx, srv, keyName)
	if string(kv.Value) != other.Public().String() || kv.Lease != 0 || kv.ModRevision == nlRev {
		t.Fatalf("replacement %q lease %d rev %d (was %d)", kv.Value, kv.Lease, kv.ModRevision, nlRev)
	}
}

func pubkeyKV(t *testing.T, ctx context.Context, srv *etcdtest.Server, key string) (kv struct {
	Value       []byte
	Lease       int64
	ModRevision int64
}) {
	t.Helper()
	resp, err := srv.Client(t).Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) != 1 {
		t.Fatalf("%s: %d values", key, len(resp.Kvs))
	}
	kv.Value = append([]byte(nil), resp.Kvs[0].Value...)
	kv.Lease = resp.Kvs[0].Lease
	kv.ModRevision = resp.Kvs[0].ModRevision
	return kv
}
