package agent

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	tdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/tdctl/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
	"github.com/BullionBear/mftiko/internal/td/account"
)

func TestEnsureSince(t *testing.T) {
	cases := []struct {
		name     string
		have     string
		absent   bool
		since    int64
		want     int64
		replaced bool
		puts     int
	}{
		{name: "create", absent: true, since: 5, want: 5, puts: 1},
		{name: "equal", have: "5", since: 5, want: 5},
		{name: "keep earlier", have: "4", since: 5, want: 4},
		{name: "nine before ten", have: "9", since: 10, want: 9},
		{name: "move earlier across width", have: "10", since: 9, want: 9, puts: 1},
		{name: "replace junk", have: "x", since: 7, want: 7, replaced: true, puts: 1},
		{name: "replace leading zero", have: "01", since: 7, want: 7, replaced: true, puts: 1},
		{name: "replace zero", have: "0", since: 7, want: 7, replaced: true, puts: 1},
		{name: "replace negative", have: "-3", since: 7, want: 7, replaced: true, puts: 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			want, ok := state.FormatSince(c.since)
			if !ok {
				t.Fatal("since")
			}
			kv := &seqKV{key: "k", next: want, val: map[string]string{}, mod: map[string]int64{}, cre: map[string]int64{}}
			if !c.absent {
				kv.val["k"] = c.have
				kv.mod["k"] = 2
				kv.cre["k"] = 1
			}
			stored, replaced, err := ensureSince(context.Background(), kv, kv, "k", c.since)
			if err != nil || stored != c.want || replaced != c.replaced || kv.puts != c.puts {
				t.Fatalf("stored %d replaced %v puts %d err %v", stored, replaced, kv.puts, err)
			}
			if c.puts > 0 {
				wantBytes, ok := state.FormatSince(c.want)
				if !ok || kv.val["k"] != wantBytes {
					t.Fatalf("stored bytes %q", kv.val["k"])
				}
			}
		})
	}
}

func TestEnsureSinceRetries(t *testing.T) {
	kv := &seqKV{
		key: "k", next: "5", nextFail: 1,
		val: map[string]string{"k": "9"}, mod: map[string]int64{"k": 3}, cre: map[string]int64{"k": 1},
	}
	stored, replaced, err := ensureSince(context.Background(), kv, kv, "k", 5)
	if err != nil || replaced || stored != 5 || kv.puts != 1 || kv.txns != 2 || kv.val["k"] != "5" {
		t.Fatalf("stored %d replaced %v puts %d txns %d val %q err %v", stored, replaced, kv.puts, kv.txns, kv.val["k"], err)
	}

	stuck := &seqKV{
		key: "k", next: "5", nextFail: scopeAttempts,
		val: map[string]string{"k": "9"}, mod: map[string]int64{"k": 3}, cre: map[string]int64{"k": 1},
	}
	if _, _, err := ensureSince(context.Background(), stuck, stuck, "k", 5); err == nil || stuck.puts != 0 || stuck.txns != scopeAttempts {
		t.Fatalf("exhausted puts %d txns %d err %v", stuck.puts, stuck.txns, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := ensureSince(ctx, stuck, stuck, "k", 5); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}
}

// seqKV is an in-memory reader and writer for ensureSince. A failed txn leaves the value alone.
type seqKV struct {
	key      string
	next     string
	nextFail int
	puts     int
	txns     int
	val      map[string]string
	mod      map[string]int64
	cre      map[string]int64
}

func (s *seqKV) Get(ctx context.Context, key string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	v, ok := s.val[key]
	if !ok {
		return &clientv3.GetResponse{}, nil
	}
	return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{{
		Key: []byte(key), Value: []byte(v), ModRevision: s.mod[key], CreateRevision: s.cre[key],
	}}}, nil
}

func (s *seqKV) Txn(ctx context.Context, _ []clientv3.Cmp, _ []clientv3.Op, _ []clientv3.Op) (*clientv3.TxnResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.txns++
	if s.nextFail > 0 {
		s.nextFail--
		return &clientv3.TxnResponse{Succeeded: false}, nil
	}
	s.puts++
	s.val[s.key] = s.next
	s.mod[s.key]++
	if s.cre[s.key] == 0 {
		s.cre[s.key] = s.mod[s.key]
	}
	return &clientv3.TxnResponse{Succeeded: true}, nil
}

func (s *seqKV) Put(context.Context, string, string, ...clientv3.OpOption) error {
	return errors.New("put")
}

func (s *seqKV) Delete(context.Context, string, ...clientv3.OpOption) error {
	return errors.New("delete")
}

func TestRecordScopes(t *testing.T) {
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
	term, err := state.Campaign(ctx, cl, state.PlaneTD, inst, state.TermOptions{ID: "scope-test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { term.Close(context.Background()) })
	reader, err := cl.Reader("tw")
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	p := &plane{inst: inst, get: reader, w: term, log: zerolog.New(&buf)}

	rep, err := scopeReport(map[uint32]int64{7: 100, 8: 50, 0: 10, 9: -1})
	if err != nil {
		t.Fatal(err)
	}
	bad, err := scopeReport(map[uint32]int64{1: 3})
	if err != nil {
		t.Fatal(err)
	}
	bad.Contract = "nope"
	observed := map[agent.WorkerKey]agent.Observed{
		WorkerKey(42): {Report: rep},
		"other/1":     {Report: rep},
		WorkerKey(43): {Report: bad},
	}
	// Adopt leaves the shim's latest Report on Observed. start calls sync, which records scope,
	// including orders submitted while this agent was down (E21-D066).
	if err := p.sync(ctx, agent.Demand[Entry]{}, observed, func(agent.WorkerKey) bool { return false }); err != nil {
		t.Fatal(err)
	}
	key7 := state.ScopeKey(inst, 42, 7)
	key8 := state.ScopeKey(inst, 42, 8)
	kv7 := scopeKV(t, ctx, srv, key7)
	kv8 := scopeKV(t, ctx, srv, key8)
	if string(kv7.Value) != "100" || kv7.Lease != 0 {
		t.Fatalf("uid 7: %q lease %d", kv7.Value, kv7.Lease)
	}
	if string(kv8.Value) != "50" || kv8.Lease != 0 {
		t.Fatalf("uid 8: %q lease %d", kv8.Value, kv8.Lease)
	}
	if !scopeAbsent(t, ctx, srv, state.ScopeKey(inst, 42, 0)) || !scopeAbsent(t, ctx, srv, state.ScopeKey(inst, 42, 9)) {
		t.Fatal("uid 0 or a non-positive since was written")
	}
	if !scopeAbsent(t, ctx, srv, state.ScopeKey(inst, 43, 1)) {
		t.Fatal("a report with the wrong contract was written")
	}

	rev := kv7.ModRevision
	p.applied = nil
	later, err := scopeReport(map[uint32]int64{7: 150})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.recordScopes(ctx, map[agent.WorkerKey]agent.Observed{WorkerKey(42): {Report: later}}); err != nil {
		t.Fatal(err)
	}
	kv7 = scopeKV(t, ctx, srv, key7)
	if string(kv7.Value) != "100" || kv7.ModRevision != rev {
		t.Fatalf("later since rewrote the key: %q rev %d -> %d", kv7.Value, rev, kv7.ModRevision)
	}

	p.applied = nil
	earlier, err := scopeReport(map[uint32]int64{7: 40})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.recordScopes(ctx, map[agent.WorkerKey]agent.Observed{WorkerKey(42): {Report: earlier}}); err != nil {
		t.Fatal(err)
	}
	kv7 = scopeKV(t, ctx, srv, key7)
	if string(kv7.Value) != "40" || kv7.ModRevision == rev || kv7.Lease != 0 {
		t.Fatalf("earlier since: %q rev %d lease %d", kv7.Value, kv7.ModRevision, kv7.Lease)
	}

	nine := state.ScopeKey(inst, 42, 11)
	if _, err := srv.Client(t).Put(ctx, nine, "9"); err != nil {
		t.Fatal(err)
	}
	p.applied = nil
	ten, err := scopeReport(map[uint32]int64{11: 10})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.recordScopes(ctx, map[agent.WorkerKey]agent.Observed{WorkerKey(42): {Report: ten}}); err != nil {
		t.Fatal(err)
	}
	if got := scopeKV(t, ctx, srv, nine); string(got.Value) != "9" {
		t.Fatalf("numeric compare kept %q, want 9", got.Value)
	}

	junk := state.ScopeKey(inst, 42, 12)
	if _, err := srv.Client(t).Put(ctx, junk, "x"); err != nil {
		t.Fatal(err)
	}
	p.applied = nil
	fix, err := scopeReport(map[uint32]int64{12: 7})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.recordScopes(ctx, map[agent.WorkerKey]agent.Observed{WorkerKey(42): {Report: fix}}); err != nil {
		t.Fatal(err)
	}
	if got := scopeKV(t, ctx, srv, junk); string(got.Value) != "7" || got.Lease != 0 {
		t.Fatalf("junk replaced: %q lease %d", got.Value, got.Lease)
	}
	if !bytes.Contains(buf.Bytes(), []byte("scope value replaced")) {
		t.Fatalf("log %q", buf.String())
	}
	buf.Reset()
	if _, err := srv.Client(t).Put(ctx, junk, "y"); err != nil {
		t.Fatal(err)
	}
	p.applied = nil
	if err := p.recordScopes(ctx, map[agent.WorkerKey]agent.Observed{WorkerKey(42): {Report: fix}}); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 0 {
		t.Fatalf("warned twice: %s", buf.String())
	}

	before := scopeKV(t, ctx, srv, key7).ModRevision
	if err := p.sync(ctx, agent.Demand[Entry]{}, map[agent.WorkerKey]agent.Observed{}, func(agent.WorkerKey) bool { return false }); err != nil {
		t.Fatal(err)
	}
	if got := scopeKV(t, ctx, srv, key7); got.ModRevision != before || string(got.Value) != "40" {
		t.Fatalf("empty observe deleted scope: %q rev %d", got.Value, got.ModRevision)
	}
}

func scopeReport(m map[uint32]int64) (*workerv1.Report, error) {
	b, err := proto.Marshal(&tdctlv1.Report{FirstSubmitNs: m})
	if err != nil {
		return nil, err
	}
	return &workerv1.Report{Contract: account.Contract, Payload: b}, nil
}

func scopeKV(t *testing.T, ctx context.Context, srv *etcdtest.Server, key string) *mvccpb.KeyValue {
	t.Helper()
	resp, err := srv.Client(t).Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) != 1 {
		t.Fatalf("%s: %d values", key, len(resp.Kvs))
	}
	return resp.Kvs[0]
}

func scopeAbsent(t *testing.T, ctx context.Context, srv *etcdtest.Server, key string) bool {
	t.Helper()
	resp, err := srv.Client(t).Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	return len(resp.Kvs) == 0
}
