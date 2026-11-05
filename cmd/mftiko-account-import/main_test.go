package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/accountkey"
	"github.com/BullionBear/mftiko/internal/platform/pgtest"
	"github.com/BullionBear/mftiko/internal/platform/sealbox"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
	"github.com/BullionBear/mftiko/migrations"
)

const (
	inst1 = "td-1"
	inst2 = "td-2"
)

// fixture is made at run time so the secret is easy to look for in output.
func fixture(tag string) string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return tag + "-" + hex.EncodeToString(b)
}

var creds = sealbox.Credentials{KeyType: "hmac", Key: fixture("key"), Secret: fixture("sec"), Passphrase: fixture("pp")}

func noSecret(t *testing.T, where string, b []byte) {
	t.Helper()
	for _, s := range []string{creds.Key, creds.Secret, creds.Passphrase} {
		if bytes.Contains(b, []byte(s)) {
			t.Fatalf("%s shows a credential", where)
		}
	}
}

func mustKey(t *testing.T) *sealbox.PrivateKey {
	t.Helper()
	k, err := sealbox.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	return k
}

type etcdEnv struct {
	topo state.Topology
	root *clientv3.Client
	path string // topology file
}

func startEtcd(t *testing.T) etcdEnv {
	t.Helper()
	s := etcdtest.Start(t)
	topo := state.Topology{
		Regions:   map[string]state.Region{"tw": {Endpoints: s.Endpoints()}},
		Instances: map[string]string{inst1: "tw", inst2: "tw", state.ControllerInstance: "tw"},
	}
	tables, err := state.AuthTables(topo, map[string]state.Plane{inst1: state.PlaneTD, inst2: state.PlaneTD})
	if err != nil {
		t.Fatal(err)
	}
	root := s.Client(t)
	if err := state.EnsureAuth(t.Context(), root, tables["tw"], etcdtest.Password); err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(topo)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "topology.json")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return etcdEnv{topo: topo, root: root, path: path}
}

// putOld writes an account the way the pre-E21 API did: sealed to the TD key, in etcd only.
func putOld(t *testing.T, root *clientv3.Client, inst string, id int64, name, kind string, pub sealbox.PublicKey, cancel uint32) []byte {
	t.Helper()
	sealed, err := sealbox.SealCredentials(pub, id, kind, creds)
	if err != nil {
		t.Fatal(err)
	}
	b, err := proto.Marshal(&statev1.Account{
		Name: name, Kind: kind, SealedCredentials: sealed,
		Settings: &statev1.AccountSettings{CancelOnDisconnectS: cancel}, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := root.Put(t.Context(), state.AccountKey(inst, id), string(b)); err != nil {
		t.Fatal(err)
	}
	return sealed
}

// runExport is the export subcommand as the TD host runs it, with the instance's own etcd user.
func runExport(t *testing.T, env etcdEnv, inst string, key *sealbox.PrivateKey, transfer sealbox.PublicKey) (string, string) {
	t.Helper()
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "seal.key")
	if err := sealbox.WriteKeyFile(keyPath, key); err != nil {
		t.Fatal(err)
	}
	pubPath := filepath.Join(dir, "transfer.pub")
	if err := os.WriteFile(pubPath, []byte(transfer.String()+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, inst+".json")
	var stdout, stderr bytes.Buffer
	err := run([]string{"export",
		"-topology", env.path, "-instance", inst, "-etcd-password", etcdtest.Password(inst),
		"-seal-key", keyPath, "-transfer-pubkey", "@" + pubPath, "-out", out,
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("export: %v\n%s", err, stderr.String())
	}
	fi, err := os.Stat(out)
	if err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("export file mode %v %v", fi, err)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	noSecret(t, "export file", b)
	noSecret(t, "export stdout", stdout.Bytes())
	return out, stdout.String()
}

func TestExport(t *testing.T) {
	env := startEtcd(t)
	tdKey, transfer, other := mustKey(t), mustKey(t), mustKey(t)
	sealed := putOld(t, env.root, inst1, 41, "desk", "BinanceUM", tdKey.Public(), 30)
	putOld(t, env.root, inst1, 42, "elsewhere", "BinanceUM", other.Public(), 0)
	if _, err := env.root.Put(t.Context(), state.AccountPrefix(inst1)+"junk", "x"); err != nil {
		t.Fatal(err)
	}
	putOld(t, env.root, inst2, 43, "not mine", "Okx", tdKey.Public(), 0)

	path, stdout := runExport(t, env, inst1, tdKey, transfer.Public())
	f, err := readExport(path)
	if err != nil {
		t.Fatal(err)
	}
	if f.TDInstance != inst1 || len(f.Accounts) != 1 {
		t.Fatalf("export %+v", f)
	}
	a := f.Accounts[0]
	if a.APIID != 41 || a.Name != "desk" || a.Kind != "BinanceUM" || a.Cancel != 30 || !a.Enabled || !bytes.Equal(a.Replica, sealed) {
		t.Fatalf("account %+v", a)
	}
	got, err := sealbox.OpenCredentials(transfer, a.Transfer, 41, "BinanceUM")
	if err != nil || got != creds {
		t.Fatalf("transfer blob: %v", err)
	}
	reasons := map[string]string{}
	for _, s := range f.Skipped {
		reasons[s.Key] = s.Reason
	}
	if reasons[state.AccountKey(inst1, 42)] != "credentials_other_key" || reasons[state.AccountPrefix(inst1)+"junk"] != "not an account key" || len(reasons) != 2 {
		t.Fatalf("skipped %v", f.Skipped)
	}
	if !strings.Contains(stdout, "exported 1 account(s) of td-1") {
		t.Fatalf("stdout %q", stdout)
	}

	// The file is never overwritten.
	var out bytes.Buffer
	keyPath := filepath.Join(t.TempDir(), "k")
	if err := sealbox.WriteKeyFile(keyPath, tdKey); err != nil {
		t.Fatal(err)
	}
	err = run([]string{"export", "-topology", env.path, "-instance", inst1, "-etcd-password", etcdtest.Password(inst1),
		"-seal-key", keyPath, "-transfer-pubkey", transfer.Public().String(), "-out", path}, &out, &out)
	if err == nil {
		t.Fatal("export overwrote its file")
	}

	// The instance user cannot read another instance's accounts.
	err = run([]string{"export", "-topology", env.path, "-instance", inst2, "-etcd-user", inst1,
		"-etcd-password", etcdtest.Password(inst1), "-seal-key", keyPath,
		"-transfer-pubkey", transfer.Public().String(), "-out", filepath.Join(t.TempDir(), "x.json")}, &out, &out)
	if err == nil {
		t.Fatal("td-1 read td-2's accounts")
	}
}

var testKeys = func() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return "v3:" + base64.StdEncoding.EncodeToString(b)
}()

type pgEnv struct {
	dsn  string
	conn *pgx.Conn
}

func startPG(t *testing.T) pgEnv {
	t.Helper()
	pg := pgtest.Start(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := migrations.Up(ctx, pg.DSN()); err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(t.Context(), pg.DSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return pgEnv{dsn: pg.DSN(), conn: conn}
}

func writeJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "f.json")
	if err := os.WriteFile(p, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func runImport(t *testing.T, pg pgEnv, transfer string, uids map[string]string, apply bool, files ...string) (string, error) {
	t.Helper()
	t.Setenv("MFTIKO_ACCOUNT_KEYS", testKeys)
	t.Setenv("MFTIKO_POSTGRES_DSN", pg.dsn)
	args := []string{"import", "-transfer-key", transfer, "-uids", writeJSON(t, uids)}
	if apply {
		args = append(args, "-apply")
	}
	var out bytes.Buffer
	err := run(append(args, files...), &out, &out)
	noSecret(t, "import output", out.Bytes())
	return out.String(), err
}

func count(t *testing.T, pg pgEnv, q string, args ...any) int {
	t.Helper()
	var n int
	if err := pg.conn.QueryRow(t.Context(), q, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestExportImport is the whole move: an old etcd account on two TD instances becomes one
// accounts row with the same api_id and two replica rows holding the etcd bytes unchanged.
func TestExportImport(t *testing.T) {
	env := startEtcd(t)
	pg := startPG(t)
	k1, k2, transfer := mustKey(t), mustKey(t), mustKey(t)
	const id = int64(8172133045237979)
	s1 := putOld(t, env.root, inst1, id, "scylla", "BinanceUM", k1.Public(), 15)
	s2 := putOld(t, env.root, inst2, id, "scylla", "BinanceUM", k2.Public(), 15)
	f1, _ := runExport(t, env, inst1, k1, transfer.Public())
	f2, _ := runExport(t, env, inst2, k2, transfer.Public())
	tkey := filepath.Join(t.TempDir(), "transfer.key")
	if err := sealbox.WriteKeyFile(tkey, transfer); err != nil {
		t.Fatal(err)
	}
	ids := strconv.FormatInt(id, 10)

	// No uid: the plan names the problem and nothing is written.
	out, err := runImport(t, pg, tkey, map[string]string{}, true, f1, f2)
	if err == nil || !strings.Contains(out, "exch_account_uid missing") {
		t.Fatalf("missing uid: %v\n%s", err, out)
	}
	if n := count(t, pg, `SELECT count(*) FROM accounts`); n != 0 {
		t.Fatalf("%d accounts after a refused import", n)
	}

	// Dry run: the plan, then a rollback.
	out, err = runImport(t, pg, tkey, map[string]string{ids: " 12345678 "}, false, f1, f2)
	if err != nil || strings.Count(out, "insert") != 2 || !strings.Contains(out, "dry run") {
		t.Fatalf("dry run: %v\n%s", err, out)
	}
	if n := count(t, pg, `SELECT count(*) FROM accounts`); n != 0 {
		t.Fatalf("dry run wrote %d accounts", n)
	}

	out, err = runImport(t, pg, tkey, map[string]string{ids: " 12345678 "}, true, f1, f2)
	if err != nil || !strings.Contains(out, "committed") {
		t.Fatalf("apply: %v\n%s", err, out)
	}
	var (
		kind, name, uid, settings string
		cred                      []byte
		ver, rev                  int64
		enabled                   bool
	)
	err = pg.conn.QueryRow(t.Context(), `
SELECT kind, name, exch_account_uid, settings::text, credentials, key_version, revision, enabled
FROM accounts WHERE api_id = $1 AND deleted_at IS NULL`, id).Scan(&kind, &name, &uid, &settings, &cred, &ver, &rev, &enabled)
	if err != nil {
		t.Fatal(err)
	}
	if kind != "BinanceUM" || name != "scylla" || uid != "12345678" || ver != 3 || rev != 1 || !enabled || !strings.Contains(settings, `"cancel_on_disconnect_s": 15`) {
		t.Fatalf("row %s %s %s %s v%d r%d %v", kind, name, uid, settings, ver, rev, enabled)
	}
	noSecret(t, "credentials column", cred)
	box, err := accountkey.Parse(testKeys)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := box.Open(id, "BinanceUM", cred)
	if err != nil {
		t.Fatal(err)
	}
	var msg statev1.AccountCredentials
	if err := proto.Unmarshal(plain, &msg); err != nil {
		t.Fatal(err)
	}
	if msg.ApiId != id || msg.Kind != "BinanceUM" || msg.KeyType != creds.KeyType || msg.Key != creds.Key || msg.Secret != creds.Secret || msg.Passphrase != creds.Passphrase {
		t.Fatal("AEAD plaintext differs from the etcd credentials")
	}
	for inst, want := range map[string][]byte{inst1: s1, inst2: s2} {
		var got []byte
		if err := pg.conn.QueryRow(t.Context(), `SELECT sealed_credentials FROM account_replicas WHERE td_instance = $1 AND api_id = $2`, inst, id).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%s replica differs from the etcd bytes", inst)
		}
	}

	// Again: nothing changes.
	out, err = runImport(t, pg, tkey, map[string]string{ids: "12345678"}, true, f1, f2)
	if err != nil || strings.Count(out, "exists") != 2 {
		t.Fatalf("second apply: %v\n%s", err, out)
	}
	if n := count(t, pg, `SELECT revision FROM accounts WHERE api_id = $1`, id); n != 1 {
		t.Fatalf("revision %d after a second import", n)
	}

	// A lost replica row comes back; the account row is left alone.
	if _, err := pg.conn.Exec(t.Context(), `DELETE FROM account_replicas WHERE td_instance = $1`, inst2); err != nil {
		t.Fatal(err)
	}
	out, err = runImport(t, pg, tkey, map[string]string{ids: "12345678"}, true, f1, f2)
	if err != nil || !strings.Contains(out, "add replica") {
		t.Fatalf("replica: %v\n%s", err, out)
	}
}

func TestImportRefuses(t *testing.T) {
	env := startEtcd(t)
	pg := startPG(t)
	k1, transfer := mustKey(t), mustKey(t)
	putOld(t, env.root, inst1, 51, "taken", "BinanceUM", k1.Public(), 0)
	putOld(t, env.root, inst1, 52, "kind", "BinanceUM", k1.Public(), 0)
	f1, _ := runExport(t, env, inst1, k1, transfer.Public())
	tkey := filepath.Join(t.TempDir(), "transfer.key")
	if err := sealbox.WriteKeyFile(tkey, transfer); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for _, q := range []struct {
		id         int64
		kind, name string
	}{{99, "Okx", "taken"}, {52, "Okx", "other"}} {
		if _, err := pg.conn.Exec(t.Context(), `
INSERT INTO accounts (api_id, kind, name, credentials, key_version, settings, enabled, exch_account_uid, created_at, updated_at, revision)
VALUES ($1, $2, $3, $4, 1, '{}', true, 'u', $5, $5, 1)`, q.id, q.kind, q.name, make([]byte, 44), now); err != nil {
			t.Fatal(err)
		}
	}
	uids := map[string]string{"51": "u", "52": "u"}
	out, err := runImport(t, pg, tkey, uids, true, f1)
	if err == nil || !strings.Contains(out, "name is already used") || !strings.Contains(out, "kind Okx") {
		t.Fatalf("%v\n%s", err, out)
	}
	if n := count(t, pg, `SELECT count(*) FROM account_replicas`); n != 0 {
		t.Fatalf("%d replicas after a refused import", n)
	}

	// The wrong transfer key does not open the blobs.
	wrong := filepath.Join(t.TempDir(), "wrong.key")
	if err := sealbox.WriteKeyFile(wrong, mustKey(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := pg.conn.Exec(t.Context(), `DELETE FROM accounts`); err != nil {
		t.Fatal(err)
	}
	out, err = runImport(t, pg, wrong, uids, false, f1)
	if err == nil || !strings.Contains(out, "credentials_other_key") {
		t.Fatalf("wrong key: %v\n%s", err, out)
	}
}

func TestImportNeedsSchema(t *testing.T) {
	pg := pgtest.Start(t)
	t.Setenv("MFTIKO_ACCOUNT_KEYS", testKeys)
	t.Setenv("MFTIKO_POSTGRES_DSN", pg.DSN())
	tkey := filepath.Join(t.TempDir(), "transfer.key")
	if err := sealbox.WriteKeyFile(tkey, mustKey(t)); err != nil {
		t.Fatal(err)
	}
	f := writeJSON(t, exportFile{Version: fileVersion, TDInstance: inst1})
	var out bytes.Buffer
	err := run([]string{"import", "-transfer-key", tkey, "-uids", writeJSON(t, map[string]string{}), f}, &out, &out)
	if err == nil || !strings.Contains(err.Error(), "mftiko-migrate up") {
		t.Fatalf("%v", err)
	}
}

func TestUsage(t *testing.T) {
	var out bytes.Buffer
	if err := run(nil, &out, &out); err == nil {
		t.Fatal("no subcommand")
	}
	if err := run([]string{"-version"}, &out, &out); err != nil || out.Len() == 0 {
		t.Fatalf("-version: %v", err)
	}
	t.Setenv("MFTIKO_POSTGRES_DSN", "")
	if err := run([]string{"import", "-transfer-key", "k", "-uids", "u", "f.json"}, &out, &out); err == nil || !strings.Contains(err.Error(), "DSN") {
		t.Fatalf("no dsn: %v", err)
	}
}
