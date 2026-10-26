package natsdev

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

func TestCheckEmbedded(t *testing.T) {
	for _, inst := range []string{"dev", "box"} {
		t.Run(inst, func(t *testing.T) {
			dir := t.TempDir()
			opt := Options{}
			if inst != "dev" {
				opt = Options{MDInsts: []string{inst}, FetchInsts: []string{inst}, SymInsts: []string{inst}}
			}
			if err := Generate(dir, opt); err != nil {
				t.Fatal(err)
			}
			lay, err := Load(dir)
			if err != nil {
				t.Fatal(err)
			}
			ns := startAuthServer(t, dir, lay.Manifest.System)
			if err := Check(dir, ns.ClientURL()); err != nil {
				t.Fatal(err)
			}
			err = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if strings.Contains(d.Name(), "session") || strings.Contains(d.Name(), "smoke") {
					t.Errorf("session material was written: %s", d.Name())
				}
				if strings.Contains(d.Name(), "account") {
					t.Errorf("account worker material was written: %s", d.Name())
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestCheckCatchesBroadAccountWorker: an account worker signed with more than its own account
// fails the check, so the check does not pass on a user that could serve another account.
func TestCheckCatchesBroadAccountWorker(t *testing.T) {
	dir := t.TempDir()
	if err := Generate(dir, Options{}); err != nil {
		t.Fatal(err)
	}
	lay, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	ns := startAuthServer(t, dir, lay.Manifest.System)
	good, err := newAccountWorker(lay, checkAPIID)
	if err != nil {
		t.Fatal(err)
	}
	if err := denyWorker(ns.ClientURL(), good); err != nil {
		t.Fatal(err)
	}
	inbox, err := TDAccountInbox(checkAPIID)
	if err != nil {
		t.Fatal(err)
	}
	pub, sub, err := tdAccountPerms(checkAPIID)
	if err != nil {
		t.Fatal(err)
	}
	broad, err := signMem(lay, "td-account-broad", inbox, pub, append(sub, "td.order.>"), true)
	if err != nil {
		t.Fatal(err)
	}
	if err := denyWorker(ns.ClientURL(), broad); err == nil {
		t.Fatal("an account worker that may subscribe to td.order.> passed")
	}
}

func startAuthServer(t *testing.T, dir, system string) *server.Server {
	t.Helper()
	conf, err := FormatServerConfig(ServerConfig{
		OperatorFile:  filepath.Join(dir, "operator.jwt"),
		SystemAccount: system,
		ResolverDir:   filepath.Join(dir, "resolver"),
		Host:          "127.0.0.1",
		Port:          -1,
		ServerName:    "dev",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, banned := range []string{"tls", "cluster", "jetstream"} {
		if strings.Contains(conf, banned) {
			t.Fatalf("config contains %q", banned)
		}
	}
	if !strings.Contains(conf, "type: full") || !strings.Contains(conf, system) {
		t.Fatalf("config %s", conf)
	}
	confPath := filepath.Join(t.TempDir(), "nats-server.conf")
	if err := os.WriteFile(confPath, []byte(conf), 0o644); err != nil {
		t.Fatal(err)
	}
	opts, err := server.ProcessConfigFile(confPath)
	if err != nil {
		t.Fatal(err)
	}
	opts.NoLog = true
	opts.NoSigs = true
	if opts.Port != -1 {
		t.Fatalf("port %d", opts.Port)
	}
	if opts.TLSConfig != nil || opts.JetStream || opts.Cluster.Name != "" || opts.Cluster.Port != 0 {
		t.Fatalf("tls %v jetstream %v cluster %q port %d", opts.TLSConfig != nil, opts.JetStream, opts.Cluster.Name, opts.Cluster.Port)
	}
	if opts.SystemAccount != system {
		t.Fatalf("system account %s", opts.SystemAccount)
	}
	ns, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	ns.Start()
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	if !ns.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats-server did not accept connections")
	}
	if ns.JetStreamEnabled() {
		t.Fatal("jetstream is enabled")
	}
	if _, ok := ns.AccountResolver().(*server.DirAccResolver); !ok {
		t.Fatalf("resolver %T", ns.AccountResolver())
	}
	return ns
}
