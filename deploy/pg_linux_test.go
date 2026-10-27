package release_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/BullionBear/mftiko/internal/platform/pgtest"
)

// sharedPG is the one PostgreSQL for this package. TestMain starts it and stops
// it after the tests. A test that needs a database calls dsn, which clones an
// empty one. Tests that do not call it leave the server alone.
var sharedPG *pgServer

const pgReadyTimeout = 20 * time.Second

// pgServer is one cluster on 127.0.0.1. Trust authentication, superuser mftiko,
// no password: the same shape as pgtest, started once instead of per test.
type pgServer struct {
	admin  string
	port   int
	out    *outBuf
	stop   func()
	absent bool
	err    error
	seq    atomic.Uint64
}

// startPostgres launches the cluster. A missing binary is not an error here:
// dsn skips, or fails the test when MFTIKO_REQUIRE_PG is set, as pgtest.Start does.
func startPostgres() *pgServer {
	bin, initdb := pgBinaries()
	if bin == "" || initdb == "" {
		return &pgServer{absent: true, stop: func() {}}
	}
	var errs []error
	for range 3 {
		s, err := launchPostgres(bin, initdb)
		if err == nil {
			return s
		}
		errs = append(errs, err)
	}
	return &pgServer{err: errors.Join(errs...), stop: func() {}}
}

func (p *pgServer) close() {
	if p == nil || p.stop == nil {
		return
	}
	p.stop()
}

// dsn is a new empty database on the shared server. CREATE DATABASE copies
// template0, which has no objects and accepts no connections, so the copy
// cannot see another test's schema_migrations. The release tests run
// mftiko-migrate themselves and assert that first apply.
func (p *pgServer) dsn(t *testing.T) string {
	t.Helper()
	if p == nil || p.absent {
		if os.Getenv(pgtest.EnvRequire) != "" {
			t.Fatalf("pgtest: postgres not found and %s is set: set %s or put postgres on PATH", pgtest.EnvRequire, pgtest.EnvBin)
		}
		t.Skipf("postgres not found: set %s or put postgres on PATH", pgtest.EnvBin)
	}
	if p.err != nil {
		t.Fatalf("pgtest: postgres did not start: %v", p.err)
	}
	name := fmt.Sprintf("d%d", p.seq.Add(1))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, p.admin)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	createStarted := time.Now()
	if err := pgtest.ExecSQL(ctx, conn, "CREATE DATABASE "+name+" WITH TEMPLATE template0"); err != nil {
		t.Fatal(err)
	}
	measureLog(t.Name(), "setup", "create_database", time.Since(createStarted).Milliseconds(), "")
	mark := p.out.Len()
	t.Cleanup(func() {
		if t.Failed() {
			if s := p.out.Since(mark); s != "" {
				t.Logf("postgres output:\n%s", s)
			}
		}
	})
	return fmt.Sprintf("postgres://mftiko@127.0.0.1:%d/%s?sslmode=disable", p.port, name)
}

func pgBinaries() (postgres, initdb string) {
	postgres = lookBin(os.Getenv(pgtest.EnvBin))
	if postgres == "" {
		postgres = lookBin("postgres")
	}
	if postgres == "" {
		return "", ""
	}
	initdb = lookBin(filepath.Join(filepath.Dir(postgres), "initdb"))
	if initdb == "" {
		return "", ""
	}
	return postgres, initdb
}

func lookBin(p string) string {
	if p == "" {
		return ""
	}
	found, err := exec.LookPath(p)
	if err != nil {
		return ""
	}
	return found
}

func launchPostgres(bin, initdb string) (*pgServer, error) {
	dir, err := os.MkdirTemp("", "mftiko-pg-")
	if err != nil {
		return nil, err
	}
	data := filepath.Join(dir, "data")
	sock := filepath.Join(dir, "sock")
	if err := os.Mkdir(sock, 0o700); err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	port, err := freePort()
	if err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	out := &outBuf{}
	init := exec.Command(initdb,
		"-D", data,
		"--username=mftiko",
		"--auth-local=trust",
		"--auth-host=trust",
		"--no-sync",
	)
	init.Env = cleanPGEnv()
	init.Stdout, init.Stderr = out, out
	initStarted := time.Now()
	if err := init.Run(); err != nil {
		err = fmt.Errorf("initdb: %w\n%s", err, out.Since(0))
		os.RemoveAll(dir)
		return nil, err
	}
	measureLog("-", "setup", "initdb", time.Since(initStarted).Milliseconds(), "")
	admin := fmt.Sprintf("postgres://mftiko@127.0.0.1:%d/postgres?sslmode=disable", port)
	cmd := exec.Command(bin,
		"-D", data,
		"-p", strconv.Itoa(port),
		"-c", "listen_addresses=127.0.0.1",
		"-c", "unix_socket_directories="+sock,
		"-c", "fsync=off",
		"-c", "synchronous_commit=off",
		"-c", "full_page_writes=off",
		"-c", "log_min_messages=error",
	)
	cmd.Env = cleanPGEnv()
	cmd.Stdout, cmd.Stderr = out, out
	execStarted := time.Now()
	if err := cmd.Start(); err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	measureLog("-", "setup", "postgres_exec", time.Since(execStarted).Milliseconds(), "")
	exited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(exited)
	}()
	stop := func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			<-exited
		}
		os.RemoveAll(dir)
	}
	if err := waitPG(admin, exited); err != nil {
		stop()
		return nil, fmt.Errorf("%w\n%s", err, out.Since(0))
	}
	return &pgServer{admin: admin, port: port, out: out, stop: stop}, nil
}

func waitPG(dsn string, exited <-chan struct{}) error {
	started := time.Now()
	sleeps := 0
	defer func() {
		measureLog("-", "setup", "postgres_ready", time.Since(started).Milliseconds(), fmt.Sprintf("\tsleep_ms=%d\tsleeps=%d", sleeps*20, sleeps))
	}()
	deadline := time.Now().Add(pgReadyTimeout)
	var err error
	for {
		err = pgPing(dsn)
		if err == nil {
			return nil
		}
		select {
		case <-exited:
			return errors.New("postgres exited before it was ready")
		default:
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("not ready after %s: %w", pgReadyTimeout, err)
		}
		time.Sleep(20 * time.Millisecond)
		sleeps++
	}
}

func pgPing(dsn string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	return conn.Ping(ctx)
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port, nil
}

// cleanPGEnv drops PG* variables so a developer's cluster does not redirect this one.
func cleanPGEnv() []string {
	var out []string
	for _, e := range os.Environ() {
		if strings.HasPrefix(e, "PG") {
			continue
		}
		out = append(out, e)
	}
	return out
}

// outBuf collects server output. The process writes it from another goroutine.
type outBuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (b *outBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *outBuf) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Len()
}

func (b *outBuf) Since(mark int) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.b.String()
	if mark > len(s) {
		return ""
	}
	return s[mark:]
}
