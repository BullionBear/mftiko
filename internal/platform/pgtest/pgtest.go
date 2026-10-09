// Package pgtest runs a throwaway PostgreSQL for tests. It is imported by tests only.
//
// The server binary is $MFTIKO_PG_BIN, else postgres on PATH. initdb is the binary beside it.
// A test without either is skipped, unless $MFTIKO_REQUIRE_PG is set (CI sets it), in which case
// it fails: a run that skipped every Postgres test must not look green.
//
// Each Start is a new cluster on 127.0.0.1 and a free port, with trust authentication and a
// superuser named mftiko. Its database is mftiko. Tests do not use the compose service in
// deploy/dev.
package pgtest

import (
	"bytes"
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
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Environment variables read by Start.
const (
	// EnvBin is the postgres server binary to run. initdb is taken from the same directory.
	EnvBin = "MFTIKO_PG_BIN"
	// EnvRequire, when set to anything, turns a missing binary into a test failure instead of a skip.
	EnvRequire = "MFTIKO_REQUIRE_PG"
)

const (
	readyTimeout = 20 * time.Second
	startTries   = 3 // a free port can be taken between picking it and postgres binding it
)

// Server is one PostgreSQL cluster. It is killed when the test that started it ends.
type Server struct {
	dsn   string
	admin string // postgres maintenance database; mftiko is created from here
	bin   string
	args  []string
	out   *lockedBuffer

	mu   sync.Mutex
	stop func() // kills the running process and waits for it; nil while stopped
}

// Start launches a cluster and waits until it accepts connections. It skips the test if no
// postgres binary is found, and fails it if the server does not come up.
func Start(t testing.TB) *Server {
	t.Helper()
	bin, initdb := binaries()
	if bin == "" || initdb == "" {
		if os.Getenv(EnvRequire) != "" {
			t.Fatalf("pgtest: postgres not found and %s is set: set %s or put postgres on PATH", EnvRequire, EnvBin)
		}
		t.Skipf("postgres not found: set %s or put postgres on PATH", EnvBin)
	}
	var errs []error
	for range startTries {
		s, err := start(t, bin, initdb)
		if err == nil {
			return s
		}
		errs = append(errs, err)
	}
	t.Fatalf("pgtest: postgres did not start: %v", errors.Join(errs...))
	return nil
}

// DSN is the URL of the mftiko database. The user is the superuser mftiko and there is no password.
func (s *Server) DSN() string { return s.dsn }

// ExecSQL runs sql with the simple query protocol, so a migration file may contain more than one
// statement. pgx's extended-protocol Exec does not.
func ExecSQL(ctx context.Context, conn *pgx.Conn, sql string) error {
	return conn.PgConn().Exec(ctx, sql).Close()
}

func binaries() (postgres, initdb string) {
	postgres = look(os.Getenv(EnvBin))
	if postgres == "" {
		postgres = look("postgres")
	}
	if postgres == "" {
		return "", ""
	}
	initdb = look(filepath.Join(filepath.Dir(postgres), "initdb"))
	if initdb == "" {
		return "", ""
	}
	return postgres, initdb
}

func look(p string) string {
	if p == "" {
		return ""
	}
	found, err := exec.LookPath(p)
	if err != nil {
		return ""
	}
	return found
}

func start(t testing.TB, bin, initdb string) (*Server, error) {
	dir := t.TempDir()
	data := filepath.Join(dir, "data")
	sock := filepath.Join(dir, "sock")
	if err := os.Mkdir(sock, 0o700); err != nil {
		return nil, err
	}
	port, err := freePort()
	if err != nil {
		return nil, err
	}
	out := &lockedBuffer{}
	// Locale and encoding follow the machine. Forcing UTF8 with locale C fails
	// on some distributions, and the tests do not depend on either.
	init := exec.Command(initdb,
		"-D", data,
		"--username=mftiko",
		"--auth-local=trust",
		"--auth-host=trust",
		"--no-sync",
	)
	init.Env = cleanEnv()
	init.Stdout = out
	init.Stderr = out
	if err := init.Run(); err != nil {
		return nil, fmt.Errorf("pgtest: initdb: %w\n%s", err, out.String())
	}

	s := &Server{
		dsn:   fmt.Sprintf("postgres://mftiko@127.0.0.1:%d/mftiko?sslmode=disable", port),
		admin: fmt.Sprintf("postgres://mftiko@127.0.0.1:%d/postgres?sslmode=disable", port),
		bin:   bin,
		out:   out,
		args: []string{
			"-D", data,
			"-p", strconv.Itoa(port),
			"-c", "listen_addresses=127.0.0.1",
			"-c", "unix_socket_directories=" + sock,
			"-c", "fsync=off",
			"-c", "synchronous_commit=off",
			"-c", "full_page_writes=off",
			"-c", "log_min_messages=error",
		},
	}
	if err := s.launch(); err != nil {
		return nil, err
	}
	// initdb creates the maintenance database postgres, not one named after --username.
	if err := createDatabase(s.admin); err != nil {
		s.Stop()
		return nil, err
	}
	t.Cleanup(func() {
		s.Stop()
		if t.Failed() {
			t.Logf("postgres output:\n%s", s.out.String())
		}
	})
	return s, nil
}

// Stop kills the server. Its data directory goes away with the test's temporary directory.
func (s *Server) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stop != nil {
		s.stop()
		s.stop = nil
	}
}

// launch starts the process and waits until it accepts connections. Called before s is shared.
func (s *Server) launch() error {
	cmd := exec.Command(s.bin, s.args...)
	cmd.Env = cleanEnv()
	cmd.Stdout = s.out
	cmd.Stderr = s.out
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("pgtest: start %s: %w", s.bin, err)
	}
	exited := make(chan struct{})
	var waitErr error
	go func() {
		waitErr = cmd.Wait()
		close(exited)
	}()
	stop := func() {
		_ = cmd.Process.Kill()
		<-exited
	}
	if err := waitReady(s.admin, exited); err != nil {
		stop()
		return fmt.Errorf("%w (exit: %v)\n%s", err, waitErr, s.out.String())
	}
	s.stop = stop
	return nil
}

func waitReady(dsn string, exited <-chan struct{}) error {
	deadline := time.Now().Add(readyTimeout)
	var err error
	for {
		err = ping(dsn)
		if err == nil {
			return nil
		}
		select {
		case <-exited:
			return errors.New("pgtest: postgres exited before it was ready")
		default:
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("pgtest: not ready after %s: %w", readyTimeout, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func createDatabase(adminDSN string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		return fmt.Errorf("pgtest: connect: %w", err)
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, `CREATE DATABASE mftiko`)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42P04" { // duplicate_database
		return nil
	}
	if err != nil {
		return fmt.Errorf("pgtest: create database: %w", err)
	}
	return nil
}

func ping(dsn string) error {
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
		return 0, fmt.Errorf("pgtest: free port: %w", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port, nil
}

// cleanEnv drops PG* variables so a developer's cluster does not redirect this one.
func cleanEnv() []string {
	var out []string
	for _, e := range os.Environ() {
		if strings.HasPrefix(e, "PG") {
			continue
		}
		out = append(out, e)
	}
	return out
}

// lockedBuffer collects server output; exec copies into it from its own goroutine.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
