package latest

import (
	"context"
	"debug/elf"
	"encoding/binary"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/internal/platform/artifact"
	"github.com/BullionBear/mftiko/internal/platform/artifact/artifacttest"
)

// linuxBinary is an ELF header for linux/amd64 with tail appended, so each tail is a different digest.
func linuxBinary(tail string) []byte {
	buf := make([]byte, 64)
	buf[0], buf[1], buf[2], buf[3] = 0x7f, 'E', 'L', 'F'
	buf[4], buf[5], buf[6] = byte(elf.ELFCLASS64), byte(elf.ELFDATA2LSB), 1
	binary.LittleEndian.PutUint16(buf[16:], uint16(elf.ET_EXEC))
	binary.LittleEndian.PutUint16(buf[18:], uint16(elf.EM_X86_64))
	binary.LittleEndian.PutUint32(buf[20:], 1)
	return append(buf, tail...)
}

const repo = "bullionbear/mftiko-md-conn"

// push packs a worker of kind and tags it latest. It returns the image digest.
func push(t *testing.T, reg *artifacttest.Registry, kind, tail string) string {
	t.Helper()
	return reg.Push(t, repo, Tag, kind, linuxBinary(tail), map[string]string{"A": "1"})
}

func newSource(t *testing.T, reg *artifacttest.Registry) *Source {
	t.Helper()
	s, err := New(Config{
		Registry: artifact.Registry{Address: reg.Addr(), PlainHTTP: true}, DataDir: t.TempDir(),
		Repository: repo, Kind: artifact.KindMDConn, OS: "linux", Arch: "amd64", Log: zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSourceFollowsTag(t *testing.T) {
	reg := artifacttest.New(t)
	s := newSource(t, reg)
	if got := s.Pending(); got != ReasonPulling {
		t.Fatalf("pending before the first pull %q", got)
	}
	if _, err := s.Launch(nil); !errors.Is(err, ErrNotReady) {
		t.Fatalf("launch before the first pull: %v", err)
	}
	ctx := context.Background()

	a := push(t, reg, artifact.KindMDConn, "a")
	if err := s.step(ctx); err != nil {
		t.Fatal(err)
	}
	if s.Digest() != a || s.Pending() != "" {
		t.Fatalf("digest %s pending %q, want %s", s.Digest(), s.Pending(), a)
	}
	l, err := s.Launch([]string{"MFTIKO_NATS_URL=x"})
	if err != nil {
		t.Fatal(err)
	}
	rootA := l.GetDir()
	if l.GetPath() != filepath.Join(rootA, "bin/worker") || !strings.Contains(rootA, strings.TrimPrefix(a, "sha256:")) {
		t.Fatalf("launch %v", l)
	}
	if env := l.GetEnv(); len(env) != 2 || env[0] != "A=1" || env[1] != "MFTIKO_NATS_URL=x" {
		t.Fatalf("env %q", env)
	}

	b := push(t, reg, artifact.KindMDConn, "b")
	if err := s.step(ctx); err != nil {
		t.Fatal(err)
	}
	if s.Digest() != b {
		t.Fatalf("digest %s, want %s", s.Digest(), b)
	}
	// The old rootfs stays: a worker started from it keeps running (P9). GC is E12-03.
	if _, err := os.Stat(filepath.Join(rootA, "bin/worker")); err != nil {
		t.Fatal(err)
	}
}

func TestSourceKeepsReadyOnBadLatest(t *testing.T) {
	reg := artifacttest.New(t)
	s := newSource(t, reg)
	ctx := context.Background()
	good := push(t, reg, artifact.KindMDConn, "good")
	if err := s.step(ctx); err != nil {
		t.Fatal(err)
	}

	bad := push(t, reg, artifact.KindTDAccount, "bad")
	err := s.step(ctx)
	if !artifact.Permanent(err) || artifact.ReasonOf(err) != artifact.ReasonWrongKind {
		t.Fatalf("wrong kind: %v", err)
	}
	if s.Digest() != good || s.Pending() != "" {
		t.Fatalf("digest %s pending %q after a bad latest, want %s", s.Digest(), s.Pending(), good)
	}
	if s.rejected[bad] != artifact.ReasonWrongKind {
		t.Fatalf("rejected %v", s.rejected)
	}
	// A rejected digest is not pulled again.
	if err := s.step(ctx); err != nil {
		t.Fatalf("second step on the rejected digest: %v", err)
	}

	reg.SetDown(true)
	if err := s.step(ctx); artifact.Permanent(err) || artifact.ReasonOf(err) != artifact.ReasonUnavailable {
		t.Fatalf("registry down: %v", err)
	}
	if s.Digest() != good {
		t.Fatalf("digest %s with the registry down", s.Digest())
	}
}

func TestSourceNothingReady(t *testing.T) {
	reg := artifacttest.New(t)
	s := newSource(t, reg)
	reg.SetDown(true)
	if err := s.step(context.Background()); err == nil {
		t.Fatal("step with the registry down")
	}
	if got := s.Pending(); got != artifact.ReasonUnavailable {
		t.Fatalf("pending %q", got)
	}
	_, err := s.Launch(nil)
	if !errors.Is(err, ErrNotReady) || !strings.Contains(err.Error(), artifact.ReasonUnavailable) {
		t.Fatalf("launch: %v", err)
	}

	reg.SetDown(false)
	push(t, reg, artifact.KindTDAccount, "wrong")
	if err := s.step(context.Background()); err == nil {
		t.Fatal("wrong kind pulled")
	}
	if got := s.Pending(); got != artifact.ReasonWrongKind {
		t.Fatalf("pending %q", got)
	}
}

func TestSourceRunRefresh(t *testing.T) {
	reg := artifacttest.New(t)
	s := newSource(t, reg)
	s.cfg.Poll = time.Hour
	a := push(t, reg, artifact.KindMDConn, "a")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	waitDigest(t, s, a)
	b := push(t, reg, artifact.KindMDConn, "b")
	s.Refresh()
	waitDigest(t, s, b)
	cancel()
	<-done
}

func waitDigest(t *testing.T, s *Source, want string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for s.Digest() != want {
		if time.Now().After(deadline) {
			t.Fatalf("digest %q, want %q", s.Digest(), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestNewRejectsKind(t *testing.T) {
	for _, kind := range []string{"", artifact.KindStrategy, "session"} {
		if _, err := New(Config{Registry: artifact.Registry{Address: "r:5000"}, DataDir: t.TempDir(), Repository: repo, Kind: kind}); err == nil {
			t.Fatalf("kind %q accepted", kind)
		}
	}
}

func TestFlagsLauncher(t *testing.T) {
	parse := func(args ...string) Flags {
		var f Flags
		fs := flag.NewFlagSet("t", flag.ContinueOnError)
		f.Register(fs, RepoMDConn)
		if err := fs.Parse(args); err != nil {
			t.Fatal(err)
		}
		return f
	}
	if f := parse(); f.Repository != RepoMDConn || f.Poll != DefaultPoll || f.MaxBytes != artifact.DefaultMaxBytes {
		t.Fatalf("defaults %+v", f)
	}
	if _, err := parse().Launcher(artifact.KindMDConn, "conn", "", zerolog.Nop()); err == nil {
		t.Fatal("neither -conn nor -registry")
	}
	if _, err := parse("-registry", "r:5000", "-data", t.TempDir()).Launcher(artifact.KindMDConn, "conn", "/bin/conn", zerolog.Nop()); err == nil {
		t.Fatal("both -conn and -registry")
	}
	if _, err := parse("-registry", "r:5000").Launcher(artifact.KindMDConn, "conn", "", zerolog.Nop()); err == nil {
		t.Fatal("-registry without -data")
	}
	l, err := parse().Launcher(artifact.KindMDConn, "conn", "/bin/conn", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := l.(Local); !ok {
		t.Fatalf("local %T", l)
	}
	l, err = parse("-registry", "r:5000", "-data", t.TempDir()).Launcher(artifact.KindMDConn, "conn", "", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := l.(*Source); !ok {
		t.Fatalf("registry %T", l)
	}
}
