package agent

import (
	"bytes"
	"context"
	"debug/elf"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/rs/zerolog"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/artifact"
	"github.com/BullionBear/mftiko/internal/platform/artifact/artifacttest"
)

func TestLatestProtect(t *testing.T) {
	reg := artifacttest.New(t)
	data := t.TempDir()
	c := openArtifact(t, reg, data)
	hello := reg.Push(t, "hello", latestTag, artifact.KindMDConn, elfBin("hello"), nil)
	other := reg.Push(t, "other", latestTag, artifact.KindMDConn, elfBin("other"), nil)
	helloOld := plantRepo(t, data, "hello", "hello-old")
	otherOld := plantRepo(t, data, "other", "other-old")
	plantAt(t, data, "hello", hello)
	plantAt(t, data, "other", other)
	missing := plantRepo(t, data, "missing", "missing-only")
	var log bytes.Buffer
	tags := newLatestTags(c, time.Hour, zerolog.New(&log))

	// Nothing has resolved yet, so every recorded rootfs is kept.
	equalDigests(t, mustProtect(t, tags), hello, helloOld, other, otherOld, missing)

	tags.resolve(context.Background())
	equalDigests(t, mustProtect(t, tags), hello, other, missing)

	// protect reads the cache. A registry outage does not change it.
	reg.SetDown(true)
	equalDigests(t, mustProtect(t, tags), hello, other, missing)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	tags.resolve(cancelled)
	equalDigests(t, mustProtect(t, tags), hello, other, missing)

	tags.resolve(context.Background())
	equalDigests(t, mustProtect(t, tags), hello, helloOld, other, otherOld, missing)
}

func TestLatestRunFollowsTag(t *testing.T) {
	reg := artifacttest.New(t)
	data := t.TempDir()
	c := openArtifact(t, reg, data)
	a := reg.Push(t, "hello", latestTag, artifact.KindMDConn, elfBin("a"), nil)
	plantAt(t, data, "hello", a)
	tags := newLatestTags(c, 0, zerolog.Nop())
	if tags.poll != time.Minute {
		t.Fatalf("poll %s", tags.poll)
	}
	tags.poll = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { tags.Run(ctx); close(done) }()
	waitProtect(t, tags, a)
	b := reg.Push(t, "hello", latestTag, artifact.KindMDConn, elfBin("b"), nil)
	waitProtect(t, tags, b)
	cancel()
	<-done
}

func TestCollectKeepsLatestAndPin(t *testing.T) {
	reg := artifacttest.New(t)
	data := t.TempDir()
	c := openArtifact(t, reg, data)
	latest := reg.Push(t, "hello", latestTag, artifact.KindMDConn, elfBin("latest"), nil)
	old := plantRepo(t, data, "hello", "old")
	plantAt(t, data, "hello", latest)
	plainLatest := reg.Push(t, "plain", latestTag, artifact.KindMDConn, elfBin("plain"), nil)
	plantPlain(t, data, plainLatest)
	plainOld := plantRepo(t, data, "plain", "plain-old")
	missing := plantRepo(t, data, "missing", "missing")
	solo := plantPlain(t, data, digest.FromString("solo").String())
	pinned := plantPlain(t, data, digest.FromString("pinned").String())
	done := plantRepo(t, data, "hello", "done")
	junk := filepath.Join(data, "artifacts", "not-a-digest", "rootfs")
	if err := os.MkdirAll(junk, 0o755); err != nil {
		t.Fatal(err)
	}

	tags := newLatestTags(c, time.Hour, zerolog.Nop())
	tags.resolve(context.Background())
	reg.SetDown(true)
	p := collectPlane(t, c, tags, data)
	p.collect(map[string]*sess{
		"live": {parsed: &sessionv1.SessionSpec{Strategy: &sessionv1.StrategyRef{Code: pinned}}},
		"done": {
			parsed: &sessionv1.SessionSpec{Strategy: &sessionv1.StrategyRef{Code: done}},
			local:  &statusView{Phase: statev1.Phase_PHASE_DONE},
		},
	})
	assertRootfs(t, data, latest, true)
	assertRootfs(t, data, plainLatest, true)
	assertRootfs(t, data, missing, true)
	assertRootfs(t, data, pinned, true)
	assertRootfs(t, data, old, false)
	assertRootfs(t, data, plainOld, false)
	assertRootfs(t, data, solo, false)
	assertRootfs(t, data, done, false)
	if _, err := os.Stat(junk); !os.IsNotExist(err) {
		t.Fatalf("non-digest directory: %v", err)
	}

	// The outage is now in the cache, so the next round keeps every recorded rootfs.
	tags.resolve(context.Background())
	again := plantRepo(t, data, "hello", "again")
	p.collect(nil)
	assertRootfs(t, data, again, true)
	assertRootfs(t, data, latest, true)
}

func TestCollectProtectErrorWarns(t *testing.T) {
	reg := artifacttest.New(t)
	data := t.TempDir()
	c := openArtifact(t, reg, data)
	layer := digest.FromString("orphan-layer").String()
	if err := os.MkdirAll(filepath.Join(data, "layers"), 0o755); err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(data, "layers", layer)
	if err := os.WriteFile(orphan, []byte("layer"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(data, "artifacts")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(data, "artifacts"), []byte("not-a-directory"), 0o644); err != nil {
		t.Fatal(err)
	}
	var log bytes.Buffer
	p := collectPlane(t, c, newLatestTags(c, time.Hour, zerolog.Nop()), data)
	p.log = zerolog.New(&log).Level(zerolog.WarnLevel)
	p.collect(nil)
	if _, err := os.Stat(orphan); err != nil {
		t.Fatalf("gc ran after protect failed: %v", err)
	}
	if !bytes.Contains(log.Bytes(), []byte("artifact gc")) {
		t.Fatalf("log %s", log.Bytes())
	}
}

func TestCollectNilTagsKeepsPinsOnly(t *testing.T) {
	reg := artifacttest.New(t)
	data := t.TempDir()
	c := openArtifact(t, reg, data)
	pinned := plantPlain(t, data, digest.FromString("pinned").String())
	loose := plantPlain(t, data, digest.FromString("loose").String())
	p := collectPlane(t, c, nil, data)
	p.collect(map[string]*sess{
		"live": {parsed: &sessionv1.SessionSpec{Strategy: &sessionv1.StrategyRef{Code: pinned}}},
	})
	assertRootfs(t, data, pinned, true)
	assertRootfs(t, data, loose, false)
}

func openArtifact(t *testing.T, reg *artifacttest.Registry, data string) *artifact.Client {
	t.Helper()
	c, err := artifact.Open(artifact.Registry{Address: reg.Addr(), PlainHTTP: true}, data)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func collectPlane(t *testing.T, c *artifact.Client, tags *latestTags, data string) *plane {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &plane{
		art: c, tags: tags, log: zerolog.Nop(),
		pulls: newPuller(ctx, c, data, 0, zerolog.Nop(), time.Now),
	}
}

func mustProtect(t *testing.T, tags *latestTags) []string {
	t.Helper()
	got, err := tags.protect()
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func waitProtect(t *testing.T, tags *latestTags, want ...string) {
	t.Helper()
	slices.Sort(want)
	deadline := time.Now().Add(10 * time.Second)
	for {
		got := mustProtect(t, tags)
		if slices.Equal(got, want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("protect %v, want %v", got, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func equalDigests(t *testing.T, got []string, want ...string) {
	t.Helper()
	slices.Sort(want)
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !slices.Equal(got, want) {
		t.Fatalf("digests %v, want %v", got, want)
	}
}

func plantRepo(t *testing.T, data, repo, label string) string {
	t.Helper()
	return plantAt(t, data, repo, digest.FromString(label).String())
}

func plantAt(t *testing.T, data, repo, dig string) string {
	t.Helper()
	dir := filepath.Join(data, "artifacts", dig)
	if err := os.MkdirAll(filepath.Join(dir, "rootfs"), 0o755); err != nil {
		t.Fatal(err)
	}
	if repo != "" {
		if err := os.WriteFile(filepath.Join(dir, "repository"), []byte(repo+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dig
}

func plantPlain(t *testing.T, data, dig string) string {
	t.Helper()
	return plantAt(t, data, "", dig)
}

func assertRootfs(t *testing.T, data, dig string, want bool) {
	t.Helper()
	_, err := os.Stat(filepath.Join(data, "artifacts", dig, "rootfs"))
	if errors.Is(err, os.ErrNotExist) {
		if want {
			t.Fatalf("rootfs %s deleted", dig)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if !want {
		t.Fatalf("rootfs %s kept", dig)
	}
}

func elfBin(tail string) []byte {
	buf := make([]byte, 64)
	buf[0], buf[1], buf[2], buf[3] = 0x7f, 'E', 'L', 'F'
	buf[4], buf[5], buf[6] = byte(elf.ELFCLASS64), byte(elf.ELFDATA2LSB), 1
	binary.LittleEndian.PutUint16(buf[16:], uint16(elf.ET_EXEC))
	binary.LittleEndian.PutUint16(buf[18:], uint16(elf.EM_X86_64))
	binary.LittleEndian.PutUint32(buf[20:], 1)
	return append(buf, tail...)
}
