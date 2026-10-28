package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/BullionBear/mftiko/internal/platform/artifact"
)

func modelDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "model.pt"), []byte("weights"), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestDataUsage(t *testing.T) {
	for _, args := range [][]string{
		{"data"},
		{"data", "-dir", "d"},
		{"data", "-repo", "127.0.0.1:5000/data/model"},
		{"data", "-dir", "d", "-repo", "127.0.0.1:5000/data/model", "extra"},
		{"data", "-h"},
	} {
		if err := run(args, io.Discard); err == nil || !strings.Contains(err.Error(), "mftiko-pack data") {
			t.Errorf("%q: %v", args, err)
		}
	}
	if err := run([]string{"data", "-dir", "d", "-repo", "r.example/x", "-max-bytes", "0"}, io.Discard); err == nil ||
		!strings.Contains(err.Error(), "-max-bytes") {
		t.Fatalf("zero max bytes: %v", err)
	}
}

func TestPackData(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	reg := newCLIRegistry()
	defer reg.Close()
	dir := modelDir(t)
	var stdout, stderr bytes.Buffer
	if err := runData([]string{"-dir", dir, "-repo", reg.repo(), "-tag", "v1", "-plain-http"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	digest := strings.TrimSpace(stdout.String())
	if stdout.String() != digest+"\n" || !strings.HasPrefix(digest, "sha256:") {
		t.Fatalf("stdout %q", stdout.String())
	}
	if want := reg.repo() + ":v1: 1 entries, 7 bytes unpacked, "; !strings.HasPrefix(stderr.String(), want) {
		t.Fatalf("stderr %q, want prefix %q", stderr.String(), want)
	}
	if got := cliFetch(t, reg.repo(), "v1"); string(got) != string(cliFetch(t, reg.repo(), digest)) {
		t.Fatal("tag and digest name different manifests")
	}

	c, err := artifact.Open(artifact.Registry{PlainHTTPHosts: []string{strings.TrimSuffix(reg.repo(), "/hello")}}, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.PullData(context.Background(), artifact.DataRequest{Repository: reg.repo(), Digest: digest})
	if err != nil {
		t.Fatal(err)
	}
	if got.Config.Source != nil {
		t.Fatalf("source %+v", got.Config.Source)
	}

	var again bytes.Buffer
	if err := run([]string{"data", "-dir", dir, "-repo", reg.repo(), "-plain-http"}, &again); err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(again.String()) != digest {
		t.Fatalf("no tag pushed %q, want %s", again.String(), digest)
	}
}

func TestPackDataSymlinkHint(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	reg := newCLIRegistry()
	defer reg.Close()
	dir := modelDir(t)
	outside := filepath.Join(t.TempDir(), "big.pt")
	if err := os.WriteFile(outside, []byte("elsewhere"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(dir, "big.pt")); err != nil {
		t.Fatal(err)
	}
	args := []string{"-dir", dir, "-repo", reg.repo(), "-plain-http"}
	err := runData(args, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "big.pt") || !strings.Contains(err.Error(), "pass -L") {
		t.Fatalf("err %v", err)
	}
	if n := reg.hits(); n != 0 {
		t.Fatalf("%d registry requests before the error", n)
	}
	var stdout bytes.Buffer
	if err := runData(append(args, "-L"), &stdout, io.Discard); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(stdout.String(), "sha256:") {
		t.Fatalf("stdout %q", stdout.String())
	}
}

func TestPackDataUnpackLimitFlags(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	reg := newCLIRegistry()
	defer reg.Close()
	dir := modelDir(t)
	args := []string{"-dir", dir, "-repo", reg.repo(), "-plain-http"}
	var plain, zero bytes.Buffer
	if err := runData(args, &plain, io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := runData(append(append([]string{}, args...), "-max-unpacked-bytes", "0", "-max-unpacked-files", "0"), &zero, io.Discard); err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(plain.String()) == "" || strings.TrimSpace(plain.String()) != strings.TrimSpace(zero.String()) {
		t.Fatalf("omitted %q, explicit zero %q", plain.String(), zero.String())
	}
	before := reg.hits()
	err := runData(append(append([]string{}, args...), "-max-unpacked-bytes", "1"), io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("small byte limit: %v", err)
	}
	for _, bad := range [][]string{
		{"-max-unpacked-bytes", "-1"},
		{"-max-unpacked-files", "-1"},
	} {
		if err := runData(append(append([]string{}, args...), bad...), io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), "-max-unpacked-") {
			t.Fatalf("%q: %v", bad, err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "more.txt"), []byte("aaaa"), 0o644); err != nil {
		t.Fatal(err)
	}
	err = runData(append(append([]string{}, args...), "-max-unpacked-files", "1"), io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("file limit: %v", err)
	}
	if reg.hits() != before {
		t.Fatalf("registry requests %d after rejected uploads, want %d", reg.hits(), before)
	}
}

func TestPackDataErrors(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	reg := newCLIRegistry()
	defer reg.Close()
	if err := runData([]string{"-dir", filepath.Join(t.TempDir(), "gone"), "-repo", reg.repo(), "-plain-http"}, io.Discard, io.Discard); err == nil {
		t.Fatal("a missing directory was accepted")
	}
	err := runData([]string{"-dir", modelDir(t), "-repo", reg.repo(), "-plain-http", "-max-bytes", "10"}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "limit 10") {
		t.Fatalf("over -max-bytes: %v", err)
	}
	if n := reg.hits(); n != 0 {
		t.Fatalf("%d registry requests for uploads that failed", n)
	}
}
