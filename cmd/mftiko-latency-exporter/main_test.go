package main

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestUsage(t *testing.T) {
	var out bytes.Buffer
	dir := t.TempDir()
	for _, args := range [][]string{
		nil,
		{"-data", dir},
		{"extra"},
		{"-metrics-addr"},
		{"-metrics-addr", "127.0.0.1:1", "-data", dir, "extra"},
	} {
		out.Reset()
		err := run(context.Background(), args, &out)
		if err == nil || !strings.Contains(err.Error(), "usage:") {
			t.Fatalf("%q: %v", args, err)
		}
	}
	out.Reset()
	if err := run(context.Background(), []string{"-version"}, &out); err != nil || !strings.Contains(out.String(), "commit") {
		t.Fatalf("-version %q %v", out.String(), err)
	}
}

func TestPublicAddr(t *testing.T) {
	var out bytes.Buffer
	err := run(context.Background(), []string{"-metrics-addr", "8.8.8.8:9090", "-data", t.TempDir()}, &out)
	if err == nil || !strings.Contains(err.Error(), "private") {
		t.Fatal(err)
	}
}

func TestRun(t *testing.T) {
	var out bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := run(ctx, []string{"-metrics-addr", "127.0.0.1:0", "-data", t.TempDir()}, &out)
	if err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(t.TempDir(), "missing")
	err = run(context.Background(), []string{"-metrics-addr", "127.0.0.1:0", "-data", missing}, &out)
	if err == nil {
		t.Fatal("missing data directory")
	}
}
