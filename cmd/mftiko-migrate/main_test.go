package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
	"github.com/BullionBear/mftiko/internal/platform/pgtest"
)

func TestUsage(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	for _, args := range [][]string{
		nil,
		{"up", "extra"},
		{"-database", "", "up"},
		{"down"},
		{"-database", "postgres://mftiko@127.0.0.1:1/mftiko?sslmode=disable", "down"},
		{"-h"},
	} {
		err := run(args, io.Discard)
		if exitCode(err) != 2 {
			t.Errorf("%q: exit %d err %v", args, exitCode(err), err)
		}
	}
	var out bytes.Buffer
	if err := run([]string{"-version"}, &out); err != nil || out.String() != buildinfo.String()+"\n" {
		t.Fatalf("version: %v %q", err, out.String())
	}
}

func TestUpStatusAndChanged(t *testing.T) {
	srv := pgtest.Start(t)
	dsn := srv.DSN()
	args := []string{"-database", dsn}

	var status bytes.Buffer
	if err := run(append(args, "status"), &status); err != nil {
		t.Fatal(err)
	}
	if strings.Count(status.String(), " pending\n") != 5 {
		t.Fatalf("status before up:\n%s", status.String())
	}

	var up bytes.Buffer
	if err := run(append(args, "up"), &up); err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{
		"applied 0001_history\n",
		"applied 0002_symbol\n",
		"applied 0003_audit\n",
		"applied 0004_session_records\n",
		"applied 0005_session_records_final\n",
	} {
		if !strings.Contains(up.String(), line) {
			t.Fatalf("up missing %q\n%s", line, up.String())
		}
	}

	var again bytes.Buffer
	if err := run(append(args, "up"), &again); err != nil {
		t.Fatal(err)
	}
	if again.String() != "up to date\n" {
		t.Fatalf("rerun stdout %q", again.String())
	}

	status.Reset()
	if err := run(append(args, "status"), &status); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(status.String(), "pending") || !strings.Contains(status.String(), "0001 history applied ") {
		t.Fatalf("status after up:\n%s", status.String())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	if _, err := conn.Exec(ctx, `UPDATE schema_migrations SET sha256 = $1 WHERE version = 1`, strings.Repeat("ab", 32)); err != nil {
		t.Fatal(err)
	}

	var failed bytes.Buffer
	err = run(append(args, "up"), &failed)
	if exitCode(err) != 1 || !strings.Contains(err.Error(), "changed") {
		t.Fatalf("changed file: exit %d err %v\n%s", exitCode(err), err, failed.String())
	}
	if strings.Contains(failed.String(), "applied ") {
		t.Fatalf("changed file still applied:\n%s", failed.String())
	}

	status.Reset()
	err = run(append(args, "status"), &status)
	if exitCode(err) != 1 || !strings.Contains(status.String(), "0001 history changed recorded ") {
		t.Fatalf("status drift: exit %d err %v\n%s", exitCode(err), err, status.String())
	}
	var usage usageError
	if errors.As(err, &usage) {
		t.Fatal("drift reported as usage")
	}
}
