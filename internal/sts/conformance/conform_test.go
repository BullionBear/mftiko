package conformance_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/BullionBear/mftiko/internal/platform/shim/fakeworker"
	"github.com/BullionBear/mftiko/internal/sts/conformance"
)

func TestMain(m *testing.M) {
	fakeworker.MaybeMain()
	os.Exit(m.Run())
}

func workerCmd(extra ...string) conformance.Command {
	argv := []string{os.Args[0], "-mftiko-conformance"}
	return append(argv, extra...)
}

func TestPass(t *testing.T) {
	rep, err := conformance.Run(context.Background(), conformance.Config{Command: workerCmd()})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Failed() {
		t.Fatalf("compliant worker\n%s", rep.Table())
	}
	got := map[string]conformance.Check{}
	for _, c := range rep.Checks {
		got[c.ID] = c
	}
	for _, id := range conformance.CheckIDs() {
		c, ok := got[id]
		if !ok || c.Status != "pass" {
			t.Errorf("%s: %+v", id, c)
		}
	}
}

func TestViolate(t *testing.T) {
	// The fragment is the failure that violation produces (D833). A crash
	// before Hello is a false green and is rejected separately.
	want := map[string]string{
		conformance.CheckFlow:           "Done ok",
		conformance.CheckBeat:           "no Beat within 1s",
		conformance.CheckDeadline:       "deadline: Done ok",
		conformance.CheckShutdown:       "Stopped before Exiting",
		conformance.CheckBufferPriority: "priority:",
		conformance.CheckPrehook:        "stale seq 1",
		conformance.CheckTDOverflow:     "queued",
		conformance.CheckCloid:          "cloid:",
		conformance.CheckSubject:        "order not on",
		conformance.CheckRecon:          "recon",
		conformance.CheckCtx:            "ctx is false",
	}
	for _, id := range conformance.CheckIDs() {
		t.Run(id, func(t *testing.T) {
			rep, err := conformance.Run(context.Background(), conformance.Config{
				Command: workerCmd("-violate", id),
				Only:    []string{id},
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(rep.Checks) != 1 {
				t.Fatalf("checks: %+v", rep.Checks)
			}
			c := rep.Checks[0]
			if c.ID != id || c.Status != "fail" {
				t.Fatalf("%+v", c)
			}
			if strings.Contains(c.Detail, "before Hello") || strings.Contains(c.Detail, "exited 2") {
				t.Fatalf("false green: %s", c.Detail)
			}
			if !strings.Contains(c.Detail, want[id]) {
				t.Fatalf("detail %q, want fragment %q", c.Detail, want[id])
			}
			if !rep.Failed() {
				t.Fatal("Failed() is false")
			}
		})
	}
}

func TestSkip(t *testing.T) {
	rep, err := conformance.Run(context.Background(), conformance.Config{
		Command: workerCmd("-contracts", "worker.v1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Failed() {
		t.Fatalf("skip counted as fail\n%s", rep.Table())
	}
	want := map[string]string{}
	for _, id := range conformance.CheckIDs() {
		switch id {
		case conformance.CheckFlow, conformance.CheckBeat, conformance.CheckDeadline, conformance.CheckShutdown:
			want[id] = "pass"
		default:
			want[id] = "skip"
		}
	}
	if len(rep.Checks) != len(want) {
		t.Fatalf("%s", rep.Table())
	}
	for _, c := range rep.Checks {
		if c.Status != want[c.ID] {
			t.Errorf("%s: got %s (%s), want %s", c.ID, c.Status, c.Detail, want[c.ID])
		}
		if c.ID == conformance.CheckPrehook && c.Detail != "md.v1 not in contracts" {
			t.Errorf("prehook detail %q", c.Detail)
		}
	}
}

func TestUsage(t *testing.T) {
	if _, err := conformance.Run(context.Background(), conformance.Config{}); err == nil {
		t.Fatal("empty command returned no error")
	}
	if _, err := (conformance.Artifact{}).Args(); !errors.Is(err, conformance.ErrArtifact) {
		t.Fatalf("artifact: %v", err)
	}
	if _, err := conformance.Run(context.Background(), conformance.Config{
		Command: conformance.Command{"/bin/true"},
		Only:    []string{"not-a-check"},
	}); err == nil {
		t.Fatal("unknown check returned no error")
	}
	rep := conformance.Report{Checks: []conformance.Check{{
		ID: "flow", Status: "skip", Detail: "md.v1 not in contracts",
	}}}
	raw, err := rep.JSON()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte(`"id"`)) || !bytes.Contains(raw, []byte(`"status"`)) || !bytes.Contains(raw, []byte(`"detail"`)) {
		t.Fatalf("json: %s", raw)
	}
	if rep.Failed() {
		t.Fatal("skip-only report is a failure")
	}
	table := rep.Table()
	if !strings.Contains(table, "flow") || !strings.Contains(table, "skip") || !strings.Contains(table, "md.v1 not in contracts") {
		t.Fatalf("table: %s", table)
	}
}
