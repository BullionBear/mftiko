package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/BullionBear/mftiko/sdk/eventlog"
)

func TestDump(t *testing.T) {
	dir := t.TempDir()
	for _, inc := range []uint32{3, 4} {
		b := eventlog.AppendHeader(nil, eventlog.Header{Incarnation: inc, N: 1, CreatedNs: 900, SessionID: "s1"})
		b = eventlog.AppendFrame(b, eventlog.Record{Seq: 1, Kind: eventlog.KindTimer, RecvNs: 1000, TNs: 1100, Ref: 1})
		b = eventlog.AppendFrame(b, eventlog.Record{Seq: 2, Kind: eventlog.KindHookEnd, TNs: 1200, Ref: 1})
		if err := os.WriteFile(filepath.Join(dir, eventlog.Name(inc, 1)), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	head := `{"file":"4-1.mel","format_version":1,"incarnation":4,"n":1,"created_ns":900,"session_id":"s1","sdk_version":"","sdk_commit":""}`
	frames := `{"rec_seq":1,"kind":"timer","recv_ns":1000,"hook_start_ns":1100,"timer":1}
{"rec_seq":2,"kind":"hook_end","hook_end_ns":1200,"input":1,"ending":"returned"}
`
	var out bytes.Buffer
	if err := run([]string{"dump", "-incarnation", "4", dir}, &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != head+"\n"+frames {
		t.Fatalf("dump of incarnation 4:\n%s", got)
	}
	out.Reset()
	if err := run([]string{"dump", filepath.Join(dir, "3-1.mel")}, &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != strings.ReplaceAll(head, "4", "3")+"\n"+frames {
		t.Fatalf("dump of one file:\n%s", got)
	}
	out.Reset()
	if err := run([]string{"dump", dir}, &out); err != nil || strings.Count(out.String(), `"file"`) != 2 {
		t.Fatalf("dump of the directory: %v\n%s", err, out.String())
	}
}

func TestUsage(t *testing.T) {
	var out bytes.Buffer
	for _, args := range [][]string{nil, {"replay"}, {"dump"}, {"dump", "-incarnation", "x", "."}, {"dump", "-incarnation", "4294967296", "."}} {
		if err := run(args, &out); err == nil {
			t.Fatalf("%q: no error", args)
		}
	}
	if err := run([]string{"-version"}, &out); err != nil || !strings.Contains(out.String(), "commit") {
		t.Fatalf("-version %q %v", out.String(), err)
	}
}
