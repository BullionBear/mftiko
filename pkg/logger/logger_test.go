package logger

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/rs/zerolog"
)

func TestNewWithWriterAddsIdentity(t *testing.T) {
	var buf bytes.Buffer
	log := NewWithWriter("mftiko-test", "v1", "abc", &buf).Level(zerolog.InfoLevel)
	log.Info().Msg("hello")

	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatalf("not one JSON object: %v: %q", err, buf.String())
	}
	for _, key := range []string{"time", "level", "component", "version", "commit", "message"} {
		if _, ok := line[key]; !ok {
			t.Errorf("missing %q in %v", key, line)
		}
	}
	if line["component"] != "mftiko-test" || line["version"] != "v1" || line["commit"] != "abc" {
		t.Errorf("identity %v", line)
	}
}

func TestLevel(t *testing.T) {
	cases := map[string]zerolog.Level{
		"":         zerolog.InfoLevel,
		"debug":    zerolog.DebugLevel,
		" WARN ":   zerolog.WarnLevel,
		"nonsense": zerolog.InfoLevel,
	}
	for in, want := range cases {
		if got := level(in); got != want {
			t.Errorf("level(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestWriter(t *testing.T) {
	if _, ok := writer(" console ", io.Discard).(zerolog.ConsoleWriter); !ok {
		t.Fatal("console format did not select ConsoleWriter")
	}
	if got := writer("json", io.Discard); got != io.Discard {
		t.Fatalf("json writer %#v", got)
	}
	if got := writer("", io.Discard); got != io.Discard {
		t.Fatalf("default writer %#v", got)
	}
}
