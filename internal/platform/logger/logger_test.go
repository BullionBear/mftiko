package logger

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/rs/zerolog"
)

func TestNewWithWriterAddsIdentity(t *testing.T) {
	var buf bytes.Buffer
	log := NewWithWriter("mftiko-test", &buf)
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
	if line["component"] != "mftiko-test" {
		t.Errorf("component = %v", line["component"])
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
