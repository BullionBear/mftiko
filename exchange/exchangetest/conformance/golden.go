package conformance

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// EnvUpdate set to 1 rewrites the golden files from what the adapter produces, instead of comparing:
//
//	MFTIKO_UPDATE_GOLDEN=1 go test ./exchange/binance/...
//
// Review the diff before committing it: a golden file is the claim that this output is right.
const EnvUpdate = "MFTIKO_UPDATE_GOLDEN"

func updating() bool { return os.Getenv(EnvUpdate) == "1" }

// EnvUpdateSet reports whether golden files are being rewritten: tests that compare against them
// skip then.
func EnvUpdateSet() bool { return updating() }

// golden compares got with the file rel (relative to the fixture directory), or rewrites it. A
// missing golden file is a problem, not a pass.
func (r *runner) golden(rel string, got []byte) []Problem {
	if updating() {
		if r.spec.Dir == "" {
			return []Problem{{Check: "golden", Where: rel, What: "Spec.Dir is empty: cannot write golden files"}}
		}
		path := filepath.Join(r.spec.Dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return []Problem{{Check: "golden", Where: rel, What: err.Error()}}
		}
		if err := os.WriteFile(path, got, 0o644); err != nil {
			return []Problem{{Check: "golden", Where: rel, What: err.Error()}}
		}
		return nil
	}
	want, err := fs.ReadFile(r.fsys, rel)
	if errors.Is(err, fs.ErrNotExist) {
		return []Problem{{Check: "golden", Where: rel, What: "no golden file; run with " + EnvUpdate + "=1 and review it"}}
	}
	if err != nil {
		return []Problem{{Check: "golden", Where: rel, What: err.Error()}}
	}
	if bytes.Equal(bytes.TrimSpace(want), bytes.TrimSpace(got)) {
		return nil
	}
	return []Problem{{Check: "golden", Where: rel, What: firstDiff(want, got)}}
}

// firstDiff names the first line that differs.
func firstDiff(want, got []byte) string {
	w := strings.Split(strings.TrimSpace(string(want)), "\n")
	g := strings.Split(strings.TrimSpace(string(got)), "\n")
	for i := 0; i < len(w) || i < len(g); i++ {
		var a, b string
		if i < len(w) {
			a = w[i]
		}
		if i < len(g) {
			b = g[i]
		}
		if a != b {
			return fmt.Sprintf("line %d differs\n  want: %s\n  got:  %s", i+1, a, b)
		}
	}
	return "differs in whitespace"
}

// marshal is json.Marshal without HTML escaping: fixtures keep & and < as they are, so a query
// string reads as sent. indent selects the indented form.
func marshal(v any, indent bool) ([]byte, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if indent {
		enc.SetIndent("", "  ")
	}
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(b.Bytes(), []byte("\n")), nil
}

// jsonLines is one compact JSON value per line.
func jsonLines[T any](vs []T) []byte {
	var b bytes.Buffer
	for _, v := range vs {
		line, err := marshal(v, false)
		if err != nil {
			panic(err)
		}
		b.Write(line)
		b.WriteByte('\n')
	}
	return b.Bytes()
}

// indented is v as indented JSON, for golden files a person reads.
func indented(v any) []byte {
	b, err := marshal(v, true)
	if err != nil {
		panic(err)
	}
	return append(b, '\n')
}
