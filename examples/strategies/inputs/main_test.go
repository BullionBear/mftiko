package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/sdk"
)

// fakeCtx is the slice of Context inputs uses. The other methods are unused.
type fakeCtx struct {
	sdk.Context
	params []byte
	dir    string // MFTIKO_INPUT_DIR
	logs   *bytes.Buffer
}

func (f *fakeCtx) Params() []byte { return f.params }

func (f *fakeCtx) Log() zerolog.Logger {
	if f.logs == nil {
		return zerolog.Nop()
	}
	return zerolog.New(f.logs).With().Timestamp().Logger()
}

func (f *fakeCtx) InputDir(name string) string {
	if f.dir == "" {
		return ""
	}
	return filepath.Join(f.dir, name)
}

func sum(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

// tree writes model/model.pt and model/sub/meta.json under a new input dir. With readOnly it
// makes the tree 0444 and 0555, as STS does.
func tree(t *testing.T, readOnly bool) string {
	t.Helper()
	root := t.TempDir()
	model := filepath.Join(root, "model")
	if err := os.MkdirAll(filepath.Join(model, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"model.pt": "weights", "sub/meta.json": `{"v":1}`} {
		if err := os.WriteFile(filepath.Join(model, filepath.FromSlash(name)), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink("model.pt", filepath.Join(model, "latest.pt")); err != nil {
		t.Fatal(err)
	}
	if !readOnly {
		return root
	}
	for _, p := range []string{"model.pt", "sub/meta.json"} {
		if err := os.Chmod(filepath.Join(model, filepath.FromSlash(p)), 0o444); err != nil {
			t.Fatal(err)
		}
	}
	for _, d := range []string{"sub", "."} {
		if err := os.Chmod(filepath.Join(model, d), 0o555); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_ = os.Chmod(model, 0o755)
		_ = os.Chmod(filepath.Join(model, "sub"), 0o755)
	})
	return root
}

func params(t *testing.T, files map[string]string) []byte {
	t.Helper()
	raw, err := json.Marshal(inputsParams{Input: "model", SHA256: files})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func goodFiles() map[string]string {
	return map[string]string{"model.pt": sum("weights"), "sub/meta.json": sum(`{"v":1}`), "latest.pt": sum("weights")}
}

func skipRoot(t *testing.T) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("root writes through 0444 and 0555")
	}
}

func TestInputsReadOnlyMatch(t *testing.T) {
	skipRoot(t)
	var out bytes.Buffer
	s := &inputs{}
	if err := s.OnInit(&fakeCtx{params: params(t, goodFiles()), dir: tree(t, true), logs: &out}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"files":3`) || !strings.Contains(out.String(), `"hook":"OnInit"`) {
		t.Fatalf("log %q", out.String())
	}
}

func TestInputsRefusals(t *testing.T) {
	skipRoot(t)
	bad := goodFiles()
	bad["model.pt"] = sum("other")
	cases := []struct {
		name   string
		params []byte
		dir    string
		want   string
	}{
		{"bad params", []byte("{"), tree(t, true), "params"},
		{"no files", params(t, nil), tree(t, true), "required"},
		{"no input dir", params(t, goodFiles()), "", "no directory"},
		{"mismatch", params(t, bad), tree(t, true), "model.pt: sha256"},
		{"missing", params(t, map[string]string{"gone.pt": sum("x")}), tree(t, true), "gone.pt"},
		{"outside", params(t, map[string]string{"../model/model.pt": sum("weights")}), tree(t, true), "not a path under"},
		{"writable", params(t, goodFiles()), tree(t, false), "created a file"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := (&inputs{}).OnInit(&fakeCtx{params: c.params, dir: c.dir})
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err %v, want %q", err, c.want)
			}
		})
	}
}

// A writable file in a read-only directory is still refused.
func TestInputsWritableFile(t *testing.T) {
	skipRoot(t)
	root := tree(t, true)
	model := filepath.Join(root, "model")
	if err := os.Chmod(filepath.Join(model, "model.pt"), 0o644); err != nil {
		t.Fatal(err)
	}
	err := (&inputs{}).OnInit(&fakeCtx{params: params(t, goodFiles()), dir: root})
	if err == nil || !strings.Contains(err.Error(), "opened for writing") {
		t.Fatalf("err %v", err)
	}
}
