package bearer

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	bad := map[string]string{
		"empty":        "\n# only a comment\n",
		"one field":    "pms\n",
		"three fields": "pms sekrit extra\n",
		"name twice":   "pms sekrit\npms other\n",
		"token twice":  "pms sekrit\nops sekrit\n",
	}
	for name, in := range bad {
		_, err := Parse(strings.NewReader(in))
		if err == nil {
			t.Fatalf("%s was accepted", name)
		}
		if strings.Contains(err.Error(), "sekrit") {
			t.Fatalf("%s: the error names the token: %v", name, err)
		}
	}
	tokens, err := Parse(strings.NewReader("  pms   tok1  \nops\ttok2\n"))
	if err != nil {
		t.Fatal(err)
	}
	if tokens.Len() != 2 {
		t.Fatalf("%d tokens", tokens.Len())
	}
	if name, ok := tokens.Match("tok2"); !ok || name != "ops" {
		t.Fatalf("tok2 -> %q %v", name, ok)
	}
	if _, ok := tokens.Match("tok3"); ok {
		t.Fatal("unknown token matched")
	}
	if (*Tokens)(nil).Len() != 0 {
		t.Fatal("nil has tokens")
	}
}

func TestLoad(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "absent")); err == nil || !strings.HasPrefix(err.Error(), "token file: ") {
		t.Fatalf("a missing file: %v", err)
	}
	path := filepath.Join(t.TempDir(), "tokens")
	if err := os.WriteFile(path, []byte("pms tok1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	tokens, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if name, ok := tokens.Match("tok1"); !ok || name != "pms" {
		t.Fatalf("tok1 -> %q %v", name, ok)
	}
}

func TestFromHeader(t *testing.T) {
	cases := map[string]string{
		"Bearer abc":   "abc",
		"bearer  abc ": "abc",
		" BEARER abc":  "abc",
		"Basic abc":    "",
		"Bearer":       "",
		"Bearer ":      "",
		"":             "",
		"abc":          "",
	}
	for in, want := range cases {
		h := http.Header{}
		if in != "" {
			h.Set("Authorization", in)
		}
		got, ok := FromHeader(h)
		if got != want || ok != (want != "") {
			t.Fatalf("%q -> %q %v, want %q", in, got, ok, want)
		}
	}
}
