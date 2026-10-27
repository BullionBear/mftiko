package cli

import (
	"strings"
	"testing"
)

const sessionYAML = `name: demo
sts_instance: sts1
strategy:
  repository: example.invalid/chase
  ref: v1
params:
  big: 123456789012345678901234567890
  dec: 1.2300
  flag: no
  switch: on
  yesno: yes
  power: off
  ok: true
  stop: false
`

const sessionJSON = `{"name":"demo","sts_instance":"sts1","strategy":{"repository":"example.invalid/chase","ref":"v1"},"params":{"big":123456789012345678901234567890,"dec":1.2300,"flag":"no","switch":"on","yesno":"yes","power":"off","ok":true,"stop":false}}`

func TestConvertSessionNumbersAndBools(t *testing.T) {
	got, _, err := convertSession("demo.yaml", []byte(sessionYAML))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != sessionJSON {
		t.Fatalf("yaml json\n%s", got)
	}
	got, _, err = convertSession("demo.json", []byte(sessionJSON))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != sessionJSON {
		t.Fatalf("json json\n%s", got)
	}
	got, _, err = convertSession("-", []byte(sessionYAML))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != sessionJSON {
		t.Fatalf("stdin json\n%s", got)
	}

	got, _, err = convertSession("spell.yaml", []byte("ok: True\nstop: FALSE\nflag: NO\nswitch: on\nyesno: yes\npower: off\n"))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"ok":true,"stop":false,"flag":"NO","switch":"on","yesno":"yes","power":"off"}`
	if string(got) != want {
		t.Fatalf("spellings %s", got)
	}
	got, _, err = convertSession("quoted.yaml", []byte("mode: \"0755\"\n"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"mode":"0755"}` {
		t.Fatalf("quoted %s", got)
	}
	for _, src := range []string{"n: 0\n", "n: -0\n", "n: 1e2\n", "n: 1.0\n", "n: 1.2e+10\n"} {
		got, _, err = convertSession("n.yaml", []byte(src))
		if err != nil {
			t.Fatal(err)
		}
		want := `{"n":` + strings.TrimSpace(strings.TrimPrefix(src, "n:")) + `}`
		if string(got) != want {
			t.Fatalf("%q -> %s", src, got)
		}
	}
}

func TestConvertSessionAmbiguousNumbers(t *testing.T) {
	for _, n := range []string{"0755", "0x1F", "0o17", ".inf", ".nan", "-.inf", "+1", "00", ".5", "1.", "1_000", "0b10"} {
		_, _, err := convertSession("n.yaml", []byte("n: "+n+"\n"))
		if err == nil {
			t.Fatalf("%s was accepted", n)
		}
		msg := err.Error()
		if !strings.Contains(msg, "n.yaml:1:") || !strings.Contains(msg, "quote it") || !strings.Contains(msg, n) {
			t.Fatalf("%s: %s", n, msg)
		}
	}
	_, _, err := convertSession("key.yaml", []byte("0755: 1\n"))
	if err == nil || !strings.Contains(err.Error(), "quote it") || strings.Contains(err.Error(), "object key is not a string") {
		t.Fatalf("key: %v", err)
	}
	_, _, err = convertSession("key.yaml", []byte("1: 2\n"))
	if err == nil || !strings.Contains(err.Error(), "object key is not a string") {
		t.Fatalf("numeric key: %v", err)
	}
}

func TestConvertSessionDuplicateAndShape(t *testing.T) {
	_, _, err := convertSession("dup.yaml", []byte("name: a\nname: b\n"))
	if err == nil || !strings.Contains(err.Error(), `dup.yaml:2:`) || !strings.Contains(err.Error(), `duplicate key "name"`) {
		t.Fatalf("dup: %v", err)
	}
	_, _, err = convertSession("dup.json", []byte(`{"a":1,"a":2}`))
	if err == nil || !strings.Contains(err.Error(), `duplicate key "a"`) {
		t.Fatalf("json dup: %v", err)
	}
	_, _, err = convertSession("empty.yaml", nil)
	if err == nil || !strings.Contains(err.Error(), "empty") {
		t.Fatalf("empty: %v", err)
	}
	_, _, err = convertSession("two.yaml", []byte("name: a\n---\nname: b\n"))
	if err == nil || !strings.Contains(err.Error(), "more than one") {
		t.Fatalf("docs: %v", err)
	}
	_, _, err = convertSession("merge.yaml", []byte("a: &a\n  x: 1\nb:\n  <<: *a\n"))
	if err == nil || !strings.Contains(err.Error(), "YAML merge is not JSON") {
		t.Fatalf("merge: %v", err)
	}
	got, _, err := convertSession("merge.yaml", []byte("\"<<\": 1\n"))
	if err != nil || string(got) != `{"<<":1}` {
		t.Fatalf("quoted merge %s %v", got, err)
	}
	got, locs, err := convertSession("alias.yaml", []byte("a: &a\n  x: 1\nb: *a\n"))
	if err != nil || string(got) != `{"a":{"x":1},"b":{"x":1}}` {
		t.Fatalf("alias %s %v", got, err)
	}
	if locs["/b"].Line == locs["/b/x"].Line || locs["/a/x"] != locs["/b/x"] {
		t.Fatalf("alias locs %#v", locs)
	}
	_, _, err = convertSession("cycle.yaml", []byte("a: &a\n  b: *a\n"))
	if err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("cycle: %v", err)
	}
}

func TestConvertSessionLocations(t *testing.T) {
	const src = "name: demo\nsts_instance: sts1\nstrategy:\n  repository: example.invalid/chase\n  ref: v1\nparams:\n  big: 123\n"
	_, locs, err := convertSession("s.yaml", []byte(src))
	if err != nil {
		t.Fatal(err)
	}
	pos, ok := locs["/params/big"]
	if !ok {
		t.Fatalf("locs %#v", locs)
	}
	if pos.Line != 7 || pos.Col != 8 {
		t.Fatalf("pos %+v", pos)
	}
	line := strings.Split(src, "\n")[pos.Line-1]
	if !strings.HasPrefix(line[pos.Col-1:], "123") {
		t.Fatalf("line %q col %d", line, pos.Col)
	}
	if _, ok := locs["/a~1b"]; ok {
		t.Fatal("unexpected pointer")
	}
	_, locs, err = convertSession("esc.yaml", []byte("\"a/b\": 1\n\"c~d\": 2\n"))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := locs["/a~1b"]; !ok {
		t.Fatalf("slash %#v", locs)
	}
	if _, ok := locs["/c~0d"]; !ok {
		t.Fatalf("tilde %#v", locs)
	}
}
