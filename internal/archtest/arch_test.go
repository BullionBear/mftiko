package archtest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

const module = "github.com/BullionBear/mftiko"

// pkg is what go list says about one package.
type pkg struct {
	ImportPath string
	Imports    []string // direct, without test imports
	Deps       []string // everything it links, transitively
	Standard   bool
}

// planes are the bounded contexts under internal/; one may not import another.
var planes = []string{"internal/api", "internal/sts", "internal/md", "internal/td", "internal/sym"}

// rel is a path inside the module relative to its root, and whether it is inside the module.
func rel(importPath string) (string, bool) {
	if r, ok := strings.CutPrefix(importPath, module+"/"); ok {
		return r, true
	}
	return "", false
}

// under reports whether path is one of the prefixes or below one.
func under(path string, prefixes ...string) bool {
	for _, p := range prefixes {
		if path == p || strings.HasPrefix(path, p+"/") {
			return true
		}
	}
	return false
}

func planeOf(path string) string {
	for _, p := range planes {
		if under(path, p) {
			return p
		}
	}
	return ""
}

// agentPlanes are the planes whose root package holds the agent's decisions: the Planner, the Policy and the
// domain types (DECISIONS D110). The wiring to etcd, NATS and the shims is in their daemon subpackage.
var agentPlanes = []string{"internal/sts", "internal/md", "internal/td"}

// pureAgent reports whether a package must link no I/O: an agent plane's root package, or the skeleton's decisions
// (internal/platform/agent) that they build on.
func pureAgent(path string) bool {
	return path == "internal/platform/agent" || slices.Contains(agentPlanes, path)
}

// forbiddenInPureAgent reports whether a pure agent package must not link the package: etcd, NATS, or the platform
// packages that reach them or the shims.
func forbiddenInPureAgent(importPath string) bool {
	if under(importPath, "go.etcd.io", "github.com/nats-io") {
		return true
	}
	r, own := rel(importPath)
	return own && under(r, "internal/platform/state", "internal/platform/natsx", "internal/platform/shim",
		"internal/platform/agent/c3", "internal/platform/agent/runner", "internal/platform/agent/fleet",
		"internal/platform/agent/testplane")
}

// forbiddenInStrategy reports whether a strategy binary, which links sdk, must not contain the package.
func forbiddenInStrategy(importPath string) bool {
	if under(importPath, "go.etcd.io", "github.com/jackc", "github.com/lib/pq") {
		return true
	}
	r, own := rel(importPath)
	return own && under(r, "exchange", "internal")
}

// check returns one line per broken rule. std holds the standard library's import paths.
func check(pkgs []pkg, std map[string]bool) []string {
	var out []string
	for _, p := range pkgs {
		from, ok := rel(p.ImportPath)
		if !ok {
			continue
		}
		bad := func(imp, why string) { out = append(out, fmt.Sprintf("%s imports %s: %s", from, imp, why)) }
		for _, imp := range p.Imports {
			to, own := rel(imp)
			external := !own && !std[imp]
			switch {
			case from == "contract/symbol":
				if !std[imp] {
					bad(imp, "contract/symbol depends only on the standard library")
				}
			case under(from, "contract"):
				if own && !under(to, "contract") {
					bad(imp, "contract depends on nothing else in the repository")
				}
				if external && !under(imp, "google.golang.org/protobuf") {
					bad(imp, "contract depends only on protobuf and the standard library")
				}
			case under(from, "exchange"):
				if own && !under(to, "contract", "exchange") {
					bad(imp, "exchange depends only on contract")
				}
			case under(from, "sdk"):
				if own && !under(to, "contract", "sdk") {
					bad(imp, "sdk depends only on contract")
				}
			case under(from, "examples"):
				if own && !under(to, "contract", "sdk") {
					bad(imp, "examples use only sdk and contract, as an outside author would")
				}
			case under(from, "internal/platform"):
				if own && (planeOf(to) != "" || under(to, "exchange")) {
					bad(imp, "the platform imports no plane and no exchange")
				}
			case planeOf(from) != "":
				if !own {
					break
				}
				plane := planeOf(from)
				if other := planeOf(to); other != "" && other != plane {
					bad(imp, "planes do not import each other; they meet through etcd and NATS")
				}
				switch {
				case plane == "internal/api" && under(to, "exchange"):
					bad(imp, "the API does not use exchange")
				case plane == "internal/api" && under(to, "contract/gen/mftiko/sym"):
					bad(imp, "the API does not query SYM")
				case under(to, "exchange") && to != "exchange":
					bad(imp, "planes use exchange's interfaces only; cmd registers the exchanges")
				}
			}
		}
		if pureAgent(from) {
			for _, d := range p.Deps {
				if forbiddenInPureAgent(d) {
					out = append(out, fmt.Sprintf("%s links %s: decisions do no I/O; the wiring is in a subpackage", from, d))
				}
			}
		}
		if under(from, "sdk") {
			for _, d := range p.Deps {
				if forbiddenInStrategy(d) {
					out = append(out, fmt.Sprintf("%s links %s: a strategy binary has no etcd, PostgreSQL, exchange or internal code", from, d))
				}
			}
		}
	}
	return out
}

// goList runs go list over the whole module, with every dependency.
func goList(t *testing.T) []pkg {
	t.Helper()
	goBin, err := exec.LookPath("go")
	if err != nil {
		goBin = filepath.Join(runtime.GOROOT(), "bin", "go")
	}
	gomod, err := exec.Command(goBin, "env", "GOMOD").Output()
	if err != nil {
		t.Fatalf("go env GOMOD: %v", err)
	}
	cmd := exec.Command(goBin, "list", "-deps", "-json=ImportPath,Imports,Deps,Standard", "./...")
	cmd.Dir = filepath.Dir(strings.TrimSpace(string(gomod)))
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	raw, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, stderr.String())
	}
	var pkgs []pkg
	dec := json.NewDecoder(bytes.NewReader(raw))
	for {
		var p pkg
		if err := dec.Decode(&p); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatalf("decode go list: %v", err)
		}
		pkgs = append(pkgs, p)
	}
	return pkgs
}

func TestDependencyRules(t *testing.T) {
	pkgs := goList(t)
	std := map[string]bool{}
	var own []string
	for _, p := range pkgs {
		std[p.ImportPath] = p.Standard
		if r, ok := rel(p.ImportPath); ok {
			own = append(own, r)
		}
	}
	// The rules key on paths; if a block moved, they would pass by matching nothing.
	for _, want := range []string{"contract/symbol", "contract/wire/frame", "exchange", "sdk", "internal/platform/state",
		"internal/platform/agent", "internal/platform/agent/runner", "internal/platform/natsx", "internal/platform/shim",
		"internal/md", "internal/td", "internal/sts", "internal/sym", "internal/api"} {
		if !slices.Contains(own, want) {
			t.Fatalf("package %s not found: update the rules in this file (DECISIONS D006) to the new layout", want)
		}
	}
	for _, v := range check(pkgs, std) {
		t.Error(v)
	}
}

// The checker itself: each kind of violation is reported, and the allowed edges are not.
func TestCheckReportsViolations(t *testing.T) {
	m := func(p string) string { return module + "/" + p }
	pkgs := []pkg{
		{ImportPath: m("internal/md"), Imports: []string{
			m("internal/md/conn"), m("internal/platform/state"), m("contract/symbol"), m("exchange"), "fmt", // allowed
			m("internal/td"),      // another plane
			m("exchange/binance"), // a concrete exchange
		}},
		{ImportPath: m("internal/api"), Imports: []string{m("exchange"), m("contract/gen/mftiko/sym/v1"), m("contract/gen/mftiko/state/v1")}},
		{ImportPath: m("internal/platform/agent"), Imports: []string{m("contract/gen/mftiko/shim/v1"), m("internal/sts"), m("exchange")}},
		{ImportPath: m("exchange/okx"), Imports: []string{m("exchange"), m("contract/symbol"), m("internal/platform/state")}},
		{ImportPath: m("contract/symbol"), Imports: []string{"context", "google.golang.org/protobuf/proto", "github.com/rs/zerolog", m("exchange")}},
		{ImportPath: m("sdk"), Imports: []string{m("contract/wire/frame"), m("internal/platform/natsx")},
			Deps: []string{"fmt", m("contract/wire/frame"), "go.etcd.io/etcd/client/v3", m("exchange"), m("sdk/internal/c4")}},
		{ImportPath: m("examples/strategies/x"), Imports: []string{m("sdk"), m("internal/md")}},
		{ImportPath: m("internal/td"), Imports: []string{m("internal/platform/agent")},
			Deps: []string{m("internal/platform/agent"), m("internal/platform/agent/runner"), "github.com/nats-io/nats.go"}},
		{ImportPath: m("internal/td/daemon"), Imports: []string{m("internal/platform/agent/runner")}, // the wiring: free
			Deps: []string{m("internal/platform/agent/runner"), m("internal/platform/state"), "go.etcd.io/etcd/client/v3"}},
		{ImportPath: m("internal/platform/agent"), Deps: []string{m("internal/platform/shim")}},
		{ImportPath: m("cmd/mftiko-md-agent"), Imports: []string{m("exchange/all"), m("internal/md"), m("internal/td")}},
		{ImportPath: "go.etcd.io/etcd/client/v3", Imports: []string{m("internal/md")}}, // outside the module: ignored
	}
	std := map[string]bool{"fmt": true, "context": true}
	got := check(pkgs, std)
	want := []string{
		"internal/md imports " + m("internal/td"),
		"internal/md imports " + m("exchange/binance"),
		"internal/api imports " + m("exchange"),
		"internal/api imports " + m("contract/gen/mftiko/sym/v1"),
		"internal/platform/agent imports " + m("internal/sts"),
		"internal/platform/agent imports " + m("exchange"),
		"exchange/okx imports " + m("internal/platform/state"),
		"contract/symbol imports google.golang.org/protobuf/proto",
		"contract/symbol imports github.com/rs/zerolog",
		"contract/symbol imports " + m("exchange"),
		"sdk imports " + m("internal/platform/natsx"),
		"sdk links go.etcd.io/etcd/client/v3",
		"sdk links " + m("exchange"),
		"examples/strategies/x imports " + m("internal/md"),
		"internal/td links " + m("internal/platform/agent/runner"),
		"internal/td links github.com/nats-io/nats.go",
		"internal/platform/agent links " + m("internal/platform/shim"),
	}
	if len(got) != len(want) {
		t.Errorf("%d violations, want %d:\n%s", len(got), len(want), strings.Join(got, "\n"))
	}
	for _, w := range want {
		if !slices.ContainsFunc(got, func(g string) bool { return strings.HasPrefix(g, w+":") }) {
			t.Errorf("not reported: %s", w)
		}
	}
}
