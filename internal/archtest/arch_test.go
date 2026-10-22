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
var planes = []string{"internal/api", "internal/sts", "internal/md", "internal/td", "internal/sym", "internal/controller"}

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

// pureAgent reports whether a package must link no I/O: an agent plane's root package, the controller's decisions
// (internal/controller, DECISIONS D1301), or the skeleton's decisions (internal/platform/agent) that the agents
// build on.
func pureAgent(path string) bool {
	return path == "internal/platform/agent" || path == "internal/controller" || slices.Contains(agentPlanes, path)
}

// forbiddenInController reports whether the controller plane must not link the package: exchange, or SYM
// (DECISIONS D1301, D907).
func forbiddenInController(importPath string) bool {
	r, own := rel(importPath)
	return own && (under(r, "exchange") || under(r, "contract/gen/mftiko/sym"))
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

// forbiddenInExchange reports whether the exchange package, which holds the adapter interfaces, must not
// link the package: the transport and JSON libraries are the workers' and exchange/transport's, and sonic is also
// exchange/internal/codec's (DECISIONS D302).
func forbiddenInExchange(importPath string) bool {
	return under(importPath, "github.com/valyala/fasthttp", "github.com/coder/websocket", "github.com/bytedance/sonic")
}

// forbiddenInTD reports whether a package under internal/td must not link it. The account worker does not
// decode client_order_id (DECISIONS D633).
func forbiddenInTD(importPath string) bool {
	r, own := rel(importPath)
	return own && under(r, "contract/wire/cloid")
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
					// OpenAPI codegen links the oapi-codegen runtime. The rest of contract does not (D1044, D006).
					if under(from, "contract/gen/api") && under(imp, "github.com/oapi-codegen/runtime") {
						break
					}
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
				case plane == "internal/controller" && under(to, "exchange"):
					bad(imp, "the controller does not use exchange")
				case plane == "internal/controller" && under(to, "contract/gen/mftiko/sym"):
					bad(imp, "the controller does not query SYM")
				case under(to, "exchange") && to != "exchange" && !under(to, "exchange/transport"):
					bad(imp, "planes use exchange's interfaces and exchange/transport only; cmd registers the exchanges")
				}
			}
			if strings.HasPrefix(imp, "oras.land/oras-go") &&
				from != "internal/platform/artifact" && from != "cmd/mftiko-pack" {
				bad(imp, "oras-go is only imported by internal/platform/artifact and cmd/mftiko-pack (D909)")
			}
		}
		if from == "exchange" {
			for _, d := range p.Deps {
				if forbiddenInExchange(d) {
					out = append(out, fmt.Sprintf("%s links %s: the adapter interfaces carry no transport or JSON library", from, d))
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
		if under(from, "internal/controller") {
			for _, d := range p.Deps {
				if forbiddenInController(d) {
					out = append(out, fmt.Sprintf("%s links %s: the controller does not use exchange or SYM", from, d))
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
		if under(from, "internal/td") {
			for _, d := range p.Deps {
				if forbiddenInTD(d) {
					out = append(out, fmt.Sprintf("%s links %s: TD does not decode client_order_id", from, d))
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
		"internal/platform/artifact", "cmd/mftiko-pack",
		"internal/md", "internal/td", "internal/sts", "internal/sym", "internal/api",
		"internal/controller", "internal/controller/daemon"} {
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
		{ImportPath: m("internal/md/conn"), Imports: []string{m("exchange/transport"), m("exchange/transport/ws")}}, // allowed
		{ImportPath: m("internal/td/account"), Imports: []string{m("exchange/transport/fasthttp")}},                 // allowed
		{ImportPath: m("internal/td/account"), Deps: []string{m("contract/wire/cloid")}},                            // D633
		{ImportPath: m("internal/sym/refresh"), Imports: []string{m("exchange/transportx")}},                        // not exchange/transport
		{ImportPath: m("internal/api"), Imports: []string{m("exchange"), m("contract/gen/mftiko/sym/v1"), m("contract/gen/mftiko/state/v1")}},
		{ImportPath: m("internal/api/venue"), Imports: []string{m("exchange/transport/fasthttp")}},
		{ImportPath: m("internal/platform/agent"), Imports: []string{m("contract/gen/mftiko/shim/v1"), m("internal/sts"), m("exchange")}},
		{ImportPath: m("exchange/okx"), Imports: []string{m("exchange"), m("contract/symbol"), m("internal/platform/state")}},
		{ImportPath: m("exchange"), Imports: []string{"github.com/valyala/fasthttp"},
			Deps: []string{"github.com/valyala/fasthttp", "github.com/coder/websocket/internal/errd", "github.com/bytedance/sonic/ast"}},
		{ImportPath: m("exchange/transport/fasthttp"), Imports: []string{m("exchange"), m("exchange/transport"), "github.com/valyala/fasthttp"},
			Deps: []string{"github.com/valyala/fasthttp", m("exchange")}}, // the transport: free
		{ImportPath: m("contract/symbol"), Imports: []string{"context", "google.golang.org/protobuf/proto", "github.com/rs/zerolog", m("exchange")}},
		{ImportPath: m("sdk"), Imports: []string{m("contract/wire/frame"), m("internal/platform/natsx")},
			Deps: []string{"fmt", m("contract/wire/frame"), "go.etcd.io/etcd/client/v3", m("exchange"), m("contract/wire/c4")}},
		{ImportPath: m("examples/strategies/x"), Imports: []string{m("sdk"), m("internal/md")}},
		{ImportPath: m("internal/td"), Imports: []string{m("internal/platform/agent")},
			Deps: []string{m("internal/platform/agent"), m("internal/platform/agent/runner"), "github.com/nats-io/nats.go"}},
		{ImportPath: m("internal/td/daemon"), Imports: []string{m("internal/platform/agent/runner")}, // the wiring: free
			Deps: []string{m("internal/platform/agent/runner"), m("internal/platform/state"), "go.etcd.io/etcd/client/v3"}},
		{ImportPath: m("internal/platform/agent"), Deps: []string{m("internal/platform/shim")}},
		{ImportPath: m("internal/controller"), Deps: []string{"go.etcd.io/etcd/client/v3", m("internal/platform/state")}},
		{ImportPath: m("internal/controller/daemon"), Imports: []string{
			m("exchange"), m("exchange/binance"), m("contract/gen/mftiko/sym/v1"), m("internal/md"),
		}},
		{ImportPath: m("cmd/mftiko-md-agent"), Imports: []string{m("exchange/all"), m("internal/md"), m("internal/td")}},
		{ImportPath: "go.etcd.io/etcd/client/v3", Imports: []string{m("internal/md")}},                // outside the module: ignored
		{ImportPath: m("internal/platform/artifact"), Imports: []string{"oras.land/oras-go/v2"}},      // D909: allowed
		{ImportPath: m("cmd/mftiko-pack"), Imports: []string{"oras.land/oras-go/v2/registry/remote"}}, // D909: allowed
		{ImportPath: m("cmd/mftiko-sts-agent"), Imports: []string{"oras.land/oras-go/v2"}},            // D909
		{ImportPath: m("contract/gen/api/v1"), Imports: []string{
			"github.com/oapi-codegen/runtime", "github.com/oapi-codegen/runtime/types", // D1044: allowed
			"github.com/rs/zerolog", // not the runtime
		}},
		{ImportPath: m("contract/wire/frame"), Imports: []string{"github.com/oapi-codegen/runtime"}}, // not contract/gen/api
	}
	std := map[string]bool{"fmt": true, "context": true}
	got := check(pkgs, std)
	want := []string{
		"internal/md imports " + m("internal/td"),
		"internal/md imports " + m("exchange/binance"),
		"internal/sym/refresh imports " + m("exchange/transportx"),
		"internal/api imports " + m("exchange"),
		"internal/api/venue imports " + m("exchange/transport/fasthttp"),
		"internal/api imports " + m("contract/gen/mftiko/sym/v1"),
		"internal/platform/agent imports " + m("internal/sts"),
		"internal/platform/agent imports " + m("exchange"),
		"exchange/okx imports " + m("internal/platform/state"),
		"exchange links github.com/valyala/fasthttp",
		"exchange links github.com/coder/websocket/internal/errd",
		"exchange links github.com/bytedance/sonic/ast",
		"contract/symbol imports google.golang.org/protobuf/proto",
		"contract/symbol imports github.com/rs/zerolog",
		"contract/symbol imports " + m("exchange"),
		"sdk imports " + m("internal/platform/natsx"),
		"sdk links go.etcd.io/etcd/client/v3",
		"sdk links " + m("exchange"),
		"examples/strategies/x imports " + m("internal/md"),
		"internal/td/account links " + m("contract/wire/cloid"),
		"internal/td links " + m("internal/platform/agent/runner"),
		"internal/td links github.com/nats-io/nats.go",
		"internal/platform/agent links " + m("internal/platform/shim"),
		"internal/controller links go.etcd.io/etcd/client/v3",
		"internal/controller links " + m("internal/platform/state"),
		"internal/controller/daemon imports " + m("exchange"),
		"internal/controller/daemon imports " + m("exchange/binance"),
		"internal/controller/daemon imports " + m("contract/gen/mftiko/sym/v1"),
		"internal/controller/daemon imports " + m("internal/md"),
		"cmd/mftiko-sts-agent imports oras.land/oras-go/v2",
		"contract/gen/api/v1 imports github.com/rs/zerolog",
		"contract/wire/frame imports github.com/oapi-codegen/runtime",
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
