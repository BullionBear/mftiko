package openapi

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"testing"

	"gopkg.in/yaml.v3"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

// TestParamsStayRawJSON locks E10-D033: params are json.RawMessage, so a decimal
// string is not parsed as a float and written back.
func TestParamsStayRawJSON(t *testing.T) {
	var created apiv1.SessionCreate
	if err := json.Unmarshal([]byte(`{"params":{"px":"1.0"}}`), &created); err != nil {
		t.Fatal(err)
	}
	if created.Params == nil || string(*created.Params) != `{"px":"1.0"}` {
		t.Fatalf("create params = %s", created.Params)
	}

	view := apiv1.SessionSpecView{Params: json.RawMessage(`{"px":"1.0"}`)}
	raw, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	var back apiv1.SessionSpecView
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if string(back.Params) != `{"px":"1.0"}` {
		t.Fatalf("view params = %s", back.Params)
	}
}

// TestServerIsStdHTTP locks E10-D045: the strict server is a net/http ServeMux,
// and the SSE event schemas survive generation.
func TestServerIsStdHTTP(t *testing.T) {
	var _ apiv1.SessionRemovedEvent
	var _ apiv1.ResetEvent

	handler := apiv1.Handler(apiv1.NewStrictHandler(nil, nil))
	if _, ok := handler.(*http.ServeMux); !ok {
		t.Fatalf("handler type %T, want *http.ServeMux", handler)
	}
	client, err := apiv1.NewClient("http://api.example")
	if err != nil {
		t.Fatal(err)
	}
	if client == nil {
		t.Fatal("client")
	}
	var _ apiv1.LogEvent
	var _ apiv1.LogGapEvent
	var _ apiv1.LogEndEvent
	var _ apiv1.LogStream
	var _ apiv1.LogEndReason
}

// TestEventSchemasMatchGeneratedTypes locks E10-D005: every x-mftiko-events value
// is a component schema, and that schema has a Go type and a TypeScript type.
func TestEventSchemasMatchGeneratedTypes(t *testing.T) {
	root := sourceDir(t)
	spec := readYAML(t, filepath.Join(root, "api", "v1.yaml"))
	paths, ok := spec["paths"].(map[string]any)
	if !ok {
		t.Fatal("paths")
	}
	components, ok := spec["components"].(map[string]any)
	if !ok {
		t.Fatal("components")
	}
	schemas, ok := components["schemas"].(map[string]any)
	if !ok {
		t.Fatal("schemas")
	}

	goSrc := string(mustRead(t, filepath.Join(root, "..", "..", "contract", "gen", "api", "v1", "types.gen.go")))
	tsSrc := string(mustRead(t, filepath.Join(root, "..", "..", "web", "src", "lib", "gen", "api", "v1.ts")))

	seen := map[string]map[string]string{}
	for path, item := range paths {
		ops, ok := item.(map[string]any)
		if !ok {
			continue
		}
		for method, op := range ops {
			body, ok := op.(map[string]any)
			if !ok {
				continue
			}
			raw, ok := body["x-mftiko-events"]
			if !ok {
				continue
			}
			events, ok := raw.(map[string]any)
			if !ok {
				t.Fatalf("%s %s x-mftiko-events is %T", method, path, raw)
			}
			got := map[string]string{}
			for name, schema := range events {
				s, ok := schema.(string)
				if !ok || s == "" {
					t.Fatalf("%s %s event %s schema %v", method, path, name, schema)
				}
				if _, ok := schemas[s]; !ok {
					t.Fatalf("%s %s event %s schema %s is not a component", method, path, name, s)
				}
				if !goTypeExists(goSrc, s) {
					t.Fatalf("Go type %s missing from types.gen.go", s)
				}
				if !tsSchemaExists(tsSrc, s) {
					t.Fatalf("TypeScript schema %s missing from v1.ts", s)
				}
				got[name] = s
			}
			seen[path] = got
		}
	}

	wantLogs := map[string]string{"log": "LogEvent", "gap": "LogGapEvent", "end": "LogEndEvent"}
	if !sameStrings(seen["/sessions/{session_id}/logs"], wantLogs) {
		t.Fatalf("logs events = %v", seen["/sessions/{session_id}/logs"])
	}
	wantEvents := map[string]string{
		"session":         "SessionDetail",
		"session_removed": "SessionRemovedEvent",
		"instance":        "InstanceDetail",
		"reset":           "ResetEvent",
	}
	if !sameStrings(seen["/events"], wantEvents) {
		t.Fatalf("/events = %v", seen["/events"])
	}
	if len(seen) != 2 {
		t.Fatalf("operations with x-mftiko-events = %d", len(seen))
	}
}

func sameStrings(got, want map[string]string) bool {
	if len(got) != len(want) {
		return false
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

func sourceDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("caller")
	}
	return filepath.Dir(file)
}

func readYAML(t *testing.T, path string) map[string]any {
	t.Helper()
	var doc map[string]any
	if err := yaml.Unmarshal(mustRead(t, path), &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func goTypeExists(src, name string) bool {
	for _, kind := range []string{"struct", "string"} {
		if regexp.MustCompile(`(?m)^type ` + regexp.QuoteMeta(name) + ` ` + kind + `\b`).MatchString(src) {
			return true
		}
	}
	return false
}

func tsSchemaExists(src, name string) bool {
	return regexp.MustCompile(`(?m)^[ \t]+` + regexp.QuoteMeta(name) + `:`).MatchString(src)
}
