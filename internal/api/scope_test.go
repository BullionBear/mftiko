package api

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

func TestScopesIn(t *testing.T) {
	topo := state.Topology{
		Regions: map[string]state.Region{"tw": {}, "jp": {}},
		Instances: map[string]string{
			"td-tw-1": "tw",
			"td-jp-1": "jp",
		},
	}
	views := map[string]state.View{
		"tw": {Entries: map[string]state.Entry{
			state.ScopeKey("td-tw-1", 2, 7):       {Value: []byte("200")},
			state.ScopeKey("td-tw-1", 2, 8):       {Value: []byte("10")},
			state.ScopeKey("td-jp-1", 2, 7):       {Value: []byte("1")}, // not this region's instance
			state.ScopeKey("td-tw-1", 2, 9):       {Value: []byte("nope")},
			state.ScopePrefix("td-tw-1") + "2/0":  {Value: []byte("5")},
			state.ScopePrefix("td-tw-1") + "02/1": {Value: []byte("5")},
			state.ScopePrefix("td-tw-1") + "2/01": {Value: []byte("5")},
			state.ScopeKey("td-tw-1", 3, 1):       {Value: []byte("50")},
			state.AccountKey("td-tw-1", 2):        {Value: []byte("x")},
		}},
		"jp": {Entries: map[string]state.Entry{
			state.ScopeKey("td-jp-1", 2, 7): {Value: []byte("100")},
			state.ScopeKey("td-jp-1", 2, 8): {Value: []byte("9")},
			state.ScopeKey("td-jp-1", 4, 1): {Value: []byte("1")},
		}},
	}
	rows := scopesIn(topo, views, nil)
	want := []scopeRow{{2, 7, 100}, {2, 8, 9}, {3, 1, 50}, {4, 1, 1}}
	if len(rows) != len(want) {
		t.Fatalf("rows %#v", rows)
	}
	for i := range want {
		if rows[i] != want[i] {
			t.Fatalf("row %d %#v, want %#v", i, rows[i], want[i])
		}
	}
	only := int64(2)
	filtered := scopesIn(topo, views, &only)
	if len(filtered) != 2 || filtered[0].uid != 7 || filtered[1].uid != 8 || filtered[0].since != 100 || filtered[1].since != 9 {
		t.Fatalf("filter %#v", filtered)
	}

	keys := make([]string, len(rows))
	for i, row := range rows {
		keys[i] = scopePageKey(row.apiID, row.uid)
		if !canonicalScopeCursor(keys[i]) {
			t.Fatalf("cursor %q", keys[i])
		}
	}
	if canonicalScopeCursor("2") || canonicalScopeCursor(keys[0]+"\x00") {
		t.Fatal("accepted a cursor this API would not issue")
	}
	start, end, next := pageKeys(keys, "", 1)
	if start != 0 || end != 1 || next == nil {
		t.Fatalf("page %d %d %v", start, end, next)
	}
	raw, err := base64.RawURLEncoding.DecodeString(*next)
	if err != nil || string(raw) != keys[0] {
		t.Fatalf("cursor %q", *next)
	}
	start, end, next = pageKeys(keys, string(raw), 1)
	if start != 1 || end != 2 || next == nil {
		t.Fatalf("second page %d %d %v", start, end, next)
	}
}

func TestListScopes(t *testing.T) {
	e := newWriteEnv(t)
	e.cfg.Users = []User{{Name: "ada", Token: sha256.Sum256([]byte(tokenAda))}}
	s := e.replica(t)
	ctx := t.Context()
	client := &http.Client{Timeout: 5 * time.Second}
	base := "http://" + s.Addr()
	call := func(path string) (int, []byte) {
		t.Helper()
		code, _, body := (apiCall{method: http.MethodGet, path: path, bearer: tokenAda}).do(t, client, base)
		return code, body
	}

	plant := []struct{ region, key, val string }{
		{"tw", state.AgentKey(state.PlaneCtl, state.ControllerInstance), "agent"},
		{"tw", state.ScopeKey("td-tw-1", 2, 7), "200"},
		{"jp", state.ScopeKey("td-jp-1", 2, 7), "100"},
		{"tw", state.ScopeKey("td-tw-1", 2, 8), "10"},
		{"jp", state.ScopeKey("td-jp-1", 2, 8), "9"},
		{"jp", state.ScopeKey("td-jp-1", 2, 15), "1"},
		{"tw", state.ScopeKey("td-tw-1", 3, 1), "50"},
		{"tw", state.ScopePrefix("td-tw-1") + "2/nope", "1"},
		{"tw", state.ScopeKey("td-tw-1", 2, 11), "nope"},
		{"tw", state.ScopeKey("td-tw-1", 2, 12), "0"},
		{"tw", state.ScopePrefix("td-tw-1") + "2/01", "5"},
	}
	for _, p := range plant {
		putKey(t, ctx, e.servers[p.region], p.key, p.val)
		seen(t, s, p.region, p.key)
	}

	code, body := call("/api/v1/scopes")
	list := decodeScopes(t, code, body)
	if len(list.Freshness.UnavailableRegions) != 0 || list.Freshness.ControllerAbsent {
		t.Fatalf("freshness %+v", list.Freshness)
	}
	want := []scopeWant{{2, 7, 100}, {2, 8, 9}, {2, 15, 1}, {3, 1, 50}}
	if !sameScopes(list.Items, want) {
		t.Fatalf("items %#v", list.Items)
	}

	var paged []apiv1.Scope
	cursor := ""
	for n := 0; n < 6; n++ {
		path := "/api/v1/scopes?limit=1"
		if cursor != "" {
			path += "&cursor=" + url.QueryEscape(cursor)
		}
		code, body = call(path)
		page := decodeScopes(t, code, body)
		if cursor == "" && len(page.Items) != 1 {
			t.Fatalf("first page %#v", page.Items)
		}
		paged = append(paged, page.Items...)
		if page.NextCursor == nil {
			break
		}
		cursor = *page.NextCursor
	}
	if !sameScopes(paged, want) {
		t.Fatalf("paged %#v", paged)
	}

	code, body = call("/api/v1/scopes?api_id=2")
	if !sameScopes(decodeScopes(t, code, body).Items, want[:3]) {
		t.Fatalf("api 2 %s", body)
	}
	code, body = call("/api/v1/scopes?api_id=99")
	empty := decodeScopes(t, code, body)
	if len(empty.Items) != 0 || !strings.Contains(string(body), `"items":[]`) {
		t.Fatalf("empty %s", body)
	}

	for _, path := range []string{
		"/api/v1/scopes?limit=0",
		"/api/v1/scopes?limit=1001",
		"/api/v1/scopes?api_id=0",
		"/api/v1/scopes?api_id=9007199254740992",
		"/api/v1/scopes?api_id=nope",
		"/api/v1/scopes?cursor=abc",
		"/api/v1/scopes?cursor=" + base64.RawURLEncoding.EncodeToString([]byte("2")),
	} {
		code, body = call(path)
		if code != http.StatusBadRequest || !strings.Contains(string(body), `"validation_failed"`) {
			t.Fatalf("%s: %d %s", path, code, body)
		}
	}
	code, _, body = (apiCall{method: http.MethodGet, path: "/api/v1/scopes"}).do(t, client, base)
	if code != http.StatusUnauthorized {
		t.Fatalf("anonymous %d %s", code, body)
	}

	e.proxy.Cut()
	until(t, 15*time.Second, "jp unavailable", func() error {
		got, _ := s.readiness(time.Now())
		if len(got.UnavailableRegions) != 1 || got.UnavailableRegions[0] != "jp" {
			return fmt.Errorf("%v", got.UnavailableRegions)
		}
		return nil
	})
	code, body = call("/api/v1/scopes")
	partial := decodeScopes(t, code, body)
	if len(partial.Freshness.UnavailableRegions) != 1 || partial.Freshness.UnavailableRegions[0] != "jp" || partial.Freshness.ControllerAbsent {
		t.Fatalf("partial freshness %+v", partial.Freshness)
	}
	// jp held the earlier since. What remains is tw only, and the uid that existed only on jp is gone.
	if !sameScopes(partial.Items, []scopeWant{{2, 7, 200}, {2, 8, 10}, {3, 1, 50}}) {
		t.Fatalf("partial %#v", partial.Items)
	}
}

type scopeWant struct {
	api, uid, since int64
}

func decodeScopes(t *testing.T, code int, body []byte) apiv1.ScopeList {
	t.Helper()
	if code != http.StatusOK {
		t.Fatalf("status %d %s", code, body)
	}
	var list apiv1.ScopeList
	if err := json.Unmarshal(body, &list); err != nil {
		t.Fatal(err)
	}
	return list
}

func sameScopes(got []apiv1.Scope, want []scopeWant) bool {
	if len(got) != len(want) {
		return false
	}
	for i, w := range want {
		if got[i].ApiId != w.api || got[i].Uid != w.uid || got[i].Since.UnixNano() != w.since {
			return false
		}
	}
	return true
}
