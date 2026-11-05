package api

import (
	"cmp"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// ListScopes is GET /scopes (E21-D010, E21-D068). Rows come from the mirrors.
// One (api_id, uid) across instances keeps the earliest since. An unavailable
// region is named in freshness and its keys are absent from this response.
func (s *Server) ListScopes(w http.ResponseWriter, _ *http.Request, params apiv1.ListScopesParams) {
	limit, after, fields := bindPage(params.Limit, params.Cursor)
	if after != "" && len(fields) == 0 && !canonicalScopeCursor(after) {
		fields = append(fields, apiv1.FieldError{Path: "/cursor", Reason: "is not a cursor from this API"})
	}
	var only *int64
	if params.ApiId != nil {
		id := int64(*params.ApiId)
		if id < 1 || id > maxAPIID {
			fields = append(fields, apiv1.FieldError{Path: "/api_id", Reason: "must be between 1 and 9007199254740991"})
		} else {
			only = &id
		}
	}
	if len(fields) > 0 {
		writeFields(w, fields)
		return
	}
	views, unavail := s.regionViews()
	rows := scopesIn(s.cfg.Topology, views, only)
	keys := make([]string, len(rows))
	for i, row := range rows {
		keys[i] = scopePageKey(row.apiID, row.uid)
	}
	start, end, next := pageKeys(keys, after, limit)
	page := rows[start:end]
	items := make([]apiv1.Scope, 0, len(page))
	for _, row := range page {
		items = append(items, apiv1.Scope{
			ApiId: row.apiID,
			Uid:   int64(row.uid),
			Since: time.Unix(0, row.since).UTC(),
		})
	}
	writeJSON(w, http.StatusOK, apiv1.ScopeList{
		Items: items, NextCursor: next, Freshness: s.freshness(views, unavail),
	})
}

type scopeRow struct {
	apiID int64
	uid   uint32
	since int64
}

// scopesIn merges scope keys from the available region views. A key is taken only from
// its instance's home region. since is the earliest value (E21-D010).
func scopesIn(topo state.Topology, views map[string]state.View, only *int64) []scopeRow {
	type id struct {
		api int64
		uid uint32
	}
	best := map[id]int64{}
	for region, view := range views {
		for key, e := range view.Entries {
			inst, apiID, uid, ok := scopeOf(key)
			if !ok {
				continue
			}
			if only != nil && apiID != *only {
				continue
			}
			home, err := topo.RegionOf(inst)
			if err != nil || home != region {
				continue
			}
			since, ok := state.ParseSince(e.Value)
			if !ok {
				continue
			}
			k := id{apiID, uid}
			if cur, seen := best[k]; !seen || since < cur {
				best[k] = since
			}
		}
	}
	rows := make([]scopeRow, 0, len(best))
	for k, since := range best {
		rows = append(rows, scopeRow{apiID: k.api, uid: k.uid, since: since})
	}
	slices.SortFunc(rows, func(a, b scopeRow) int {
		if c := cmp.Compare(a.apiID, b.apiID); c != 0 {
			return c
		}
		return cmp.Compare(a.uid, b.uid)
	})
	return rows
}

// scopeOf parses /td/{inst}/scope/{api_id}/{uid}. The two ids must be canonical decimals.
func scopeOf(key string) (inst string, apiID int64, uid uint32, ok bool) {
	p, inst, err := state.Owner(key)
	if err != nil || p != state.PlaneTD {
		return "", 0, 0, false
	}
	rest, ok := strings.CutPrefix(key, state.ScopePrefix(inst))
	if !ok {
		return "", 0, 0, false
	}
	apiPart, uidPart, ok := strings.Cut(rest, "/")
	if !ok || apiPart == "" || uidPart == "" || strings.Contains(uidPart, "/") {
		return "", 0, 0, false
	}
	apiID, err = strconv.ParseInt(apiPart, 10, 64)
	if err != nil || apiID < 1 || apiID > maxAPIID || strconv.FormatInt(apiID, 10) != apiPart {
		return "", 0, 0, false
	}
	u, err := strconv.ParseUint(uidPart, 10, 32)
	if err != nil || u < 1 || strconv.FormatUint(u, 10) != uidPart {
		return "", 0, 0, false
	}
	return inst, apiID, uint32(u), true
}

// scopePageKey sorts by api_id then uid. Both are zero-padded so the byte order matches the numeric order.
func scopePageKey(apiID int64, uid uint32) string {
	return fmt.Sprintf("%016d\x00%010d", apiID, uid)
}

// canonicalScopeCursor reports whether after is a scopePageKey this API would have issued.
func canonicalScopeCursor(after string) bool {
	apiPart, uidPart, ok := strings.Cut(after, "\x00")
	if !ok || strings.Contains(uidPart, "\x00") {
		return false
	}
	apiID, err := strconv.ParseInt(apiPart, 10, 64)
	if err != nil || apiID < 1 || apiID > maxAPIID {
		return false
	}
	u, err := strconv.ParseUint(uidPart, 10, 32)
	if err != nil || u < 1 {
		return false
	}
	return after == scopePageKey(apiID, uint32(u))
}
