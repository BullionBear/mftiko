package api

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestReadyStatus(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	threshold := 15 * time.Second

	t.Run("one fresh region is enough", func(t *testing.T) {
		body, code := readyStatus([]string{"tw", "jp"}, []regionSnap{
			{name: "tw", synced: true, contact: now, rev: 3},
			{name: "jp", synced: true, contact: now.Add(-time.Minute), rev: 4},
		}, now, threshold, false)
		if code != http.StatusOK {
			t.Fatalf("status %d", code)
		}
		if !body.Regions["tw"].Available || !body.Regions["tw"].Synced || body.Regions["tw"].Revision != 3 {
			t.Fatalf("tw %+v", body.Regions["tw"])
		}
		if body.Regions["jp"].Available || !body.Regions["jp"].Synced || body.Regions["jp"].Revision != 4 {
			t.Fatalf("jp %+v", body.Regions["jp"])
		}
		if !slices.Equal(body.UnavailableRegions, []string{"jp"}) {
			t.Fatalf("%v", body.UnavailableRegions)
		}
	})

	t.Run("every region unavailable", func(t *testing.T) {
		body, code := readyStatus([]string{"tw", "jp"}, []regionSnap{
			{name: "tw", synced: false, rev: 9},
			{name: "jp", synced: true, contact: now.Add(-time.Minute), rev: 4},
		}, now, threshold, false)
		if code != http.StatusServiceUnavailable {
			t.Fatalf("status %d", code)
		}
		if body.Regions["tw"].Available || body.Regions["tw"].Synced || body.Regions["tw"].Revision != 0 {
			t.Fatalf("tw %+v", body.Regions["tw"])
		}
		if !slices.Equal(body.UnavailableRegions, []string{"jp", "tw"}) {
			t.Fatalf("%v", body.UnavailableRegions)
		}
	})

	t.Run("not synced reports revision 0", func(t *testing.T) {
		body, code := readyStatus([]string{"tw"}, []regionSnap{
			{name: "tw", synced: false, contact: now, rev: 9},
		}, now, threshold, false)
		if code != http.StatusServiceUnavailable {
			t.Fatalf("status %d", code)
		}
		got := body.Regions["tw"]
		if got.Available || got.Synced || got.Revision != 0 {
			t.Fatalf("%+v", got)
		}
	})

	t.Run("missing snap is unavailable", func(t *testing.T) {
		body, code := readyStatus([]string{"tw"}, nil, now, threshold, false)
		if code != http.StatusServiceUnavailable || body.Regions["tw"].Available || body.Regions["tw"].Revision != 0 {
			t.Fatalf("%d %+v", code, body.Regions["tw"])
		}
	})

	t.Run("zero contact is unavailable", func(t *testing.T) {
		body, code := readyStatus([]string{"tw"}, []regionSnap{
			{name: "tw", synced: true, rev: 5},
		}, now, threshold, false)
		if code != http.StatusServiceUnavailable || body.Regions["tw"].Available || body.Regions["tw"].Revision != 5 {
			t.Fatalf("%d %+v", code, body.Regions["tw"])
		}
	})

	t.Run("exactly the threshold is available", func(t *testing.T) {
		body, code := readyStatus([]string{"tw"}, []regionSnap{
			{name: "tw", synced: true, contact: now.Add(-threshold), rev: 1},
		}, now, threshold, false)
		if code != http.StatusOK || !body.Regions["tw"].Available {
			t.Fatalf("%d %+v", code, body.Regions["tw"])
		}
	})

	t.Run("strictly past the threshold is unavailable", func(t *testing.T) {
		body, code := readyStatus([]string{"tw"}, []regionSnap{
			{name: "tw", synced: true, contact: now.Add(-threshold - time.Nanosecond), rev: 1},
		}, now, threshold, false)
		if code != http.StatusServiceUnavailable || body.Regions["tw"].Available {
			t.Fatalf("%d %+v", code, body.Regions["tw"])
		}
	})

	t.Run("draining is 503 while regions are available", func(t *testing.T) {
		body, code := readyStatus([]string{"tw"}, []regionSnap{
			{name: "tw", synced: true, contact: now, rev: 1},
		}, now, threshold, true)
		if code != http.StatusServiceUnavailable || !body.Regions["tw"].Available {
			t.Fatalf("%d %+v", code, body.Regions["tw"])
		}
		if len(body.UnavailableRegions) != 0 {
			t.Fatalf("%v", body.UnavailableRegions)
		}
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		if !json.Valid(b) || !strings.Contains(string(b), `"unavailable_regions":[]`) {
			t.Fatalf("%s", b)
		}
	})

	t.Run("no regions", func(t *testing.T) {
		body, code := readyStatus(nil, nil, now, threshold, false)
		if code != http.StatusServiceUnavailable || len(body.UnavailableRegions) != 0 {
			t.Fatalf("%d %+v", code, body)
		}
	})
}
