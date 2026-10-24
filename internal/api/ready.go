package api

import (
	"net/http"
	"slices"
	"time"
)

// regionSnap is one cluster as its mirror last reported it.
type regionSnap struct {
	name    string
	synced  bool
	contact time.Time
	rev     int64
}

// available is E10-D014's test: the first sync has finished, and the cluster was heard from no more than threshold ago
// (strictly older is unavailable, as for the controller). Readiness and the id checks of writes (E10-D042) share it.
func (s regionSnap) available(now time.Time, threshold time.Duration) bool {
	return s.synced && !s.contact.IsZero() && now.Sub(s.contact) <= threshold
}

// regionStatus is one region in the /readyz body (E10-D026).
type regionStatus struct {
	Available bool  `json:"available"`
	Synced    bool  `json:"synced"`
	Revision  int64 `json:"revision"`
}

// readyBody is the /readyz body. UnavailableRegions is empty, never null, when every region is available.
type readyBody struct {
	Regions            map[string]regionStatus `json:"regions"`
	UnavailableRegions []string                `json:"unavailable_regions"`
}

// readyStatus decides /readyz. A region is unavailable until its first sync, or once LastContact is older than
// the threshold (E10-D014, the same comparison as the controller: strictly older). Any available region is 200.
// Every region unavailable is 503. draining is the shutdown path: readiness is 503 while the process is still
// answering (E10-D027), ahead of closing the listener.
func readyStatus(names []string, snaps []regionSnap, now time.Time, threshold time.Duration, draining bool) (readyBody, int) {
	byName := make(map[string]regionSnap, len(snaps))
	for _, s := range snaps {
		byName[s.name] = s
	}
	body := readyBody{
		Regions:            make(map[string]regionStatus, len(names)),
		UnavailableRegions: []string{},
	}
	for _, name := range names {
		s := byName[name]
		available := s.available(now, threshold)
		rev := int64(0)
		if s.synced {
			rev = s.rev
		}
		body.Regions[name] = regionStatus{Available: available, Synced: s.synced, Revision: rev}
		if !available {
			body.UnavailableRegions = append(body.UnavailableRegions, name)
		}
	}
	slices.Sort(body.UnavailableRegions)
	if draining || len(body.UnavailableRegions) == len(names) {
		return body, http.StatusServiceUnavailable
	}
	return body, http.StatusOK
}
