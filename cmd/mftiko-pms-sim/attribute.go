package main

import (
	"slices"
	"time"

	"github.com/BullionBear/mftiko/contract/wire/cloid"
)

// interval is when one session could have placed orders on one api_id (E21-D011).
// Empty means never: it never spawned, or its record was abandoned. Open means no end yet.
type interval struct {
	Start time.Time
	End   time.Time
	Open  bool
	Empty bool
	// Source names what ended it: last_exit, sts_retired_at, or none.
	Source string
}

func (iv interval) contains(t time.Time) bool {
	if iv.Empty || t.Before(iv.Start) {
		return false
	}
	return iv.Open || !t.After(iv.End)
}

// sessionFacts is what the API said about one session record. Status is the record's final, or,
// while the spec is still in etcd, the live status from /sessions/{id}.
type sessionFacts struct {
	State     string // pending, created, abandoned, closed
	CreatedAt time.Time
	Terminal  bool       // the status phase is done or failed
	LastExit  *time.Time // status.last_exit.at
	Retired   *time.Time // sts_retired_at
	NoStatus  bool       // the STS has written no status yet
}

// sessionInterval applies E21-D011 and E21-D083:
//   - start is created_at;
//   - a terminal status ends it at last_exit.at; terminal with no last_exit never spawned (empty);
//   - an abandoned record never reached etcd (empty);
//   - a retired STS ends it at sts_retired_at, or earlier at last_exit.at;
//   - anything else is still open.
func sessionInterval(f sessionFacts) interval {
	iv := interval{Start: f.CreatedAt, Source: "none"}
	switch {
	case f.State == "abandoned":
		iv.Empty = true
		return iv
	case f.Terminal && f.LastExit == nil:
		iv.Empty = true
		return iv
	case f.Terminal:
		iv.End = *f.LastExit
		iv.Source = "last_exit"
	default:
		iv.Open = true
	}
	if f.Retired != nil && (iv.Open || f.Retired.Before(iv.End)) {
		iv.Open = false
		iv.End = *f.Retired
		iv.Source = "sts_retired_at"
	}
	return iv
}

// owner is one (session, api_id) of an exchange account, with its algos and interval.
type owner struct {
	RecordID  string
	SessionID string
	APIID     int64
	Algos     []uint32
	Interval  interval
}

// attribution says which session placed an order.
type attribution struct {
	// Kind is session (exactly one owner), manual (not a platform client_order_id), unattributed
	// (a platform id no session owned), or ambiguous (more than one owner matched).
	Kind      string `json:"kind"`
	RecordID  string `json:"record_id,omitempty"`
	SessionID string `json:"session_id,omitempty"`
	APIID     int64  `json:"api_id,omitempty"`
	Algo      uint32 `json:"algo,omitempty"`
	TsMs      int64  `json:"ts_ms,omitempty"`
	// Candidates lists the sessions of an ambiguous match.
	Candidates []string `json:"candidates,omitempty"`
}

// attribute decodes the client_order_id and finds the owner whose algo_ids hold its algo and
// whose interval holds its ts_ms (E21-D011). The fill time plays no part: an order resting after
// its session ended still belongs to that session.
func attribute(clientOrderID string, owners []owner) attribution {
	id, err := cloid.Decode(clientOrderID)
	if err != nil {
		// Placed by hand or by another system (cloid.ErrNotPlatform), or not this layout at all.
		return attribution{Kind: "manual"}
	}
	at := time.UnixMilli(id.TsMs)
	out := attribution{Kind: "unattributed", Algo: id.Algo, TsMs: id.TsMs}
	// One session can declare two api_ids of the same exchange account; that is still one owner.
	var hits []owner
	for _, o := range owners {
		if !slices.Contains(o.Algos, id.Algo) || !o.Interval.contains(at) {
			continue
		}
		if !slices.ContainsFunc(hits, func(h owner) bool { return h.RecordID == o.RecordID }) {
			hits = append(hits, o)
		}
	}
	switch len(hits) {
	case 0:
		return out
	case 1:
		out.Kind = "session"
		out.RecordID, out.SessionID, out.APIID = hits[0].RecordID, hits[0].SessionID, hits[0].APIID
		return out
	}
	out.Kind = "ambiguous"
	for _, h := range hits {
		out.Candidates = append(out.Candidates, h.SessionID)
	}
	return out
}
