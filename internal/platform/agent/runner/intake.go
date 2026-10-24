package runner

import (
	"time"

	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// Source is the agent's demand/ prefix as Intake reads it: a state.Mirror of state.DemandPrefix.
type Source interface {
	Synced() <-chan struct{}
	Notify() <-chan struct{}
	Snapshot() state.View
	LastContact() time.Time
}

var _ Source = (*state.Mirror)(nil)

// Decode turns one demand entry into the plane's own type. An error rejects the entry: an unknown enum, a missing
// field, a value that does not parse. The Planner never sees what Decode refused (PROTOCOL §3.3).
type Decode[I any] func(key string, value []byte) (I, error)

// Intake reads the demand the controller writes for this instance (E01-D001, E09-D007), decoded into the plane's type.
type Intake[I any] struct {
	src    Source
	decode Decode[I]
	cache  map[string]decoded[I] // by key: what the entry decoded to, at the revisions it had
}

type decoded[I any] struct {
	create, mod int64
	item        I
	err         error
}

// NewIntake reads demand from src, decoding with decode.
func NewIntake[I any](src Source, decode Decode[I]) *Intake[I] {
	return &Intake[I]{src: src, decode: decode, cache: map[string]decoded[I]{}}
}

// Demand is the demand now: every entry decoded, those that would not be in Rejected. An entry is decoded once per
// version. LastContact is read before the view, so the view holds at least everything before it.
func (in *Intake[I]) Demand() agent.Demand[I] {
	contact := in.src.LastContact()
	v := in.src.Snapshot()
	d := agent.Demand[I]{Rev: v.Rev, Items: map[string]I{}, Rejected: map[string]error{}, LastContact: contact}
	for k, e := range v.Entries {
		c, ok := in.cache[k]
		if !ok || c.create != e.CreateRevision || c.mod != e.ModRevision {
			c = decoded[I]{create: e.CreateRevision, mod: e.ModRevision}
			c.item, c.err = in.decode(k, e.Value)
			in.cache[k] = c
		}
		if c.err != nil {
			d.Rejected[k] = c.err
		} else {
			d.Items[k] = c.item
		}
	}
	for k := range in.cache {
		if _, ok := v.Entries[k]; !ok {
			delete(in.cache, k)
		}
	}
	return d
}
