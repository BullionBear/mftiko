package agent

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent/runner"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// decoder reads the keys of one instance.
type decoder struct{ inst string }

// Decode reads one key: a demand, the value a state.v1.DemandEntry whose api_id is the key's; or an
// account, the value a state.v1.Account.
func (d decoder) Decode(key string, value []byte) (Entry, error) {
	if rest, ok := strings.CutPrefix(key, state.DemandPrefix(state.PlaneTD, d.inst)); ok {
		session, id, ok := splitDemand(rest)
		if !ok {
			return Entry{}, errBadKey
		}
		e := &statev1.DemandEntry{}
		if err := proto.Unmarshal(value, e); err != nil {
			return Entry{}, errBadDemand
		}
		x, isAccount := e.GetTarget().(*statev1.DemandEntry_ApiId)
		switch {
		case !isAccount:
			return Entry{}, errNotAccount
		case x.ApiId != id:
			return Entry{}, errMismatch
		}
		return Entry{Demand: &Demand{Session: session, APIID: id}}, nil
	}
	if rest, ok := strings.CutPrefix(key, state.AccountPrefix(d.inst)); ok {
		id, err := strconv.ParseInt(rest, 10, 64)
		if err != nil || id <= 0 {
			return Entry{}, errBadKey
		}
		a := &statev1.Account{}
		if err := proto.Unmarshal(value, a); err != nil {
			return Entry{}, errBadAccount
		}
		return Entry{Account: &Account{
			APIID: id, Kind: a.GetKind(), Sealed: append([]byte(nil), a.GetSealedCredentials()...),
			Enabled: a.GetEnabled(), CancelOnDisconnectS: a.GetSettings().GetCancelOnDisconnectS(),
		}}, nil
	}
	return Entry{}, errBadKey
}

// sources is the demand and the account mirrors as one runner.Source: the agent plans from both, and
// either changing is a reason to plan again. The two prefixes do not overlap.
type sources struct {
	demand, account runner.Source

	once   sync.Once
	synced chan struct{}
	notify chan struct{}
}

func newSources(demand, account runner.Source) *sources {
	return &sources{demand: demand, account: account, synced: make(chan struct{}), notify: make(chan struct{}, 1)}
}

// run fans the two mirrors' notifications in, until done.
func (s *sources) run(done <-chan struct{}) {
	go func() {
		for _, src := range []runner.Source{s.demand, s.account} {
			select {
			case <-src.Synced():
			case <-done:
				return
			}
		}
		close(s.synced)
	}()
	for {
		select {
		case <-done:
			return
		case <-s.demand.Notify():
		case <-s.account.Notify():
		}
		select {
		case s.notify <- struct{}{}:
		default:
		}
	}
}

func (s *sources) Synced() <-chan struct{} { return s.synced }
func (s *sources) Notify() <-chan struct{} { return s.notify }

// Snapshot is both prefixes. Rev is the demand's: the runner reports it, and the accounts change
// rarely.
func (s *sources) Snapshot() state.View {
	d, a := s.demand.Snapshot(), s.account.Snapshot()
	out := state.View{Rev: max(d.Rev, a.Rev), Entries: make(map[string]state.Entry, len(d.Entries)+len(a.Entries))}
	for k, v := range d.Entries {
		out.Entries[k] = v
	}
	for k, v := range a.Entries {
		out.Entries[k] = v
	}
	return out
}

// LastContact is the older of the two: the copy is only as fresh as its staler half.
func (s *sources) LastContact() time.Time {
	d, a := s.demand.LastContact(), s.account.LastContact()
	if d.Before(a) {
		return d
	}
	return a
}
