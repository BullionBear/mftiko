package agent

import (
	"crypto/sha256"
	"encoding/hex"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/sealbox"
	"github.com/BullionBear/mftiko/internal/td/account"
)

// DefaultLinger is how long an account with no owners keeps its worker (E09-D005).
const DefaultLinger = 5 * time.Second

// Reasons of a REJECTED ack the plan decides. The worker's own (auth, hedge_mode,
// kind_unsupported) come from its ending; the sealing ones from sealbox.Reason.
const (
	ReasonNoAccount       = "account_not_found"
	ReasonDisabled        = "account_disabled"
	ReasonUnknownKind     = "unknown_kind"
	ReasonKindUnsupported = account.ReasonKindUnsupported
)

// Endpoint is the venue hosts this instance uses for one account kind (E06-D016, as MD's url, E04-D003).
// Empty is the adapter's default.
type Endpoint struct {
	REST      string `json:"rest,omitempty"`
	Stream    string `json:"stream,omitempty"`
	Simulated bool   `json:"simulated,omitempty"`
}

// Spec is one account worker's identity: what Init carries. A change is a new incarnation: a
// resealed credential, another kind, another host. The credentials are in it sealed; Hash has
// their digest only.
type Spec struct {
	APIID               int64
	Kind                string
	Sealed              []byte
	CancelOnDisconnectS uint32
	Endpoint            Endpoint
}

// Hash is a stable identity of the spec.
func (s Spec) Hash() string {
	sealed := sha256.Sum256(s.Sealed)
	h := sha256.New()
	for _, part := range []string{
		strconv.FormatInt(s.APIID, 10), s.Kind, hex.EncodeToString(sealed[:]),
		strconv.FormatUint(uint64(s.CancelOnDisconnectS), 10),
		s.Endpoint.REST, s.Endpoint.Stream, strconv.FormatBool(s.Endpoint.Simulated),
	} {
		h.Write([]byte(part))
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil)[:8])
}

// Live is empty: TD has no live config; Init carries everything (E01-D011).
type Live struct{}

// Hash is "" for every Live, which is also what a worker that applied nothing reports: nothing is
// ever pushed.
func (Live) Hash() string { return "" }

// WorkerKey is the account worker of apiID.
func WorkerKey(apiID int64) agent.WorkerKey {
	return agent.WorkerKey("account/" + strconv.FormatInt(apiID, 10))
}

// Kinds are the account kinds the exchange registry declares.
type Kinds interface {
	AccountKind(name string) (exchange.AccountKind, bool)
}

// Opener checks that an account's sealed credentials open with this instance's key and were sealed
// for it (E06-D013). It returns sealbox's error and keeps nothing.
type Opener func(sealed []byte, apiID int64, kind string) error

// outcome is what the plan decided for one demand key: the worker it is served by, or why not.
type outcome struct {
	Session string
	APIID   int64
	Worker  agent.WorkerKey
	Reject  string
}

// planIn is what one plan reads. lingering is the previous plan's: when each account lost its last
// owner.
type planIn struct {
	now       time.Time
	linger    time.Duration
	kinds     Kinds
	open      Opener
	endpoints map[string]Endpoint
	demand    agent.Demand[Entry]
	lingering map[int64]time.Time
	prev      map[int64]Spec // the specs the previous plan ran, kept through linger
}

type planOut struct {
	plans     map[agent.WorkerKey]agent.Plan[Spec, Live]
	outcomes  map[string]outcome // by demand key
	lingering map[int64]time.Time
	specs     map[int64]Spec
	wake      time.Time // when the next lingering account is due to stop; zero if none
}

// plan is the agent's decision, with no I/O: which accounts get a worker, and each demand's verdict.
func plan(in planIn) planOut {
	out := planOut{
		plans: map[agent.WorkerKey]agent.Plan[Spec, Live]{}, outcomes: map[string]outcome{},
		lingering: map[int64]time.Time{}, specs: map[int64]Spec{},
	}
	accounts := map[int64]*Account{}
	owners := map[int64][]string{}
	for key, e := range in.demand.Items {
		switch {
		case e.Account != nil:
			accounts[e.Account.APIID] = e.Account
		case e.Demand != nil:
			owners[e.Demand.APIID] = append(owners[e.Demand.APIID], key)
		}
	}
	// A demand key Decode refused is REJECTED with Decode's reason. An account key it refused
	// leaves that account missing, which its demands report.
	for key, err := range in.demand.Rejected {
		if session, id, ok := demandParts(key); ok {
			out.outcomes[key] = outcome{Session: session, APIID: id, Reject: err.Error()}
		}
	}

	verdicts := map[int64]string{} // api_id -> reject reason, "" when it runs
	for id, keys := range owners {
		spec, reject := in.specOf(id, accounts[id])
		verdicts[id] = reject
		if reject == "" {
			out.specs[id] = spec
			out.plans[WorkerKey(id)] = agent.Plan[Spec, Live]{Spawn: spec, Start: true}
		}
		for _, key := range keys {
			session, _, _ := demandParts(key)
			o := outcome{Session: session, APIID: id, Reject: reject}
			if reject == "" {
				o.Worker = WorkerKey(id)
			}
			out.outcomes[key] = o
		}
	}

	// An account whose last owner left keeps its worker for linger (E09-D005), with the spec it ran,
	// unless its declaration now rejects it: that stops at once.
	for _, id := range slices.Sorted(maps.Keys(in.prev)) {
		if _, owned := owners[id]; owned {
			continue
		}
		if _, reject := in.specOf(id, accounts[id]); reject != "" {
			continue
		}
		since, ok := in.lingering[id]
		if !ok {
			since = in.now
		}
		due := since.Add(in.linger)
		if !in.now.Before(due) {
			continue
		}
		out.lingering[id] = since
		out.specs[id] = in.prev[id]
		out.plans[WorkerKey(id)] = agent.Plan[Spec, Live]{Spawn: in.prev[id], Start: true}
		if out.wake.IsZero() || due.Before(out.wake) {
			out.wake = due
		}
	}
	return out
}

// specOf is the worker an account runs, or why it cannot run.
func (in planIn) specOf(id int64, a *Account) (Spec, string) {
	switch {
	case a == nil:
		return Spec{}, ReasonNoAccount
	case !a.Enabled:
		return Spec{}, ReasonDisabled
	}
	kind, ok := in.kinds.AccountKind(a.Kind)
	if !ok || kind.Account == nil {
		return Spec{}, ReasonUnknownKind
	}
	// One trade connection per api_id: a kind that trades several venues (BinancePM) is not run in
	// phase 1 (E06-D014).
	if len(kind.Trades) != 1 {
		return Spec{}, ReasonKindUnsupported
	}
	if err := in.open(a.Sealed, id, a.Kind); err != nil {
		return Spec{}, sealbox.Reason(err)
	}
	return Spec{APIID: id, Kind: a.Kind, Sealed: a.Sealed, CancelOnDisconnectS: a.CancelOnDisconnectS, Endpoint: in.endpoints[a.Kind]}, ""
}

// demandParts reads a demand key's session and api_id, whatever the instance.
func demandParts(key string) (session string, apiID int64, ok bool) {
	// .../td/{inst}/demand/{session}/{api_id}
	i := strings.LastIndex(key, "/demand/")
	if i < 0 {
		return "", 0, false
	}
	return splitDemand(key[i+len("/demand/"):])
}
