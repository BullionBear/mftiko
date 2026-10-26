package agent

import (
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/artifact"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// artifactPullType is the SessionStatus condition written while an STS pull keeps failing (E12-D068).
// The API repeats this string; it does not import this package.
const artifactPullType = "artifact_pull"

// dependencyConditions is the terminal SessionStatus conditions for the demand
// that is still present (E10-D093, E10-D014). A nil demand writes none. REJECTED and
// DEAD bindings each become one Condition, in binding order. The message is
// four single-space tokens: kind, ident, provider instance, rejected|dead.
func dependencyConditions(d *statev1.SessionDemand, atNs int64) []*commonv1.Condition {
	if d == nil {
		return nil
	}
	var out []*commonv1.Condition
	for _, b := range d.GetBindings() {
		if b == nil || b.GetStatus() == nil {
			continue
		}
		word, ok := terminalWord(b.GetStatus().GetState())
		if !ok {
			continue
		}
		kind, ident, ok := bindingTokens(b)
		if !ok {
			continue
		}
		out = append(out, &commonv1.Condition{
			Type:    "dependency",
			Status:  false,
			Reason:  b.GetStatus().GetReason(),
			Message: kind + " " + ident + " " + b.GetInstance() + " " + word,
			AtNs:    atNs,
		})
	}
	return out
}

func terminalWord(st statev1.EntryState) (string, bool) {
	switch st {
	case statev1.EntryState_ENTRY_STATE_REJECTED:
		return "rejected", true
	case statev1.EntryState_ENTRY_STATE_DEAD:
		return "dead", true
	default:
		return "", false
	}
}

func bindingTokens(b *statev1.Binding) (kind, ident string, ok bool) {
	switch b.GetTarget().(type) {
	case *statev1.Binding_Feed:
		if b.GetFeed() == "" {
			return "", "", false
		}
		return "feed", b.GetFeed(), true
	case *statev1.Binding_ApiId:
		return "account", strconv.FormatInt(b.GetApiId(), 10), true
	case *statev1.Binding_Selector:
		if b.GetSelector() == "" {
			return "", "", false
		}
		return "selector", b.GetSelector(), true
	default:
		return "", "", false
	}
}

// liveDemand is the SessionDemand currently in the demand mirror. A missing
// mirror, a missing key, or a value that will not decode is nil. The remembered
// assignment is not a substitute (E10-D093).
func (p *plane) liveDemand(id string) *statev1.SessionDemand {
	if p == nil || p.demand == nil {
		return nil
	}
	e, ok := p.demand.Snapshot().Entries[state.SessionDemandKey(p.inst, id)]
	if !ok {
		return nil
	}
	raw := append([]byte(nil), e.Value...)
	d, err := parseDemand(raw)
	if err != nil {
		return nil
	}
	return d
}

// pullFact is one digest's pull jobs for this session (E12-D068).
type pullFact struct {
	ready   bool
	perm    bool
	unknown bool
	haveJob bool
	streak  bool
	fails   uint32
	since   time.Time
	reason  string
}

func (f *pullFact) observe(exists, ready bool, perm string, fails uint32, since time.Time, reason string) {
	if !exists {
		return
	}
	f.haveJob = true
	if ready && perm == "" {
		f.ready = true
		return
	}
	if perm != "" {
		f.perm = true
		return
	}
	if fails > 0 && !since.IsZero() {
		if !f.streak || since.Before(f.since) || (since.Equal(f.since) && fails > f.fails) {
			f.streak = true
			f.fails = fails
			f.since = since
			f.reason = reason
		}
		return
	}
	f.unknown = true
}

// artifactPullConditions is the artifact_pull conditions for one non-terminal session.
// A ready job drops the digest. A streak from this process overrides a condition already
// on the status. A permanent rejection emits nothing. Until this process has a result,
// a well-formed published condition is kept so a restart does not flap the gauge (E12-D068).
func (p *puller) artifactPullConditions(id string, spec *sessionv1.SessionSpec, published []*commonv1.Condition) []*commonv1.Condition {
	if p == nil || spec == nil {
		return nil
	}
	digs := specPullDigests(spec)
	if len(digs) == 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	kept := indexArtifactPull(published)
	var out []*commonv1.Condition
	for _, dig := range digs {
		if c := conditionFor(dig, p.pullFact(id, spec, dig), kept[dig]); c != nil {
			out = append(out, c)
		}
	}
	return out
}

func (p *puller) pullFact(id string, spec *sessionv1.SessionSpec, dig string) pullFact {
	var f pullFact
	if spec.GetStrategy().GetCode() == dig {
		if job, ok := p.jobs[dig]; ok {
			f.observe(true, job.ready, job.perm, job.streakFails, job.streakSince, job.streakReason)
		}
	}
	for _, in := range spec.GetInputs() {
		if in.GetDigest() != dig {
			continue
		}
		if job, ok := p.inputs[inputKey(id, in.GetName())]; ok {
			f.observe(true, job.ready, job.perm, job.streakFails, job.streakSince, job.streakReason)
		}
	}
	return f
}

func conditionFor(dig string, f pullFact, published *commonv1.Condition) *commonv1.Condition {
	if f.ready {
		return nil
	}
	if f.streak {
		reason := f.reason
		if reason == "" {
			reason = artifact.ReasonUnavailable
		}
		return &commonv1.Condition{
			Type:    artifactPullType,
			Reason:  reason,
			Message: strconv.FormatUint(uint64(f.fails), 10) + " " + dig,
			AtNs:    f.since.UnixNano(),
		}
	}
	if f.haveJob && f.perm && !f.unknown {
		return nil
	}
	if published == nil {
		return nil
	}
	return proto.Clone(published).(*commonv1.Condition)
}

func specPullDigests(spec *sessionv1.SessionSpec) []string {
	set := map[string]struct{}{}
	add := func(d string) {
		if digestOK(d) {
			set[d] = struct{}{}
		}
	}
	if spec != nil {
		add(spec.GetStrategy().GetCode())
		for _, in := range spec.GetInputs() {
			add(in.GetDigest())
		}
	}
	return slices.Sorted(maps.Keys(set))
}

// parseArtifactPull reports a well-formed artifact_pull condition (E12-D068).
func parseArtifactPull(c *commonv1.Condition) (dig string, fails uint32, at int64, ok bool) {
	if c == nil || c.GetType() != artifactPullType || c.GetStatus() || c.GetAtNs() <= 0 || c.GetReason() == "" {
		return "", 0, 0, false
	}
	failsStr, dig, found := strings.Cut(c.GetMessage(), " ")
	if !found || strings.Contains(dig, " ") || !digestOK(dig) {
		return "", 0, 0, false
	}
	n, err := strconv.ParseUint(failsStr, 10, 32)
	if err != nil || n == 0 || strconv.FormatUint(n, 10) != failsStr {
		return "", 0, 0, false
	}
	return dig, uint32(n), c.GetAtNs(), true
}

func indexArtifactPull(conds []*commonv1.Condition) map[string]*commonv1.Condition {
	out := map[string]*commonv1.Condition{}
	for _, c := range conds {
		dig, fails, at, ok := parseArtifactPull(c)
		if !ok {
			continue
		}
		prev, exists := out[dig]
		if !exists {
			out[dig] = c
			continue
		}
		_, prevFails, prevAt, _ := parseArtifactPull(prev)
		if at < prevAt || (at == prevAt && fails > prevFails) {
			out[dig] = c
		}
	}
	return out
}

func conditionsEqual(a, b []*commonv1.Condition) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !proto.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

// notePullConditions records artifact_pull for non-terminal sessions. The caller holds p.mu.
// A nil puller clears the record so a previous round cannot linger (E12-D068).
func (p *plane) notePullConditions(sessions map[string]*sess) {
	if p.pulls == nil {
		p.pullConds = map[string][]*commonv1.Condition{}
		return
	}
	out := map[string][]*commonv1.Condition{}
	for _, id := range slices.Sorted(maps.Keys(sessions)) {
		s := sessions[id]
		if s == nil || s.terminal() || s.parsed == nil {
			continue
		}
		conds := p.pulls.artifactPullConditions(id, s.parsed, p.publishedConditions(id))
		if len(conds) > 0 {
			out[id] = conds
		}
	}
	p.pullConds = out
}

// publishedConditions is the last status this process wrote, or the mirror's after a restart.
// The caller holds p.mu.
func (p *plane) publishedConditions(id string) []*commonv1.Condition {
	if st := p.saved[id]; st != nil {
		return st.GetConditions()
	}
	if p.status == nil {
		return nil
	}
	e, ok := p.status.Snapshot().Entries[state.SessionStatusKey(p.inst, id)]
	if !ok {
		return nil
	}
	st, err := parseStatus(e.Value)
	if err != nil {
		return nil
	}
	return st.GetConditions()
}
