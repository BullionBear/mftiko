package api

import (
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/shim"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

func (s *Server) sessionView(sts, id string, specBytes []byte, rev int64, rec *sessionRecord) (apiv1.Session, error) {
	views, _ := s.regionViews()
	return s.sessionIn(views, sts, id, specBytes, rev, rec)
}

func (s *Server) sessionIn(views map[string]state.View, sts, id string, specBytes []byte, rev int64, rec *sessionRecord) (apiv1.Session, error) {
	var spec sessionv1.SessionSpec
	if err := proto.Unmarshal(specBytes, &spec); err != nil {
		return apiv1.Session{}, err
	}
	region, err := s.cfg.Topology.RegionOf(sts)
	if err != nil {
		return apiv1.Session{}, err
	}
	view := views[region]
	st := statusOf(view, sts, id)
	out := apiv1.Session{
		Id:           id,
		StsInstance:  sts,
		Region:       region,
		Revision:     rev,
		Spec:         s.specView(&spec),
		Dependencies: s.dependencies(views, view, sts, id, st),
		Status:       statusView(st),
		Assignment:   assignmentOf(view, sts, id),
		Logs:         s.logLocation(view, region, views, sts, id),
	}
	if rec != nil {
		created := rec.CreatedAt.UTC()
		out.CreatedAt = &created
		if rec.CreatedBy != nil && *rec.CreatedBy != "" {
			out.CreatedBy = rec.CreatedBy
		}
	}
	return out, nil
}

func detailFrom(s apiv1.Session, fresh apiv1.Freshness) apiv1.SessionDetail {
	if s.Dependencies == nil {
		s.Dependencies = []apiv1.Dependency{}
	}
	return apiv1.SessionDetail{
		Assignment:   s.Assignment,
		CreatedAt:    s.CreatedAt,
		CreatedBy:    s.CreatedBy,
		Dependencies: s.Dependencies,
		Freshness:    fresh,
		Id:           s.Id,
		Logs:         s.Logs,
		Region:       s.Region,
		Revision:     s.Revision,
		Spec:         s.Spec,
		Status:       s.Status,
		StsInstance:  s.StsInstance,
	}
}

func (s *Server) specView(spec *sessionv1.SessionSpec) apiv1.SessionSpecView {
	labels := apiv1.Labels{}
	if spec.GetLabels() != nil {
		labels = apiv1.Labels{}
		for k, v := range spec.GetLabels() {
			labels[k] = v
		}
	}
	feeds := make([]apiv1.FeedDeclInput, 0, len(spec.GetFeeds()))
	for _, f := range spec.GetFeeds() {
		feeds = append(feeds, apiv1.FeedDeclInput{Feed: f.GetFeed(), MdInstance: f.GetMdInstance()})
	}
	accounts := make([]apiv1.AccountDecl, 0, len(spec.GetAccounts()))
	for _, a := range spec.GetAccounts() {
		algos := make([]int, 0, len(a.GetAlgoIds()))
		for _, id := range a.GetAlgoIds() {
			algos = append(algos, int(id))
		}
		accounts = append(accounts, apiv1.AccountDecl{
			Name: a.GetName(), ApiId: a.GetApiId(), TdInstance: a.GetTdInstance(), AlgoIds: algos,
		})
	}
	inputs := make([]apiv1.InputDecl, 0, len(spec.GetInputs()))
	for _, in := range spec.GetInputs() {
		inputs = append(inputs, apiv1.InputDecl{Name: in.GetName(), Digest: in.GetDigest(), Repository: in.GetRepository()})
	}
	des := apiv1.DesiredStateRunning
	if spec.GetDesiredState() == sessionv1.DesiredState_DESIRED_STATE_ENDED {
		des = apiv1.DesiredStateEnded
	}
	strat := spec.GetStrategy()
	ref := apiv1.StrategyRef{
		Name:       strat.GetName(),
		Digest:     strat.GetCode(),
		Repository: s.displayRepository(strat.GetRepository()),
		Id:         s.strategyIDForPath(strat.GetRepository()),
	}
	return apiv1.SessionSpecView{
		Name:          spec.GetName(),
		Labels:        labels,
		DesiredState:  des,
		Strategy:      ref,
		Params:        paramsView(spec.GetParams()),
		Feeds:         feeds,
		Accounts:      accounts,
		Inputs:        inputs,
		Restart:       restartView(spec.GetRestart()),
		Limits:        limitsView(spec.GetLimits()),
		DepsTimeoutS:  int(spec.GetDepsTimeoutS()),
		StartTimeoutS: int(spec.GetStartTimeoutS()),
		RecvWindowMs:  int(spec.GetRecvWindowMs()),
		GraceMs:       int(spec.GetGraceMs()),
	}
}

func (s *Server) displayRepository(path string) string {
	if path == "" || s.cfg.Registry.Address == "" {
		return path
	}
	return s.cfg.Registry.Address + "/" + path
}

func (s *Server) strategyIDForPath(path string) *string {
	if path == "" {
		return nil
	}
	repos, err := s.strategyRepos()
	if err != nil {
		return nil
	}
	for _, repo := range repos {
		if repo.path == path {
			id := repo.id
			return &id
		}
	}
	return nil
}

func paramsView(b []byte) []byte {
	if len(b) == 0 {
		return []byte(`{}`)
	}
	return append([]byte(nil), b...)
}

func restartView(p *sessionv1.RestartPolicy) apiv1.RestartPolicy {
	mode := apiv1.OnFailure
	out := apiv1.RestartPolicy{Mode: &mode}
	if p == nil {
		return out
	}
	switch p.GetMode() {
	case sessionv1.RestartMode_RESTART_MODE_NEVER:
		never := apiv1.Never
		out.Mode = &never
	default:
		out.Mode = &mode
	}
	maxRestarts := int(p.GetMaxRestarts())
	window := int(p.GetWindowS())
	out.MaxRestarts = &maxRestarts
	out.WindowS = &window
	return out
}

func limitsView(p *sessionv1.Limits) apiv1.Limits {
	if p == nil {
		return apiv1.Limits{}
	}
	mem := int(p.GetMemoryMb())
	procs := int(p.GetGomaxprocs())
	vol := int(p.GetVolumeMb())
	hook := int(p.GetHookTimeoutS())
	return apiv1.Limits{MemoryMb: &mem, Gomaxprocs: &procs, VolumeMb: &vol, HookTimeoutS: &hook}
}

func (s *Server) dependencies(views map[string]state.View, view state.View, sts, id string, st *statev1.SessionStatus) []apiv1.Dependency {
	if sessionTerminal(st) {
		return conditionsToDeps(st)
	}
	out := []apiv1.Dependency{}
	entry, ok := view.Entries[state.SessionDemandKey(sts, id)]
	if !ok {
		return out
	}
	var demand statev1.SessionDemand
	if proto.Unmarshal(entry.Value, &demand) != nil {
		return out
	}
	for _, b := range demand.GetBindings() {
		dep, ok := bindingDep(b)
		if !ok {
			continue
		}
		out = append(out, s.withProviderSilence(views, dep))
	}
	return out
}

// conditionsToDeps restores feed and account dependencies from the terminal
// status. Selector conditions stay on the status and are not dependencies.
// The terminal path does not re-apply the live silent rule (D1014, D1093).
func conditionsToDeps(st *statev1.SessionStatus) []apiv1.Dependency {
	out := []apiv1.Dependency{}
	if st == nil {
		return out
	}
	for _, c := range st.GetConditions() {
		dep, ok := depFromCondition(c)
		if !ok {
			continue
		}
		out = append(out, dep)
	}
	return out
}

func depFromCondition(c *commonv1.Condition) (apiv1.Dependency, bool) {
	if c == nil || c.GetType() != "dependency" {
		return apiv1.Dependency{}, false
	}
	kind, ident, inst, word, ok := splitConditionMessage(c.GetMessage())
	if !ok {
		return apiv1.Dependency{}, false
	}
	var state apiv1.EntryState
	switch word {
	case "rejected":
		state = apiv1.EntryStateRejected
	case "dead":
		state = apiv1.EntryStateDead
	default:
		return apiv1.Dependency{}, false
	}
	dep := apiv1.Dependency{ProviderInstance: inst, State: state}
	switch kind {
	case "feed":
		dep.Kind = apiv1.DependencyKindFeed
		dep.Feed = &ident
	case "account":
		id, err := strconv.ParseInt(ident, 10, 64)
		if err != nil {
			return apiv1.Dependency{}, false
		}
		dep.Kind = apiv1.DependencyKindAccount
		dep.ApiId = &id
	default:
		return apiv1.Dependency{}, false
	}
	if c.GetReason() != "" {
		reason := c.GetReason()
		dep.Reason = &reason
	}
	if c.GetAtNs() != 0 {
		at := time.Unix(0, c.GetAtNs()).UTC()
		dep.UpdatedAt = &at
	}
	return dep, true
}

// splitConditionMessage reads the four tokens written with the terminal
// condition. ident may itself contain spaces; kind is the first token, the
// state word the last, and the provider instance the one before that.
func splitConditionMessage(msg string) (kind, ident, inst, word string, ok bool) {
	parts := strings.Split(msg, " ")
	if len(parts) < 4 || parts[0] == "" || parts[len(parts)-1] == "" || parts[len(parts)-2] == "" {
		return "", "", "", "", false
	}
	kind = parts[0]
	word = parts[len(parts)-1]
	inst = parts[len(parts)-2]
	ident = strings.Join(parts[1:len(parts)-2], " ")
	if ident == "" {
		return "", "", "", "", false
	}
	return kind, ident, inst, word, true
}

// withProviderSilence applies D1036: a provider whose region is available and
// whose agent key is absent is silent. The ack's reason, subject, and time stay.
// An unavailable provider region is not treated as an absent agent.
func (s *Server) withProviderSilence(views map[string]state.View, dep apiv1.Dependency) apiv1.Dependency {
	plane := state.PlaneMD
	if dep.Kind == apiv1.DependencyKindAccount {
		plane = state.PlaneTD
	}
	if s.agentMissing(views, plane, dep.ProviderInstance) {
		dep.State = apiv1.EntryStateSilent
	}
	return dep
}

// logLocation is the on-host log paths for the session worker (D1014, D1015).
// It is omitted when the STS region is down or the AgentRecord has no host or
// no work_dir, including a last-known record that never had them.
func (s *Server) logLocation(view state.View, region string, views map[string]state.View, sts, id string) *apiv1.LogLocation {
	if _, ok := views[region]; !ok {
		return nil
	}
	ag := s.agentOf(view, true, state.PlaneSTS, sts)
	if ag == nil || ag.Host == nil || *ag.Host == "" || ag.WorkDir == nil || *ag.WorkDir == "" {
		return nil
	}
	dir := shim.LogDir(*ag.WorkDir, string(state.PlaneSTS))
	h := state.WorkerHash("sts/" + id)
	return &apiv1.LogLocation{
		Host:   *ag.Host,
		Stdout: filepath.Join(dir, h+".stdout"),
		Stderr: filepath.Join(dir, h+".stderr"),
	}
}

func bindingDep(b *statev1.Binding) (apiv1.Dependency, bool) {
	if b == nil {
		return apiv1.Dependency{}, false
	}
	dep := apiv1.Dependency{ProviderInstance: b.GetInstance(), State: apiv1.EntryStateSilent}
	switch b.GetTarget().(type) {
	case *statev1.Binding_Feed:
		feed := b.GetFeed()
		dep.Kind = apiv1.DependencyKindFeed
		dep.Feed = &feed
	case *statev1.Binding_ApiId:
		id := b.GetApiId()
		dep.Kind = apiv1.DependencyKindAccount
		dep.ApiId = &id
	default:
		return apiv1.Dependency{}, false
	}
	if st := b.GetStatus(); st != nil {
		dep.State = entryStateOf(st.GetState())
		if st.GetReason() != "" {
			reason := st.GetReason()
			dep.Reason = &reason
		}
		if st.GetSubject() != "" {
			subject := st.GetSubject()
			dep.Subject = &subject
		}
		if st.GetUpdatedAtNs() != 0 {
			at := time.Unix(0, st.GetUpdatedAtNs()).UTC()
			dep.UpdatedAt = &at
		}
	}
	return dep, true
}

func entryStateOf(st statev1.EntryState) apiv1.EntryState {
	switch st {
	case statev1.EntryState_ENTRY_STATE_PENDING:
		return apiv1.EntryStatePending
	case statev1.EntryState_ENTRY_STATE_READY:
		return apiv1.EntryStateReady
	case statev1.EntryState_ENTRY_STATE_REJECTED:
		return apiv1.EntryStateRejected
	case statev1.EntryState_ENTRY_STATE_DEAD:
		return apiv1.EntryStateDead
	default:
		return apiv1.EntryStateSilent
	}
}

func assignmentOf(view state.View, sts, id string) *apiv1.Assignment {
	entry, ok := view.Entries[state.SessionDemandKey(sts, id)]
	if !ok {
		return nil
	}
	var demand statev1.SessionDemand
	if proto.Unmarshal(entry.Value, &demand) != nil {
		return nil
	}
	des := apiv1.DesiredStateRunning
	if demand.GetDesired() == sessionv1.DesiredState_DESIRED_STATE_ENDED {
		des = apiv1.DesiredStateEnded
	}
	out := &apiv1.Assignment{
		DesiredState: des,
		Start:        demand.GetStart(),
		IssuedAt:     time.Unix(0, demand.GetIssuedAtNs()).UTC(),
	}
	if demand.GetEndReason() != "" {
		reason := demand.GetEndReason()
		out.EndReason = &reason
	}
	if demand.GetUpdatedAtNs() != 0 {
		at := time.Unix(0, demand.GetUpdatedAtNs()).UTC()
		out.UpdatedAt = &at
	}
	return out
}

func statusView(st *statev1.SessionStatus) *apiv1.SessionStatus {
	if st == nil {
		return nil
	}
	conds := make([]apiv1.Condition, 0, len(st.GetConditions()))
	for _, c := range st.GetConditions() {
		conds = append(conds, apiv1.Condition{
			Type:    c.GetType(),
			Status:  c.GetStatus(),
			Reason:  c.GetReason(),
			Message: c.GetMessage(),
			At:      time.Unix(0, c.GetAtNs()).UTC(),
		})
	}
	out := &apiv1.SessionStatus{
		Phase:        phaseOf(st.GetPhase()),
		Conditions:   conds,
		Incarnation:  int(st.GetIncarnation()),
		RestartCount: int(st.GetRestartCount()),
		Reason:       st.GetReason(),
		UpdatedAt:    time.Unix(0, st.GetUpdatedAtNs()).UTC(),
	}
	if exit := st.GetLastExit(); exit != nil {
		view := exitView(exit)
		out.LastExit = &view
	}
	if snap := st.GetStateOut(); snap != nil {
		view := snapshotView(snap)
		out.StateOut = &view
	}
	return out
}

func phaseOf(p statev1.Phase) apiv1.Phase {
	switch p {
	case statev1.Phase_PHASE_PENDING:
		return apiv1.PhasePending
	case statev1.Phase_PHASE_WAITING:
		return apiv1.PhaseWaiting
	case statev1.Phase_PHASE_STARTING:
		return apiv1.PhaseStarting
	case statev1.Phase_PHASE_RUNNING:
		return apiv1.PhaseRunning
	case statev1.Phase_PHASE_STOPPING:
		return apiv1.PhaseStopping
	case statev1.Phase_PHASE_RESTARTING:
		return apiv1.PhaseRestarting
	case statev1.Phase_PHASE_DONE:
		return apiv1.PhaseDone
	case statev1.Phase_PHASE_FAILED:
		return apiv1.PhaseFailed
	default:
		return ""
	}
}

func exitView(exit *commonv1.ExitInfo) apiv1.Exit {
	return apiv1.Exit{
		Code:            int(exit.GetCode()),
		Signal:          int(exit.GetSignal()),
		At:              time.Unix(0, exit.GetAtNs()).UTC(),
		Outcome:         outcomeOf(exit.GetOutcome()),
		Reason:          exit.GetReason(),
		OnStopCompleted: exit.GetOnStopCompleted(),
	}
}

func outcomeOf(o commonv1.Outcome) apiv1.Outcome {
	switch o {
	case commonv1.Outcome_OUTCOME_DONE:
		return apiv1.OutcomeDone
	case commonv1.Outcome_OUTCOME_INIT_FAILED:
		return apiv1.OutcomeInitFailed
	case commonv1.Outcome_OUTCOME_FAILED:
		return apiv1.OutcomeFailed
	case commonv1.Outcome_OUTCOME_CRASHED:
		return apiv1.OutcomeCrashed
	case commonv1.Outcome_OUTCOME_HUNG:
		return apiv1.OutcomeHung
	default:
		return apiv1.OutcomeUnknown
	}
}

func snapshotView(snap *statev1.StateSnapshot) apiv1.StateSnapshot {
	size := snap.GetSizeBytes()
	if size > uint64(int64(^uint64(0)>>1)) {
		size = uint64(int64(^uint64(0) >> 1))
	}
	return apiv1.StateSnapshot{
		Digest:      snap.GetDigest(),
		Repository:  snap.GetRepository(),
		Clean:       snap.GetClean(),
		Incarnation: int(snap.GetIncarnation()),
		At:          time.Unix(0, snap.GetAtNs()).UTC(),
		SizeBytes:   int64(size),
	}
}
