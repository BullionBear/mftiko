package state

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Root is the prefix of every key (PROTOCOL §3.1). An incompatible change moves to /mftiko/v2/.
const Root = "/mftiko/v1"

// Plane is the first path segment after Root: which kind of agent an instance is (DECISIONS D1), or the
// controller (D33).
type Plane string

const (
	PlaneSTS Plane = "sts"
	PlaneMD  Plane = "md"
	PlaneTD  Plane = "td"
	// PlaneCtl is the controller. It campaigns and fences like an agent but has no workers.
	PlaneCtl Plane = "ctl"
)

// ControllerInstance is the controller's instance name. Every controller process uses it, so two of them fence
// each other in every cluster even when their topologies disagree on the home region (PROTOCOL §3.2 W2, D33).
// Topology.Instances[ControllerInstance] is the home region.
const ControllerInstance = "controller"

func (p Plane) valid() bool { return p.agent() || p == PlaneCtl }

// agent reports whether p is one of the three agent planes, the ones with workers.
func (p Plane) agent() bool { return p == PlaneSTS || p == PlaneMD || p == PlaneTD }

// instanceName is the instance name syntax (PROTOCOL §2).
var instanceName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)

// WorkerHash is h in PROTOCOL §2: hex(sha256(worker key))[:16].
func WorkerHash(workerKey string) string {
	sum := sha256.Sum256([]byte(workerKey))
	return hex.EncodeToString(sum[:8])
}

func instPrefix(p Plane, inst string) string { return Root + "/" + string(p) + "/" + inst + "/" }

func apiIDString(id int64) string { return strconv.FormatInt(id, 10) }

// SessionSpecKey is the session spec, written by the API server, watched by the controller and the STS agent
// (PROTOCOL §3.1).
func SessionSpecKey(stsInst, sessionID string) string { return SessionSpecPrefix(stsInst) + sessionID }

// SessionSpecPrefix lists every session spec of an STS instance.
func SessionSpecPrefix(stsInst string) string { return instPrefix(PlaneSTS, stsInst) + "spec/" }

// SessionStatusKey is the session status, written by the STS agent, watched by the controller and the API server
// (PROTOCOL §3.1).
func SessionStatusKey(stsInst, sessionID string) string {
	return SessionStatusPrefix(stsInst) + sessionID
}

// SessionStatusPrefix lists every session status of an STS instance.
func SessionStatusPrefix(stsInst string) string { return instPrefix(PlaneSTS, stsInst) + "status/" }

// AccountKey is an account declaration, written by the API server, watched by the TD agent (PROTOCOL §3.1).
func AccountKey(tdInst string, apiID int64) string { return AccountPrefix(tdInst) + apiIDString(apiID) }

// AccountPrefix lists every account of a TD instance.
func AccountPrefix(tdInst string) string { return instPrefix(PlaneTD, tdInst) + "account/" }

// DemandPrefix lists every demand the controller wrote for an agent instance (PROTOCOL §3.3): an STS instance's
// SessionDemands, an MD or TD instance's DemandEntries.
func DemandPrefix(p Plane, inst string) string { return instPrefix(p, inst) + "demand/" }

// SessionDemandKey is the controller's assignment of a session to an STS instance (PROTOCOL §3.1, §3.3).
func SessionDemandKey(stsInst, sessionID string) string {
	return DemandPrefix(PlaneSTS, stsInst) + sessionID
}

// MDDemandKey is an MD demand entry, written by the controller into the MD instance's cluster (PROTOCOL §3.1, D17).
// entry is feed/{feed key} or select/{name}, appended verbatim.
func MDDemandKey(mdInst, sessionID, entry string) string {
	return DemandPrefix(PlaneMD, mdInst) + sessionID + "/" + entry
}

// TDDemandKey is a TD demand entry, written by the controller into the TD instance's cluster (PROTOCOL §3.1, D17).
func TDDemandKey(tdInst, sessionID string, apiID int64) string {
	return DemandPrefix(PlaneTD, tdInst) + sessionID + "/" + apiIDString(apiID)
}

// MDAckKey is the MD agent's EntryStatus for one demand entry (PROTOCOL §3.1, §3.5).
func MDAckKey(mdInst, sessionID, entry string) string {
	return MDAckPrefix(mdInst) + sessionID + "/" + entry
}

// MDAckPrefix lists every ack of an MD instance.
func MDAckPrefix(mdInst string) string { return instPrefix(PlaneMD, mdInst) + "ack/" }

// TDAckKey is the TD agent's EntryStatus for one demand entry (PROTOCOL §3.1, §3.5).
func TDAckKey(tdInst, sessionID string, apiID int64) string {
	return TDAckPrefix(tdInst) + sessionID + "/" + apiIDString(apiID)
}

// TDAckPrefix lists every ack of a TD instance.
func TDAckPrefix(tdInst string) string { return instPrefix(PlaneTD, tdInst) + "ack/" }

// ElectionPrefix is where an instance's agents, or the controller's processes, campaign (PROTOCOL §3.2 W2).
// concurrency.Election appends the "/".
func ElectionPrefix(p Plane, inst string) string { return instPrefix(p, inst) + "election" }

// EpochKey holds the instance's epoch, 20-digit zero-padded decimal (PROTOCOL §2, §3.2 W2).
func EpochKey(p Plane, inst string) string { return instPrefix(p, inst) + "epoch" }

// WriterKey is the fencing key, one copy in every cluster the instance writes to (PROTOCOL §3.2 W2).
func WriterKey(p Plane, inst string) string { return instPrefix(p, inst) + "writer" }

// AgentKey is the AgentRecord of an agent or of the controller (PROTOCOL §3.1).
func AgentKey(p Plane, inst string) string { return instPrefix(p, inst) + "agent" }

// WorkerStatusKey is one worker's WorkerStatus; h is WorkerHash of the worker key (PROTOCOL §3.1).
func WorkerStatusKey(p Plane, inst, h string) string { return WorkerStatusPrefix(p, inst) + h }

// WorkerStatusPrefix lists every WorkerStatus of an instance.
func WorkerStatusPrefix(p Plane, inst string) string { return instPrefix(p, inst) + "worker/" }

// IncarnationKey is the last incarnation taken for a worker key; h is WorkerHash of it (PROTOCOL §3.2 W6).
func IncarnationKey(p Plane, inst, h string) string { return IncarnationPrefix(p, inst) + h }

// IncarnationPrefix lists every incarnation counter of an instance.
func IncarnationPrefix(p Plane, inst string) string { return instPrefix(p, inst) + "incarnation/" }

// SelectorKey is an MD selector's state, shared by selectors with the same spec (PROTOCOL §3.4).
func SelectorKey(mdInst, specHash string) string { return SelectorPrefix(mdInst) + specHash }

// SelectorPrefix lists every selector state of an MD instance, read back on restart (PROTOCOL §3.4).
func SelectorPrefix(mdInst string) string { return instPrefix(PlaneMD, mdInst) + "selector/" }

// PlacementKey is where an MD atom is placed (PROTOCOL §3.4).
func PlacementKey(mdInst, atomHash string) string { return PlacementPrefix(mdInst) + atomHash }

// PlacementPrefix lists every placement of an MD instance, read back on restart (PROTOCOL §3.4).
func PlacementPrefix(mdInst string) string { return instPrefix(PlaneMD, mdInst) + "placement/" }

// Owner returns the plane and instance a key belongs to: the two path segments after Root. Every key in
// PROTOCOL §3.1 is /{plane}/{inst}/..., and that instance is the one the routing rule (PROTOCOL §3, D17) names:
// session spec, demand and status -> the STS instance, demand entry and ack -> the provider, everything else -> the
// instance.
// A prefix that ends right after the instance (Root/{plane}/{inst}/) also resolves.
func Owner(key string) (Plane, string, error) {
	rest, ok := strings.CutPrefix(key, Root+"/")
	if !ok {
		return "", "", fmt.Errorf("%w: %q is not under %s", ErrBadKey, key, Root)
	}
	seg := strings.SplitN(rest, "/", 3)
	if len(seg) < 3 {
		return "", "", fmt.Errorf("%w: %q has no /{plane}/{inst}/", ErrBadKey, key)
	}
	p, inst := Plane(seg[0]), seg[1]
	if !p.valid() {
		return "", "", fmt.Errorf("%w: %q has unknown plane %q", ErrBadKey, key, seg[0])
	}
	if !instanceName.MatchString(inst) {
		return "", "", fmt.Errorf("%w: %q has bad instance name %q", ErrBadKey, key, inst)
	}
	return p, inst, nil
}
