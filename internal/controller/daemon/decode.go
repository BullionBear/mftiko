package daemon

import (
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/controller"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// failure is one key that did not decode, or whose shape is not the key it
// sits under (E13-D021). mod is the entry's mod revision, so a rewrite is logged
// again and an unchanged value is not.
type failure struct {
	key string
	mod int64
	err error
}

// instancePrefix is the mirror prefix of one instance (E13-D017): the whole
// /{plane}/{inst}/, not one subpath.
func instancePrefix(p state.Plane, inst string) string {
	return state.Root + "/" + string(p) + "/" + inst + "/"
}

// classify splits one mirror's keys into the snapshot Assemble consumes.
// Subpaths the controller does not read (election, epoch, writer, incarnation,
// selector, placement, account, and worker on MD and TD) are ignored. A key
// under a watched subpath that is not that subpath's shape, or whose bytes
// are not the protobuf, is undecodable.
func classify(plane state.Plane, inst string, entries map[string]state.Entry) (controller.Snapshot, []failure) {
	prefix := instancePrefix(plane, inst)
	var snap controller.Snapshot
	var fails []failure
	reject := func(e state.Entry, err error) {
		snap.Undecodable = append(snap.Undecodable, e.Key)
		fails = append(fails, failure{key: e.Key, mod: e.ModRevision, err: err})
	}
	for _, e := range entries {
		rest, ok := strings.CutPrefix(e.Key, prefix)
		if !ok || rest == "" {
			continue
		}
		if rest == "agent" {
			rec := &statev1.AgentRecord{}
			if err := unmarshal(e, rec); err != nil {
				reject(e, err)
				continue
			}
			snap.Agent = rec
			continue
		}
		kind, tail, ok := strings.Cut(rest, "/")
		if !ok || !watchedKind(plane, kind) {
			continue
		}
		if err := putClass(plane, inst, kind, tail, e, &snap); err != nil {
			reject(e, err)
		}
	}
	return snap, fails
}

func watchedKind(p state.Plane, kind string) bool {
	switch p {
	case state.PlaneSTS:
		return kind == "spec" || kind == "status" || kind == "demand" || kind == "worker"
	case state.PlaneMD, state.PlaneTD:
		return kind == "demand" || kind == "ack"
	default:
		return false
	}
}

func putClass(plane state.Plane, inst, kind, tail string, e state.Entry, snap *controller.Snapshot) error {
	switch plane {
	case state.PlaneSTS:
		id, err := oneSegment(tail)
		if err != nil {
			return fmt.Errorf("%s: %w", e.Key, err)
		}
		switch kind {
		case "spec":
			m := &sessionv1.SessionSpec{}
			if err := unmarshal(e, m); err != nil {
				return err
			}
			snap.Specs = append(snap.Specs, controller.SpecIn{Session: id, Rev: e.ModRevision, Value: m})
		case "status":
			m := &statev1.SessionStatus{}
			if err := unmarshal(e, m); err != nil {
				return err
			}
			snap.Statuses = append(snap.Statuses, controller.StatusIn{Session: id, Value: m})
		case "demand":
			m := &statev1.SessionDemand{}
			if err := unmarshal(e, m); err != nil {
				return err
			}
			snap.SessionDemands = append(snap.SessionDemands, controller.SessionDemandIn{Session: id, Value: m})
		case "worker":
			m := &statev1.WorkerStatus{}
			if err := unmarshal(e, m); err != nil {
				return err
			}
			snap.Workers = append(snap.Workers, controller.WorkerIn{Key: e.Key, Hash: id, Value: m})
		}
	case state.PlaneMD, state.PlaneTD:
		session, entry, err := splitEntry(tail)
		if err != nil {
			return fmt.Errorf("%s: %w", e.Key, err)
		}
		var apiID int64
		if plane == state.PlaneTD {
			apiID, err = canonicalID(entry)
			if err != nil {
				return fmt.Errorf("%s: %w", e.Key, err)
			}
		}
		switch kind {
		case "demand":
			m := &statev1.DemandEntry{}
			if err := unmarshal(e, m); err != nil {
				return err
			}
			snap.Demands = append(snap.Demands, controller.DemandIn{Key: e.Key, Session: session, Entry: entry, Value: m})
		case "ack":
			m := &statev1.EntryStatus{}
			if err := unmarshal(e, m); err != nil {
				return err
			}
			demandKey := state.MDDemandKey(inst, session, entry)
			if plane == state.PlaneTD {
				demandKey = state.TDDemandKey(inst, session, apiID)
			}
			snap.Acks = append(snap.Acks, controller.AckIn{
				Key: e.Key, DemandKey: demandKey, Session: session, Entry: entry, Value: m,
			})
		}
	}
	return nil
}

func unmarshal(e state.Entry, m proto.Message) error {
	if err := proto.Unmarshal(e.Value, m); err != nil {
		return fmt.Errorf("decode %s: %w", e.Key, err)
	}
	return nil
}

func oneSegment(tail string) (string, error) {
	if tail == "" || strings.Contains(tail, "/") {
		return "", fmt.Errorf("want one path segment")
	}
	return tail, nil
}

func splitEntry(tail string) (session, entry string, err error) {
	session, entry, ok := strings.Cut(tail, "/")
	if !ok || session == "" || entry == "" {
		return "", "", fmt.Errorf("want session/entry")
	}
	return session, entry, nil
}

// canonicalID is an api id written the way TDDemandKey writes it.
func canonicalID(entry string) (int64, error) {
	if strings.Contains(entry, "/") {
		return 0, fmt.Errorf("want an api id")
	}
	id, err := strconv.ParseInt(entry, 10, 64)
	if err != nil || strconv.FormatInt(id, 10) != entry {
		return 0, fmt.Errorf("want a canonical api id")
	}
	return id, nil
}
