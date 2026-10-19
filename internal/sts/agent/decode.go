package agent

import (
	"errors"
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// Reasons Decode returns. The runner logs them; the plan reads the mirrors itself.
var (
	errBadKey    = errors.New("bad key")
	errBadSpec   = errors.New("bad spec")
	errBadDemand = errors.New("bad demand")
	errBadStatus = errors.New("bad status")
)

// Entry is one key of the three prefixes the agent mirrors. Planning does not
// read it: observed_generation is the spec key's mod_revision, which Decode is
// not given. Decode exists so a value that will not parse is logged.
type Entry struct {
	Session string
}

// decoder reads the keys of one instance.
type decoder struct{ inst string }

// Decode parses one spec, assignment, or status key.
func (d decoder) Decode(key string, value []byte) (Entry, error) {
	switch {
	case strings.HasPrefix(key, state.SessionSpecPrefix(d.inst)):
		id, ok := idFrom(state.SessionSpecPrefix(d.inst), key)
		if !ok {
			return Entry{}, errBadKey
		}
		if _, err := parseSpec(value); err != nil {
			return Entry{}, err
		}
		return Entry{Session: id}, nil
	case strings.HasPrefix(key, state.DemandPrefix(state.PlaneSTS, d.inst)):
		id, ok := idFrom(state.DemandPrefix(state.PlaneSTS, d.inst), key)
		if !ok {
			return Entry{}, errBadKey
		}
		if _, err := parseDemand(value); err != nil {
			return Entry{}, err
		}
		return Entry{Session: id}, nil
	case strings.HasPrefix(key, state.SessionStatusPrefix(d.inst)):
		id, ok := idFrom(state.SessionStatusPrefix(d.inst), key)
		if !ok {
			return Entry{}, errBadKey
		}
		if _, err := parseStatus(value); err != nil {
			return Entry{}, err
		}
		return Entry{Session: id}, nil
	default:
		return Entry{}, errBadKey
	}
}

func idFrom(prefix, key string) (string, bool) {
	rest, ok := strings.CutPrefix(key, prefix)
	if !ok || rest == "" || strings.Contains(rest, "/") {
		return "", false
	}
	return rest, true
}

func parseSpec(raw []byte) (*sessionv1.SessionSpec, error) {
	msg := &sessionv1.SessionSpec{}
	if err := proto.Unmarshal(raw, msg); err != nil {
		return nil, fmt.Errorf("%w: %v", errBadSpec, err)
	}
	return msg, nil
}

func parseDemand(raw []byte) (*statev1.SessionDemand, error) {
	msg := &statev1.SessionDemand{}
	if err := proto.Unmarshal(raw, msg); err != nil {
		return nil, fmt.Errorf("%w: %v", errBadDemand, err)
	}
	return msg, nil
}

func parseStatus(raw []byte) (*statev1.SessionStatus, error) {
	msg := &statev1.SessionStatus{}
	if err := proto.Unmarshal(raw, msg); err != nil {
		return nil, fmt.Errorf("%w: %v", errBadStatus, err)
	}
	return msg, nil
}
