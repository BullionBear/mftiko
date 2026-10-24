package agent

import (
	"strconv"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

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
