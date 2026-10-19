package agent

import (
	"crypto/sha256"
	"encoding/hex"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	stsctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/stsctl/v1"
)

// toBindings converts the assignment's bindings into the snapshot Start and Command carry
// (PROTOCOL §6.1). Order is the assignment's. A missing status is UNSPECIFIED. The version
// is left unset: the agent fills epoch and seq (D961).
func toBindings(spec *sessionv1.SessionSpec, in []*statev1.Binding) *stsctlv1.Bindings {
	names := map[int64]string{}
	for _, a := range spec.GetAccounts() {
		names[a.GetApiId()] = a.GetName()
	}
	out := &stsctlv1.Bindings{}
	for _, b := range in {
		if b == nil {
			continue
		}
		st := b.GetStatus()
		switch t := b.GetTarget().(type) {
		case *statev1.Binding_Feed:
			out.Feeds = append(out.Feeds, &stsctlv1.FeedBinding{
				Feed: t.Feed, MdInstance: b.GetInstance(), Subject: st.GetSubject(), State: bindingState(st.GetState()),
			})
		case *statev1.Binding_Selector:
			out.Selectors = append(out.Selectors, &stsctlv1.SelectorBinding{
				Name: t.Selector, MdInstance: b.GetInstance(), Universe: universeOf(st.GetUniverse()), SubjectPrefix: st.GetSubject(),
			})
		case *statev1.Binding_ApiId:
			var worker *commonv1.WorkerRef
			if st.GetWorker() != nil {
				worker = proto.Clone(st.GetWorker()).(*commonv1.WorkerRef)
			}
			out.Accounts = append(out.Accounts, &stsctlv1.AccountBinding{
				Name: names[t.ApiId], ApiId: t.ApiId, State: bindingState(st.GetState()), Worker: worker,
			})
		}
	}
	return out
}

func bindingState(s statev1.EntryState) stsctlv1.BindingState {
	switch s {
	case statev1.EntryState_ENTRY_STATE_PENDING:
		return stsctlv1.BindingState_BINDING_STATE_PENDING
	case statev1.EntryState_ENTRY_STATE_READY:
		return stsctlv1.BindingState_BINDING_STATE_READY
	case statev1.EntryState_ENTRY_STATE_REJECTED:
		return stsctlv1.BindingState_BINDING_STATE_REJECTED
	case statev1.EntryState_ENTRY_STATE_DEAD:
		return stsctlv1.BindingState_BINDING_STATE_DEAD
	default:
		return stsctlv1.BindingState_BINDING_STATE_UNSPECIFIED
	}
}

func universeOf(u *statev1.Universe) *stsctlv1.Universe {
	if u == nil {
		return nil
	}
	return &stsctlv1.Universe{
		Epoch: u.GetEpoch(), Members: append([]string(nil), u.GetMembers()...),
		Current: u.GetCurrent(), Topics: append([]string(nil), u.GetTopics()...),
	}
}

// contentHash identifies bindings apart from their version (D961).
func contentHash(b *stsctlv1.Bindings) string {
	if b == nil {
		b = &stsctlv1.Bindings{}
	}
	c := proto.Clone(b).(*stsctlv1.Bindings)
	c.Version = nil
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func cloneDemandBindings(in []*statev1.Binding) []*statev1.Binding {
	if in == nil {
		return nil
	}
	out := make([]*statev1.Binding, len(in))
	for i, b := range in {
		if b != nil {
			out[i] = proto.Clone(b).(*statev1.Binding)
		}
	}
	return out
}
