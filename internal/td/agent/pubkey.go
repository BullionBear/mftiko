package agent

import (
	"context"

	"github.com/BullionBear/mftiko/internal/platform/sealbox"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// publishPubkey writes pub to /td/{inst}/pubkey when the stored value does not parse to the same
// key (E21-D004, E21-D035). A value that parses equal, including a trailing newline, is left
// alone. The put carries no lease: Term.Put does not attach one.
func publishPubkey(ctx context.Context, r reader, w writer, inst string, pub sealbox.PublicKey) error {
	key := state.PubkeyKey(inst)
	resp, err := r.Get(ctx, key)
	if err != nil {
		return err
	}
	if resp != nil && len(resp.Kvs) == 1 {
		got, perr := sealbox.ParsePublicKey(string(resp.Kvs[0].Value))
		if perr == nil && got == pub {
			return nil
		}
	}
	return w.Put(ctx, key, pub.String())
}
