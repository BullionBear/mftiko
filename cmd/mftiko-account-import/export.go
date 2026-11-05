package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/sealbox"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

type exportOptions struct {
	topology string
	instance string
	user     string
	password string
	sealKey  string
	transfer string
	out      string
}

func exportMain(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("export", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var o exportOptions
	fs.StringVar(&o.topology, "topology", "", "deployment topology JSON (the TD agent's -topology)")
	fs.StringVar(&o.instance, "instance", "", "TD instance whose accounts to export")
	fs.StringVar(&o.user, "etcd-user", "", "etcd user (default: -instance)")
	fs.StringVar(&o.password, "etcd-password", os.Getenv("MFTIKO_ETCD_PASSWORD"), "etcd password of -etcd-user")
	fs.StringVar(&o.sealKey, "seal-key", "", "this instance's private key (the TD agent's -seal-key)")
	fs.StringVar(&o.transfer, "transfer-pubkey", "", "transfer public key in base64, or @FILE")
	fs.StringVar(&o.out, "out", "", "export file to create (0600); it must not exist")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if o.user == "" {
		o.user = o.instance
	}
	switch {
	case o.topology == "" || o.instance == "" || o.sealKey == "" || o.transfer == "" || o.out == "":
		return errors.New("export needs -topology, -instance, -seal-key, -transfer-pubkey and -out")
	case o.password == "":
		return errors.New("etcd password is empty")
	}
	topo, err := readTopology(o.topology)
	if err != nil {
		return err
	}
	region, err := topo.RegionOf(o.instance)
	if err != nil {
		return err
	}
	key, err := sealbox.LoadKeyFile(o.sealKey)
	if err != nil {
		return err
	}
	pub, err := readPublic(o.transfer)
	if err != nil {
		return err
	}
	cli, err := state.AuthDial(o.user, o.password)(region, topo.Regions[region].Endpoints)
	if err != nil {
		return err
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	f, err := export(ctx, cli, o.instance, key, pub, time.Now())
	if err != nil {
		return err
	}
	if err := writeNew(o.out, f); err != nil {
		return err
	}
	return printExport(stdout, f, o.out)
}

// export reads every account key of inst, opens its credentials with key and reseals them to
// transfer. Nothing is written to etcd. Plaintext stays in this function.
func export(ctx context.Context, cli *clientv3.Client, inst string, key *sealbox.PrivateKey, transfer sealbox.PublicKey, now time.Time) (exportFile, error) {
	prefix := state.AccountPrefix(inst)
	resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return exportFile{}, fmt.Errorf("read %s: %w", prefix, err)
	}
	f := exportFile{
		Version: fileVersion, TDInstance: inst, ExportedAt: now.UTC().Format(time.RFC3339),
		Accounts: []exportAccount{}, Skipped: []skipped{},
	}
	for _, kv := range resp.Kvs {
		k := string(kv.Key)
		rest := strings.TrimPrefix(k, prefix)
		id, perr := strconv.ParseInt(rest, 10, 64)
		if perr != nil || id < 1 || id > maxAPIID || strconv.FormatInt(id, 10) != rest {
			f.Skipped = append(f.Skipped, skipped{Key: k, Reason: "not an account key"})
			continue
		}
		var rec statev1.Account
		if err := proto.Unmarshal(kv.Value, &rec); err != nil {
			f.Skipped = append(f.Skipped, skipped{Key: k, APIID: id, Reason: "account_unreadable"})
			continue
		}
		creds, err := sealbox.OpenCredentials(key, rec.GetSealedCredentials(), id, rec.GetKind())
		if err != nil {
			f.Skipped = append(f.Skipped, skipped{Key: k, APIID: id, Reason: sealbox.Reason(err)})
			continue
		}
		moved, err := sealbox.SealCredentials(transfer, id, rec.GetKind(), creds)
		if err != nil {
			// Only a malformed account fails here (empty kind or key); the error names no secret.
			f.Skipped = append(f.Skipped, skipped{Key: k, APIID: id, Reason: err.Error()})
			continue
		}
		f.Accounts = append(f.Accounts, exportAccount{
			APIID: id, Name: rec.GetName(), Kind: rec.GetKind(),
			Cancel:   rec.GetSettings().GetCancelOnDisconnectS(),
			Enabled:  rec.GetEnabled(),
			Replica:  append([]byte(nil), rec.GetSealedCredentials()...),
			Transfer: moved,
		})
	}
	sort.Slice(f.Accounts, func(i, j int) bool { return f.Accounts[i].APIID < f.Accounts[j].APIID })
	return f, nil
}

func printExport(w io.Writer, f exportFile, path string) error {
	fmt.Fprintf(w, "exported %d account(s) of %s to %s\n", len(f.Accounts), f.TDInstance, path)
	for _, a := range f.Accounts {
		fmt.Fprintf(w, "  %d\t%s\t%s\n", a.APIID, a.Kind, a.Name)
	}
	for _, s := range f.Skipped {
		fmt.Fprintf(w, "skipped %s: %s\n", s.Key, s.Reason)
	}
	return nil
}

// maxAPIID is 2^53 − 1 (E10-D008, E21-D002).
const maxAPIID int64 = 1<<53 - 1

func readTopology(path string) (state.Topology, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return state.Topology{}, err
	}
	var topo state.Topology
	if err := json.Unmarshal(b, &topo); err != nil {
		return state.Topology{}, fmt.Errorf("topology: %w", err)
	}
	if err := topo.Validate(); err != nil {
		return state.Topology{}, err
	}
	return topo, nil
}

func readPublic(s string) (sealbox.PublicKey, error) {
	if path, ok := strings.CutPrefix(s, "@"); ok {
		b, err := os.ReadFile(path)
		if err != nil {
			return sealbox.PublicKey{}, err
		}
		s = string(b)
	}
	return sealbox.ParsePublicKey(s)
}
