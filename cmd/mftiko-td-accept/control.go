package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/sealbox"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

const region = "accept"

func topology() state.Topology {
	return state.Topology{
		Regions:   map[string]state.Region{region: {Endpoints: []string{*etcdURL}}},
		Instances: map[string]string{*instance: region, state.ControllerInstance: region},
	}
}

// setup gives the TD instance its etcd role and user (state.AuthTables), adds root, and turns auth
// on, so the agent runs under the same RBAC as in a deployment. Running it again, or after
// mftiko-md-accept setup, changes nothing else. It writes the topology td-agent reads.
func setup() error {
	out := flag.String("topology", "td-topology.json", "where to write the topology for td-agent")
	flag.Parse()
	if *password == "" {
		return errors.New("-etcd-password is empty")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := setupClient(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	tables, err := state.AuthTables(topology(), map[string]state.Plane{*instance: state.PlaneTD})
	if err != nil {
		return err
	}
	if err := state.EnsureAuth(ctx, c, tables[region], func(string) string { return *password }); err != nil {
		return err
	}
	if u, err := c.UserGet(ctx, "root"); err != nil {
		if _, err := c.UserAdd(ctx, "root", *password); err != nil {
			return err
		}
		if _, err := c.UserGrantRole(ctx, "root", "root"); err != nil {
			return err
		}
	} else if !slices.Contains(u.Roles, "root") {
		if _, err := c.UserGrantRole(ctx, "root", "root"); err != nil {
			return err
		}
	}
	if _, err := c.AuthEnable(ctx); err != nil {
		return err
	}
	b, err := json.MarshalIndent(topology(), "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(*out, append(b, '\n'), 0o644); err != nil {
		return err
	}
	fmt.Printf("%s etcd auth on, instance %s, topology %s\n", stamp(), *instance, *out)
	return nil
}

// setupClient is an anonymous client on a cluster whose auth is still off, and root on one where
// an earlier setup turned it on.
func setupClient(ctx context.Context) (*clientv3.Client, error) {
	c, err := clientv3.New(clientv3.Config{Endpoints: []string{*etcdURL}, DialTimeout: 5 * time.Second, Logger: zap.NewNop()})
	if err != nil {
		return nil, err
	}
	if _, err := c.Get(ctx, "/"); err == nil {
		return c, nil
	}
	c.Close()
	return root()
}

func root() (*clientv3.Client, error) {
	if *password == "" {
		return nil, errors.New("-etcd-password is empty")
	}
	return clientv3.New(clientv3.Config{Endpoints: []string{*etcdURL}, DialTimeout: 5 * time.Second, Username: "root", Password: *password, Logger: zap.NewNop()})
}

// put writes one protobuf value as root.
func put(key string, m proto.Message) error {
	c, err := root()
	if err != nil {
		return err
	}
	defer c.Close()
	b, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = c.Put(ctx, key, string(b))
	return err
}

// credsJSON is mftiko-seal's credentials JSON.
type credsJSON struct {
	KeyType    string `json:"key_type"`
	Key        string `json:"key"`
	Secret     string `json:"secret"`
	Passphrase string `json:"passphrase"`
}

func readCreds(r io.Reader) (sealbox.Credentials, error) {
	var c credsJSON
	if err := json.NewDecoder(r).Decode(&c); err != nil {
		return sealbox.Credentials{}, fmt.Errorf("credentials JSON: %w", err)
	}
	if c.KeyType == "" || c.Key == "" || c.Secret == "" {
		return sealbox.Credentials{}, errors.New("credentials need key_type, key and secret")
	}
	return sealbox.Credentials{KeyType: c.KeyType, Key: c.Key, Secret: c.Secret, Passphrase: c.Passphrase}, nil
}

// account writes what the API server will write (E10): a state.v1.Account under the instance,
// its credentials (mftiko-seal's JSON, on stdin) sealed to the instance's key.
func account() error {
	kind := flag.String("kind", "", "account kind, e.g. BinanceUM")
	keyFile := flag.String("seal-key", "", "the instance's private key file (mftiko-seal keygen); its public key seals")
	name := flag.String("name", "accept", "account name")
	disabled := flag.Bool("disabled", false, "write the account disabled")
	flag.Parse()
	id := apiID
	if *id <= 0 || *kind == "" || *keyFile == "" {
		return errors.New("-api-id, -kind and -seal-key are required")
	}
	key, err := sealbox.LoadKeyFile(*keyFile)
	if err != nil {
		return err
	}
	c, err := readCreds(os.Stdin)
	if err != nil {
		return err
	}
	sealed, err := sealbox.SealCredentials(key.Public(), *id, *kind, c)
	if err != nil {
		return err
	}
	k := state.AccountKey(*instance, *id)
	if err := put(k, &statev1.Account{Name: *name, Kind: *kind, SealedCredentials: sealed, Enabled: !*disabled}); err != nil {
		return err
	}
	fmt.Printf("%s put %s kind=%s enabled=%v\n", stamp(), k, *kind, !*disabled)
	return nil
}

// demand writes what the controller will write (E13): a state.v1.DemandEntry for an api_id under
// demand/{session}/.
func demand() error {
	flag.Parse()
	args := flag.Args()
	if len(args) != 3 || (args[0] != "put" && args[0] != "del") {
		return errors.New("demand put|del SESSION API_ID")
	}
	id, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil || id <= 0 {
		return fmt.Errorf("api_id %q", args[2])
	}
	key := state.TDDemandKey(*instance, args[1], id)
	if args[0] == "put" {
		if err := put(key, &statev1.DemandEntry{Target: &statev1.DemandEntry_ApiId{ApiId: id}}); err != nil {
			return err
		}
	} else {
		c, err := root()
		if err != nil {
			return err
		}
		defer c.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := c.Delete(ctx, key); err != nil {
			return err
		}
	}
	fmt.Printf("%s %s %s\n", stamp(), args[0], key)
	return nil
}

// acks prints every ack of the instance.
func acks() error {
	flag.Parse()
	c, err := root()
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := c.Get(ctx, state.TDAckPrefix(*instance), clientv3.WithPrefix())
	if err != nil {
		return err
	}
	for _, kv := range resp.Kvs {
		st := &statev1.EntryStatus{}
		if err := proto.Unmarshal(kv.Value, st); err != nil {
			return fmt.Errorf("%s: %w", kv.Key, err)
		}
		fmt.Printf("%s %s %v\n", stamp(), kv.Key, st)
	}
	return nil
}
