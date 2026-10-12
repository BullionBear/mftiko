package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	symv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/internal/platform/natsdev"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/sym"
)

const region = "accept"

func topology() state.Topology {
	return state.Topology{
		Regions:   map[string]state.Region{region: {Endpoints: []string{*etcdURL}}},
		Instances: map[string]string{*instance: region, state.ControllerInstance: region},
	}
}

// setup gives the instance its etcd role and user (state.AuthTables), adds root, and turns auth
// on, so the agent runs under the same RBAC as in a deployment. Running it again changes nothing.
// It writes the topology md-agent reads.
func setup() error {
	out := flag.String("topology", "topology.json", "where to write the topology for md-agent")
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
	tables, err := state.AuthTables(topology(), map[string]state.Plane{*instance: state.PlaneMD})
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

// signCreds signs a user for every subject with the account signing key mftiko-nats-dev wrote.
// The observer reads md.f and asks md.view as a session would; SYM uses it because mftiko-nats-dev
// issues no SYM user.
func signCreds() error {
	dir := flag.String("nats-dir", "deploy/dev/data/nats", "mftiko-nats-dev output")
	out := flag.String("out", "observer.creds", "credentials file to write")
	flag.Parse()
	lay, err := natsdev.Load(*dir)
	if err != nil {
		return err
	}
	seed, err := os.ReadFile(filepath.Join(lay.Dir, filepath.FromSlash(lay.Manifest.SigningKeyFile)))
	if err != nil {
		return err
	}
	signer, err := nkeys.FromSeed([]byte(strings.TrimSpace(string(seed))))
	if err != nil {
		return err
	}
	kp, err := nkeys.CreateUser()
	if err != nil {
		return err
	}
	pub, err := kp.PublicKey()
	if err != nil {
		return err
	}
	uc := jwt.NewUserClaims(pub)
	uc.Name = "mftiko-md-accept"
	uc.Pub.Allow.Add(">")
	uc.Sub.Allow.Add(">")
	uc.IssuerAccount = lay.Manifest.Account
	token, err := uc.Encode(signer)
	if err != nil {
		return err
	}
	us, err := kp.Seed()
	if err != nil {
		return err
	}
	b, err := jwt.FormatUserConfig(token, us)
	if err != nil {
		return err
	}
	if err := os.WriteFile(*out, b, 0o600); err != nil {
		return err
	}
	fmt.Printf("%s wrote %s\n", stamp(), *out)
	return nil
}

// refresh asks SYM to list a venue now instead of after its first jittered wait, then shows the
// ticker the demand will name: the conn worker needs it TRADABLE and with a price_tick.
func refresh() error {
	venue := flag.String("venue", "", "venue to refresh, e.g. BinanceUM")
	ticker := flag.String("ticker", "", "UniversalTicker to show afterwards, e.g. BinanceUM_Perp_BTCUSDT")
	flag.Parse()
	if *venue == "" {
		return errors.New("-venue is empty")
	}
	nc, err := dialx()
	if err != nil {
		return err
	}
	defer nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	rep := &symv1.Reply{}
	err = nc.Request(ctx, sym.Subject, &symv1.Request{Req: &symv1.Request_Refresh{Refresh: &symv1.RefreshRequest{Venue: *venue}}}, rep)
	if err != nil {
		return err
	}
	r := rep.GetRefresh()
	fmt.Printf("%s refreshed %v deactivated %v failed %v\n", stamp(), r.GetRefreshed(), r.GetDeactivated(), r.GetFailed())
	if len(r.GetFailed()) > 0 {
		return errors.New("a category failed to refresh")
	}
	if *ticker == "" {
		return nil
	}
	info, err := symbolInfo(ctx, *ticker)
	if err != nil {
		return err
	}
	fmt.Printf("%s %s uid=%d exch=%s status=%s price_tick=%v\n", stamp(), *ticker, info.GetUid(), info.GetExchTicker(),
		info.GetStatus(), info.GetFilters()[symbol.PriceTick].GetValue())
	return nil
}

func symbolInfo(ctx context.Context, ticker string) (*symv1.SymbolInfo, error) {
	nc, err := dialx()
	if err != nil {
		return nil, err
	}
	defer nc.Close()
	rep := &symv1.Reply{}
	if err := nc.Request(ctx, sym.Subject, &symv1.Request{Req: &symv1.Request_Get{Get: &symv1.GetRequest{UniversalTicker: ticker}}}, rep); err != nil {
		return nil, err
	}
	return rep.GetInfo(), nil
}

// demand writes what the controller will write (E13): a state.v1.DemandEntry under
// demand/{session}/feed/{topic.UniversalTicker}.
func demand() error {
	flag.Parse()
	args := flag.Args()
	if len(args) != 3 || (args[0] != "put" && args[0] != "del") {
		return errors.New("demand put|del SESSION FEED")
	}
	c, err := root()
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	key := state.MDDemandKey(*instance, args[1], "feed/"+args[2])
	if args[0] == "put" {
		b, err := proto.Marshal(&statev1.DemandEntry{Target: &statev1.DemandEntry_Feed{Feed: args[2]}})
		if err != nil {
			return err
		}
		_, err = c.Put(ctx, key, string(b))
		if err != nil {
			return err
		}
	} else if _, err := c.Delete(ctx, key); err != nil {
		return err
	}
	fmt.Printf("%s %s %s\n", stamp(), args[0], key)
	return nil
}

// acks prints every ack and placement of the instance.
func acks() error {
	flag.Parse()
	c, err := root()
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, p := range []string{state.MDAckPrefix(*instance), state.PlacementPrefix(*instance)} {
		resp, err := c.Get(ctx, p, clientv3.WithPrefix())
		if err != nil {
			return err
		}
		for _, kv := range resp.Kvs {
			var m proto.Message = &statev1.EntryStatus{}
			if p == state.PlacementPrefix(*instance) {
				m = &statev1.Placement{}
			}
			if err := proto.Unmarshal(kv.Value, m); err != nil {
				return fmt.Errorf("%s: %w", kv.Key, err)
			}
			fmt.Printf("%s %s %v\n", stamp(), kv.Key, m)
		}
	}
	return nil
}
