package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/BullionBear/mftiko/internal/platform/sealbox"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// deployment is the mftiko-etcd-roles input that the controller and the API read. The agents read
// the same file as their topology; they ignore planes.
type deployment struct {
	state.Topology
	Planes map[string]state.Plane `json:"planes"`
}

func accDeployment() deployment {
	return deployment{
		Topology: state.Topology{
			Regions: map[string]state.Region{
				region:  {Endpoints: []string{*etcdURL}},
				region2: {Endpoints: []string{*etcd2URL}},
			},
			Instances: map[string]string{
				state.ControllerInstance: region, mdInst: region, tdInst: region, stsInst: region, sts2Inst: region2,
			},
		},
		Planes: map[string]state.Plane{
			mdInst: state.PlaneMD, tdInst: state.PlaneTD, stsInst: state.PlaneSTS, sts2Inst: state.PlaneSTS,
		},
	}
}

// setup gives every instance, the controller and the API their etcd users in both regions
// (state.AuthTables), adds root and turns auth on, like mftiko-md-accept setup does for one
// instance. It writes deployment.json, api-a.json and api-b.json (the replicas differ only in
// their port) and the token, which it keeps across runs. Running it again changes nothing in etcd.
func setup() error {
	out := flag.String("out", "/work/e2e", "directory for deployment.json, api-*.json and token")
	sealKey := flag.String("td-seal-key", "/work/td-seal.key", "td1's private key; the API seals accounts to its public half")
	registry := flag.String("registry", "registry:5000", "registry host:port the API's catalog reads (plain HTTP)")
	retention := flag.String("retention", "1m", "W5 retention; short, so main sees the spec deleted")
	flag.Parse()
	if *password == "" {
		return errors.New("-etcd-password is empty")
	}
	dep := accDeployment()
	tables, err := state.AuthTables(dep.Topology, dep.Planes)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for name, r := range dep.Regions {
		if err := ensureAuth(ctx, r.Endpoints[0], tables[name]); err != nil {
			return fmt.Errorf("region %s: %w", name, err)
		}
		say("region %s: users and roles in place, auth on", name)
	}
	key, err := sealbox.LoadKeyFile(*sealKey)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*out, 0o700); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(*out, "deployment.json"), dep, 0o644); err != nil {
		return err
	}
	token, err := ensureToken(filepath.Join(*out, "token"))
	if err != nil {
		return err
	}
	// Both replicas sign the console cookie with this key (D1009); accept.sh passes it in the environment.
	if _, err := ensureToken(filepath.Join(*out, "cookie-key")); err != nil {
		return err
	}
	sum := sha256.Sum256([]byte(token))
	for _, r := range []struct{ name, listen string }{{"a", ":8080"}, {"b", ":8081"}} {
		conf := map[string]any{
			"users":             []any{map[string]string{"name": userName, "token_sha256": hex.EncodeToString(sum[:])}},
			"repositories":      []string{"strategies/" + strategyID},
			"registry":          *registry,
			"plain_http":        true,
			"td_keys":           map[string]string{tdInst: key.Public().String()},
			"retention":         *retention,
			"unavailable_after": "5s",
			"listen":            r.listen,
		}
		if err := writeJSON(filepath.Join(*out, "api-"+r.name+".json"), conf, 0o600); err != nil {
			return err
		}
	}
	say("wrote %s: deployment.json, api-a.json, api-b.json, token, cookie-key", *out)
	return nil
}

func ensureAuth(ctx context.Context, endpoint string, table state.AuthTable) error {
	c, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second, Logger: zap.NewNop()})
	if err != nil {
		return err
	}
	if _, err := c.Get(ctx, "/"); err != nil {
		// Auth is on already: come back as root.
		c.Close()
		c, err = clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second,
			Username: "root", Password: *password, Logger: zap.NewNop()})
		if err != nil {
			return err
		}
	}
	defer c.Close()
	if err := state.EnsureAuth(ctx, c, table, func(string) string { return *password }); err != nil {
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
	_, err = c.AuthEnable(ctx)
	return err
}

// ensureToken returns the secret in path, or makes one: 32 random bytes, base64 (D1055).
func ensureToken(path string) (string, error) {
	if b, err := os.ReadFile(path); err == nil {
		if t := strings.TrimSpace(string(b)); t != "" {
			return t, nil
		}
	}
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	t := base64.StdEncoding.EncodeToString(b)
	return t, os.WriteFile(path, []byte(t+"\n"), 0o600)
}

func writeJSON(path string, v any, mode os.FileMode) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), mode)
}
