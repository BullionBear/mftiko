package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"go.etcd.io/etcd/api/v3/authpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/BullionBear/mftiko/internal/platform/state"
)

// runApply applies the tables to each region's cluster as root, with state.EnsureAuth (E12-D049). Auth must be on
// already. secrets holds one file per user: root's password in "root", and each service user's in a file named like
// the user. A user the cluster lacks gets the password in its file; with no file, a new one is generated and written
// there first. A user the cluster has keeps its password: rotation is etcdctl user passwd (E12-D050). What the
// cluster has beyond the table is listed, not removed. region "" is every region.
func runApply(ctx context.Context, in, region, secrets string, stdin io.Reader, out io.Writer) error {
	if secrets == "" {
		return errors.New("-secrets is required with -apply")
	}
	d, err := readDeployment(in, stdin)
	if err != nil {
		return err
	}
	tables, err := state.AuthTables(d.Topology, d.Planes)
	if err != nil {
		return err
	}
	regions := slices.Sorted(maps.Keys(tables))
	if region != "" {
		if _, ok := tables[region]; !ok {
			return fmt.Errorf("region %q is not in the deployment", region)
		}
		regions = []string{region}
	}
	rootPass, err := readSecret(secrets, state.UserRoot)
	if err != nil {
		return err
	}
	for _, r := range regions {
		if err := applyRegion(ctx, tables[r], d.Regions[r].Endpoints, rootPass, secrets, out); err != nil {
			return fmt.Errorf("region %s: %w", r, err)
		}
	}
	return nil
}

func applyRegion(ctx context.Context, table state.AuthTable, endpoints []string, rootPass, secrets string, out io.Writer) error {
	c, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: state.DialTimeout,
		Username: state.UserRoot, Password: rootPass, Logger: zap.NewNop()})
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if _, err := c.AuthStatus(ctx); err != nil {
		return err
	}
	users, err := c.UserList(ctx)
	if err != nil {
		return err
	}
	// Every password a new user needs is read or written before anything changes, so that a file that cannot be
	// written stops the apply instead of leaving a user nobody knows the password of.
	passwords := map[string]string{}
	for _, u := range table.Users() {
		if slices.Contains(users.Users, u) {
			continue
		}
		p, made, err := ensureSecret(secrets, u)
		if err != nil {
			return err
		}
		passwords[u] = p
		how := "from"
		if made {
			how = "generated into"
		}
		fmt.Fprintf(out, "# region %s: add user %s, password %s %s\n", table.Region, u, how, filepath.Join(secrets, u))
	}
	if err := state.EnsureAuth(ctx, c, table, func(u string) string { return passwords[u] }); err != nil {
		return err
	}
	return listExtra(ctx, c, table, out)
}

// listExtra prints what the cluster has beyond table: EnsureAuth never removes, so a role, user or permission the
// table no longer has stays until removed by hand (for a retired instance, -drop prints the commands).
func listExtra(ctx context.Context, c *clientv3.Client, table state.AuthTable, out io.Writer) error {
	want := map[string]state.Role{}
	for _, r := range table.Roles {
		want[r.Name] = r
	}
	roles, err := c.RoleList(ctx)
	if err != nil {
		return err
	}
	for _, name := range roles.Roles {
		if name == state.UserRoot {
			continue
		}
		r, ok := want[name]
		if !ok {
			fmt.Fprintf(out, "# region %s: role %s is not in the table\n", table.Region, name)
			continue
		}
		got, err := c.RoleGet(ctx, name)
		if err != nil {
			return err
		}
		for _, p := range got.Perm {
			have := state.Permission{Key: string(p.Key), RangeEnd: string(p.RangeEnd), Write: p.PermType == authpb.READWRITE}
			if !slices.Contains(r.Permissions, have) {
				fmt.Fprintf(out, "# region %s: role %s has %s, not in the table\n", table.Region, name, permString(have))
			}
		}
	}
	users, err := c.UserList(ctx)
	if err != nil {
		return err
	}
	for _, name := range users.Users {
		if name == state.UserRoot {
			continue
		}
		if _, ok := want[name]; !ok {
			fmt.Fprintf(out, "# region %s: user %s is not in the table\n", table.Region, name)
		}
	}
	return nil
}

func permString(p state.Permission) string {
	kind := "read"
	if p.Write {
		kind = "readwrite"
	}
	if p.RangeEnd == "" {
		return kind + " " + p.Key
	}
	return kind + " " + p.Key + " " + p.RangeEnd
}

// readSecret is the password in dir/name, without surrounding whitespace.
func readSecret(dir, name string) (string, error) {
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return "", err
	}
	p := strings.TrimSpace(string(b))
	if p == "" {
		return "", fmt.Errorf("%s is empty", filepath.Join(dir, name))
	}
	return p, nil
}

// ensureSecret is the password in dir/name, or a new one written there (0600) if the file does not exist: 32 random
// bytes, base64url without padding, so that it is one word in an EnvironmentFile and on etcdctl's stdin.
func ensureSecret(dir, name string) (p string, made bool, err error) {
	p, err = readSecret(dir, name)
	if !errors.Is(err, fs.ErrNotExist) {
		return p, false, err
	}
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", false, err
	}
	p = base64.RawURLEncoding.EncodeToString(b)
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", false, err
	}
	if _, err := f.WriteString(p + "\n"); err != nil {
		f.Close()
		return "", false, err
	}
	return p, true, f.Close()
}
