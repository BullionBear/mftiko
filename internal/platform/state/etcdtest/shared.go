package etcdtest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Shared is one warm server that the tests of a binary take in turn, so that each does not pay for starting etcd.
// The zero value is ready; TestMain calls Close after the tests have run. Tests that take it must not run in
// parallel with each other.
//
// Between tests the server is reset to what it was after it started: every key and lease gone, no user or role but
// root's, auth on, and the history compacted. The reset is checked: a test whose reset leaves anything fails, and so
// does the next test if something of the previous one wrote after that reset.
//
// The server always has DefaultTokenTTL. A test that stops, restarts or rebuilds etcd, or needs other options,
// starts one of its own with Start.
type Shared struct {
	mu        sync.Mutex
	s         *Server
	root      *clientv3.Client
	dir       string
	base      baseline
	compacted int64  // the revision of the last compaction
	holder    string // the test that has the server now; "" when free
	last      string // the test that had it before
	lastClean bool   // the reset after last left the server empty
}

// baseline is what the server holds of auth right after it started.
type baseline struct {
	users, roles, rootRoles []string
}

// Take hands the test the shared server, starting it on first use, after resetting it and checking that the reset
// left nothing. When the test ends the server is reset and checked again. It skips the test if no etcd binary is
// found, and fails it if etcd does not come up or cannot be reset.
func (sh *Shared) Take(t testing.TB) *Server {
	t.Helper()
	bin := requireBinary(t)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.holder != "" {
		t.Fatalf("etcdtest: %s took the shared server while %s has it: tests that share it must not run in parallel", t.Name(), sh.holder)
		return nil
	}
	if sh.s == nil {
		if err := sh.start(bin); err != nil {
			t.Fatalf("etcdtest: shared etcd did not start: %v", err)
			return nil
		}
	}
	found, err := sh.reset()
	if err != nil {
		t.Fatalf("etcdtest: shared etcd %s not reset before %s: %v", sh.s.name, t.Name(), err)
		return nil
	}
	if !found.empty() {
		if sh.lastClean {
			t.Fatalf("etcdtest: shared etcd %s was reset after %s, then had %s when %s took it: something of %s wrote after its end",
				sh.s.name, sh.last, found, t.Name(), sh.last)
			return nil
		}
		t.Logf("etcdtest: shared etcd %s had %s left after %s's failed reset; removed", sh.s.name, found, sh.last)
	}
	sh.holder = t.Name()
	mark := sh.s.out.Len()
	t.Cleanup(func() {
		sh.mu.Lock()
		defer sh.mu.Unlock()
		_, err := sh.reset()
		sh.holder, sh.last, sh.lastClean = "", t.Name(), err == nil
		if err != nil {
			t.Errorf("etcdtest: shared etcd %s not reset after %s: %v", sh.s.name, t.Name(), err)
		}
		if out := sh.s.out.From(mark); t.Failed() && out != "" {
			t.Logf("etcd %s output during %s:\n%s", sh.s.name, t.Name(), out)
		}
	})
	return sh.s
}

// Close stops the server and removes its data.
func (sh *Shared) Close() {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.root != nil {
		sh.root.Close()
	}
	if sh.s != nil {
		sh.s.kill()
	}
	if sh.dir != "" {
		os.RemoveAll(sh.dir)
	}
	sh.s, sh.root, sh.dir = nil, nil, ""
}

func (sh *Shared) start(bin string) error {
	dir, err := os.MkdirTemp("", "etcdtest-shared-")
	if err != nil {
		return fmt.Errorf("etcdtest: %w", err)
	}
	var errs []error
	for i := range startTries {
		data, keys := filepath.Join(dir, fmt.Sprint(i, "-data")), filepath.Join(dir, fmt.Sprint(i, "-keys"))
		for _, d := range []string{data, keys} {
			if err := os.Mkdir(d, 0o700); err != nil {
				os.RemoveAll(dir)
				return fmt.Errorf("etcdtest: %w", err)
			}
		}
		s, err := newServer(bin, data, keys, options{tokenTTL: DefaultTokenTTL})
		if err != nil {
			errs = append(errs, err)
			continue
		}
		root, base, err := rootAndBaseline(s.endpoints)
		if err != nil {
			s.kill()
			errs = append(errs, err)
			continue
		}
		s.shared = true
		sh.s, sh.root, sh.dir, sh.base = s, root, dir, base
		return nil
	}
	os.RemoveAll(dir)
	return errors.Join(errs...)
}

func rootAndBaseline(endpoints []string) (*clientv3.Client, baseline, error) {
	c, err := clientv3.New(clientv3.Config{
		Endpoints: endpoints, DialTimeout: 5 * time.Second, Username: RootUser, Password: RootPassword(),
	})
	if err != nil {
		return nil, baseline{}, fmt.Errorf("etcdtest: root client: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), readyTimeout)
	defer cancel()
	var b baseline
	users, err := c.UserList(ctx)
	if err == nil {
		b.users = sorted(users.Users)
		var roles *clientv3.AuthRoleListResponse
		if roles, err = c.RoleList(ctx); err == nil {
			b.roles = sorted(roles.Roles)
			var root *clientv3.AuthUserGetResponse
			if root, err = c.UserGet(ctx, RootUser); err == nil {
				b.rootRoles = sorted(root.Roles)
			}
		}
	}
	if err != nil {
		c.Close()
		return nil, baseline{}, fmt.Errorf("etcdtest: read the baseline: %w", err)
	}
	return c, b, nil
}

// reset empties the server down to its baseline and compacts its history, then checks that nothing is left. found
// is what was there before; err says what the reset could not remove, or why it could not tell.
func (sh *Shared) reset() (found leftovers, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), readyTimeout)
	defer cancel()
	c := sh.root
	// Users and roles go first. Changing them makes the root client's token older than the auth revision, and its
	// next key or lease request authenticates again, a bcrypt compare in etcd: this way that happens once.
	if err = surveyAuth(ctx, c, sh.base, &found); err != nil {
		return found, err
	}
	for _, r := range found.roles {
		if _, err := c.RoleDelete(ctx, r); err != nil && !errors.Is(err, rpctypes.ErrRoleNotFound) {
			return found, fmt.Errorf("delete role %s: %w", r, err)
		}
	}
	for _, u := range found.users {
		if _, err := c.UserDelete(ctx, u); err != nil && !errors.Is(err, rpctypes.ErrUserNotFound) {
			return found, fmt.Errorf("delete user %s: %w", u, err)
		}
	}
	if err = surveyData(ctx, c, &found); err != nil {
		return found, err
	}
	for _, id := range found.leases { // their keys go with them
		if _, err := c.Revoke(ctx, id); err != nil && !errors.Is(err, rpctypes.ErrLeaseNotFound) {
			return found, fmt.Errorf("revoke lease %x: %w", id, err)
		}
	}
	if _, err := c.Delete(ctx, "\x00", clientv3.WithFromKey()); err != nil {
		return found, fmt.Errorf("delete every key: %w", err)
	}
	var left leftovers
	if err = surveyAuth(ctx, c, sh.base, &left); err != nil {
		return found, err
	}
	if err = surveyData(ctx, c, &left); err != nil {
		return found, err
	}
	if !left.empty() {
		return found, fmt.Errorf("still there after the reset: %s", left)
	}
	if left.rev > sh.compacted {
		if _, err := c.Compact(ctx, left.rev, clientv3.WithCompactPhysical()); err != nil && !errors.Is(err, rpctypes.ErrCompacted) {
			return found, fmt.Errorf("compact at %d: %w", left.rev, err)
		}
		sh.compacted = left.rev
	}
	return found, nil
}

// leftovers is what a server holds beyond its baseline.
type leftovers struct {
	keys         int64
	leases       []clientv3.LeaseID
	users, roles []string // not in the baseline
	changed      []string // the baseline itself is not as it was
	rev          int64    // the server's revision when surveyed
}

func (l leftovers) empty() bool {
	return l.keys == 0 && len(l.leases) == 0 && len(l.users) == 0 && len(l.roles) == 0 && len(l.changed) == 0
}

func (l leftovers) String() string {
	var parts []string
	if l.keys > 0 {
		parts = append(parts, fmt.Sprintf("%d keys", l.keys))
	}
	if len(l.leases) > 0 {
		parts = append(parts, fmt.Sprintf("%d leases", len(l.leases)))
	}
	if len(l.users) > 0 {
		parts = append(parts, fmt.Sprintf("users %v", l.users))
	}
	if len(l.roles) > 0 {
		parts = append(parts, fmt.Sprintf("roles %v", l.roles))
	}
	parts = append(parts, l.changed...)
	if len(parts) == 0 {
		return "nothing"
	}
	return strings.Join(parts, ", ")
}

// surveyAuth fills in l's users, roles and changes to the baseline.
func surveyAuth(ctx context.Context, c *clientv3.Client, base baseline, l *leftovers) error {
	users, err := c.UserList(ctx)
	if err != nil {
		return fmt.Errorf("list users: %w", err)
	}
	var missing []string
	l.users, missing = extra(users.Users, base.users)
	for _, u := range missing {
		l.changed = append(l.changed, "user "+u+" gone")
	}
	roles, err := c.RoleList(ctx)
	if err != nil {
		return fmt.Errorf("list roles: %w", err)
	}
	l.roles, missing = extra(roles.Roles, base.roles)
	for _, r := range missing {
		l.changed = append(l.changed, "role "+r+" gone")
	}
	root, err := c.UserGet(ctx, RootUser)
	if err != nil {
		return fmt.Errorf("get %s: %w", RootUser, err)
	}
	if got := sorted(root.Roles); !slices.Equal(got, base.rootRoles) {
		l.changed = append(l.changed, fmt.Sprintf("%s has roles %v, not %v", RootUser, got, base.rootRoles))
	}
	auth, err := c.AuthStatus(ctx)
	if err != nil {
		return fmt.Errorf("auth status: %w", err)
	}
	if !auth.Enabled {
		l.changed = append(l.changed, "auth off")
	}
	return nil
}

// surveyData fills in l's keys, leases and revision.
func surveyData(ctx context.Context, c *clientv3.Client, l *leftovers) error {
	kvs, err := c.Get(ctx, "\x00", clientv3.WithFromKey(), clientv3.WithCountOnly())
	if err != nil {
		return fmt.Errorf("count keys: %w", err)
	}
	l.keys, l.rev = kvs.Count, kvs.Header.Revision
	leases, err := c.Leases(ctx)
	if err != nil {
		return fmt.Errorf("list leases: %w", err)
	}
	for _, s := range leases.Leases {
		l.leases = append(l.leases, s.ID)
	}
	return nil
}

// extra is what have has beyond want, and what it lacks of want.
func extra(have, want []string) (more, missing []string) {
	for _, h := range have {
		if !slices.Contains(want, h) {
			more = append(more, h)
		}
	}
	for _, w := range want {
		if !slices.Contains(have, w) {
			missing = append(missing, w)
		}
	}
	return more, missing
}

func sorted(s []string) []string { return slices.Sorted(slices.Values(s)) }
