package etcdtest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// recorder is a testing.TB that records Fatalf and Errorf instead of failing the test. Its Fatalf does not end the
// caller, so the code under test must return right after calling it.
type recorder struct {
	testing.TB
	failures []string
}

func (r *recorder) Fatalf(format string, args ...any) {
	r.failures = append(r.failures, fmt.Sprintf(format, args...))
}

func (r *recorder) Errorf(format string, args ...any) {
	r.failures = append(r.failures, fmt.Sprintf(format, args...))
}

// failedWith reports an error unless exactly one failure was recorded, and it says want.
func (r *recorder) failedWith(want string) error {
	if len(r.failures) != 1 || !strings.Contains(r.failures[0], want) {
		return fmt.Errorf("failures %q, want one saying %q", r.failures, want)
	}
	return nil
}

// authOf is the server's users and roles.
func authOf(t *testing.T, ctx context.Context, c *clientv3.Client) (users, roles []string) {
	t.Helper()
	u, err := c.UserList(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := c.RoleList(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return sorted(u.Users), sorted(r.Roles)
}

// The tests that take a shared server in turn each find it empty: no key, no lease, only root, no history of the
// test before. What a reset cannot undo, and what a test leaves running after its reset, fail a test.
func TestShared(t *testing.T) {
	var sh Shared
	t.Cleanup(sh.Close)
	ctx := t.Context()

	var first *Server
	var users, roles []string
	var rev int64
	t.Run("dirty", func(t *testing.T) {
		first = sh.Take(t)
		c := first.Client(t)
		users, roles = authOf(t, ctx, c)
		lease, err := c.Grant(ctx, 60)
		if err != nil {
			t.Fatal(err)
		}
		for _, err := range []error{
			errOf(c.Put(ctx, "/a", "v")),
			errOf(c.Put(ctx, "/b/c", "v")),
			errOf(c.Put(ctx, "/leased", "v", clientv3.WithLease(lease.ID))),
			errOf(c.Grant(ctx, 60)), // a lease with no key
			errOf(c.RoleAdd(ctx, "r")),
			errOf(c.UserAdd(ctx, "u", Password("u"))),
			errOf(c.UserGrantRole(ctx, "u", "r")),
			errOf(c.RoleAdd(ctx, "unused")),
		} {
			if err != nil {
				t.Fatal(err)
			}
		}
		resp, err := c.Put(ctx, "/last", "v")
		if err != nil {
			t.Fatal(err)
		}
		rev = resp.Header.Revision
	})

	t.Run("clean", func(t *testing.T) {
		s := sh.Take(t)
		if s != first {
			t.Fatal("a second server: the shared one was not reused")
		}
		c := s.Client(t)
		if resp, err := c.Get(ctx, "\x00", clientv3.WithFromKey(), clientv3.WithCountOnly()); err != nil || resp.Count != 0 {
			t.Fatalf("keys after the reset: %v, %v", resp, err)
		}
		if resp, err := c.Leases(ctx); err != nil || len(resp.Leases) != 0 {
			t.Fatalf("leases after the reset: %v, %v", resp, err)
		}
		if u, r := authOf(t, ctx, c); !slices.Equal(u, users) || !slices.Equal(r, roles) || !slices.Equal(u, []string{RootUser}) {
			t.Fatalf("users %v and roles %v after the reset, want %v and %v as it started", u, r, users, roles)
		}
		if _, err := c.Get(ctx, "/last", clientv3.WithRev(rev)); !errors.Is(err, rpctypes.ErrCompacted) {
			t.Fatalf("the previous test's history: %v, want it compacted", err)
		}
	})

	var lateName string
	t.Run("late writer", func(t *testing.T) {
		lateName = t.Name()
		c := ClientAs(t, first.Endpoints(), RootUser)
		t.Cleanup(func() { // runs after Take's reset
			if _, err := c.Put(context.Background(), "/late", "v"); err != nil {
				t.Error(err)
			}
		})
		sh.Take(t)
	})
	t.Run("after a late writer", func(t *testing.T) {
		r := &recorder{TB: t}
		if s := sh.Take(r); s != nil {
			t.Error("handed out a server written after the last test's reset")
		}
		if err := r.failedWith(lateName + " wrote after its end"); err != nil {
			t.Fatal(err)
		}
	})

	auth := &recorder{}
	t.Run("auth turned off", func(t *testing.T) {
		auth.TB = t
		s := sh.Take(auth)
		if s == nil {
			t.Fatalf("not taken: %q", auth.failures)
		}
		if _, err := s.Client(t).AuthDisable(ctx); err != nil {
			t.Fatal(err)
		}
	})
	if err := auth.failedWith("auth off"); err != nil {
		t.Fatalf("a reset that cannot turn auth back on: %v", err)
	}
	if _, err := sh.root.AuthEnable(ctx); err != nil {
		t.Fatal(err)
	}

	t.Run("one at a time", func(t *testing.T) {
		s := sh.Take(t) // empty again: the late write and the auth change are gone
		for _, c := range []struct {
			what string
			do   func(testing.TB)
		}{
			{"in parallel", func(t testing.TB) { sh.Take(t) }},
			{"Restart on the shared server", s.Restart},
			{"Rebuild on the shared server", s.Rebuild},
		} {
			r := &recorder{TB: t}
			c.do(r)
			if err := r.failedWith(c.what); err != nil {
				t.Error(err)
			}
		}
		if _, err := s.Client(t).Put(ctx, "/still", "serving"); err != nil {
			t.Fatalf("the shared server after the refusals: %v", err)
		}
	})
}

func errOf[T any](_ T, err error) error { return err }
