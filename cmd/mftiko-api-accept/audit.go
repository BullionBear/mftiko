package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
)

// audited checks that every request the flow sent is an audit row with the same method, path,
// status and user (E10-D010); a 401 has no user. Rows are written off the request path (E10-D049),
// so it waits a little.
func (f *flow) audited() error {
	if *dbURL == "" {
		return errors.New("-database is empty: the audit check needs Postgres")
	}
	reqs := f.rec.all()
	ids := make([]string, 0, len(reqs))
	for _, r := range reqs {
		if r.id == "" {
			return fmt.Errorf("%s %s (%d) carried no X-Request-Id", r.method, r.path, r.status)
		}
		ids = append(ids, r.id)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, *dbURL)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	type row struct {
		user, method, path string
		status             int
	}
	var missing []sent
	err = until(ctx, 20*time.Second, "audit rows", func() error {
		rows, err := conn.Query(ctx, `SELECT request_id, user_name, method, path, status FROM audit WHERE request_id = ANY($1)`, ids)
		if err != nil {
			return stop(err)
		}
		got := map[string]row{}
		for rows.Next() {
			var id string
			var r row
			if err := rows.Scan(&id, &r.user, &r.method, &r.path, &r.status); err != nil {
				rows.Close()
				return stop(err)
			}
			got[id] = r
		}
		if err := rows.Err(); err != nil {
			return stop(err)
		}
		missing = missing[:0]
		for _, s := range reqs {
			r, ok := got[s.id]
			if !ok {
				missing = append(missing, s)
				continue
			}
			// A 401 was not authenticated, so its row names no user.
			user := userName
			if s.status == http.StatusUnauthorized {
				user = ""
			}
			if r.user != user || r.method != s.method || r.path != s.path || r.status != s.status {
				return stop(fmt.Errorf("request %s: sent %s %s %d as %q, audit has %s %s %d as %q",
					s.id, s.method, s.path, s.status, user, r.method, r.path, r.status, r.user))
			}
		}
		if len(missing) > 0 {
			return fmt.Errorf("%d of %d requests have no row, first %s %s %s", len(missing), len(reqs), missing[0].id, missing[0].method, missing[0].path)
		}
		return nil
	})
	if err != nil {
		return err
	}
	say("audit: all %d requests have a row with their method, path, status and user", len(reqs))
	return nil
}
