package main

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

// apiClient is the PMS's view of mftiko-api: an ordinary bearer token, no other role (E21-D010).
type apiClient struct {
	c *apiv1.ClientWithResponses
}

func newAPI(base, token string) (*apiClient, error) {
	auth := apiv1.WithRequestEditorFn(func(_ context.Context, req *http.Request) error {
		req.Header.Set("Authorization", "Bearer "+token)
		return nil
	})
	c, err := apiv1.NewClientWithResponses(strings.TrimRight(base, "/")+"/api/v1",
		apiv1.WithHTTPClient(&http.Client{Timeout: 15 * time.Second}), auth)
	if err != nil {
		return nil, err
	}
	return &apiClient{c: c}, nil
}

func status(op string, resp *http.Response, body []byte) error {
	code := 0
	if resp != nil {
		code = resp.StatusCode
	}
	return fmt.Errorf("%s: %d %s", op, code, body)
}

const pageSize = 1000

// accounts is every account, soft-deleted included: history keeps its provenance (E21-D012).
func (a *apiClient) accounts(ctx context.Context) ([]apiv1.Account, error) {
	var out []apiv1.Account
	limit, all := pageSize, true
	var cursor *string
	for {
		r, err := a.c.ListAccountsWithResponse(ctx, &apiv1.ListAccountsParams{Limit: &limit, Cursor: cursor, IncludeDeleted: &all})
		if err != nil {
			return nil, err
		}
		if r.JSON200 == nil {
			return nil, status("GET /accounts", r.HTTPResponse, r.Body)
		}
		out = append(out, r.JSON200.Items...)
		if r.JSON200.NextCursor == nil {
			return out, nil
		}
		cursor = r.JSON200.NextCursor
	}
}

type scopeRow struct {
	apiID int64
	uid   uint32
	since time.Time
}

// scopes is every (api_id, uid, since). Regions that were unavailable are returned too: the PMS
// must treat the result as incomplete, not as "no scope" (E21-D010).
func (a *apiClient) scopes(ctx context.Context) ([]scopeRow, []string, error) {
	var out []scopeRow
	var down []string
	limit := pageSize
	var cursor *string
	for {
		r, err := a.c.ListScopesWithResponse(ctx, &apiv1.ListScopesParams{Limit: &limit, Cursor: cursor})
		if err != nil {
			return nil, nil, err
		}
		if r.JSON200 == nil {
			return nil, nil, status("GET /scopes", r.HTTPResponse, r.Body)
		}
		for _, s := range r.JSON200.Items {
			if s.Uid < 1 || s.Uid > 1<<32-1 {
				continue
			}
			out = append(out, scopeRow{apiID: s.ApiId, uid: uint32(s.Uid), since: s.Since})
		}
		for _, reg := range r.JSON200.Freshness.UnavailableRegions {
			if !slices.Contains(down, reg) {
				down = append(down, reg)
			}
		}
		if r.JSON200.NextCursor == nil {
			return out, down, nil
		}
		cursor = r.JSON200.NextCursor
	}
}

type accountGroup struct {
	exchange, uid string
	apiIDs        []int64
}

// groupAccounts joins api_ids by (exchange, exch_account_uid) (E21-D008). An account without
// either is its own group under its api_id, so it is not merged with anything.
func groupAccounts(accounts []apiv1.Account) []accountGroup {
	var out []accountGroup
	idx := map[[2]string]int{}
	for _, acc := range accounts {
		ex, uid := deref(acc.Exchange), deref(acc.ExchAccountUid)
		if ex == "" || uid == "" {
			ex, uid = "unknown", fmt.Sprintf("api_id:%d", acc.ApiId)
		}
		k := [2]string{ex, uid}
		i, ok := idx[k]
		if !ok {
			i = len(out)
			idx[k] = i
			out = append(out, accountGroup{exchange: ex, uid: uid})
		}
		out[i].apiIDs = append(out[i].apiIDs, acc.ApiId)
	}
	for i := range out {
		slices.Sort(out[i].apiIDs)
	}
	return out
}

func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// owners is every (session, apiID) that declared apiID, with its algos and interval. Records come
// from /session-records?api_id=, which keeps sessions W5 deleted (E21-D072). A record whose spec is
// still in etcd reads its live status from /sessions/{id} (E21-D011).
func (a *apiClient) owners(ctx context.Context, apiID int64) ([]owner, error) {
	var out []owner
	limit := pageSize
	var cursor *string
	for {
		r, err := a.c.ListSessionRecordsWithResponse(ctx, &apiv1.ListSessionRecordsParams{Limit: &limit, Cursor: cursor, ApiId: &apiID})
		if err != nil {
			return nil, err
		}
		if r.JSON200 == nil {
			return nil, status("GET /session-records", r.HTTPResponse, r.Body)
		}
		for _, rec := range r.JSON200.Items {
			facts, err := a.facts(ctx, rec)
			if err != nil {
				return nil, err
			}
			iv := sessionInterval(facts)
			for _, decl := range rec.Spec.Accounts {
				if decl.ApiId != apiID {
					continue
				}
				algos := make([]uint32, 0, len(decl.AlgoIds))
				for _, al := range decl.AlgoIds {
					algos = append(algos, uint32(al))
				}
				out = append(out, owner{
					RecordID: rec.RecordId.String(), SessionID: rec.SessionId, APIID: apiID,
					Algos: algos, Interval: iv,
				})
			}
		}
		if r.JSON200.NextCursor == nil {
			return out, nil
		}
		cursor = r.JSON200.NextCursor
	}
}

func (a *apiClient) facts(ctx context.Context, rec apiv1.SessionRecord) (sessionFacts, error) {
	f := sessionFacts{State: string(rec.State), CreatedAt: rec.CreatedAt, Retired: rec.StsRetiredAt}
	st := rec.Final
	if rec.State == apiv1.SessionRecordStateCreated || rec.State == apiv1.SessionRecordStatePending {
		r, err := a.c.GetSessionWithResponse(ctx, rec.SessionId)
		if err != nil {
			return sessionFacts{}, err
		}
		switch {
		case r.JSON200 != nil && sameSession(r.JSON200.CreatedAt, rec.CreatedAt):
			st = r.JSON200.Status
		case r.JSON200 != nil, r.StatusCode() == http.StatusNotFound:
			// The spec is gone (W5 ran, or the id is reused) and the record is not closed yet:
			// the next run sees final. Until then the session is taken as open.
		default:
			return sessionFacts{}, status("GET /sessions/"+rec.SessionId, r.HTTPResponse, r.Body)
		}
	}
	if st == nil {
		f.NoStatus = true
		return f, nil
	}
	f.Terminal = st.Phase == apiv1.PhaseDone || st.Phase == apiv1.PhaseFailed
	if st.LastExit != nil {
		at := st.LastExit.At
		f.LastExit = &at
	}
	return f, nil
}

// sameSession is true when /sessions/{id} is the session of this record, not a later one that
// reused the id (E10-D007).
func sameSession(live *time.Time, rec time.Time) bool {
	return live != nil && live.Equal(rec)
}
