package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	"github.com/google/uuid"
	openapi_types "github.com/oapi-codegen/runtime/types"
)

// sessionIDRe is the session id syntax (E10-D007).
var sessionIDRe = regexp.MustCompile(`^[0-9a-f]{6}$`)

// lastKnownNote is the table annotation when the STS AgentRecord is the last
// known value (E11-D025). -o json does not add it: the API body is unchanged
// (E11-D008), and last_known is on the instance, not on LogLocation.
const lastKnownNote = "最後已知，agent 目前不在線"

func runSessionList(a *App) int {
	if code := a.onlyFlags("phase", "active", "sts-instance", "limit", "cursor"); code != 0 {
		return code
	}
	if len(a.Args) != 0 {
		return a.exit("session list takes no arguments")
	}
	phases, active, sts, code := a.sessionQuery()
	if code != 0 {
		return code
	}
	if code := a.pageFlags(); code != 0 {
		return code
	}
	api, code := a.client()
	if code != 0 {
		return code
	}
	var capN *int
	if a.opt.limitSet {
		n := a.opt.limit
		capN = &n
	}
	return a.listSessions(api, phases, active, sts, capN, a.opt.cursorSet, a.opt.cursor)
}

func runSessionShow(a *App) int {
	if code := a.onlyFlags(); code != 0 {
		return code
	}
	id, code := a.sessionArg("session show")
	if code != 0 {
		return code
	}
	api, code := a.client()
	if code != 0 {
		return code
	}
	res, err := api.GetSession(a.ctx, id)
	if code := a.checked(res, err); code != 0 {
		return code
	}
	return a.writeSession(api, res)
}

func runSessionEnd(a *App) int {
	if code := a.onlyFlags(); code != 0 {
		return code
	}
	id, code := a.sessionArg("session end")
	if code != 0 {
		return code
	}
	if err := a.confirm("end session " + id); err != nil {
		return a.exit(err.Error())
	}
	api, code := a.client()
	if code != 0 {
		return code
	}
	res, err := api.EndSession(a.ctx, id)
	if code := a.checked(res, err); code != 0 {
		return code
	}
	return a.writeSession(api, res)
}

func runSessionDelete(a *App) int {
	if code := a.onlyFlags(); code != 0 {
		return code
	}
	id, code := a.sessionArg("session delete")
	if code != 0 {
		return code
	}
	if err := a.confirm("delete session " + id); err != nil {
		return a.exit(err.Error())
	}
	api, code := a.client()
	if code != 0 {
		return code
	}
	res, err := api.DeleteSession(a.ctx, id)
	code = a.checked(res, err)
	if code == ExitNotTerminal {
		fmt.Fprintf(a.Stderr, "mftiko: run session end %s first\n", a.sec.redact(id))
	}
	if code != 0 {
		return code
	}
	a.noteResult(res)
	return ExitOK
}

func runRecordList(a *App) int {
	if code := a.onlyFlags("session-id", "strategy-id", "created-by", "created-after", "created-before", "limit", "cursor"); code != 0 {
		return code
	}
	if len(a.Args) != 0 {
		return a.exit("record list takes no arguments")
	}
	params, code := a.recordQuery()
	if code != 0 {
		return code
	}
	if code := a.pageFlags(); code != 0 {
		return code
	}
	api, code := a.client()
	if code != 0 {
		return code
	}
	got, code := a.collect(func(limit int, cursor string) (Result, error) {
		p := params
		lim := apiv1.Limit(limit)
		p.Limit = &lim
		if cursor != "" {
			c := apiv1.Cursor(cursor)
			p.Cursor = &c
		}
		return api.ListSessionRecords(a.ctx, &p)
	})
	if code != 0 {
		return code
	}
	return a.finishPaged(got, recordsText)
}

func runRecordShow(a *App) int {
	if code := a.onlyFlags(); code != 0 {
		return code
	}
	if len(a.Args) != 1 {
		return a.exit("record show takes a record id")
	}
	id, err := uuid.Parse(a.Args[0])
	if err != nil {
		return a.exit("record id must be a UUID")
	}
	api, code := a.client()
	if code != 0 {
		return code
	}
	res, err := api.GetSessionRecord(a.ctx, openapi_types.UUID(id))
	if code := a.checked(res, err); code != 0 {
		return code
	}
	return a.finishOne(res, recordText)
}

func (a *App) pageFlags() int {
	if a.opt.limitSet && a.opt.cursorSet {
		return a.exit("-limit and -cursor cannot both be set")
	}
	if a.opt.cursorSet && a.opt.cursor == "" {
		return a.exit("-cursor needs a value")
	}
	return 0
}

func (a *App) sessionArg(cmd string) (string, int) {
	if len(a.Args) != 1 {
		return "", a.exit(cmd + " takes a session id")
	}
	id := a.Args[0]
	if !sessionIDRe.MatchString(id) {
		return "", a.exit("session id must be 6 lowercase hex digits")
	}
	return id, 0
}

func (a *App) sessionQuery() ([]apiv1.Phase, *bool, *string, int) {
	var phases []apiv1.Phase
	if a.opt.phase.set {
		for _, raw := range a.opt.phase.v {
			if raw == "" {
				return nil, nil, nil, a.exit("-phase needs a value")
			}
			phase := apiv1.Phase(raw)
			if !phase.Valid() {
				return nil, nil, nil, a.exit("-phase is not a session phase")
			}
			phases = append(phases, phase)
		}
	}
	var active *bool
	if a.opt.active.set {
		v := a.opt.active.v
		active = &v
	}
	sts, code := a.optString("sts-instance", a.opt.stsInstance)
	if code != 0 {
		return nil, nil, nil, code
	}
	return phases, active, sts, 0
}

func (a *App) recordQuery() (apiv1.ListSessionRecordsParams, int) {
	var p apiv1.ListSessionRecordsParams
	if a.opt.sessionID.set {
		if a.opt.sessionID.v == "" {
			return p, a.exit("-session-id needs a value")
		}
		if !sessionIDRe.MatchString(a.opt.sessionID.v) {
			return p, a.exit("session id must be 6 lowercase hex digits")
		}
		v := a.opt.sessionID.v
		p.SessionId = &v
	}
	id, code := a.optString("strategy-id", a.opt.strategyID)
	if code != 0 {
		return p, code
	}
	p.StrategyId = id
	by, code := a.optString("created-by", a.opt.createdBy)
	if code != 0 {
		return p, code
	}
	p.CreatedBy = by
	if a.opt.createdAfter.set {
		t, err := parseWhen("created-after", a.opt.createdAfter.v)
		if err != nil {
			return p, a.exit(err.Error())
		}
		p.CreatedAfter = &t
	}
	if a.opt.createdBefore.set {
		t, err := parseWhen("created-before", a.opt.createdBefore.v)
		if err != nil {
			return p, a.exit(err.Error())
		}
		p.CreatedBefore = &t
	}
	return p, 0
}

func (a *App) optString(flag string, opt stringOpt) (*string, int) {
	if !opt.set {
		return nil, 0
	}
	if opt.v == "" {
		return nil, a.exit("-" + flag + " needs a value")
	}
	v := opt.v
	return &v, 0
}

// parseWhen accepts RFC 3339, or YYYY-MM-DD at local midnight (E11-D028).
// It does not trim, and it does not accept a relative time.
func parseWhen(flag, s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, fmt.Errorf("-%s needs a time", flag)
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	if len(s) == 10 {
		if t, err := time.ParseInLocation("2006-01-02", s, time.Local); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("-%s: time must be RFC 3339 or YYYY-MM-DD", flag)
}

func sessionParams(phases []apiv1.Phase, active *bool, sts *string, limit int, cursor string) *apiv1.ListSessionsParams {
	lim := apiv1.Limit(limit)
	p := &apiv1.ListSessionsParams{Limit: &lim, Active: active, StsInstance: sts}
	if len(phases) > 0 {
		p.Phase = &phases
	}
	if cursor != "" {
		c := apiv1.Cursor(cursor)
		p.Cursor = &c
	}
	return p
}

func sessionRequestLimit(capN *int, have int) int {
	n := pageLimit
	if capN != nil {
		n = *capN - have
		if n > pageLimit {
			n = pageLimit
		}
	}
	if n < 1 {
		n = 1
	}
	return n
}

// listSessions follows pages unless cursorSet (E11-D010, E11-D011). SessionList
// carries freshness, so the merged JSON matches account list (E11-D012).
func (a *App) listSessions(api *API, phases []apiv1.Phase, active *bool, sts *string, capN *int, cursorSet bool, cursor string) int {
	if cursorSet {
		res, err := api.ListSessions(a.ctx, sessionParams(phases, active, sts, sessionRequestLimit(capN, 0), cursor))
		if code := a.checked(res, err); code != 0 {
			return code
		}
		fresh, err := freshnessOf(res.Body)
		if err != nil {
			return a.exit("response is not a session list")
		}
		if a.JSON {
			if _, err := a.Stdout.Write(a.sec.redactBytes(res.Body)); err != nil {
				return a.exit(err.Error())
			}
		} else if code := a.writeSessionRows(res.Body); code != 0 {
			return code
		}
		a.warnFreshness(fresh)
		a.noteResult(res)
		return ExitOK
	}

	var (
		raws     []json.RawMessage
		sessions []apiv1.Session
		fresh    apiv1.Freshness
		seen     = map[string]bool{}
		last     Result
		have     bool
	)
	for {
		if cursor != "" {
			if seen[cursor] {
				return a.exit("list cursor repeated")
			}
			seen[cursor] = true
		}
		res, err := api.ListSessions(a.ctx, sessionParams(phases, active, sts, sessionRequestLimit(capN, len(raws)), cursor))
		if code := a.checked(res, err); code != 0 {
			return code
		}
		last = res
		var page struct {
			Items     []json.RawMessage `json:"items"`
			Next      *string           `json:"next_cursor"`
			Freshness apiv1.Freshness   `json:"freshness"`
		}
		if err := json.Unmarshal(res.Body, &page); err != nil {
			return a.exit("response is not a session list")
		}
		take := page.Items
		if capN != nil && len(raws)+len(take) > *capN {
			take = take[:*capN-len(raws)]
		}
		for _, item := range take {
			var sess apiv1.Session
			if err := json.Unmarshal(item, &sess); err != nil {
				return a.exit("response is not a session list")
			}
			sessions = append(sessions, sess)
			raws = append(raws, item)
		}
		fresh = unionFresh(fresh, page.Freshness, have)
		have = true
		if capN != nil && len(raws) >= *capN {
			break
		}
		if page.Next == nil || *page.Next == "" || len(page.Items) == 0 {
			break
		}
		cursor = *page.Next
	}
	if a.JSON {
		raw, err := mergeAccountJSON(raws, fresh)
		if err != nil {
			return a.exit(err.Error())
		}
		if _, err := a.Stdout.Write(a.sec.redactBytes(raw)); err != nil {
			return a.exit(err.Error())
		}
	} else if code := a.writeSessionTable(sessions); code != 0 {
		return code
	}
	a.warnFreshness(fresh)
	a.noteResult(last)
	return ExitOK
}

func (a *App) writeSessionRows(raw []byte) int {
	var page struct {
		Items []apiv1.Session `json:"items"`
	}
	if err := json.Unmarshal(raw, &page); err != nil {
		return a.exit("response is not a session list")
	}
	return a.writeSessionTable(page.Items)
}

func (a *App) writeSessionTable(items []apiv1.Session) int {
	var buf strings.Builder
	header, rows := sessionListTable(items)
	if err := WriteTable(&buf, header, rows); err != nil {
		return a.exit(err.Error())
	}
	if _, err := io.WriteString(a.Stdout, a.sec.redact(buf.String())); err != nil {
		return a.exit(err.Error())
	}
	return 0
}

func sessionListTable(items []apiv1.Session) ([]string, [][]string) {
	rows := make([][]string, 0, len(items))
	for _, s := range items {
		phase, reason := "", ""
		if s.Status != nil {
			phase = string(s.Status.Phase)
			reason = s.Status.Reason
		}
		rows = append(rows, []string{
			s.Id,
			s.Spec.Name,
			phase,
			reason,
			s.StsInstance,
			s.Spec.Strategy.Name,
			stampPtr(s.CreatedAt),
			deref(s.CreatedBy),
		})
	}
	return []string{"ID", "NAME", "PHASE", "REASON", "STS_INSTANCE", "STRATEGY", "CREATED_AT", "CREATED_BY"}, rows
}

// writeSession prints a session. Table mode reads the STS instance when logs
// are present, so a last-known AgentRecord can be annotated (E11-D025). A
// failed instance read writes no stdout. JSON mode writes the session body
// and does not call GET /instances.
func (a *App) writeSession(api *API, res Result) int {
	if a.JSON {
		if code := a.writeRaw(res.Body); code != 0 {
			return code
		}
		if meta, err := readMeta(res.Body); err == nil {
			warnPartial(a.Stderr, meta.regions, meta.absent, a.sec)
		}
		a.noteResult(res)
		return ExitOK
	}
	var d apiv1.SessionDetail
	if err := json.Unmarshal(res.Body, &d); err != nil {
		return a.exit("response is not a session")
	}
	var agent *apiv1.AgentStatus
	var extra []string
	if d.Logs != nil {
		ir, err := api.GetInstance(a.ctx, d.StsInstance)
		if code := a.checked(ir, err); code != 0 {
			return code
		}
		var inst apiv1.InstanceDetail
		if err := json.Unmarshal(ir.Body, &inst); err != nil {
			return a.exit("response is not an instance")
		}
		agent = inst.Agent
		if ir.RequestID != "" {
			extra = append(extra, ir.RequestID)
		}
	}
	text, err := sessionText(d, agent)
	if err != nil {
		return a.exit(err.Error())
	}
	if _, err := io.WriteString(a.Stdout, a.sec.redact(text)); err != nil {
		return a.exit(err.Error())
	}
	if meta, err := readMeta(res.Body); err == nil {
		warnPartial(a.Stderr, meta.regions, meta.absent, a.sec)
	}
	ids := make([]string, 0, 1+len(extra))
	if res.RequestID != "" {
		ids = append(ids, res.RequestID)
	}
	ids = append(ids, extra...)
	a.note(ids)
	return ExitOK
}

func sessionText(d apiv1.SessionDetail, agent *apiv1.AgentStatus) (string, error) {
	var b strings.Builder
	b.WriteString("SPEC\n")
	// E11-D024 revision: the session stores the digest only. The table shows
	// id, name, and shortDigest. It does not look up the registry. -o json
	// writes the API body, digest included.
	if err := WriteTable(&b, []string{"STRATEGY_ID", "STRATEGY_NAME", "DIGEST", "STS_INSTANCE", "CREATED_AT", "CREATED_BY"}, [][]string{{
		deref(d.Spec.Strategy.Id),
		d.Spec.Strategy.Name,
		shortDigest(d.Spec.Strategy.Digest),
		d.StsInstance,
		stampPtr(d.CreatedAt),
		deref(d.CreatedBy),
	}}); err != nil {
		return "", err
	}
	phase, reason, restarts := "", "", ""
	if d.Status != nil {
		phase = string(d.Status.Phase)
		reason = d.Status.Reason
		restarts = strconv.Itoa(d.Status.RestartCount)
	}
	if err := writeSection(&b, "STATUS", []string{"PHASE", "REASON", "RESTART_COUNT"}, [][]string{{phase, reason, restarts}}); err != nil {
		return "", err
	}
	if err := writeSection(&b, "DEPENDENCIES", []string{"KIND", "ITEM", "PROVIDER", "STATE", "REASON", "MARK"}, depRows(d.Dependencies)); err != nil {
		return "", err
	}
	if err := writeSection(&b, "CONDITIONS", []string{"TYPE", "STATUS", "REASON", "MESSAGE"}, condRows(d.Status)); err != nil {
		return "", err
	}
	outcome, code, signal := "", "", ""
	if d.Status != nil && d.Status.LastExit != nil {
		outcome = string(d.Status.LastExit.Outcome)
		code = strconv.Itoa(d.Status.LastExit.Code)
		signal = strconv.Itoa(d.Status.LastExit.Signal)
	}
	if err := writeSection(&b, "EXIT", []string{"OUTCOME", "CODE", "SIGNAL"}, [][]string{{outcome, code, signal}}); err != nil {
		return "", err
	}
	host, stdout, stderr, note := "", "", "", ""
	if d.Logs != nil {
		host = d.Logs.Host
		// E11-D025: <sts_instance> <host>:<path>
		stdout = d.StsInstance + " " + host + ":" + d.Logs.Stdout
		stderr = d.StsInstance + " " + host + ":" + d.Logs.Stderr
		if agent != nil && agent.LastKnown != nil && *agent.LastKnown {
			note = lastKnownNote
		}
	}
	if err := writeSection(&b, "LOGS", []string{"HOST", "STDOUT", "STDERR", "NOTE"}, [][]string{{host, stdout, stderr, note}}); err != nil {
		return "", err
	}
	return b.String(), nil
}

func depRows(deps []apiv1.Dependency) [][]string {
	rows := make([][]string, 0, len(deps))
	for _, d := range deps {
		item := deref(d.Feed)
		if item == "" && d.ApiId != nil {
			item = strconv.FormatInt(*d.ApiId, 10)
		}
		mark := ""
		switch d.State {
		case apiv1.EntryStateRejected:
			mark = "REJECTED"
		case apiv1.EntryStateDead:
			mark = "DEAD"
		}
		rows = append(rows, []string{string(d.Kind), item, d.ProviderInstance, string(d.State), deref(d.Reason), mark})
	}
	return rows
}

func condRows(st *apiv1.SessionStatus) [][]string {
	if st == nil {
		return nil
	}
	var rows [][]string
	for _, c := range st.Conditions {
		if c.Type != "dependency" {
			continue
		}
		rows = append(rows, []string{c.Type, strconv.FormatBool(c.Status), c.Reason, c.Message})
	}
	return rows
}

func stampPtr(t *time.Time) string {
	if t == nil {
		return ""
	}
	return localStamp(*t)
}

func recordsText(items []json.RawMessage) (string, error) {
	recs := make([]apiv1.SessionRecord, 0, len(items))
	for _, raw := range items {
		var rec apiv1.SessionRecord
		if err := json.Unmarshal(raw, &rec); err != nil {
			return "", errNotList
		}
		recs = append(recs, rec)
	}
	var b strings.Builder
	header, rows := recordListTable(recs)
	if err := WriteTable(&b, header, rows); err != nil {
		return "", err
	}
	return b.String(), nil
}

func recordListTable(items []apiv1.SessionRecord) ([]string, [][]string) {
	rows := make([][]string, 0, len(items))
	for _, r := range items {
		rows = append(rows, []string{
			r.RecordId.String(),
			r.SessionId,
			string(r.State),
			r.Spec.Strategy.Name,
			r.StsInstance,
			localStamp(r.CreatedAt),
			deref(r.CreatedBy),
		})
	}
	return []string{"RECORD_ID", "SESSION_ID", "STATE", "STRATEGY", "STS_INSTANCE", "CREATED_AT", "CREATED_BY"}, rows
}

func recordText(body []byte) (string, error) {
	var r apiv1.SessionRecord
	if err := json.Unmarshal(body, &r); err != nil {
		return "", fmt.Errorf("response is not a record")
	}
	var b strings.Builder
	b.WriteString("RECORD\n")
	closed := ""
	if r.ClosedAt != nil {
		closed = localStamp(*r.ClosedAt)
	}
	if err := WriteTable(&b, []string{"RECORD_ID", "SESSION_ID", "STATE", "STS_INSTANCE", "CREATED_AT", "CREATED_BY", "CLOSED_AT"}, [][]string{{
		r.RecordId.String(),
		r.SessionId,
		string(r.State),
		r.StsInstance,
		localStamp(r.CreatedAt),
		deref(r.CreatedBy),
		closed,
	}}); err != nil {
		return "", err
	}
	if err := writeSection(&b, "SPEC", []string{"STRATEGY_ID", "STRATEGY_NAME", "DIGEST"}, [][]string{{
		deref(r.Spec.Strategy.Id),
		r.Spec.Strategy.Name,
		r.Spec.Strategy.Digest,
	}}); err != nil {
		return "", err
	}
	phase, reason, outcome, code, signal := "", "", "", "", ""
	if r.Final != nil {
		phase = string(r.Final.Phase)
		reason = r.Final.Reason
		if r.Final.LastExit != nil {
			outcome = string(r.Final.LastExit.Outcome)
			code = strconv.Itoa(r.Final.LastExit.Code)
			signal = strconv.Itoa(r.Final.LastExit.Signal)
		}
	}
	if err := writeSection(&b, "FINAL", []string{"PHASE", "REASON", "OUTCOME", "CODE", "SIGNAL"}, [][]string{{phase, reason, outcome, code, signal}}); err != nil {
		return "", err
	}
	return b.String(), nil
}
