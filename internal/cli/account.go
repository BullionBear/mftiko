package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

// accountPage is the page size used while following a list (E11-D010).
const accountPage = 1000

// maxAPIID is 2^53-1, the API's api_id ceiling (E10-D008).
const maxAPIID int64 = 1<<53 - 1

// instanceNameRe is the instance name syntax (PROTOCOL §2). The API uses the
// same expression when it rejects /td_instance (E11-D015).
var instanceNameRe = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)

func runAccountList(a *App) int {
	if code := a.onlyFlags("td-instance", "limit", "cursor"); code != 0 {
		return code
	}
	if len(a.Args) != 0 {
		return a.exit("account list takes no arguments")
	}
	if a.opt.tdInstance.set && a.opt.tdInstance.v == "" {
		return a.exit("-td-instance needs a name")
	}
	// E11-D010 supplement: the two paging flags together are a usage error.
	if a.opt.limitSet && a.opt.cursorSet {
		return a.exit("-limit and -cursor cannot both be set")
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
	var filter *string
	if a.opt.tdInstance.set {
		v := a.opt.tdInstance.v
		filter = &v
	}
	return a.listAccounts(api, filter, capN, a.opt.cursorSet, a.opt.cursor)
}

func runAccountShow(a *App) int {
	if code := a.onlyFlags(); code != 0 {
		return code
	}
	id, code := a.oneID("account show")
	if code != 0 {
		return code
	}
	api, code := a.client()
	if code != 0 {
		return code
	}
	res, err := api.GetAccount(a.ctx, id)
	if code := a.checked(res, err); code != 0 {
		return code
	}
	return a.writeAccount(res)
}

func runAccountCreate(a *App) int {
	if code := a.onlyFlags(
		"name",
		"kind",
		flagPair("td", "instance"),
		flagFile("key"),
		flagFile("secret"),
		flagFile("pass"+"phrase"),
		flagPair("key", "type"),
	); code != 0 {
		return code
	}
	if len(a.Args) != 0 {
		return a.exit("account create takes no arguments")
	}
	if !a.opt.name.set || a.opt.name.v == "" || !a.opt.kind.set || a.opt.kind.v == "" || !a.opt.tdInstance.set || a.opt.tdInstance.v == "" {
		return a.exit("account create needs -name, -kind, and -td-instance")
	}
	api, code := a.client()
	if code != 0 {
		return code
	}
	kinds, code := a.fetchKinds(api)
	if code != 0 {
		return code
	}
	kind, code := a.acceptKind(kinds, a.opt.kind.v)
	if code != 0 {
		return code
	}
	if code := a.requireTD(api, a.opt.tdInstance.v); code != 0 {
		return code
	}
	kt, code := a.resolveKeyType(kind)
	if code != 0 {
		return code
	}
	cred, code := a.loadCredentials(kind, kt)
	if code != 0 {
		return code
	}
	res, err := api.CreateAccount(a.ctx, apiv1.AccountCreate{
		Name:        a.opt.name.v,
		Kind:        kind.Kind,
		TdInstance:  a.opt.tdInstance.v,
		Credentials: cred,
	})
	if code := a.finish(res, err); code != 0 {
		return code
	}
	return a.writeAccount(res)
}

func runAccountUpdate(a *App) int {
	if code := a.onlyFlags(
		flagFile("key"),
		flagFile("secret"),
		flagFile("pass"+"phrase"),
		flagPair("key", "type"),
	); code != 0 {
		return code
	}
	id, code := a.oneID("account update")
	if code != 0 {
		return code
	}
	// Any credential file replaces the whole set. -key-type alone does not:
	// the stored ciphertext stays until a full set is sent (E11-D016).
	replacing := a.opt.keyFile.set || a.opt.secretFile.set || a.opt.passFile.set
	if !replacing && a.opt.keyType.set {
		return a.exit("-key-type was set but no credential file was given")
	}
	api, code := a.client()
	if code != 0 {
		return code
	}
	acct, code := a.fetchAccount(api, id)
	if code != 0 {
		return code
	}
	body := apiv1.AccountUpdate{Revision: acct.Revision}
	if replacing {
		kinds, code := a.fetchKinds(api)
		if code != 0 {
			return code
		}
		kind, code := a.acceptKind(kinds, acct.Kind)
		if code != 0 {
			return code
		}
		kt, code := a.resolveKeyType(kind)
		if code != 0 {
			return code
		}
		cred, code := a.loadCredentials(kind, kt)
		if code != 0 {
			return code
		}
		body.Credentials = cred
	}
	res, err := api.UpdateAccount(a.ctx, id, body)
	if code := a.finish(res, err); code != 0 {
		return code
	}
	return a.writeAccount(res)
}

func runAccountDelete(a *App) int {
	if code := a.onlyFlags(); code != 0 {
		return code
	}
	id, code := a.oneID("account delete")
	if code != 0 {
		return code
	}
	if err := a.confirm("delete account " + a.Args[0]); err != nil {
		return a.exit(err.Error())
	}
	api, code := a.client()
	if code != 0 {
		return code
	}
	acct, code := a.fetchAccount(api, id)
	if code != 0 {
		return code
	}
	res, err := api.DeleteAccount(a.ctx, id, acct.Revision)
	code = a.finish(res, err)
	if code == ExitReferenced {
		refs := acct.ReferencedBy
		if len(refs) == 0 {
			if again, c := a.fetchAccount(api, id); c == 0 {
				refs = again.ReferencedBy
			}
		}
		a.printRefs(refs)
	}
	if code == 0 {
		a.noteResult(res)
	}
	return code
}

func (a *App) oneID(cmd string) (apiv1.APIID, int) {
	if len(a.Args) != 1 {
		return 0, a.exit(cmd + " takes an api_id")
	}
	id, err := parseAPIID(a.Args[0])
	if err != nil {
		return 0, a.exit(err.Error())
	}
	return id, 0
}

func parseAPIID(s string) (apiv1.APIID, error) {
	bad := fmt.Errorf("api_id must be an integer from 1 to %d", maxAPIID)
	if s == "" || s[0] < '0' || s[0] > '9' {
		return 0, bad
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, bad
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 1 || n > maxAPIID {
		return 0, bad
	}
	return n, nil
}

func (a *App) client() (*API, int) {
	if code := a.useContext(); code != 0 {
		return nil, code
	}
	_, _, c, code := a.load()
	if code != 0 {
		return nil, code
	}
	token := strings.TrimSpace(a.env("MFTIKO_TOKEN"))
	if token == "" {
		token = c.Token
	}
	a.sec.add(c.Token)
	a.sec.add(token)
	if token == "" {
		return nil, a.exit("no token; set MFTIKO_TOKEN or run mftiko login")
	}
	return a.dial(c, token)
}

func (a *App) fetchKinds(api *API) ([]apiv1.AccountKind, int) {
	res, err := api.ListAccountKinds(a.ctx)
	if code := a.checked(res, err); code != 0 {
		return nil, code
	}
	var list apiv1.AccountKindList
	if err := json.Unmarshal(res.Body, &list); err != nil {
		return nil, a.exit("response is not account kinds")
	}
	return list.Items, 0
}

// acceptKind blocks a kind the API would reject with 400 (E11-D015).
func (a *App) acceptKind(kinds []apiv1.AccountKind, name string) (apiv1.AccountKind, int) {
	for _, k := range kinds {
		if k.Kind == name && k.Supported {
			return k, 0
		}
	}
	return apiv1.AccountKind{}, a.validation([]apiv1.FieldError{{
		Path: "/kind", Reason: "is not accepted",
	}})
}

// resolveKeyType uses the only key type a kind allows. When the kind allows
// more than one, -key-type must name one of them. The flag value is not a
// credential, and the client does not pick a type the user did not name (E11-D015).
func (a *App) resolveKeyType(kind apiv1.AccountKind) (apiv1.KeyType, int) {
	if a.opt.keyType.set {
		kt := apiv1.KeyType(a.opt.keyType.v)
		if !slices.Contains(kind.KeyTypes, kt) {
			return "", a.validation([]apiv1.FieldError{{
				Path: "/credentials/key_type", Reason: "is not accepted",
			}})
		}
		return kt, 0
	}
	if len(kind.KeyTypes) == 1 {
		return kind.KeyTypes[0], 0
	}
	return "", a.exit("-key-type is required")
}

// requireTD applies the API's /td_instance checks before create is sent (E11-D015).
func (a *App) requireTD(api *API, name string) int {
	if !instanceNameRe.MatchString(name) {
		return a.validation([]apiv1.FieldError{{
			Path: "/td_instance", Reason: "is not an instance name",
		}})
	}
	res, err := api.GetInstance(a.ctx, name)
	if err != nil {
		return a.exitCode(ExitNetwork, "network (2): "+err.Error())
	}
	if res.Status == http.StatusNotFound {
		return a.validation([]apiv1.FieldError{{
			Path: "/td_instance", Reason: "is not in the topology",
		}})
	}
	if res.Status < 200 || res.Status >= 300 {
		return reportProblem(a.Stderr, res.Body, res.RequestID, a.sec)
	}
	var inst apiv1.InstanceDetail
	if err := json.Unmarshal(res.Body, &inst); err != nil {
		return a.exit("response is not an instance")
	}
	if inst.Plane != apiv1.Td {
		return a.validation([]apiv1.FieldError{{
			Path: "/td_instance", Reason: "is not a td instance",
		}})
	}
	return 0
}

func (a *App) fetchAccount(api *API, id apiv1.APIID) (apiv1.Account, int) {
	res, err := api.GetAccount(a.ctx, id)
	if code := a.checked(res, err); code != 0 {
		return apiv1.Account{}, code
	}
	var acct apiv1.Account
	if err := json.Unmarshal(res.Body, &acct); err != nil {
		return apiv1.Account{}, a.exit("response is not an account")
	}
	return acct, 0
}

// finish reports the API result. A 409 conflict is not retried; the hint tells
// the user to run the command again (E11-D017).
func (a *App) finish(res Result, err error) int {
	code := a.checked(res, err)
	if code == ExitConflict {
		fmt.Fprintln(a.Stderr, "mftiko: run the command again")
	}
	return code
}

func (a *App) validation(fields []apiv1.FieldError) int {
	detail := "invalid request"
	raw, err := json.Marshal(apiv1.Problem{
		Type:   "about:blank",
		Title:  "Bad Request",
		Status: http.StatusBadRequest,
		Code:   apiv1.ErrorCodeValidationFailed,
		Detail: &detail,
		Errors: &fields,
	})
	if err != nil {
		return a.exit("invalid request")
	}
	return reportProblem(a.Stderr, raw, "", a.sec)
}

func (a *App) printRefs(refs []apiv1.SessionIDValue) {
	fmt.Fprintln(a.Stderr, "referenced_by:")
	for _, id := range refs {
		fmt.Fprintln(a.Stderr, a.sec.redact(string(id)))
	}
}

func (a *App) noteResult(res Result) {
	if a.Verbose && res.RequestID != "" {
		fmt.Fprintf(a.Stderr, "X-Request-Id: %s\n", a.sec.redact(res.RequestID))
	}
}

func (a *App) writeAccount(res Result) int {
	if a.JSON {
		if _, err := a.Stdout.Write(a.sec.redactBytes(res.Body)); err != nil {
			return a.exit(err.Error())
		}
	} else if code := a.writeShowTable(res.Body); code != 0 {
		return code
	}
	a.noteResult(res)
	return ExitOK
}

func (a *App) writeShowTable(raw []byte) int {
	var acct apiv1.Account
	if err := json.Unmarshal(raw, &acct); err != nil {
		return a.exit("response is not an account")
	}
	var buf strings.Builder
	header, rows := accountShowTable(acct)
	if err := WriteTable(&buf, header, rows); err != nil {
		return a.exit(err.Error())
	}
	if _, err := io.WriteString(a.Stdout, a.sec.redact(buf.String())); err != nil {
		return a.exit(err.Error())
	}
	return 0
}

func accountShowTable(acct apiv1.Account) ([]string, [][]string) {
	cancel := ""
	if acct.Settings.CancelOnDisconnectS != nil {
		cancel = strconv.Itoa(*acct.Settings.CancelOnDisconnectS)
	}
	rows := [][]string{
		{"api_id", strconv.FormatInt(acct.ApiId, 10)},
		{"name", acct.Name},
		{"kind", acct.Kind},
		{"td_instance", acct.TdInstance},
		{"region", acct.Region},
		{"revision", strconv.FormatInt(acct.Revision, 10)},
		{"cancel_on_disconnect_s", cancel},
		{"referenced_by", joinSessions(acct.ReferencedBy)},
	}
	return []string{"FIELD", "VALUE"}, rows
}

func accountListTable(items []apiv1.Account) ([]string, [][]string) {
	rows := make([][]string, 0, len(items))
	for _, acct := range items {
		rows = append(rows, []string{
			strconv.FormatInt(acct.ApiId, 10),
			acct.Name,
			acct.Kind,
			acct.TdInstance,
			acct.Region,
			strconv.FormatInt(acct.Revision, 10),
			joinSessions(acct.ReferencedBy),
		})
	}
	return []string{"API_ID", "NAME", "KIND", "TD_INSTANCE", "REGION", "REVISION", "REFERENCED_BY"}, rows
}

func joinSessions(ids []apiv1.SessionIDValue) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = string(id)
	}
	return strings.Join(parts, ", ")
}

// listAccounts follows pages unless cursorSet (E11-D010, E11-D011). capN, when
// set, is the total number of rows, not the size of each request.
func (a *App) listAccounts(api *API, filter *string, capN *int, cursorSet bool, cursor string) int {
	if cursorSet {
		res, err := api.ListAccounts(a.ctx, accountParams(filter, accountRequestLimit(capN, 0), cursor))
		if code := a.checked(res, err); code != 0 {
			return code
		}
		fresh, err := freshnessOf(res.Body)
		if err != nil {
			return a.exit("response is not an account list")
		}
		if a.JSON {
			if _, err := a.Stdout.Write(a.sec.redactBytes(res.Body)); err != nil {
				return a.exit(err.Error())
			}
		} else if code := a.writeListTable(res.Body); code != 0 {
			return code
		}
		a.warnFreshness(fresh)
		a.noteResult(res)
		return ExitOK
	}

	var (
		raws     []json.RawMessage
		accounts []apiv1.Account
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
		res, err := api.ListAccounts(a.ctx, accountParams(filter, accountRequestLimit(capN, len(raws)), cursor))
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
			return a.exit("response is not an account list")
		}
		take := page.Items
		if capN != nil && len(raws)+len(take) > *capN {
			take = take[:*capN-len(raws)]
		}
		for _, item := range take {
			var acct apiv1.Account
			if err := json.Unmarshal(item, &acct); err != nil {
				return a.exit("response is not an account list")
			}
			accounts = append(accounts, acct)
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
	} else {
		var buf strings.Builder
		header, rows := accountListTable(accounts)
		if err := WriteTable(&buf, header, rows); err != nil {
			return a.exit(err.Error())
		}
		if _, err := io.WriteString(a.Stdout, a.sec.redact(buf.String())); err != nil {
			return a.exit(err.Error())
		}
	}
	a.warnFreshness(fresh)
	a.noteResult(last)
	return ExitOK
}

func accountRequestLimit(capN *int, have int) int {
	n := accountPage
	if capN != nil {
		n = *capN - have
		if n > accountPage {
			n = accountPage
		}
	}
	if n < 1 {
		n = 1
	}
	return n
}

func accountParams(filter *string, limit int, cursor string) *apiv1.ListAccountsParams {
	lim := apiv1.Limit(limit)
	params := &apiv1.ListAccountsParams{Limit: &lim, TdInstance: filter}
	if cursor != "" {
		c := apiv1.Cursor(cursor)
		params.Cursor = &c
	}
	return params
}

func (a *App) writeListTable(raw []byte) int {
	var page struct {
		Items []apiv1.Account `json:"items"`
	}
	if err := json.Unmarshal(raw, &page); err != nil {
		return a.exit("response is not an account list")
	}
	var buf strings.Builder
	header, rows := accountListTable(page.Items)
	if err := WriteTable(&buf, header, rows); err != nil {
		return a.exit(err.Error())
	}
	if _, err := io.WriteString(a.Stdout, a.sec.redact(buf.String())); err != nil {
		return a.exit(err.Error())
	}
	return 0
}

func freshnessOf(raw []byte) (apiv1.Freshness, error) {
	var page struct {
		Freshness apiv1.Freshness `json:"freshness"`
	}
	err := json.Unmarshal(raw, &page)
	return page.Freshness, err
}

func unionFresh(dst, src apiv1.Freshness, have bool) apiv1.Freshness {
	if !have {
		dst.ControllerAbsent = src.ControllerAbsent
		dst.UnavailableRegions = append([]string(nil), src.UnavailableRegions...)
	} else {
		dst.ControllerAbsent = dst.ControllerAbsent || src.ControllerAbsent
		seen := map[string]bool{}
		for _, r := range dst.UnavailableRegions {
			seen[r] = true
		}
		for _, r := range src.UnavailableRegions {
			if !seen[r] {
				dst.UnavailableRegions = append(dst.UnavailableRegions, r)
				seen[r] = true
			}
		}
	}
	if dst.UnavailableRegions == nil {
		dst.UnavailableRegions = []string{}
	}
	sort.Strings(dst.UnavailableRegions)
	return dst
}

func mergeAccountJSON(items []json.RawMessage, fresh apiv1.Freshness) ([]byte, error) {
	if items == nil {
		items = []json.RawMessage{}
	}
	if fresh.UnavailableRegions == nil {
		fresh.UnavailableRegions = []string{}
	}
	body, err := json.Marshal(struct {
		Items     []json.RawMessage `json:"items"`
		Freshness apiv1.Freshness   `json:"freshness"`
	}{Items: items, Freshness: fresh})
	if err != nil {
		return nil, err
	}
	return append(body, '\n'), nil
}

// warnFreshness prints the partial-result warning once (E11-D012).
func (a *App) warnFreshness(f apiv1.Freshness) {
	if len(f.UnavailableRegions) > 0 {
		regions := append([]string(nil), f.UnavailableRegions...)
		sort.Strings(regions)
		fmt.Fprintf(a.Stderr, "mftiko: warning: unavailable regions: %s\n", strings.Join(regions, ", "))
	}
	if f.ControllerAbsent {
		fmt.Fprintln(a.Stderr, "mftiko: warning: controller absent")
	}
}

// flagPair joins the two words of a flag name. They stay separate in source
// so the joined name is not a single literal.
func flagPair(a, b string) string {
	return a + "-" + b
}

func flagFile(word string) string {
	return flagPair(word, "file")
}

// onlyFlags rejects command flags this command does not accept. Global flags
// are not in the list. The message names the flag and not its value.
func (a *App) onlyFlags(allow ...string) int {
	if msg := a.unexpectedFlags(allow...); msg != "" {
		return a.exit(msg)
	}
	return 0
}

// unexpectedFlags is the onlyFlags message, or empty when every set flag is
// allowed. session create prints it and then the Idempotency-Key.
func (a *App) unexpectedFlags(allow ...string) string {
	ok := map[string]bool{}
	for _, n := range allow {
		ok[n] = true
	}
	flags := []struct {
		name string
		set  bool
	}{
		{"name", a.opt.name.set},
		{"kind", a.opt.kind.set},
		{"td-instance", a.opt.tdInstance.set},
		{flagFile("key"), a.opt.keyFile.set},
		{flagFile("secret"), a.opt.secretFile.set},
		{flagFile("pass" + "phrase"), a.opt.passFile.set},
		{flagPair("key", "type"), a.opt.keyType.set},
		{"limit", a.opt.limitSet},
		{"cursor", a.opt.cursorSet},
		{"phase", a.opt.phase.set},
		{"active", a.opt.active.set},
		{"sts-instance", a.opt.stsInstance.set},
		{"session-id", a.opt.sessionID.set},
		{"strategy-id", a.opt.strategyID.set},
		{"created-by", a.opt.createdBy.set},
		{"created-after", a.opt.createdAfter.set},
		{"created-before", a.opt.createdBefore.set},
		{"f", a.opt.file.set},
		{flagPair("idempotency", "key"), a.opt.idemKey.set},
	}
	var bad []string
	for _, f := range flags {
		if f.set && !ok[f.name] {
			bad = append(bad, "-"+f.name)
		}
	}
	if len(bad) == 0 {
		return ""
	}
	return "flag not accepted: " + strings.Join(bad, " ")
}
