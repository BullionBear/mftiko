package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// instanceNameRe is the instance name syntax (PROTOCOL §2). state keeps its copy unexported.
var instanceNameRe = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)

var (
	errNameTaken  = errors.New("api: name is already in use")
	errReferenced = errors.New("api: account is referenced")
	errUnreadable = errors.New("api: stored record is unreadable")
)

func (s *Server) ListAccountKinds(w http.ResponseWriter, _ *http.Request) {
	items := make([]apiv1.AccountKind, 0, len(s.cfg.Kinds))
	for _, k := range s.cfg.Kinds {
		types := make([]apiv1.KeyType, len(k.KeyTypes))
		for i, t := range k.KeyTypes {
			types[i] = apiv1.KeyType(t)
		}
		if types == nil {
			types = []apiv1.KeyType{}
		}
		trades := append([]string(nil), k.Trades...)
		if trades == nil {
			trades = []string{}
		}
		items = append(items, apiv1.AccountKind{
			Kind: k.Name, Label: k.Label, Trades: trades, KeyTypes: types,
			Passphrase: k.Passphrase, Supported: k.Supported(),
		})
	}
	slices.SortFunc(items, func(a, b apiv1.AccountKind) int { return strings.Compare(a.Kind, b.Kind) })
	writeJSON(w, http.StatusOK, apiv1.AccountKindList{Items: items})
}

func (s *Server) ListAccounts(w http.ResponseWriter, r *http.Request, params apiv1.ListAccountsParams) {
	limit, after, fields := bindPage(params.Limit, params.Cursor)
	var afterID int64
	if after != "" && len(fields) == 0 {
		id, err := strconv.ParseInt(after, 10, 64)
		if err != nil || strconv.FormatInt(id, 10) != after {
			fields = append(fields, apiv1.FieldError{Path: "/cursor", Reason: "is not a cursor from this API"})
		} else {
			afterID = id
		}
	}
	if r.URL.Query().Has("td_instance") {
		fields = append(fields, apiv1.FieldError{Path: "/td_instance", Reason: "is not accepted"})
	}
	if len(fields) > 0 {
		writeFields(w, fields)
		return
	}
	views, unavail := s.regionViews()
	fresh := s.freshness(views, unavail)
	rows, err := s.listAccounts(r.Context(), flagBool(params.IncludeDeleted), afterID, limit+1)
	if err != nil {
		s.writeAccountErr(w, err)
		return
	}
	refs := referencesOf(views)
	var next *string
	if len(rows) > limit {
		rows = rows[:limit]
		c := base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(rows[len(rows)-1].ID, 10)))
		next = &c
	}
	items := make([]apiv1.Account, 0, len(rows))
	for _, row := range rows {
		items = append(items, accountJSON(row, refs[row.ID]))
	}
	writeJSON(w, http.StatusOK, apiv1.AccountList{Items: items, NextCursor: next, Freshness: fresh})
}

func (s *Server) CreateAccount(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeFields(w, nil)
		return
	}
	if fields := rejectTDInstance(raw); len(fields) > 0 {
		writeFields(w, fields)
		return
	}
	var body apiv1.AccountCreate
	if err := json.Unmarshal(raw, &body); err != nil {
		writeFields(w, nil)
		return
	}
	fields := checkName(body.Name)
	uid, uidFields := checkUID(body.ExchAccountUid, true)
	fields = append(fields, uidFields...)
	kind, accepted := s.acceptedKind(body.Kind)
	if !accepted {
		fields = append(fields, apiv1.FieldError{Path: "/kind", Reason: "is not accepted"})
	}
	fields = append(fields, checkCredentials(body.Credentials, kind, true, accepted)...)
	fields = append(fields, checkSettings(body.Settings)...)
	if len(fields) > 0 {
		writeFields(w, fields)
		return
	}
	taken, err := s.liveNameTaken(r.Context(), body.Name, 0)
	if err != nil {
		s.writeAccountErr(w, err)
		return
	}
	if taken {
		s.writeAccountErr(w, errNameTaken)
		return
	}
	settings := mergeSettings(body.Settings)
	now := s.now().UTC()
	// One reader for the whole allocation, same as createSession. A fresh reader on
	// every draw would repeat the first id (E21-D030).
	src := s.ids()
	var created accountPublic
	_, err = allocate(idAttempts,
		func() (int64, error) { return newAPIID(src) },
		func(int64) bool { return false },
		func(id int64) error {
			ct, ver, err := s.sealAEAD(id, kind.Name, body.Credentials)
			if err != nil {
				return err
			}
			row, err := s.insertAccount(r.Context(), accountInsert{
				ID: id, Kind: kind.Name, Name: body.Name, Credentials: ct, KeyVersion: ver,
				Cancel: settings.GetCancelOnDisconnectS(), UID: uid, At: now,
			})
			if err != nil {
				clear(ct)
				return err
			}
			clear(ct)
			created = row
			return nil
		})
	if err != nil {
		s.writeAccountErr(w, err)
		return
	}
	created.Exchange = kind.Exchange
	w.Header().Set("Location", "/api/v1/accounts/"+strconv.FormatInt(created.ID, 10))
	writeJSON(w, http.StatusCreated, accountJSON(created, nil))
}

func (s *Server) GetAccount(w http.ResponseWriter, r *http.Request, apiID apiv1.APIID, params apiv1.GetAccountParams) {
	row, err := s.getAccount(r.Context(), int64(apiID))
	if err != nil {
		s.writeAccountErr(w, err)
		return
	}
	if row.Deleted != nil && !flagBool(params.IncludeDeleted) {
		s.writeAccountErr(w, errNotFound)
		return
	}
	views, _ := s.regionViews()
	writeJSON(w, http.StatusOK, accountJSON(row, referencesOf(views)[row.ID]))
}

func (s *Server) UpdateAccount(w http.ResponseWriter, r *http.Request, apiID apiv1.APIID) {
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeFields(w, nil)
		return
	}
	if fields := rejectTDInstance(raw); len(fields) > 0 {
		writeFields(w, fields)
		return
	}
	var body apiv1.AccountUpdate
	if err := json.Unmarshal(raw, &body); err != nil {
		writeFields(w, nil)
		return
	}
	if body.Revision < 1 {
		writeFields(w, []apiv1.FieldError{{Path: "/revision", Reason: "must be at least 1"}})
		return
	}
	cur, err := s.getAccount(r.Context(), int64(apiID))
	if err != nil {
		s.writeAccountErr(w, err)
		return
	}
	if cur.Deleted != nil {
		s.writeAccountErr(w, errNotFound)
		return
	}
	kind, kindOK := s.kindByName(cur.Kind)
	accepted := kindOK && kind.Supported() && kind.Exchange != ""
	if body.Credentials != nil && !accepted {
		writeFields(w, []apiv1.FieldError{{Path: "/kind", Reason: "is not accepted"}})
		return
	}
	var fields []apiv1.FieldError
	name := cur.Name
	if body.Name != nil {
		fields = append(fields, checkName(*body.Name)...)
		name = *body.Name
	}
	if body.ExchAccountUid != nil {
		uid, uidFields := checkUID(*body.ExchAccountUid, true)
		fields = append(fields, uidFields...)
		if len(uidFields) == 0 && uid != cur.UID {
			fields = append(fields, apiv1.FieldError{Path: "/exch_account_uid", Reason: "cannot be changed"})
		}
	}
	fields = append(fields, checkCredentials(body.Credentials, kind, false, accepted)...)
	fields = append(fields, checkSettings(body.Settings)...)
	if len(fields) > 0 {
		writeFields(w, fields)
		return
	}
	reps, err := s.listReplicas(r.Context(), cur.ID)
	if err != nil {
		s.writeAccountErr(w, err)
		return
	}
	if err := s.replicasReachable(reps); err != nil {
		s.writeAccountErr(w, err)
		return
	}
	if body.Credentials != nil {
		for _, rep := range reps {
			if _, err := s.pubkeyFor(r.Context(), rep.Inst); err != nil {
				s.writeAccountErr(w, err)
				return
			}
		}
	}
	if name != cur.Name {
		taken, err := s.liveNameTaken(r.Context(), name, cur.ID)
		if err != nil {
			s.writeAccountErr(w, err)
			return
		}
		if taken {
			s.writeAccountErr(w, errNameTaken)
			return
		}
	}
	cancel := cur.Cancel
	if body.Settings != nil && body.Settings.CancelOnDisconnectS != nil {
		cancel = uint32(*body.Settings.CancelOnDisconnectS)
	}
	next := accountChange{Name: name, Cancel: cancel, At: s.now().UTC()}
	if body.Credentials != nil {
		ct, ver, err := s.sealAEAD(cur.ID, cur.Kind, body.Credentials)
		if err != nil {
			s.writeAccountErr(w, err)
			return
		}
		next.Creds = &accountCipher{AEAD: ct, Version: ver}
		next.Plain = body.Credentials
	}
	stored, written, changed, err := s.mutateAccount(r.Context(), cur.ID, int64(body.Revision), next)
	if next.Creds != nil {
		clear(next.Creds.AEAD)
	}
	if err != nil {
		s.writeAccountErr(w, err)
		return
	}
	if changed {
		if err := s.publishReplicas(r.Context(), stored, written, false); err != nil {
			s.writeAccountErr(w, err)
			return
		}
	}
	s.decorate(&stored)
	views, _ := s.regionViews()
	writeJSON(w, http.StatusOK, accountJSON(stored, referencesOf(views)[stored.ID]))
}

func (s *Server) DeleteAccount(w http.ResponseWriter, r *http.Request, apiID apiv1.APIID, params apiv1.DeleteAccountParams) {
	if params.Revision < 1 {
		writeFields(w, []apiv1.FieldError{{Path: "/revision", Reason: "must be at least 1"}})
		return
	}
	cur, err := s.getAccount(r.Context(), int64(apiID))
	if err != nil {
		s.writeAccountErr(w, err)
		return
	}
	if cur.Deleted != nil {
		s.writeAccountErr(w, errNotFound)
		return
	}
	views, _ := s.regionViews()
	if len(referencesOf(views)[cur.ID]) > 0 {
		s.writeAccountErr(w, errReferenced)
		return
	}
	reps, err := s.listReplicas(r.Context(), cur.ID)
	if err != nil {
		s.writeAccountErr(w, err)
		return
	}
	if err := s.replicasReachable(reps); err != nil {
		s.writeAccountErr(w, err)
		return
	}
	at := s.now().UTC()
	stored, err := s.softDeleteAccount(r.Context(), cur.ID, int64(params.Revision), at)
	if err != nil {
		s.writeAccountErr(w, err)
		return
	}
	if err := s.publishReplicas(r.Context(), stored, reps, true); err != nil {
		s.writeAccountErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) writeAccountErr(w http.ResponseWriter, err error) {
	var unavailable *pubkeyUnavailable
	switch {
	case errors.As(err, &unavailable):
		writeProblem(w, http.StatusConflict, apiv1.ErrorCodeTdPubkeyUnavailable, http.StatusText(http.StatusConflict), unavailable.Error(), nil)
	case errors.Is(err, errNameTaken):
		writeProblem(w, http.StatusConflict, apiv1.ErrorCodeConflict, http.StatusText(http.StatusConflict), "name is already in use", []apiv1.FieldError{{
			Path: "/name", Reason: "already in use",
		}})
	case errors.Is(err, errReferenced):
		writeProblem(w, http.StatusConflict, apiv1.ErrorCodeReferenced, http.StatusText(http.StatusConflict), "account is referenced by a session", nil)
	case errors.Is(err, errStorage):
		writeProblem(w, http.StatusServiceUnavailable, apiv1.ErrorCodeStorageUnavailable, "Storage Unavailable", "storage is unavailable", nil)
	default:
		status, name := problemOf(err)
		switch name {
		case "conflict":
			writeProblem(w, status, apiv1.ErrorCodeConflict, http.StatusText(status), "revision conflict", []apiv1.FieldError{{
				Path: "/revision", Reason: "does not match",
			}})
		case "region_unavailable":
			writeProblem(w, status, apiv1.ErrorCodeRegionUnavailable, http.StatusText(status), "region unavailable", nil)
		case "not_found":
			writeProblem(w, status, apiv1.ErrorCodeNotFound, http.StatusText(status), "account not found", nil)
		default:
			// sealbox, accountkey, and etcd errors do not quote credential bytes. A stored record
			// that does not unmarshal is reported as errUnreadable, which does not carry the bytes either.
			s.log.Error().Err(err).Msg("account")
			writeProblem(w, status, apiv1.ErrorCodeInternal, http.StatusText(status), "internal error", nil)
		}
	}
}

func writeFields(w http.ResponseWriter, fields []apiv1.FieldError) {
	writeProblem(w, http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, http.StatusText(http.StatusBadRequest), "invalid request", fields)
}

// decodeBody reads one JSON value. The error text can quote the body, so callers do not log it or
// return it (E10-D064).
func decodeBody(r *http.Request, out any) error {
	return json.NewDecoder(r.Body).Decode(out)
}

func checkName(name string) []apiv1.FieldError {
	n := utf8.RuneCountInString(name)
	if n < 1 || n > 128 {
		return []apiv1.FieldError{{Path: "/name", Reason: "must be 1 to 128 characters"}}
	}
	return nil
}

// checkUID applies E21-D025. required is create, and an update that sends the field.
// The stored value is the trimmed string.
func checkUID(uid string, required bool) (string, []apiv1.FieldError) {
	trimmed := strings.TrimSpace(uid)
	if trimmed == "" {
		if required {
			return "", []apiv1.FieldError{{Path: "/exch_account_uid", Reason: "required"}}
		}
		return "", nil
	}
	if utf8.RuneCountInString(trimmed) > 64 {
		return "", []apiv1.FieldError{{Path: "/exch_account_uid", Reason: "must be 1 to 64 characters"}}
	}
	return trimmed, nil
}

func flagBool(p *bool) bool {
	return p != nil && *p
}

// checkCredentials reports format problems (E10-D064). kindOK false skips the checks that need the
// kind, so an unknown kind is not also "key_type is not accepted". Values are never copied into the
// reason. required is create (E10-D062); an update with no credentials object keeps the ciphertext.
func checkCredentials(c *apiv1.Credentials, k Kind, required, kindOK bool) []apiv1.FieldError {
	if c == nil {
		if required {
			return []apiv1.FieldError{{Path: "/credentials", Reason: "required"}}
		}
		return nil
	}
	var fields []apiv1.FieldError
	if kindOK && !slices.Contains(k.KeyTypes, string(c.KeyType)) {
		fields = append(fields, apiv1.FieldError{Path: "/credentials/key_type", Reason: "is not accepted"})
	}
	if c.Key == "" {
		fields = append(fields, apiv1.FieldError{Path: "/credentials/key", Reason: "required"})
	}
	if c.Secret == "" {
		fields = append(fields, apiv1.FieldError{Path: "/credentials/secret", Reason: "required"})
	}
	if kindOK {
		pass := ""
		if c.Passphrase != nil {
			pass = *c.Passphrase
		}
		switch {
		case k.Passphrase && pass == "":
			fields = append(fields, apiv1.FieldError{Path: "/credentials/passphrase", Reason: "required"})
		case !k.Passphrase && pass != "":
			fields = append(fields, apiv1.FieldError{Path: "/credentials/passphrase", Reason: "is not used by this account kind"})
		}
	}
	return fields
}

func checkSettings(in *apiv1.AccountSettings) []apiv1.FieldError {
	if in == nil || in.CancelOnDisconnectS == nil {
		return nil
	}
	v := *in.CancelOnDisconnectS
	if v < 0 || uint64(v) > math.MaxUint32 {
		return []apiv1.FieldError{{Path: "/settings/cancel_on_disconnect_s", Reason: "must be between 0 and 4294967295"}}
	}
	return nil
}

// mergeSettings applies cancel_on_disconnect_s when the request sets it.
// checkSettings has already accepted the value (E10-D034).
func mergeSettings(in *apiv1.AccountSettings) *statev1.AccountSettings {
	out := &statev1.AccountSettings{}
	if in != nil && in.CancelOnDisconnectS != nil {
		out.CancelOnDisconnectS = uint32(*in.CancelOnDisconnectS)
	}
	return out
}

func (s *Server) kindByName(name string) (Kind, bool) {
	for _, k := range s.cfg.Kinds {
		if k.Name == name {
			return k, true
		}
	}
	return Kind{}, false
}

// acceptedKind is a supported kind whose registry exchange is set (E21-D008, E21-D031).
func (s *Server) acceptedKind(name string) (Kind, bool) {
	k, ok := s.kindByName(name)
	if !ok || !k.Supported() || k.Exchange == "" {
		return Kind{}, false
	}
	return k, true
}

// classifyInstance is the topology half of E10-D065: a name, in the deployment, on the TD plane.
// path is the field the caller is checking, for example /accounts/0/td_instance.
// The public-key half is pubkeyFor.
func (s *Server) classifyInstance(path, name string) (region string, ok bool, fields []apiv1.FieldError) {
	if !instanceNameRe.MatchString(name) {
		return "", false, []apiv1.FieldError{{Path: path, Reason: "is not an instance name"}}
	}
	region, err := s.cfg.Topology.RegionOf(name)
	if err != nil {
		return "", false, []apiv1.FieldError{{Path: path, Reason: "is not in the topology"}}
	}
	if s.cfg.Planes[name] != state.PlaneTD {
		return "", false, []apiv1.FieldError{{Path: path, Reason: "is not a td instance"}}
	}
	return region, true, nil
}

func (s *Server) regionViews() (map[string]state.View, []string) {
	now := s.now()
	views := make(map[string]state.View, len(s.clusters))
	var unavail []string
	for _, c := range s.clusters {
		view, contact, synced := c.observe()
		if (regionSnap{synced: synced, contact: contact}).available(now, s.cfg.UnavailableAfter) {
			views[c.name] = view
		} else {
			unavail = append(unavail, c.name)
		}
	}
	slices.Sort(unavail)
	s.rememberAgents(views)
	return views, unavail
}

// freshness is E10-D014. controller_absent is set only when the controller's home region is available
// and its agent record is missing. A down home region is listed in unavailable_regions instead, and
// the flag stays false.
func (s *Server) freshness(views map[string]state.View, unavail []string) apiv1.Freshness {
	return freshnessOf(s.cfg.Topology, views, unavail)
}

func freshnessOf(topo state.Topology, views map[string]state.View, unavail []string) apiv1.Freshness {
	if unavail == nil {
		unavail = []string{}
	}
	absent := false
	if home, err := topo.RegionOf(state.ControllerInstance); err == nil {
		if view, ok := views[home]; ok {
			_, present := view.Entries[state.AgentKey(state.PlaneCtl, state.ControllerInstance)]
			absent = !present
		}
	}
	return apiv1.Freshness{UnavailableRegions: unavail, ControllerAbsent: absent}
}

// referencesOf is E10-D011: a non-terminal spec that names the api_id. A missing or unreadable status
// counts as non-terminal. An unreadable spec is skipped. Both are the replica's best effort.
func referencesOf(views map[string]state.View) map[int64][]string {
	out := map[int64][]string{}
	for _, view := range views {
		for key, e := range view.Entries {
			inst, id, ok := specOf(key)
			if !ok {
				continue
			}
			var spec sessionv1.SessionSpec
			if err := proto.Unmarshal(e.Value, &spec); err != nil {
				continue
			}
			if terminalStatus(view, inst, id) {
				continue
			}
			seen := map[int64]struct{}{}
			for _, a := range spec.GetAccounts() {
				apiID := a.GetApiId()
				if apiID <= 0 {
					continue
				}
				if _, dup := seen[apiID]; dup {
					continue
				}
				seen[apiID] = struct{}{}
				out[apiID] = append(out[apiID], id)
			}
		}
	}
	for id, refs := range out {
		slices.Sort(refs)
		out[id] = slices.Compact(refs)
	}
	return out
}

func terminalStatus(view state.View, inst, id string) bool {
	e, ok := view.Entries[state.SessionStatusKey(inst, id)]
	if !ok {
		return false
	}
	var st statev1.SessionStatus
	if err := proto.Unmarshal(e.Value, &st); err != nil {
		return false
	}
	return st.GetPhase() == statev1.Phase_PHASE_DONE || st.GetPhase() == statev1.Phase_PHASE_FAILED
}

func accountOf(key string) (inst string, id int64, ok bool) {
	p, inst, err := state.Owner(key)
	if err != nil || p != state.PlaneTD {
		return "", 0, false
	}
	rest, ok := strings.CutPrefix(key, state.AccountPrefix(inst))
	if !ok || rest == "" || strings.Contains(rest, "/") {
		return "", 0, false
	}
	id, err = strconv.ParseInt(rest, 10, 64)
	if err != nil || id < 1 {
		return "", 0, false
	}
	return inst, id, true
}

func specOf(key string) (inst, id string, ok bool) {
	p, inst, err := state.Owner(key)
	if err != nil || p != state.PlaneSTS {
		return "", "", false
	}
	rest, ok := strings.CutPrefix(key, state.SessionSpecPrefix(inst))
	if !ok || rest == "" || strings.Contains(rest, "/") {
		return "", "", false
	}
	return inst, rest, true
}

func accountJSON(row accountPublic, refs []string) apiv1.Account {
	if refs == nil {
		refs = []string{}
	}
	ids := make([]apiv1.SessionIDValue, len(refs))
	for i, id := range refs {
		ids[i] = id
	}
	cancel := int(row.Cancel)
	ex := row.Exchange
	uid := row.UID
	out := apiv1.Account{
		ApiId: row.ID, Name: row.Name, Kind: row.Kind,
		Settings:       apiv1.AccountSettings{CancelOnDisconnectS: &cancel},
		Revision:       row.Revision,
		ReferencedBy:   ids,
		Exchange:       &ex,
		ExchAccountUid: &uid,
	}
	if row.Deleted != nil {
		t := row.Deleted.UTC()
		out.DeletedAt = &t
	}
	return out
}
