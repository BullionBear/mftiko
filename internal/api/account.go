package api

import (
	"cmp"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/proto"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/sealbox"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// instanceNameRe is the instance name syntax (PROTOCOL §2). state keeps its copy unexported.
var instanceNameRe = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)

var (
	errNameTaken  = errors.New("api: name is already in use")
	errReferenced = errors.New("api: account is referenced")
	errUnreadable = errors.New("api: stored record is unreadable")
)

// accountRec is one account as a mirror holds it. rec's sealed bytes stay out of responses and logs (E10-D063).
type accountRec struct {
	key    string
	inst   string
	region string
	id     int64
	rev    int64
	rec    *statev1.Account
}

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

func (s *Server) ListAccounts(w http.ResponseWriter, _ *http.Request, params apiv1.ListAccountsParams) {
	limit, after, fields := bindPage(params.Limit, params.Cursor)
	var afterID int64
	hasAfter := false
	if after != "" && len(fields) == 0 {
		id, err := strconv.ParseInt(after, 10, 64)
		if err != nil || strconv.FormatInt(id, 10) != after {
			fields = append(fields, apiv1.FieldError{Path: "/cursor", Reason: "is not a cursor from this API"})
		} else {
			afterID, hasAfter = id, true
		}
	}
	filter, filterRegion := "", ""
	if params.TdInstance != nil && *params.TdInstance != "" {
		region, ok, instFields := s.classifyInstance(*params.TdInstance)
		fields = append(fields, instFields...)
		if ok {
			filter, filterRegion = *params.TdInstance, region
		}
	}
	if len(fields) > 0 {
		writeFields(w, fields)
		return
	}
	views, unavail := s.regionViews()
	if len(views) == 0 {
		s.writeAccountErr(w, errRegionUnavailable)
		return
	}
	fresh := s.freshness(views, unavail)
	if filter != "" {
		if _, up := views[filterRegion]; !up {
			writeJSON(w, http.StatusOK, apiv1.AccountList{Items: []apiv1.Account{}, Freshness: fresh})
			return
		}
	}
	refs := referencesOf(views)
	var hits []accountRec
	for region, view := range views {
		for key, e := range view.Entries {
			inst, id, ok := accountOf(key)
			if !ok || (filter != "" && inst != filter) {
				continue
			}
			rec, err := unmarshalAccount(e.Value)
			if err != nil {
				s.writeAccountErr(w, err)
				return
			}
			hits = append(hits, accountRec{key: key, inst: inst, region: region, id: id, rev: e.ModRevision, rec: rec})
		}
	}
	slices.SortFunc(hits, func(a, b accountRec) int { return cmp.Compare(a.id, b.id) })
	start := 0
	if hasAfter {
		start = sort.Search(len(hits), func(i int) bool { return hits[i].id > afterID })
	}
	end := start + limit
	if end > len(hits) {
		end = len(hits)
	}
	items := make([]apiv1.Account, 0, end-start)
	for _, h := range hits[start:end] {
		items = append(items, accountJSON(h, refs[h.id]))
	}
	var next *string
	if end < len(hits) && end > start {
		c := base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(hits[end-1].id, 10)))
		next = &c
	}
	writeJSON(w, http.StatusOK, apiv1.AccountList{Items: items, NextCursor: next, Freshness: fresh})
}

func (s *Server) CreateAccount(w http.ResponseWriter, r *http.Request) {
	var body apiv1.AccountCreate
	if err := decodeBody(r, &body); err != nil {
		writeFields(w, nil)
		return
	}
	fields := checkName(body.Name)
	kind, kindOK := s.kindByName(body.Kind)
	if !kindOK || !kind.Supported() {
		fields = append(fields, apiv1.FieldError{Path: "/kind", Reason: "is not accepted"})
	}
	pub, instFields := s.checkTDInstance(string(body.TdInstance))
	fields = append(fields, instFields...)
	fields = append(fields, checkCredentials(body.Credentials, kind, true, kindOK && kind.Supported())...)
	fields = append(fields, checkSettings(body.Settings)...)
	if len(fields) > 0 {
		writeFields(w, fields)
		return
	}
	views, err := s.availableViews(string(body.TdInstance))
	if err != nil {
		s.writeAccountErr(w, err)
		return
	}
	if nameTaken(views, body.Name, "") {
		s.writeAccountErr(w, errNameTaken)
		return
	}
	region, err := s.cfg.Topology.RegionOf(string(body.TdInstance))
	if err != nil {
		s.writeAccountErr(w, err)
		return
	}
	settings := mergeSettings(body.Settings, nil)
	id, rev, err := s.createAccount(r.Context(), string(body.TdInstance), func(apiID int64) ([]byte, error) {
		rec, err := newAccount(pub, apiID, kind.Name, body.Name, body.Credentials, settings)
		if err != nil {
			return nil, err
		}
		return proto.Marshal(rec)
	})
	if err != nil {
		s.writeAccountErr(w, err)
		return
	}
	w.Header().Set("Location", "/api/v1/accounts/"+strconv.FormatInt(id, 10))
	writeJSON(w, http.StatusCreated, accountJSON(accountRec{
		inst: string(body.TdInstance), region: region, id: id, rev: rev,
		rec: &statev1.Account{Name: body.Name, Kind: kind.Name, Settings: settings, Enabled: true},
	}, nil))
}

func (s *Server) GetAccount(w http.ResponseWriter, _ *http.Request, apiID apiv1.APIID) {
	hit, err := s.findAccount(int64(apiID))
	if err != nil {
		s.writeAccountErr(w, err)
		return
	}
	views, _ := s.regionViews()
	writeJSON(w, http.StatusOK, accountJSON(hit, referencesOf(views)[hit.id]))
}

func (s *Server) UpdateAccount(w http.ResponseWriter, r *http.Request, apiID apiv1.APIID) {
	var body apiv1.AccountUpdate
	if err := decodeBody(r, &body); err != nil {
		writeFields(w, nil)
		return
	}
	if body.Revision < 1 {
		writeFields(w, []apiv1.FieldError{{Path: "/revision", Reason: "must be at least 1"}})
		return
	}
	hit, err := s.findAccount(int64(apiID))
	if err != nil {
		s.writeAccountErr(w, err)
		return
	}
	kind, kindOK := s.kindByName(hit.rec.GetKind())
	accepted := kindOK && kind.Supported()
	if body.Credentials != nil && !accepted {
		writeFields(w, []apiv1.FieldError{{Path: "/kind", Reason: "is not accepted"}})
		return
	}
	var fields []apiv1.FieldError
	if body.Name != nil {
		fields = append(fields, checkName(*body.Name)...)
	}
	fields = append(fields, checkCredentials(body.Credentials, kind, false, accepted)...)
	fields = append(fields, checkSettings(body.Settings)...)
	if len(fields) > 0 {
		writeFields(w, fields)
		return
	}
	views, unavail := s.regionViews()
	if _, up := views[hit.region]; !up {
		if len(unavail) > 0 {
			s.writeAccountErr(w, errRegionUnavailable)
			return
		}
	}
	name := hit.rec.GetName()
	if body.Name != nil {
		name = *body.Name
	}
	if nameTaken(views, name, hit.key) {
		s.writeAccountErr(w, errNameTaken)
		return
	}
	var pub sealbox.PublicKey
	if body.Credentials != nil {
		var instFields []apiv1.FieldError
		pub, instFields = s.checkTDInstance(hit.inst)
		if len(instFields) > 0 {
			writeFields(w, instFields)
			return
		}
	}
	var next *statev1.Account
	rev, err := update(r.Context(), s.cl.APIWriter(), hit.key, 1, func(cur *mvccpb.KeyValue) ([]byte, bool, error) {
		if cur == nil {
			return nil, false, errNotFound
		}
		if cur.ModRevision != int64(body.Revision) {
			return nil, false, errConflict
		}
		rec, err := unmarshalAccount(cur.Value)
		if err != nil {
			return nil, false, err
		}
		if rec.GetKind() != hit.rec.GetKind() {
			return nil, false, errConflict
		}
		built, err := applyUpdate(rec, body, pub, rec.GetKind(), int64(apiID))
		if err != nil {
			return nil, false, err
		}
		next = built
		if accountEqual(rec, built) {
			next = rec
			return nil, false, nil
		}
		b, err := proto.Marshal(built)
		if err != nil {
			return nil, false, errUnreadable
		}
		return b, true, nil
	})
	if err != nil {
		s.writeAccountErr(w, err)
		return
	}
	views, _ = s.regionViews()
	writeJSON(w, http.StatusOK, accountJSON(accountRec{
		inst: hit.inst, region: hit.region, id: hit.id, rev: rev, rec: next,
	}, referencesOf(views)[hit.id]))
}

func (s *Server) DeleteAccount(w http.ResponseWriter, r *http.Request, apiID apiv1.APIID, params apiv1.DeleteAccountParams) {
	if params.Revision < 1 {
		writeFields(w, []apiv1.FieldError{{Path: "/revision", Reason: "must be at least 1"}})
		return
	}
	hit, err := s.findAccount(int64(apiID))
	if err != nil {
		s.writeAccountErr(w, err)
		return
	}
	views, _ := s.regionViews()
	if len(referencesOf(views)[hit.id]) > 0 {
		s.writeAccountErr(w, errReferenced)
		return
	}
	if _, err := deleteIf(r.Context(), s.cl.APIWriter(), cond{key: hit.key, rev: int64(params.Revision)}); err != nil {
		s.writeAccountErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) writeAccountErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errNameTaken):
		writeProblem(w, http.StatusConflict, apiv1.ErrorCodeConflict, http.StatusText(http.StatusConflict), "name is already in use", []apiv1.FieldError{{
			Path: "/name", Reason: "already in use",
		}})
	case errors.Is(err, errReferenced):
		writeProblem(w, http.StatusConflict, apiv1.ErrorCodeReferenced, http.StatusText(http.StatusConflict), "account is referenced by a session", nil)
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
			// sealbox and etcd errors do not quote credential bytes. A stored record that does not
			// unmarshal is reported as errUnreadable, which does not carry the bytes either.
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

// mergeSettings keeps the backfill fields, which the API does not expose (E10-D034), and applies
// cancel_on_disconnect_s when the request sets it. checkSettings has already accepted the value.
func mergeSettings(in *apiv1.AccountSettings, prev *statev1.AccountSettings) *statev1.AccountSettings {
	out := &statev1.AccountSettings{}
	if prev != nil {
		out = proto.Clone(prev).(*statev1.AccountSettings)
	}
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

// classifyInstance is the topology half of E10-D065: a name, in the deployment, on the TD plane.
// The public-key half is checkTDInstance; a list filter does not need a key.
func (s *Server) classifyInstance(name string) (region string, ok bool, fields []apiv1.FieldError) {
	if !instanceNameRe.MatchString(name) {
		return "", false, []apiv1.FieldError{{Path: "/td_instance", Reason: "is not an instance name"}}
	}
	region, err := s.cfg.Topology.RegionOf(name)
	if err != nil {
		return "", false, []apiv1.FieldError{{Path: "/td_instance", Reason: "is not in the topology"}}
	}
	if s.cfg.Planes[name] != state.PlaneTD {
		return "", false, []apiv1.FieldError{{Path: "/td_instance", Reason: "is not a td instance"}}
	}
	return region, true, nil
}

func (s *Server) checkTDInstance(name string) (sealbox.PublicKey, []apiv1.FieldError) {
	_, ok, fields := s.classifyInstance(name)
	if !ok {
		return sealbox.PublicKey{}, fields
	}
	pub, ok := s.cfg.TDKeys[name]
	if !ok {
		return sealbox.PublicKey{}, []apiv1.FieldError{{Path: "/td_instance", Reason: "has no public key"}}
	}
	return pub, nil
}

func newAccount(pub sealbox.PublicKey, apiID int64, kind, name string, cred *apiv1.Credentials, settings *statev1.AccountSettings) (*statev1.Account, error) {
	sealed, err := sealCreds(pub, apiID, kind, cred)
	if err != nil {
		return nil, err
	}
	return &statev1.Account{
		Name: name, Kind: kind, SealedCredentials: sealed, Settings: settings, Enabled: true,
	}, nil
}

func sealCreds(pub sealbox.PublicKey, apiID int64, kind string, cred *apiv1.Credentials) ([]byte, error) {
	if cred == nil {
		return nil, errors.New("api: credentials are required")
	}
	pass := ""
	if cred.Passphrase != nil {
		pass = *cred.Passphrase
	}
	return sealbox.SealCredentials(pub, apiID, kind, sealbox.Credentials{
		KeyType: string(cred.KeyType), Key: cred.Key, Secret: cred.Secret, Passphrase: pass,
	})
}

// applyUpdate copies cur, sets enabled (E10-D034), and applies the fields the request sent. Omitted
// credentials keep the sealed bytes (E10-D062). Sending credentials always reseals: the nonce is random,
// so the ciphertext changes and the worker restarts.
func applyUpdate(cur *statev1.Account, body apiv1.AccountUpdate, pub sealbox.PublicKey, kind string, apiID int64) (*statev1.Account, error) {
	next := proto.Clone(cur).(*statev1.Account)
	next.Enabled = true
	if body.Name != nil {
		next.Name = *body.Name
	}
	if body.Settings != nil && body.Settings.CancelOnDisconnectS != nil {
		if next.Settings == nil {
			next.Settings = &statev1.AccountSettings{}
		}
		next.Settings.CancelOnDisconnectS = uint32(*body.Settings.CancelOnDisconnectS)
	}
	if body.Credentials != nil {
		sealed, err := sealCreds(pub, apiID, kind, body.Credentials)
		if err != nil {
			return nil, err
		}
		next.SealedCredentials = sealed
	}
	return next, nil
}

// accountEqual ignores a settings message that carries only zeros, so an explicit cancel of 0 does
// not rewrite an account that left the field unset (E10-D034: an unchanged body does not restart the worker).
func accountEqual(a, b *statev1.Account) bool {
	return proto.Equal(normalizeAccount(a), normalizeAccount(b))
}

func normalizeAccount(a *statev1.Account) *statev1.Account {
	c := proto.Clone(a).(*statev1.Account)
	s := c.GetSettings()
	if s != nil && s.GetCancelOnDisconnectS() == 0 && s.GetBackfillIntervalS() == 0 && s.GetBackfillRequestedAtNs() == 0 {
		c.Settings = nil
	}
	return c
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

func (s *Server) findAccount(apiID int64) (accountRec, error) {
	views, unavail := s.regionViews()
	var hits []accountRec
	for region, view := range views {
		for key, e := range view.Entries {
			inst, id, ok := accountOf(key)
			if !ok || id != apiID {
				continue
			}
			rec, err := unmarshalAccount(e.Value)
			if err != nil {
				return accountRec{}, err
			}
			hits = append(hits, accountRec{key: key, inst: inst, region: region, id: id, rev: e.ModRevision, rec: rec})
		}
	}
	if len(hits) == 0 {
		if len(unavail) > 0 {
			return accountRec{}, fmt.Errorf("%w: account %d", errRegionUnavailable, apiID)
		}
		return accountRec{}, errNotFound
	}
	slices.SortFunc(hits, func(a, b accountRec) int {
		if c := strings.Compare(a.region, b.region); c != 0 {
			return c
		}
		return strings.Compare(a.inst, b.inst)
	})
	return hits[0], nil
}

// nameTaken is E10-D066: every account the replica can see, on any TD instance. except is the account
// being updated. An unreadable record is skipped, the same best-effort as a region that is down.
func nameTaken(views map[string]state.View, name, except string) bool {
	for _, view := range views {
		for key, e := range view.Entries {
			if key == except {
				continue
			}
			if _, _, ok := accountOf(key); !ok {
				continue
			}
			rec, err := unmarshalAccount(e.Value)
			if err != nil {
				continue
			}
			if rec.GetName() == name {
				return true
			}
		}
	}
	return false
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

func unmarshalAccount(b []byte) (*statev1.Account, error) {
	rec := &statev1.Account{}
	if err := proto.Unmarshal(b, rec); err != nil {
		return nil, errUnreadable
	}
	return rec, nil
}

func accountJSON(h accountRec, refs []string) apiv1.Account {
	if refs == nil {
		refs = []string{}
	}
	ids := make([]apiv1.SessionIDValue, len(refs))
	for i, id := range refs {
		ids[i] = id
	}
	cancel := int(h.rec.GetSettings().GetCancelOnDisconnectS())
	return apiv1.Account{
		ApiId: h.id, Name: h.rec.GetName(), Kind: h.rec.GetKind(),
		TdInstance: h.inst, Region: h.region,
		Settings:     apiv1.AccountSettings{CancelOnDisconnectS: &cancel},
		Revision:     h.rev,
		ReferencedBy: ids,
	}
}
