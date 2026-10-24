package cli

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

func runStrategyList(a *App) int {
	if len(a.Args) != 0 {
		return a.exit("strategy list takes no arguments")
	}
	api, code := a.preparePaged()
	if code != 0 {
		return code
	}
	got, code := a.collect(func(limit int, cursor string) (Result, error) {
		return api.ListStrategies(a.ctx, limit, cursor)
	})
	if code != 0 {
		return code
	}
	return a.finishPaged(got, strategiesText)
}

func runStrategyVersions(a *App) int {
	if len(a.Args) != 1 {
		return a.exit("strategy versions takes a strategy id")
	}
	id := a.Args[0]
	api, code := a.preparePaged()
	if code != 0 {
		return code
	}
	got, code := a.collect(func(limit int, cursor string) (Result, error) {
		return api.ListStrategyVersions(a.ctx, id, limit, cursor)
	})
	if code != 0 {
		return code
	}
	return a.finishPaged(got, versionsText)
}

func runStrategyShow(a *App) int {
	if len(a.Args) != 2 {
		return a.exit("strategy show takes a strategy id and a ref")
	}
	api, code := a.prepareOne()
	if code != 0 {
		return code
	}
	res, err := api.GetStrategyVersion(a.ctx, a.Args[0], a.Args[1])
	if code := a.checked(res, err); code != 0 {
		return code
	}
	return a.finishOne(res, strategyShowText)
}

func runInstanceList(a *App) int {
	if len(a.Args) != 0 {
		return a.exit("instance list takes no arguments")
	}
	api, code := a.prepareOne()
	if code != 0 {
		return code
	}
	res, err := api.ListInstances(a.ctx)
	if code := a.checked(res, err); code != 0 {
		return code
	}
	return a.finishOne(res, instanceListText)
}

func runInstanceShow(a *App) int {
	if len(a.Args) != 1 {
		return a.exit("instance show takes an instance name")
	}
	api, code := a.prepareOne()
	if code != 0 {
		return code
	}
	res, err := api.GetInstance(a.ctx, a.Args[0])
	if code := a.checked(res, err); code != 0 {
		return code
	}
	return a.finishOne(res, instanceShowText)
}

func runAccountKindList(a *App) int {
	if len(a.Args) != 0 {
		return a.exit("account-kind list takes no arguments")
	}
	api, code := a.prepareOne()
	if code != 0 {
		return code
	}
	res, err := api.ListAccountKinds(a.ctx)
	if code := a.checked(res, err); code != 0 {
		return code
	}
	return a.finishOne(res, accountKindText)
}

func strategiesText(items []json.RawMessage) (string, error) {
	rows := make([][]string, 0, len(items))
	for _, raw := range items {
		var s apiv1.Strategy
		if err := json.Unmarshal(raw, &s); err != nil {
			return "", errNotList
		}
		tag, dig := "", ""
		if s.Latest != nil {
			tag = s.Latest.Tag
			dig = shortDigest(string(s.Latest.Digest))
		}
		rows = append(rows, []string{s.Id, s.Repository, tag, dig})
	}
	var b strings.Builder
	if err := WriteTable(&b, []string{"ID", "REPOSITORY", "LATEST_TAG", "LATEST_DIGEST"}, rows); err != nil {
		return "", err
	}
	return b.String(), nil
}

// versionsText marks every row whose digest equals the digest of a tag named
// latest in this result (E11-D013). Rows outside this result are not consulted.
func versionsText(items []json.RawMessage) (string, error) {
	tags := make([]apiv1.StrategyTag, 0, len(items))
	for _, raw := range items {
		var t apiv1.StrategyTag
		if err := json.Unmarshal(raw, &t); err != nil {
			return "", errNotList
		}
		tags = append(tags, t)
	}
	mark := ""
	for _, t := range tags {
		if t.Tag == "latest" && t.Digest != "" {
			mark = string(t.Digest)
			break
		}
	}
	rows := make([][]string, 0, len(tags))
	for _, t := range tags {
		flag := ""
		if mark != "" && string(t.Digest) == mark {
			flag = "latest"
		}
		rows = append(rows, []string{t.Tag, string(t.Digest), flag})
	}
	var b strings.Builder
	if err := WriteTable(&b, []string{"TAG", "DIGEST", "LATEST"}, rows); err != nil {
		return "", err
	}
	return b.String(), nil
}

func strategyShowText(body []byte) (string, error) {
	var v apiv1.StrategyVersion
	if err := json.Unmarshal(body, &v); err != nil || v.StrategyId == "" {
		return "", errors.New("response is not a strategy version")
	}
	sdk := ""
	if v.Sdk != nil {
		sdk = *v.Sdk
	}
	size := ""
	if v.SizeBytes != nil {
		size = strconv.FormatInt(*v.SizeBytes, 10)
	}
	rows := [][]string{
		{"STRATEGY_ID", v.StrategyId},
		{"REPOSITORY", v.Repository},
		{"DIGEST", string(v.Digest)},
		{"TAGS", strings.Join(v.Tags, ", ")},
		{"NAME", v.Name},
		{"RUNTIME", v.Runtime},
		{"OS", v.Os},
		{"ARCH", v.Arch},
		{"SDK", sdk},
		{"CONTRACTS", strings.Join(v.Contracts, ", ")},
		{"SIZE_BYTES", size},
	}
	var b strings.Builder
	if err := WriteTable(&b, []string{"FIELD", "VALUE"}, rows); err != nil {
		return "", err
	}
	return b.String(), nil
}

func instanceListText(body []byte) (string, error) {
	if _, _, _, err := decodePage(body); err != nil {
		return "", err
	}
	var list apiv1.InstanceList
	if err := json.Unmarshal(body, &list); err != nil {
		return "", errNotList
	}
	rows := make([][]string, 0, len(list.Items))
	for _, it := range list.Items {
		present, known := agentCells(it.Agent)
		rows = append(rows, []string{
			it.Name, string(it.Plane), it.Region,
			strconv.FormatBool(it.RegionAvailable), present, known,
		})
	}
	var b strings.Builder
	if err := WriteTable(&b, []string{"NAME", "PLANE", "REGION", "REGION_AVAILABLE", "AGENT", "LAST_KNOWN"}, rows); err != nil {
		return "", err
	}
	present, known := agentCells(list.Controller.Agent)
	if err := writeSection(&b, "CONTROLLER", []string{"PRESENT", "HOME_REGION", "AGENT", "LAST_KNOWN"}, [][]string{{
		strconv.FormatBool(list.Controller.Present), list.Controller.HomeRegion, present, known,
	}}); err != nil {
		return "", err
	}
	return b.String(), nil
}

func instanceShowText(body []byte) (string, error) {
	var d apiv1.InstanceDetail
	if err := json.Unmarshal(body, &d); err != nil || d.Name == "" {
		return "", errors.New("response is not an instance")
	}
	present, known := agentCells(d.Agent)
	rows := [][]string{
		{"NAME", d.Name},
		{"PLANE", string(d.Plane)},
		{"REGION", d.Region},
		{"REGION_AVAILABLE", strconv.FormatBool(d.RegionAvailable)},
		{"AGENT", present},
		{"LAST_KNOWN", known},
	}
	if d.Agent != nil {
		rows = append(rows,
			[]string{"EPOCH", strconv.FormatInt(d.Agent.Epoch, 10)},
			[]string{"RELEASE", d.Agent.Release},
			[]string{"STARTED_AT", localStamp(d.Agent.StartedAt)},
			[]string{"HOST", deref(d.Agent.Host)},
			[]string{"WORK_DIR", deref(d.Agent.WorkDir)},
		)
	}
	var b strings.Builder
	if err := WriteTable(&b, []string{"FIELD", "VALUE"}, rows); err != nil {
		return "", err
	}
	if err := writeSection(&b, "WORKERS", []string{"KEY", "PHASE", "INCARNATION", "RESTARTS", "STAGE"}, workerRows(d.Workers)); err != nil {
		return "", err
	}
	if d.Demands != nil {
		if err := writeSection(&b, "DEMANDS", []string{"SESSION_ID", "KIND", "STATE", "SUBJECT", "REASON"}, demandRows(*d.Demands)); err != nil {
			return "", err
		}
	}
	if d.Sessions != nil {
		if err := writeSection(&b, "SESSIONS", []string{"SESSION_ID", "NAME", "PHASE"}, sessionRows(*d.Sessions)); err != nil {
			return "", err
		}
	}
	return b.String(), nil
}

func accountKindText(body []byte) (string, error) {
	if _, _, _, err := decodePage(body); err != nil {
		return "", err
	}
	var list apiv1.AccountKindList
	if err := json.Unmarshal(body, &list); err != nil {
		return "", errNotList
	}
	rows := make([][]string, 0, len(list.Items))
	for _, k := range list.Items {
		rows = append(rows, []string{
			k.Kind, k.Label, strings.Join(k.Trades, ", "), joinKeys(k.KeyTypes),
			strconv.FormatBool(k.Passphrase), strconv.FormatBool(k.Supported),
		})
	}
	var b strings.Builder
	if err := WriteTable(&b, []string{"KIND", "LABEL", "TRADES", "KEY_TYPES", "PASSPHRASE", "SUPPORTED"}, rows); err != nil {
		return "", err
	}
	return b.String(), nil
}

func writeSection(b *strings.Builder, title string, header []string, rows [][]string) error {
	b.WriteByte('\n')
	b.WriteString(title)
	b.WriteByte('\n')
	return WriteTable(b, header, rows)
}

func workerRows(workers []apiv1.Worker) [][]string {
	rows := make([][]string, 0, len(workers))
	for _, w := range workers {
		rows = append(rows, []string{
			w.Key, string(w.Phase), strconv.Itoa(w.Incarnation), strconv.Itoa(w.Restarts), deref(w.Stage),
		})
	}
	return rows
}

func demandRows(demands []apiv1.DemandStatus) [][]string {
	rows := make([][]string, 0, len(demands))
	for _, d := range demands {
		rows = append(rows, []string{
			d.SessionId, string(d.Kind), string(d.State), demandSubject(d), deref(d.Reason),
		})
	}
	return rows
}

func sessionRows(sessions []apiv1.InstanceSession) [][]string {
	rows := make([][]string, 0, len(sessions))
	for _, s := range sessions {
		phase := ""
		if s.Phase != nil {
			phase = string(*s.Phase)
		}
		rows = append(rows, []string{s.SessionId, s.Name, phase})
	}
	return rows
}

// demandSubject is the feed when the demand has one, otherwise the api_id.
func demandSubject(d apiv1.DemandStatus) string {
	if d.Feed != nil {
		return *d.Feed
	}
	if d.ApiId != nil {
		return strconv.FormatInt(int64(*d.ApiId), 10)
	}
	return ""
}

// agentCells reports whether the agent is present. A missing record or
// last_known true means it is not (E10-D096).
func agentCells(agent *apiv1.AgentStatus) (present, lastKnown string) {
	if agent == nil {
		return "no", "false"
	}
	if agent.LastKnown != nil && *agent.LastKnown {
		return "no", "true"
	}
	return "yes", "false"
}

// shortDigest is the first 12 characters of the digest string (E11-D013).
// The table uses it. JSON keeps the digest the API sent.
func shortDigest(d string) string {
	if utf8.RuneCountInString(d) <= 12 {
		return d
	}
	i := 0
	for n := 0; n < 12 && i < len(d); n++ {
		_, size := utf8.DecodeRuneInString(d[i:])
		i += size
	}
	return d[:i]
}

func localStamp(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.In(time.Local).Format(time.RFC3339)
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func joinKeys(keys []apiv1.KeyType) string {
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = string(k)
	}
	return strings.Join(parts, ", ")
}
