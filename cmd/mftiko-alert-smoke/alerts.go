package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"
)

// notification is the body of one received.jsonl line: one Alertmanager webhook payload.
type notification struct {
	Status      string            `json:"status"`
	GroupLabels map[string]string `json:"groupLabels"`
	Alerts      []struct {
		Status      string            `json:"status"`
		Labels      map[string]string `json:"labels"`
		Annotations map[string]string `json:"annotations"`
	} `json:"alerts"`
	raw string
}

// readReceived returns the notifications in path after its first from bytes. A missing file has none,
// and a file shorter than from is read from the start: it was removed and started again. A line that
// from falls inside was being written before from was taken and is skipped. A last line without its
// newline is still being written and is left for the next read.
func readReceived(path string, from int64) ([]notification, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if from > 0 && int64(len(data)) >= from {
		partial := data[from-1] != '\n'
		data = data[from:]
		if partial {
			i := bytes.IndexByte(data, '\n')
			if i < 0 {
				return nil, nil
			}
			data = data[i+1:]
		}
	}
	var out []notification
	for {
		i := bytes.IndexByte(data, '\n')
		if i < 0 {
			return out, nil
		}
		var line struct {
			Body json.RawMessage `json:"body"`
		}
		if err := json.Unmarshal(data[:i], &line); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		data = data[i+1:]
		var n notification
		if json.Unmarshal(line.Body, &n) != nil {
			continue // a POST that was not a webhook payload
		}
		n.raw = string(line.Body)
		out = append(out, n)
	}
}

func fileSize(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return fi.Size()
}

// lastStatus is the status of the last notification of group, or "" when there is none.
func lastStatus(ns []notification, group map[string]string) string {
	for i := len(ns) - 1; i >= 0; i-- {
		if maps.Equal(ns[i].GroupLabels, group) {
			return ns[i].Status
		}
	}
	return ""
}

// want is one notification the smoke waits for.
type want struct {
	what    string
	status  string
	group   map[string]string // groupLabels, exactly
	labels  map[string]string // one alert of the notification has these
	noLabel string            // and not this one
	logs    string            // and this logs annotation, when it is not ""
	hidden  string            // text of the log line, which the notification must not carry (E16-D010)
}

// check reports whether n is the notification w waits for. A notification of w's group and status
// that differs from w in anything else is an error, so the smoke stops there instead of waiting.
func (w want) check(n notification) (bool, error) {
	if n.Status != w.status || !maps.Equal(n.GroupLabels, w.group) {
		return false, nil
	}
	if w.hidden != "" && strings.Contains(n.raw, w.hidden) {
		return false, fmt.Errorf("%s: the notification carries the log line's text %q", w.what, w.hidden)
	}
	for _, a := range n.Alerts {
		if a.Status != w.status || !hasLabels(a.Labels, w.labels) {
			continue
		}
		if _, ok := a.Labels[w.noLabel]; ok && w.noLabel != "" {
			return false, fmt.Errorf("%s: the alert has %s=%q", w.what, w.noLabel, a.Labels[w.noLabel])
		}
		if w.logs != "" && a.Annotations["logs"] != w.logs {
			return false, fmt.Errorf("%s: logs annotation %q, want %q", w.what, a.Annotations["logs"], w.logs)
		}
		return true, nil
	}
	return false, fmt.Errorf("%s: no %s alert with labels %v in %s", w.what, w.status, w.labels, n.raw)
}

func hasLabels(have, want map[string]string) bool {
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}

func logWant(s session) want {
	w := want{
		status: "firing",
		group:  map[string]string{"instance": stsInstance, "filename": s.stderr},
		labels: map[string]string{"instance": stsInstance, "region": region, "filename": s.stderr},
	}
	switch s.rule {
	case "R1":
		w.group["alertname"] = "MftikoStrategyLogError"
		w.hidden = "init failed"
	case "R2":
		w.group["alertname"] = "MftikoStrategyLogCrash"
		w.noLabel = "session_id"
		w.logs = `{filename="` + s.stderr + `"} |= "[mftiko-shim]"`
		w.hidden = "simulated R2 line"
	case "R5":
		w.group["alertname"] = "MftikoStrategyOverloaded"
	}
	if s.rule != "R2" {
		w.labels["session_id"] = s.id
		w.logs = "mftiko session logs " + s.id + " --stream stderr --tail 50"
	}
	w.labels["alertname"] = w.group["alertname"]
	w.what = s.rule + " " + w.group["alertname"]
	return w
}

// agentDownWant is sts1's MftikoAgentDown. Prometheus gives it no filename and no annotations.
func agentDownWant(status string) want {
	return want{
		what:   "MftikoAgentDown{instance=\"sts1\"}",
		status: status,
		group:  map[string]string{"alertname": "MftikoAgentDown", "instance": stsInstance},
		labels: map[string]string{"alertname": "MftikoAgentDown", "instance": stsInstance, "region": region, "plane": "sts"},
	}
}

// waitReceived waits until every one of wants is in the receiver's file after its first from bytes.
func waitReceived(ctx context.Context, path string, from int64, wants []want, limit time.Duration) error {
	seen := make([]bool, len(wants))
	err := poll(ctx, limit, func() (bool, error) {
		ns, err := readReceived(path, from)
		if err != nil {
			return false, err
		}
		all := true
		for i, w := range wants {
			for _, n := range ns {
				if seen[i] {
					break
				}
				ok, err := w.check(n)
				if err != nil {
					return false, err
				}
				if ok {
					seen[i] = true
					say("received %s", w.what)
				}
			}
			all = all && seen[i]
		}
		return all, nil
	})
	if errors.Is(err, context.DeadlineExceeded) {
		var missing []string
		for i, w := range wants {
			if !seen[i] {
				missing = append(missing, w.what)
			}
		}
		return fmt.Errorf("%s did not reach %s in %s", strings.Join(missing, ", "), path, limit)
	}
	return err
}

// amAlert is one alert from Alertmanager's GET /api/v2/alerts.
type amAlert struct {
	Fingerprint string            `json:"fingerprint"`
	Labels      map[string]string `json:"labels"`
	Status      struct {
		State       string   `json:"state"`
		InhibitedBy []string `json:"inhibitedBy"`
	} `json:"status"`
}

// getAlerts lists Alertmanager's alerts that match every filter, which are Alertmanager matchers such
// as alertname="MftikoAgentDown". Resolved alerts are not listed.
func getAlerts(ctx context.Context, base string, filters ...string) ([]amAlert, error) {
	q := url.Values{"filter": filters}
	var out []amAlert
	err := getJSON(ctx, base+"/api/v2/alerts?"+q.Encode(), &out)
	return out, err
}

// inhibited reports whether each of wants is in alerts, suppressed, and inhibited by the alert whose
// fingerprint is by. The error names the first that is not.
func inhibited(alerts []amAlert, by string, wants []want) error {
	for _, w := range wants {
		i := slices.IndexFunc(alerts, func(a amAlert) bool { return hasLabels(a.Labels, w.labels) })
		if i < 0 {
			return fmt.Errorf("%s is not in Alertmanager; its line may have left the rule's 5m window", w.what)
		}
		a := alerts[i]
		if a.Status.State != "suppressed" || !slices.Contains(a.Status.InhibitedBy, by) {
			return fmt.Errorf("%s is %s, inhibited by %v; want suppressed by MftikoAgentDown{instance=\"sts1\"} (%s)", w.what, a.Status.State, a.Status.InhibitedBy, by)
		}
	}
	return nil
}

// waitInhibited waits until Alertmanager has an active MftikoAgentDown{instance="sts1"} and reports
// each of wants as inhibited by it.
func waitInhibited(ctx context.Context, base string, wants []want, limit time.Duration) error {
	var last error
	err := poll(ctx, limit, func() (bool, error) {
		down, err := getAlerts(ctx, base, `alertname="MftikoAgentDown"`, `instance="`+stsInstance+`"`)
		if err != nil {
			return false, err
		}
		i := slices.IndexFunc(down, func(a amAlert) bool { return a.Status.State == "active" })
		if i < 0 {
			last = errors.New("Alertmanager has no active MftikoAgentDown{instance=\"sts1\"}")
			return false, nil
		}
		alerts, err := getAlerts(ctx, base, `alertname=~"MftikoStrategy.*"`, `instance="`+stsInstance+`"`)
		if err != nil {
			return false, err
		}
		last = inhibited(alerts, down[i].Fingerprint, wants)
		return last == nil, nil
	})
	if errors.Is(err, context.DeadlineExceeded) && last != nil {
		return fmt.Errorf("after %s: %w", limit, last)
	}
	if err != nil {
		return err
	}
	say("Alertmanager has MftikoAgentDown{instance=\"sts1\"} active and the three strategy alerts inhibited by it")
	return nil
}

// followQuery runs R2's logs annotation, a Loki query, and checks that it finds the shim's header
// line of s: that is how a filename leads back to its session (E16-D004).
func followQuery(ctx context.Context, base string, w want, s session) error {
	q := url.Values{"query": {w.logs}, "since": {"1h"}, "limit": {"100"}}
	var resp struct {
		Data struct {
			Result []struct {
				Values [][2]string `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := getJSON(ctx, base+"/loki/api/v1/query_range?"+q.Encode(), &resp); err != nil {
		return err
	}
	header := " worker sts/" + s.id + " incarnation "
	for _, r := range resp.Data.Result {
		for _, v := range r.Values {
			if strings.HasPrefix(v[1], "[mftiko-shim] ") && strings.Contains(v[1], header) {
				say("%s: Loki's %s finds session %s", s.rule, w.logs, s.id)
				return nil
			}
		}
	}
	return fmt.Errorf("%s: Loki's %s does not find the header line of session %s", w.what, w.logs, s.id)
}

var httpClient = &http.Client{Timeout: 10 * time.Second}

func getJSON(ctx context.Context, u string, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("GET %s: %s: %s", u, resp.Status, bytes.TrimSpace(body))
	}
	return json.NewDecoder(resp.Body).Decode(v)
}
