package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/BullionBear/mftiko/internal/platform/state"
)

const deployment = `{
  "regions":   {"tw": {"endpoints": ["http://etcd-tw:2379"]}, "jp": {"endpoints": ["http://etcd-jp:2379"]}},
  "instances": {"controller": "tw", "md-tw-1": "tw", "sts-tw-1": "tw", "td-jp-1": "jp"},
  "planes":    {"md-tw-1": "md", "sts-tw-1": "sts", "td-jp-1": "td"}
}`

func TestJSON(t *testing.T) {
	var out bytes.Buffer
	if err := run("-", "json", strings.NewReader(deployment), &out); err != nil {
		t.Fatal(err)
	}
	var tables map[string]state.AuthTable
	if err := json.Unmarshal(out.Bytes(), &tables); err != nil {
		t.Fatal(err)
	}
	if got := tables["jp"].Users(); strings.Join(got, ",") != "api,controller,td-jp-1" {
		t.Fatalf("jp users %v", got)
	}
}

func TestEtcdctl(t *testing.T) {
	var out bytes.Buffer
	if err := run("-", "etcdctl", strings.NewReader(deployment), &out); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"# region jp\nexport ETCDCTL_ENDPOINTS='http://etcd-jp:2379'\n",
		"etcdctl role grant-permission 'td-jp-1' readwrite '/mftiko/v1/td/td-jp-1/ack/' '/mftiko/v1/td/td-jp-1/ack0'\n",
		"etcdctl role grant-permission 'md-tw-1' readwrite '/mftiko/v1/md/md-tw-1/epoch'\n",
		"etcdctl role grant-permission 'controller' readwrite '/mftiko/v1/sts/sts-tw-1/demand/' '/mftiko/v1/sts/sts-tw-1/demand0'\n",
		"etcdctl user add 'api'\n",
		"etcdctl user grant-role 'sts-tw-1' 'sts-tw-1'\n",
		// api reads the keyspace and writes spec, status (D1031) and account. The range end is the prefix's last byte plus one.
		"etcdctl role grant-permission 'api' read '/mftiko/v1/' '/mftiko/v10'\n",
		"etcdctl role grant-permission 'api' readwrite '/mftiko/v1/sts/sts-tw-1/spec/' '/mftiko/v1/sts/sts-tw-1/spec0'\n",
		"etcdctl role grant-permission 'api' readwrite '/mftiko/v1/sts/sts-tw-1/status/' '/mftiko/v1/sts/sts-tw-1/status0'\n",
		"etcdctl role grant-permission 'api' readwrite '/mftiko/v1/td/td-jp-1/account/' '/mftiko/v1/td/td-jp-1/account0'\n",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q", want)
		}
	}
	if strings.Index(out.String(), "# region jp") > strings.Index(out.String(), "# region tw") {
		t.Error("regions out of order")
	}
	for _, forbidden := range []string{
		"grant-permission 'api' readwrite '/mftiko/v1/sts/sts-tw-1/demand/",
		"grant-permission 'api' readwrite '/mftiko/v1/md/",
		"grant-permission 'api' readwrite '/mftiko/v1/td/td-jp-1/ack/",
		"grant-permission 'api' readwrite '/mftiko/v1/td/td-jp-1/demand/",
	} {
		if strings.Contains(out.String(), forbidden) {
			t.Errorf("api granted %s", forbidden)
		}
	}
}

func TestRejects(t *testing.T) {
	for name, in := range map[string]string{
		"unknown field": `{"regions": {}, "instances": {}, "planes": {}, "extra": 1}`,
		"no plane":      strings.Replace(deployment, `"md-tw-1": "md", `, "", 1),
		"not json":      `{`,
	} {
		if err := run("-", "json", strings.NewReader(in), &bytes.Buffer{}); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if err := run("-", "yaml", strings.NewReader(deployment), &bytes.Buffer{}); err == nil {
		t.Error("unknown format accepted")
	}
}
