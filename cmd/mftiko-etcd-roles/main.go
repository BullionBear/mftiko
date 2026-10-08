// Command mftiko-etcd-roles prints the etcd users and roles of every regional cluster, derived from the
// deployment's instances (E1-08, PROTOCOL §3.2 W1). It only prints: applying the tables, and giving each user its
// password, is part of deployment (E12).
//
// The input is JSON on stdin or in -in:
//
//	{
//	  "regions":   {"tw": {"endpoints": ["https://etcd-tw:2379"]}},
//	  "instances": {"controller": "tw", "md-tw-1": "tw", "td-tw-1": "tw", "sts-tw-1": "tw"},
//	  "planes":    {"md-tw-1": "md", "td-tw-1": "td", "sts-tw-1": "sts"}
//	}
//
// -format json (the default) prints the tables; -format etcdctl prints the etcdctl commands that create them on a
// cluster that has none yet, one region at a time. The output holds no credentials: etcdctl prompts for them when it
// adds each user.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"strings"

	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

type input struct {
	state.Topology
	Planes map[string]state.Plane `json:"planes"`
}

func main() {
	version := flag.Bool("version", false, "print the release and exit")
	in := flag.String("in", "-", "deployment JSON; - for stdin")
	format := flag.String("format", "json", "json or etcdctl")
	flag.Parse()
	if *version {
		fmt.Println(buildinfo.String())
		return
	}
	if err := run(*in, *format, os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "mftiko-etcd-roles:", err)
		os.Exit(1)
	}
}

func run(in, format string, stdin io.Reader, out io.Writer) error {
	r := stdin
	if in != "-" {
		f, err := os.Open(in)
		if err != nil {
			return err
		}
		defer f.Close()
		r = f
	}
	var d input
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&d); err != nil {
		return fmt.Errorf("read deployment: %w", err)
	}
	tables, err := state.AuthTables(d.Topology, d.Planes)
	if err != nil {
		return err
	}
	switch format {
	case "json":
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(tables)
	case "etcdctl":
		for _, region := range slices.Sorted(maps.Keys(tables)) {
			fmt.Fprint(out, etcdctl(tables[region], d.Regions[region].Endpoints))
		}
		return nil
	default:
		return fmt.Errorf("unknown -format %q", format)
	}
}

// etcdctl renders one table as etcdctl commands against that region's cluster: each region's commands start by
// pointing etcdctl at its endpoints, so they cannot be applied to the wrong cluster by accident.
func etcdctl(t state.AuthTable, endpoints []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# region %s\n", t.Region)
	fmt.Fprintf(&b, "export ETCDCTL_ENDPOINTS=%s\n", quote(strings.Join(endpoints, ",")))
	for _, role := range t.Roles {
		fmt.Fprintf(&b, "etcdctl role add %s\n", quote(role.Name))
		for _, p := range role.Permissions {
			kind := "read"
			if p.Write {
				kind = "readwrite"
			}
			fmt.Fprintf(&b, "etcdctl role grant-permission %s %s %s", quote(role.Name), kind, quote(p.Key))
			if p.RangeEnd != "" {
				fmt.Fprintf(&b, " %s", quote(p.RangeEnd))
			}
			b.WriteString("\n")
		}
		fmt.Fprintf(&b, "etcdctl user add %s\n", quote(role.Name))
		fmt.Fprintf(&b, "etcdctl user grant-role %s %s\n", quote(role.Name), quote(role.Name))
	}
	return b.String()
}

// quote makes s one shell word.
func quote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }
