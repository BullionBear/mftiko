// Command mftiko-etcd-roles prints the etcd users and roles of every regional cluster, derived from the
// deployment's instances (E1-08, PROTOCOL §3.2 W1), and with -apply applies them (E12-D049).
//
// -drop <inst> prints the etcdctl commands that remove that instance's user and role in its own region
// (E12-D035). It does not apply them, and it does not omit the instance from the tables -format prints.
//
// The input is JSON on stdin or in -in:
//
//	{
//	  "regions":   {"tw": {"endpoints": ["https://etcd-tw:2379"]}},
//	  "instances": {"controller": "tw", "md-tw-1": "tw", "td-tw-1": "tw", "sts-tw-1": "tw"},
//	  "planes":    {"md-tw-1": "md", "td-tw-1": "td", "sts-tw-1": "sts"}
//	}
//
// A region may also set dial_timeout, request_timeout and stale_after (Go durations, E12-D046).
// This command ignores them. The controller reads them.
//
// -format json (the default) prints the tables; -format etcdctl prints the etcdctl commands that create them on a
// cluster that has none yet, one region at a time. The output holds no credentials: etcdctl prompts for them when it
// adds each user.
//
// -apply adds what each cluster lacks, as root, on a cluster whose auth is on (-region limits it to one). -secrets
// is a directory of password files: "root", and one per user named like the user. A new user gets the password in
// its file, generated there first if there is none; an existing user keeps its password. Nothing is removed: what
// the cluster has beyond the tables is printed. Running it twice changes nothing.
//
//	mftiko-etcd-roles -in deploy.json -apply -secrets /etc/mftiko/etcd
package main

import (
	"context"
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
	drop := flag.String("drop", "", "print etcdctl commands that remove this instance's user and role; does not apply them")
	apply := flag.Bool("apply", false, "add what each cluster lacks of its table, as root")
	region := flag.String("region", "", "with -apply, only this region")
	secrets := flag.String("secrets", "", "with -apply, the directory of password files: root, and one per user")
	flag.Parse()
	if *version {
		fmt.Println(buildinfo.String())
		return
	}
	var err error
	switch {
	case *drop != "" && *apply:
		err = fmt.Errorf("-drop and -apply are exclusive")
	case *drop != "":
		err = runDrop(*in, *drop, os.Stdin, os.Stdout)
	case *apply:
		err = runApply(context.Background(), *in, *region, *secrets, os.Stdin, os.Stdout)
	default:
		err = run(*in, *format, os.Stdin, os.Stdout)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "mftiko-etcd-roles:", err)
		os.Exit(1)
	}
}

func run(in, format string, stdin io.Reader, out io.Writer) error {
	d, err := readDeployment(in, stdin)
	if err != nil {
		return err
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

// runDrop prints the commands that remove inst's user and role on its own region's cluster (E12-D035).
// Applying them is deployment. The full table from -format still lists the instance.
func runDrop(in, inst string, stdin io.Reader, out io.Writer) error {
	d, err := readDeployment(in, stdin)
	if err != nil {
		return err
	}
	plane, ok := d.Planes[inst]
	if !ok || (plane != state.PlaneSTS && plane != state.PlaneMD && plane != state.PlaneTD) {
		return fmt.Errorf("instance %q is not an sts, md, or td agent", inst)
	}
	region, err := d.RegionOf(inst)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "# region %s\n", region)
	fmt.Fprintf(out, "export ETCDCTL_ENDPOINTS=%s\n", quote(strings.Join(d.Regions[region].Endpoints, ",")))
	fmt.Fprintf(out, "etcdctl user revoke-role %s %s\n", quote(inst), quote(inst))
	fmt.Fprintf(out, "etcdctl user delete %s\n", quote(inst))
	fmt.Fprintf(out, "etcdctl role delete %s\n", quote(inst))
	return nil
}

func readDeployment(in string, stdin io.Reader) (input, error) {
	r := stdin
	if in != "-" {
		f, err := os.Open(in)
		if err != nil {
			return input{}, err
		}
		defer f.Close()
		r = f
	}
	var d input
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&d); err != nil {
		return input{}, fmt.Errorf("read deployment: %w", err)
	}
	return d, nil
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
