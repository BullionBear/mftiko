// Command mftiko-retire-clean deletes the keys W1 assigns to one retired instance (E12-D035, PROTOCOL §3.7).
//
// It authenticates as that instance. It deletes election/, epoch, writer, agent, worker/, incarnation/, and the
// plane's own keys (STS status/, MD ack/ selector/ placement/, TD ack/). It does not delete spec, demand, account,
// or /retired/{inst}: a grant wide enough to delete those would let a live agent delete another writer's keys.
//
// The instance must already be retired and its AgentRecord must be absent. Removing the etcd user and role is
// mftiko-etcd-roles -drop, which only prints the commands.
//
//	mftiko-retire-clean -in deploy.json -instance sts-tw-1
//
// -etcd-password defaults to $MFTIKO_ETCD_PASSWORD. -in is the same deployment JSON as mftiko-etcd-roles.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

type input struct {
	state.Topology
	Planes map[string]state.Plane `json:"planes"`
}

func main() {
	version := flag.Bool("version", false, "print the release and exit")
	in := flag.String("in", "-", "deployment JSON, the same file as mftiko-etcd-roles; - for stdin")
	inst := flag.String("instance", "", "instance to clean")
	password := flag.String("etcd-password", os.Getenv("MFTIKO_ETCD_PASSWORD"), "etcd password of -instance")
	flag.Parse()
	if *version {
		fmt.Println(buildinfo.String())
		return
	}
	if err := run(context.Background(), *in, *inst, *password, os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "mftiko-retire-clean:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, in, inst, password string, stdin io.Reader, stdout io.Writer) error {
	if inst == "" {
		return fmt.Errorf("-instance is required")
	}
	if password == "" {
		return fmt.Errorf("etcd password is empty")
	}
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
	cli, err := state.AuthDial(inst, password)(region, d.Regions[region].Endpoints)
	if err != nil {
		return err
	}
	defer cli.Close()

	retired, err := cli.Get(ctx, state.RetiredKey(inst))
	if err != nil {
		return fmt.Errorf("read %s: %w", state.RetiredKey(inst), err)
	}
	if len(retired.Kvs) == 0 {
		return fmt.Errorf("instance %s is not retired", inst)
	}
	agent, err := cli.Get(ctx, state.AgentKey(plane, inst))
	if err != nil {
		return fmt.Errorf("read agent record: %w", err)
	}
	if len(agent.Kvs) != 0 {
		return fmt.Errorf("agent record of %s is present", inst)
	}
	prefixes, exact := state.RetireCleanTargets(plane, inst)
	for _, prefix := range prefixes {
		if _, err := cli.Delete(ctx, prefix, clientv3.WithPrefix()); err != nil {
			return fmt.Errorf("delete %s: %w", prefix, err)
		}
	}
	for _, key := range exact {
		if _, err := cli.Delete(ctx, key); err != nil {
			return fmt.Errorf("delete %s: %w", key, err)
		}
	}
	_, err = fmt.Fprintf(stdout, "cleaned %s\nnext: mftiko-etcd-roles -drop %s\n", inst, inst)
	return err
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
