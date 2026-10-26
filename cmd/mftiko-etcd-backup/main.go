// Command mftiko-etcd-backup saves a snapshot of each region's etcd cluster, keeping the newest -keep per region
// (E12-D054), and with -raise-epochs prepares a restored cluster for its agents (E12-D056).
//
// -in is the same deployment JSON as mftiko-etcd-roles. It connects as root: -secrets is the directory with root's
// password in "root", as for mftiko-etcd-roles -apply.
//
// Saving writes <dir>/<region>/<UTC time>.db, through a .part file renamed once complete, then deletes the oldest
// .db files of that region beyond -keep. -region limits it to one region. A region that fails does not stop the
// others; the command still exits 1.
//
//	mftiko-etcd-backup -in deploy.json -secrets /etc/mftiko/etcd -dir /var/backups/mftiko/etcd
//
// -raise-epochs, after etcdutl snapshot restore and before any agent of the region or the controller starts again,
// adds -by to the epoch of every instance whose epoch lives in -region, the controller's included when that region is
// its home. -by is the --bump-revision of that restore (E12-D055); -region and -by are required.
//
//	mftiko-etcd-backup -in deploy.json -secrets /etc/mftiko/etcd -raise-epochs -region tw -by 1000000123
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// DefaultKeep is how many snapshots of each region are kept: three days of hourly ones (E12-D054).
const DefaultKeep = 72

type input struct {
	state.Topology
	Planes map[string]state.Plane `json:"planes"`
}

func main() {
	version := flag.Bool("version", false, "print the release and exit")
	in := flag.String("in", "-", "deployment JSON, the same file as mftiko-etcd-roles; - for stdin")
	secrets := flag.String("secrets", "", "directory holding root's etcd password in the file root")
	region := flag.String("region", "", "only this region; required with -raise-epochs")
	dir := flag.String("dir", "", "where snapshots go, one subdirectory per region")
	keep := flag.Int("keep", DefaultKeep, "snapshots kept per region")
	raise := flag.Bool("raise-epochs", false, "after a restore: raise the epochs whose key is in -region")
	by := flag.Int64("by", 0, "with -raise-epochs, the --bump-revision of the restore")
	flag.Parse()
	if *version {
		fmt.Println(buildinfo.String())
		return
	}
	var err error
	if *raise {
		err = runRaise(context.Background(), *in, *secrets, *region, *by, os.Stdin, os.Stdout)
	} else {
		err = runSave(context.Background(), *in, *secrets, *region, *dir, *keep, time.Now, os.Stdin, os.Stdout)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "mftiko-etcd-backup:", err)
		os.Exit(1)
	}
}

// runSave snapshots each region (or only region) into dir/<region>/ and prunes it to keep files.
func runSave(ctx context.Context, in, secrets, region, dir string, keep int, now func() time.Time, stdin io.Reader, out io.Writer) error {
	switch {
	case dir == "":
		return errors.New("-dir is required")
	case keep < 1:
		return fmt.Errorf("-keep %d: keep at least 1", keep)
	}
	d, rootPass, err := load(in, secrets, stdin)
	if err != nil {
		return err
	}
	regions, err := pick(d, region)
	if err != nil {
		return err
	}
	var errs []error
	for _, r := range regions {
		path, err := save(ctx, d.Regions[r].Endpoints, rootPass, filepath.Join(dir, r), now().UTC())
		if err == nil {
			fmt.Fprintf(out, "region %s: saved %s\n", r, path)
			err = prune(filepath.Join(dir, r), keep, out)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("region %s: %w", r, err))
		}
	}
	return errors.Join(errs...)
}

// snapshotName is the file name of a snapshot taken at t (UTC): names sort in time order.
func snapshotName(t time.Time) string { return t.Format("20060102T150405Z") + ".db" }

// save streams one snapshot into dir and returns its path. The file appears under its name only once complete and
// synced.
func save(ctx context.Context, endpoints []string, rootPass, dir string, at time.Time) (path string, err error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	c, err := dial(endpoints, rootPass)
	if err != nil {
		return "", err
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	rc, err := c.Snapshot(ctx)
	if err != nil {
		return "", err
	}
	defer rc.Close()

	path = filepath.Join(dir, snapshotName(at))
	part := path + ".part"
	f, err := os.OpenFile(part, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return "", err
	}
	defer func() {
		if err != nil {
			f.Close()
			os.Remove(part)
		}
	}()
	if _, err = io.Copy(f, rc); err != nil {
		return "", err
	}
	if err = f.Sync(); err != nil {
		return "", err
	}
	if err = f.Close(); err != nil {
		return "", err
	}
	if err = os.Rename(part, path); err != nil {
		return "", err
	}
	if df, derr := os.Open(dir); derr == nil {
		_ = df.Sync()
		df.Close()
	}
	return path, nil
}

// prune deletes the oldest snapshots in dir beyond keep. Only names snapshotName makes are counted.
func prune(dir string, keep int, out io.Writer) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	var snaps []string
	for _, e := range entries {
		if e.Type().IsRegular() && isSnapshotName(e.Name()) {
			snaps = append(snaps, e.Name())
		}
	}
	slices.Sort(snaps)
	for len(snaps) > keep {
		if err := os.Remove(filepath.Join(dir, snaps[0])); err != nil {
			return err
		}
		fmt.Fprintf(out, "deleted %s\n", filepath.Join(dir, snaps[0]))
		snaps = snaps[1:]
	}
	return nil
}

func isSnapshotName(name string) bool {
	stamp, ok := strings.CutSuffix(name, ".db")
	if !ok {
		return false
	}
	t, err := time.Parse("20060102T150405Z", stamp)
	return err == nil && snapshotName(t) == name
}

// runRaise raises, in region's cluster, the epoch of every instance whose epoch is kept there (E12-D056).
func runRaise(ctx context.Context, in, secrets, region string, by int64, stdin io.Reader, out io.Writer) error {
	switch {
	case region == "":
		return errors.New("-region is required with -raise-epochs")
	case by < 1:
		return errors.New("-by is required with -raise-epochs: the --bump-revision of the restore")
	}
	d, rootPass, err := load(in, secrets, stdin)
	if err != nil {
		return err
	}
	if _, err := pick(d, region); err != nil {
		return err
	}
	keys, err := epochKeys(d, region)
	if err != nil {
		return err
	}
	c, err := dial(d.Regions[region].Endpoints, rootPass)
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	for _, k := range keys {
		from, to, err := state.RaiseEpoch(ctx, c, k, by)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "%s: %d -> %d\n", k, from, to)
	}
	return nil
}

// epochKeys are the epoch keys in region's cluster, sorted: each agent instance of the region, and the controller's
// if region is its home. An instance without a plane in the deployment is an error.
func epochKeys(d input, region string) ([]string, error) {
	var keys []string
	for inst, r := range d.Instances {
		if r != region {
			continue
		}
		p := state.PlaneCtl
		if inst != state.ControllerInstance {
			var ok bool
			if p, ok = d.Planes[inst]; !ok || (p != state.PlaneSTS && p != state.PlaneMD && p != state.PlaneTD) {
				return nil, fmt.Errorf("instance %q has no sts, md or td plane in the deployment", inst)
			}
		}
		keys = append(keys, state.EpochKey(p, inst))
	}
	slices.Sort(keys)
	return keys, nil
}

func load(in, secrets string, stdin io.Reader) (input, string, error) {
	if secrets == "" {
		return input{}, "", errors.New("-secrets is required")
	}
	d, err := readDeployment(in, stdin)
	if err != nil {
		return input{}, "", err
	}
	b, err := os.ReadFile(filepath.Join(secrets, state.UserRoot))
	if err != nil {
		return input{}, "", err
	}
	p := strings.TrimSpace(string(b))
	if p == "" {
		return input{}, "", fmt.Errorf("%s is empty", filepath.Join(secrets, state.UserRoot))
	}
	return d, p, nil
}

// pick is region, checked against the deployment, or every region when it is "".
func pick(d input, region string) ([]string, error) {
	if region == "" {
		return slices.Sorted(maps.Keys(d.Regions)), nil
	}
	if _, ok := d.Regions[region]; !ok {
		return nil, fmt.Errorf("region %q is not in the deployment", region)
	}
	return []string{region}, nil
}

func dial(endpoints []string, rootPass string) (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: state.DialTimeout,
		Username: state.UserRoot, Password: rootPass, Logger: zap.NewNop()})
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
