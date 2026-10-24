// Command mftiko-pack packs a worker binary into an OCI artifact and pushes it
// (E09-D002, E09-D035–E09-D041, E09-D085–E09-D090, PROTOCOL §8). It does not build the binary.
//
//	mftiko-pack -bin FILE -name NAME -repo REGISTRY/REPOSITORY [-kind strategy|md.conn|td.account] [-tag TAG] [-env K=V]... [-plain-http]
//
// -kind defaults to strategy. Success prints the image manifest digest on stdout.
// For a strategy that digest is StrategyRef.code.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/BullionBear/mftiko/internal/platform/artifact"
	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
)

const usage = "usage: mftiko-pack -bin FILE -name NAME -repo REGISTRY/REPOSITORY [-kind strategy|md.conn|td.account] [-tag TAG] [-env K=V]... [-plain-http]"

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "mftiko-pack:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("mftiko-pack", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	version := fs.Bool("version", false, "print the release and exit")
	bin := fs.String("bin", "", "worker binary")
	kind := fs.String("kind", artifact.KindStrategy, "strategy, md.conn, or td.account")
	name := fs.String("name", "", "manifest name")
	repo := fs.String("repo", "", "registry/repository")
	tag := fs.String("tag", "", "optional tag; the digest does not depend on it")
	plain := fs.Bool("plain-http", false, "push over HTTP instead of HTTPS")
	var envArgs []string
	fs.Func("env", "K=V, repeatable", func(s string) error {
		envArgs = append(envArgs, s)
		return nil
	})
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return errors.New(usage)
		}
		return err
	}
	if *version {
		_, err := fmt.Fprintln(stdout, buildinfo.String())
		return err
	}
	if fs.NArg() != 0 || *bin == "" || *name == "" || *repo == "" {
		return errors.New(usage)
	}
	env, err := parseEnv(envArgs)
	if err != nil {
		return err
	}
	binary, err := os.ReadFile(*bin)
	if err != nil {
		return err
	}
	img, err := pack(*kind, binary, *name, env)
	if err != nil {
		return err
	}
	if err := artifact.Push(context.Background(), img, artifact.Destination{
		Repository: *repo,
		Tag:        *tag,
		PlainHTTP:  *plain,
	}); err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, img.Digest)
	return err
}

func pack(kind string, binary []byte, name string, env map[string]string) (artifact.Image, error) {
	switch kind {
	case artifact.KindStrategy:
		return artifact.PackStrategy(binary, name, env)
	case artifact.KindMDConn, artifact.KindTDAccount:
		return artifact.PackWorker(binary, kind, name, env)
	default:
		return artifact.Image{}, fmt.Errorf("kind %q is not strategy, md.conn, or td.account", kind)
	}
}

func parseEnv(args []string) (map[string]string, error) {
	env := make(map[string]string, len(args))
	for _, s := range args {
		k, v, ok := strings.Cut(s, "=")
		if !ok || k == "" {
			return nil, fmt.Errorf("env %q is not K=V", s)
		}
		if _, dup := env[k]; dup {
			return nil, fmt.Errorf("env %s is set twice", k)
		}
		env[k] = v
	}
	return env, nil
}
