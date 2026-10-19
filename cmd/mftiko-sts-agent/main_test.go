package main

import (
	"testing"
	"time"

	"github.com/BullionBear/mftiko/internal/sts/agent"
)

func TestParseArgsVersion(t *testing.T) {
	_, version, err := parseArgs([]string{"-version"})
	if err != nil || !version {
		t.Fatalf("version=%v err=%v", version, err)
	}
}

func TestParseArgsDefaults(t *testing.T) {
	o, version, err := parseArgs([]string{
		"-instance", "sts-tw-1",
		"-topology", "topo.json",
		"-etcd-password", "secret",
		"-work-dir", "/tmp/sts",
		"-shim", "/usr/bin/mftiko-shim",
		"-worker", "/usr/bin/mftiko-worker",
		"-max-restarts", "9",
	})
	if err != nil || version {
		t.Fatalf("version=%v err=%v", version, err)
	}
	if o.instance != "sts-tw-1" || o.worker != "/usr/bin/mftiko-worker" || o.password != "secret" {
		t.Fatalf("options %+v", o)
	}
	if o.maxArtifactBytes != agent.DefaultMaxArtifactBytes ||
		o.recvWindowMS != agent.DefaultRecvWindowMS ||
		o.hookTimeoutS != agent.DefaultHookTimeoutS ||
		o.startTimeoutS != agent.DefaultStartTimeoutS ||
		o.volumeMB != agent.DefaultVolumeMB ||
		o.grace != agent.DefaultGrace ||
		o.restartWindowS != agent.DefaultRestartWindowS ||
		o.maxRestarts != 9 ||
		o.plainHTTP {
		t.Fatalf("defaults %+v, grace %s", o, o.grace)
	}
	if agent.DefaultGrace != 10*time.Second {
		t.Fatalf("grace default %s", agent.DefaultGrace)
	}
}

func TestParseArgsRequiresWorker(t *testing.T) {
	_, _, err := parseArgs([]string{
		"-instance", "sts-tw-1",
		"-topology", "topo.json",
		"-etcd-password", "secret",
		"-work-dir", "/tmp/sts",
		"-shim", "/usr/bin/mftiko-shim",
	})
	if err == nil {
		t.Fatal("missing -worker was accepted")
	}
}
