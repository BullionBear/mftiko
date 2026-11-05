// Command mftiko-account-import moves accounts from etcd into Postgres (E21-10, #522).
//
// Before E21, an account lived only in etcd at /td/{inst}/account/{api_id}. Its credentials were
// sealed to that TD instance's public key, so only the TD host can open them. After E21 the
// Postgres accounts table is the authority, and the credentials there are AEAD under the API's
// MFTIKO_ACCOUNT_KEYS (E21-D002, E21-D003). This command bridges the two. The api_id is kept, so
// session records and PMS history still point at the same account.
//
// It has two steps. No long-lived secret leaves its host:
//
//	mftiko-seal keygen -key /root/mftiko/transfer.key        (API host: a one-off transfer key pair)
//	mftiko-account-import export -topology F -instance td-1 -seal-key K -transfer-pubkey @PUB -out td-1.json
//	                                                          (TD host: open with the TD key, reseal to the transfer key)
//	mftiko-account-import import -transfer-key /root/mftiko/transfer.key -uids uids.json [-apply] td-1.json ...
//	                                                          (API host: open with the transfer key, AEAD, insert)
//
// export reads etcd as -etcd-user (default: the TD instance), password $MFTIKO_ETCD_PASSWORD. It
// writes nothing to etcd. The export file holds names, kinds and two sealed blobs per account:
// the etcd bytes as they are, and the credentials sealed to the transfer key. It never holds
// plaintext. An account whose credentials do not open is listed under skipped with the reason.
//
// import reads $MFTIKO_POSTGRES_DSN and $MFTIKO_ACCOUNT_KEYS (the API's env). Without -apply it
// only prints the plan. With -apply it writes every account in one transaction: an accounts row
// (revision 1, the operator's exch_account_uid) and an account_replicas row holding the etcd bytes
// unchanged, so the API's reconcile neither rewrites nor warns about the existing etcd key
// (E21-D055). An api_id already in Postgres with the same kind is left alone; a replica row is
// added if it is missing. The schema must already be at 0011 or later.
//
// The uids file is a JSON object from api_id (string) to exch_account_uid. Every imported api_id
// needs one; the plan says which are missing and import refuses to apply until they are there.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
)

const usage = "usage: mftiko-account-import export|import [flags], or -version"

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "mftiko-account-import:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	switch args[0] {
	case "-version", "--version":
		_, err := fmt.Fprintln(stdout, buildinfo.String())
		return err
	case "export":
		return exportMain(args[1:], stdout, stderr)
	case "import":
		return importMain(args[1:], stdout)
	}
	return errors.New(usage)
}

// fileVersion is the export file format. import refuses any other.
const fileVersion = 1

// exportFile is what export writes and import reads. One file is one TD instance.
type exportFile struct {
	Version    int             `json:"version"`
	TDInstance string          `json:"td_instance"`
	ExportedAt string          `json:"exported_at"`
	Accounts   []exportAccount `json:"accounts"`
	Skipped    []skipped       `json:"skipped"`
}

type exportAccount struct {
	APIID  int64  `json:"api_id"`
	Name   string `json:"name"`
	Kind   string `json:"kind"`
	Cancel uint32 `json:"cancel_on_disconnect_s"`
	// Enabled is what etcd held. The API stores every account enabled; import reports a false.
	Enabled bool `json:"enabled"`
	// Replica is state.v1.Account.sealed_credentials from etcd, unchanged (sealed to the TD key).
	Replica []byte `json:"replica"`
	// Transfer is the same credentials sealed to the transfer key.
	Transfer []byte `json:"transfer"`
}

type skipped struct {
	Key    string `json:"key"`
	APIID  int64  `json:"api_id,omitempty"`
	Reason string `json:"reason"`
}

func writeNew(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func readExport(path string) (exportFile, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return exportFile{}, err
	}
	var f exportFile
	if err := json.Unmarshal(b, &f); err != nil {
		return exportFile{}, fmt.Errorf("%s: %w", path, err)
	}
	if f.Version != fileVersion {
		return exportFile{}, fmt.Errorf("%s: version %d, want %d", path, f.Version, fileVersion)
	}
	if f.TDInstance == "" {
		return exportFile{}, fmt.Errorf("%s: td_instance is empty", path)
	}
	return f, nil
}
