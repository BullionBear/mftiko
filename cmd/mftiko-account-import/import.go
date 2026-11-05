package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/protobuf/proto"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/accountkey"
	"github.com/BullionBear/mftiko/internal/platform/sealbox"
)

type importOptions struct {
	dsn      string
	keys     string
	transfer string
	uids     string
	apply    bool
	files    []string
}

func importMain(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("import", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var o importOptions
	fs.StringVar(&o.dsn, "database", os.Getenv("MFTIKO_POSTGRES_DSN"), "Postgres DSN (default $MFTIKO_POSTGRES_DSN)")
	fs.StringVar(&o.transfer, "transfer-key", "", "transfer private key (mftiko-seal keygen)")
	fs.StringVar(&o.uids, "uids", "", `JSON object {"<api_id>": "<exch_account_uid>"}`)
	fs.BoolVar(&o.apply, "apply", false, "write; without it the plan is printed and rolled back")
	if err := fs.Parse(args); err != nil {
		return err
	}
	o.files = fs.Args()
	// The key ring is never a flag: it would show in the process list.
	o.keys = os.Getenv("MFTIKO_ACCOUNT_KEYS")
	switch {
	case o.transfer == "" || o.uids == "" || len(o.files) == 0:
		return errors.New("import needs -transfer-key, -uids and at least one export file")
	case o.dsn == "":
		return errors.New("database DSN is empty: set MFTIKO_POSTGRES_DSN or -database")
	}
	box, err := accountkey.Parse(o.keys)
	if err != nil {
		return fmt.Errorf("MFTIKO_ACCOUNT_KEYS: %w", err)
	}
	key, err := sealbox.LoadKeyFile(o.transfer)
	if err != nil {
		return err
	}
	uids, err := readUIDs(o.uids)
	if err != nil {
		return err
	}
	var files []exportFile
	for _, p := range o.files {
		f, err := readExport(p)
		if err != nil {
			return err
		}
		files = append(files, f)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, err := pgx.Connect(ctx, o.dsn)
	if err != nil {
		// pgx can quote the DSN, password included.
		return errors.New("connect to Postgres failed")
	}
	defer conn.Close(context.Background())
	return importAccounts(ctx, conn, importInput{
		Box: box, Transfer: key, UIDs: uids, Files: files, Apply: o.apply, Now: time.Now(),
	}, stdout)
}

func readUIDs(path string) (map[int64]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var raw map[string]string
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, fmt.Errorf("uids: want a JSON object of strings: %w", err)
	}
	out := make(map[int64]string, len(raw))
	for k, v := range raw {
		id, err := strconv.ParseInt(k, 10, 64)
		if err != nil || id < 1 || id > maxAPIID {
			return nil, fmt.Errorf("uids: %q is not an api_id", k)
		}
		out[id] = v
	}
	return out, nil
}

type importInput struct {
	Box      accountkey.Box
	Transfer *sealbox.PrivateKey
	UIDs     map[int64]string
	Files    []exportFile
	Apply    bool
	Now      time.Time
}

// planRow is one (td_instance, api_id). Action is what -apply does; Problem stops the whole run.
type planRow struct {
	Inst    string
	ID      int64
	Kind    string
	Name    string
	UID     string
	Action  string
	Problem string
	Note    string
}

type group struct {
	id    int64
	insts []string
	accts []exportAccount
}

// importAccounts runs the whole import in one transaction. It commits only with Apply and no
// problem. Without Apply the same statements run and are rolled back, so the plan includes what
// the database would refuse.
func importAccounts(ctx context.Context, conn *pgx.Conn, in importInput, w io.Writer) error {
	if in.Box.Current() == 0 {
		return errors.New("MFTIKO_ACCOUNT_KEYS has no key")
	}
	var reg *string
	if err := conn.QueryRow(ctx, `SELECT to_regclass('account_replicas')::text`).Scan(&reg); err != nil {
		return fmt.Errorf("read schema: %w", err)
	}
	if reg == nil {
		return errors.New("account_replicas does not exist: run the new release's mftiko-migrate up first (0011)")
	}

	groups := map[int64]*group{}
	var skippedAll []string
	for _, f := range in.Files {
		for _, a := range f.Accounts {
			g := groups[a.APIID]
			if g == nil {
				g = &group{id: a.APIID}
				groups[a.APIID] = g
			}
			g.insts = append(g.insts, f.TDInstance)
			g.accts = append(g.accts, a)
		}
		for _, s := range f.Skipped {
			skippedAll = append(skippedAll, fmt.Sprintf("%s: %s (%s)", f.TDInstance, s.Key, s.Reason))
		}
	}
	ids := make([]int64, 0, len(groups))
	for id := range groups {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	var plan []planRow
	failed := false
	for _, id := range ids {
		rows, err := importGroup(ctx, tx, in, groups[id])
		if err != nil {
			return err
		}
		for _, r := range rows {
			if r.Problem != "" {
				failed = true
			}
		}
		plan = append(plan, rows...)
	}
	printPlan(w, plan, skippedAll)
	switch {
	case failed:
		return errors.New("nothing was written: fix the problems above and run again")
	case !in.Apply:
		_, err := fmt.Fprintln(w, "dry run: nothing was written; run again with -apply")
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	_, err = fmt.Fprintf(w, "imported: %d account(s) checked, committed\n", len(ids))
	return err
}

// importGroup plans and stages one api_id. Every TD instance that held it gets a replica row.
// A returned error is a database failure; account problems go in the rows.
func importGroup(ctx context.Context, tx pgx.Tx, in importInput, g *group) ([]planRow, error) {
	first := g.accts[0]
	rows := make([]planRow, len(g.insts))
	for i, inst := range g.insts {
		rows[i] = planRow{Inst: inst, ID: g.id, Kind: g.accts[i].Kind, Name: g.accts[i].Name}
	}
	fail := func(msg string) []planRow {
		for i := range rows {
			rows[i].Problem = msg
		}
		return rows
	}
	for i, a := range g.accts {
		if a.Kind != first.Kind || a.Name != first.Name {
			return fail(fmt.Sprintf("%s and %s disagree on kind or name", g.insts[0], g.insts[i])), nil
		}
		if len(a.Replica) < minSealed {
			return fail("replica blob is too short"), nil
		}
	}
	uid, ok := in.UIDs[g.id]
	uid = strings.TrimSpace(uid)
	switch {
	case !ok || uid == "":
		return fail("exch_account_uid missing in -uids"), nil
	case utf8.RuneCountInString(uid) > 64:
		return fail("exch_account_uid is longer than 64 characters"), nil
	}
	if n := utf8.RuneCountInString(first.Name); n < 1 || n > 128 {
		return fail("name must be 1 to 128 characters"), nil
	}
	for i := range rows {
		rows[i].UID = uid
	}

	var (
		curKind string
		deleted *time.Time
	)
	err := tx.QueryRow(ctx, `SELECT kind, deleted_at FROM accounts WHERE api_id = $1 FOR UPDATE`, g.id).Scan(&curKind, &deleted)
	exists := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("read account %d: %w", g.id, err)
	}
	switch {
	case exists && curKind != first.Kind:
		return fail("Postgres already has this api_id with kind " + curKind), nil
	case exists && deleted != nil:
		for i := range rows {
			rows[i].Action = "skip"
			rows[i].Note = "deleted in Postgres"
		}
		return rows, nil
	}

	if !exists {
		ct, ver, problem := aeadFromTransfer(in, g.id, first)
		if problem != "" {
			return fail(problem), nil
		}
		settings, _ := json.Marshal(struct {
			Cancel uint32 `json:"cancel_on_disconnect_s"`
		}{Cancel: first.Cancel})
		_, err := tx.Exec(ctx, `SAVEPOINT acct`)
		if err != nil {
			return nil, err
		}
		_, err = tx.Exec(ctx, `
INSERT INTO accounts (
    api_id, kind, name, credentials, key_version, settings, enabled,
    exch_account_uid, created_at, updated_at, revision
) VALUES ($1, $2, $3, $4, $5, $6, true, $7, $8, $8, 1)`,
			g.id, first.Kind, first.Name, ct, ver, settings, uid, in.Now)
		clear(ct)
		if err != nil {
			if _, rerr := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT acct`); rerr != nil {
				return nil, rerr
			}
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.ConstraintName == "accounts_name_live" {
				return fail("name is already used by a live account"), nil
			}
			if errors.As(err, &pgErr) && pgErr.ConstraintName != "" {
				return fail("refused by " + pgErr.ConstraintName), nil
			}
			return nil, fmt.Errorf("insert account %d: %w", g.id, err)
		}
	} else {
		// Only check that the transfer blob opens; the stored row stays as it is.
		ct, _, problem := aeadFromTransfer(in, g.id, first)
		if problem != "" {
			return fail(problem), nil
		}
		clear(ct)
	}

	for i, inst := range g.insts {
		if !g.accts[i].Enabled {
			rows[i].Note = "disabled in etcd; the API stores it enabled"
		}
		tag, err := tx.Exec(ctx, `
INSERT INTO account_replicas (td_instance, api_id, sealed_credentials, created_at, updated_at)
VALUES ($1, $2, $3, $4, $4)
ON CONFLICT (td_instance, api_id) DO NOTHING`, inst, g.id, g.accts[i].Replica, in.Now)
		if err != nil {
			return nil, fmt.Errorf("insert replica %s/%d: %w", inst, g.id, err)
		}
		switch {
		case !exists:
			rows[i].Action = "insert"
		case tag.RowsAffected() == 1:
			rows[i].Action = "add replica"
		default:
			rows[i].Action = "exists"
		}
	}
	return rows, nil
}

// minSealed is the shortest sealbox: 9-byte header plus NaCl's anonymous overhead (migration 0011).
const minSealed = 57

// aeadFromTransfer opens the transfer blob and encrypts the same credentials for Postgres, as the
// API's create does (E21-D019). Every opened transfer blob of the group must agree on api_id and
// kind; sealbox checks both.
func aeadFromTransfer(in importInput, id int64, a exportAccount) (ct []byte, ver int64, problem string) {
	creds, err := sealbox.OpenCredentials(in.Transfer, a.Transfer, id, a.Kind)
	if err != nil {
		return nil, 0, "transfer blob: " + sealbox.Reason(err)
	}
	plain, err := proto.Marshal(&statev1.AccountCredentials{
		ApiId: id, Kind: a.Kind, KeyType: creds.KeyType,
		Key: creds.Key, Secret: creds.Secret, Passphrase: creds.Passphrase,
	})
	if err != nil {
		return nil, 0, "credentials do not encode"
	}
	defer clear(plain)
	ct, err = in.Box.Seal(id, a.Kind, plain)
	if err != nil {
		return nil, 0, "AEAD: " + err.Error()
	}
	v, err := accountkey.HeaderVersion(ct)
	if err != nil || len(ct) < accountkey.MinCiphertext {
		clear(ct)
		return nil, 0, "AEAD output is unreadable"
	}
	return ct, int64(v), ""
}

func printPlan(w io.Writer, plan []planRow, skipped []string) {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "TD_INSTANCE\tAPI_ID\tKIND\tNAME\tEXCH_ACCOUNT_UID\tACTION\tNOTE")
	for _, r := range plan {
		action, note := r.Action, r.Note
		if r.Problem != "" {
			action, note = "PROBLEM", r.Problem
		}
		fmt.Fprintf(tw, "%s\t%d\t%s\t%s\t%s\t%s\t%s\n", r.Inst, r.ID, r.Kind, r.Name, r.UID, action, note)
	}
	tw.Flush()
	for _, s := range skipped {
		fmt.Fprintf(w, "not exported, re-register by hand: %s\n", s)
	}
}
