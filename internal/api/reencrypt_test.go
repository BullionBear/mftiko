package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/accountkey"
	"github.com/BullionBear/mftiko/internal/platform/pgtest"
	"github.com/BullionBear/mftiko/migrations"
)

const (
	reencryptKind   = "Binance"
	reencryptKey    = "kkkkkkkkkkkkkkkk"
	reencryptSecret = "ssssssssssssssss"
)

var reencryptStamp = time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)

func reencryptMaterial(b byte) string {
	return base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{b}, 32))
}

func reencryptV1() string { return "v1:" + reencryptMaterial(0x11) }
func reencryptV2() string { return "v2:" + reencryptMaterial(0x22) }
func reencryptV3() string { return "v3:" + reencryptMaterial(0x33) }

func reencryptRing(t *testing.T, parts ...string) accountkey.Box {
	t.Helper()
	box, err := accountkey.Parse(strings.Join(parts, ","))
	if err != nil {
		t.Fatal(err)
	}
	return box
}

func reencryptPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	srv := pgtest.Start(t)
	migCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	if _, err := migrations.Up(migCtx, srv.DSN()); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(srv.DSN())
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 8
	cfg.MinConns = 0
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func reencryptCiphertext(t *testing.T, box accountkey.Box, id int64) []byte {
	t.Helper()
	plain, err := proto.Marshal(&statev1.AccountCredentials{
		ApiId: id, Kind: reencryptKind, KeyType: "hmac", Key: reencryptKey, Secret: reencryptSecret,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear(plain)
	ct, err := box.Seal(id, reencryptKind, plain)
	if err != nil {
		t.Fatal(err)
	}
	return ct
}

func insertReencryptRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id int64, ct []byte, ver int64, deleted bool) {
	t.Helper()
	var deletedAt *time.Time
	if deleted {
		stamp := reencryptStamp
		deletedAt = &stamp
	}
	sealed := bytes.Repeat([]byte{0xab}, minSealedCredentials)
	_, err := pool.Exec(ctx, `
		INSERT INTO accounts (
			api_id, kind, name, credentials, key_version, settings, enabled,
			exch_account_uid, created_at, updated_at, deleted_at, revision
		) VALUES (
			$1, $2, $3, $4, $5, '{"cancel_on_disconnect_s":0}', true,
			'uid', $6, $6, $7, 1
		)`, id, reencryptKind, fmt.Sprintf("n%d", id), ct, ver, reencryptStamp, deletedAt)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO account_replicas (td_instance, api_id, sealed_credentials, created_at, updated_at)
		VALUES ('td-tw-1', $1, $2, $3, $3)`, id, sealed, reencryptStamp)
	if err != nil {
		t.Fatal(err)
	}
}

func insertReencryptAccount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, box accountkey.Box, id int64, deleted bool) []byte {
	t.Helper()
	ct := reencryptCiphertext(t, box, id)
	ver, err := accountkey.HeaderVersion(ct)
	if err != nil {
		t.Fatal(err)
	}
	insertReencryptRow(t, ctx, pool, id, ct, int64(ver), deleted)
	return append([]byte(nil), ct...)
}

type reencryptStored struct {
	name    string
	ct      []byte
	ver     int64
	rev     int64
	updated time.Time
	sealed  []byte
	deleted *time.Time
}

func readReencryptAccount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id int64) reencryptStored {
	t.Helper()
	var s reencryptStored
	err := pool.QueryRow(ctx, `
		SELECT a.name, a.credentials, a.key_version, a.revision, a.updated_at, r.sealed_credentials, a.deleted_at
		FROM accounts a
		JOIN account_replicas r ON r.api_id = a.api_id AND r.td_instance = 'td-tw-1'
		WHERE a.api_id = $1`, id).Scan(
		&s.name, &s.ct, &s.ver, &s.rev, &s.updated, &s.sealed, &s.deleted)
	if err != nil {
		t.Fatal(err)
	}
	s.ct = append([]byte(nil), s.ct...)
	s.sealed = append([]byte(nil), s.sealed...)
	return s
}

func openReencrypt(t *testing.T, box accountkey.Box, id int64, ct []byte) {
	t.Helper()
	plain, err := box.Open(id, reencryptKind, ct)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(plain)
	var cred statev1.AccountCredentials
	if err := proto.Unmarshal(plain, &cred); err != nil {
		t.Fatal(err)
	}
	if cred.GetApiId() != id || cred.GetKind() != reencryptKind || cred.GetKeyType() != "hmac" ||
		cred.GetKey() != reencryptKey || cred.GetSecret() != reencryptSecret {
		t.Fatal("plaintext mismatch")
	}
}

func assertReencryptQuiet(t *testing.T, log string, blobs ...[]byte) {
	t.Helper()
	if strings.Contains(log, reencryptSecret) || strings.Contains(log, reencryptKey) {
		t.Fatal("log leaked credential material")
	}
	for _, b := range blobs {
		if len(b) == 0 {
			continue
		}
		enc := []string{
			string(b),
			base64.StdEncoding.EncodeToString(b),
			base64.RawStdEncoding.EncodeToString(b),
			base64.URLEncoding.EncodeToString(b),
			base64.RawURLEncoding.EncodeToString(b),
			hex.EncodeToString(b),
		}
		for _, e := range enc {
			if e != "" && strings.Contains(log, e) {
				t.Fatal("log leaked credential material")
			}
		}
	}
}

func assertReencryptSameMeta(t *testing.T, before, after reencryptStored) {
	t.Helper()
	if after.rev != before.rev || after.name != before.name || !after.updated.Equal(before.updated) ||
		!bytes.Equal(after.sealed, before.sealed) || (before.deleted == nil) != (after.deleted == nil) {
		t.Fatalf("row metadata changed")
	}
	if before.deleted != nil && !before.deleted.Equal(*after.deleted) {
		t.Fatal("deleted_at changed")
	}
}

func TestReencryptOldThenCurrent(t *testing.T) {
	pool := reencryptPool(t)
	ctx := t.Context()
	v1 := reencryptRing(t, reencryptV1())
	rotated := reencryptRing(t, reencryptV2(), reencryptV1())

	insertReencryptAccount(t, ctx, pool, v1, 1, false)
	insertReencryptAccount(t, ctx, pool, v1, 2, true)
	insertReencryptAccount(t, ctx, pool, rotated, 3, false)
	staleHeader := reencryptCiphertext(t, rotated, 4)
	insertReencryptRow(t, ctx, pool, 4, staleHeader, 1, false)

	before := map[int64]reencryptStored{}
	for _, id := range []int64{1, 2, 3, 4} {
		before[id] = readReencryptAccount(t, ctx, pool, id)
	}
	if before[3].ver != 2 || before[4].ver != 1 {
		t.Fatalf("setup versions %d %d", before[3].ver, before[4].ver)
	}
	openReencrypt(t, rotated, 1, before[1].ct)
	openReencrypt(t, rotated, 2, before[2].ct)
	openReencrypt(t, rotated, 4, before[4].ct)

	w := newCredentialRewriter(pool, rotated, zerolog.Nop())
	tick, err := w.tick(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if tick.rewritten != 3 || tick.remaining != 0 || tick.old != "" || tick.more {
		t.Fatalf("tick %+v", tick)
	}
	for _, id := range []int64{1, 2, 4} {
		got := readReencryptAccount(t, ctx, pool, id)
		assertReencryptSameMeta(t, before[id], got)
		if got.ver != 2 || bytes.Equal(got.ct, before[id].ct) {
			t.Fatalf("id %d was not rewritten", id)
		}
		hv, err := accountkey.HeaderVersion(got.ct)
		if err != nil || hv != 2 || got.rev != 1 {
			t.Fatalf("id %d header %d rev %d err %v", id, hv, got.rev, err)
		}
		openReencrypt(t, rotated, id, got.ct)
		if _, err := v1.Open(id, reencryptKind, got.ct); !errors.Is(err, accountkey.ErrUnknownVersion) {
			t.Fatalf("id %d old key open %v", id, err)
		}
		tag, err := pool.Exec(ctx, `UPDATE accounts SET name = name WHERE api_id = $1 AND revision = 1`, id)
		if err != nil || tag.RowsAffected() != 1 {
			t.Fatalf("id %d cas %v %v", id, tag, err)
		}
	}
	got := readReencryptAccount(t, ctx, pool, 3)
	assertReencryptSameMeta(t, before[3], got)
	if got.ver != 2 || !bytes.Equal(got.ct, before[3].ct) {
		t.Fatal("current ciphertext changed")
	}
	openReencrypt(t, rotated, 3, got.ct)
}

func TestReencryptSkipLockedNoMiss(t *testing.T) {
	pool := reencryptPool(t)
	ctx := t.Context()
	v1 := reencryptRing(t, reencryptV1())
	rotated := reencryptRing(t, reencryptV2(), reencryptV1())
	for id := int64(1); id <= 6; id++ {
		insertReencryptAccount(t, ctx, pool, v1, id, false)
	}
	before := map[int64]reencryptStored{}
	for id := int64(1); id <= 6; id++ {
		before[id] = readReencryptAccount(t, ctx, pool, id)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	holds, err := tx.Query(ctx, `
		SELECT api_id FROM accounts
		WHERE api_id = ANY($1::bigint[])
		ORDER BY api_id
		FOR UPDATE`, []int64{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	var locked []int64
	for holds.Next() {
		var id int64
		if err := holds.Scan(&id); err != nil {
			holds.Close()
			t.Fatal(err)
		}
		locked = append(locked, id)
	}
	holds.Close()
	if err := holds.Err(); err != nil || len(locked) != 3 {
		t.Fatalf("locked %v %v", locked, err)
	}

	bctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	w := newCredentialRewriter(pool, rotated, zerolog.Nop())
	selected, ids, err := w.batch(bctx, rotated.Current(), knownOldVersions(rotated))
	if err != nil {
		t.Fatalf("batch while rows were locked: %v", err)
	}
	if selected != 3 || fmt.Sprint(ids) != "[4 5 6]" {
		t.Fatalf("selected %d ids %v", selected, ids)
	}
	for id := int64(1); id <= 3; id++ {
		got := readReencryptAccount(t, ctx, pool, id)
		if !bytes.Equal(got.ct, before[id].ct) || got.ver != before[id].ver {
			t.Fatalf("locked id %d changed", id)
		}
	}
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	selected, ids, err = w.batch(ctx, rotated.Current(), knownOldVersions(rotated))
	if err != nil {
		t.Fatal(err)
	}
	if selected != 3 || fmt.Sprint(ids) != "[1 2 3]" {
		t.Fatalf("after unlock selected %d ids %v", selected, ids)
	}
	seen := map[int64]int{}
	for _, id := range []int64{4, 5, 6, 1, 2, 3} {
		seen[id]++
	}
	for id := int64(1); id <= 6; id++ {
		if seen[id] != 1 {
			t.Fatalf("id %d seen %d", id, seen[id])
		}
		got := readReencryptAccount(t, ctx, pool, id)
		assertReencryptSameMeta(t, before[id], got)
		if got.ver != 2 {
			t.Fatalf("id %d version %d", id, got.ver)
		}
		openReencrypt(t, rotated, id, got.ct)
	}
}

func TestReencryptConcurrentWorkers(t *testing.T) {
	pool := reencryptPool(t)
	ctx := t.Context()
	v1 := reencryptRing(t, reencryptV1())
	rotated := reencryptRing(t, reencryptV2(), reencryptV1())
	const n = 36
	for id := int64(1); id <= n; id++ {
		insertReencryptAccount(t, ctx, pool, v1, id, false)
	}
	current := insertReencryptAccount(t, ctx, pool, rotated, 100, false)
	before := map[int64]reencryptStored{}
	for id := int64(1); id <= n; id++ {
		before[id] = readReencryptAccount(t, ctx, pool, id)
	}
	before[100] = readReencryptAccount(t, ctx, pool, 100)

	workCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	var mu sync.Mutex
	var ids []int64
	var fail error
	setFail := func(err error) {
		if err == nil {
			return
		}
		mu.Lock()
		if fail == nil {
			fail = err
		}
		mu.Unlock()
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			w := newCredentialRewriter(pool, rotated, zerolog.Nop())
			w.page = 4
			known := knownOldVersions(rotated)
			for {
				if workCtx.Err() != nil {
					setFail(workCtx.Err())
					return
				}
				selected, got, err := w.batch(workCtx, rotated.Current(), known)
				if err != nil {
					if workCtx.Err() != nil {
						setFail(workCtx.Err())
						return
					}
					setFail(err)
					return
				}
				mu.Lock()
				ids = append(ids, got...)
				mu.Unlock()
				if selected == 0 {
					left, ok := accountKeyOldCount(workCtx, pool, rotated.Current())
					if ok && left == 0 {
						return
					}
					select {
					case <-workCtx.Done():
						setFail(workCtx.Err())
						return
					case <-time.After(time.Millisecond):
					}
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	if fail != nil {
		t.Fatal(fail)
	}
	got := map[int64]int{}
	for _, id := range ids {
		got[id]++
	}
	for id := int64(1); id <= n; id++ {
		if got[id] != 1 {
			t.Fatalf("id %d count %d", id, got[id])
		}
		row := readReencryptAccount(t, ctx, pool, id)
		assertReencryptSameMeta(t, before[id], row)
		if row.ver != 2 {
			t.Fatalf("id %d version %d", id, row.ver)
		}
		openReencrypt(t, rotated, id, row.ct)
	}
	if got[100] != 0 {
		t.Fatalf("current row rewritten %d", got[100])
	}
	row := readReencryptAccount(t, ctx, pool, 100)
	if !bytes.Equal(row.ct, current) || row.ver != 2 {
		t.Fatal("current ciphertext changed")
	}
}

func TestReencryptUnknownVersionStays(t *testing.T) {
	pool := reencryptPool(t)
	ctx := t.Context()
	v1 := reencryptRing(t, reencryptV1())
	v3 := reencryptRing(t, reencryptV3())
	insertReencryptAccount(t, ctx, pool, v1, 1, false)
	insertReencryptAccount(t, ctx, pool, v1, 2, true)
	insertReencryptAccount(t, ctx, pool, v3, 3, false)
	insertReencryptAccount(t, ctx, pool, v3, 4, false)
	before := map[int64]reencryptStored{}
	for id := int64(1); id <= 4; id++ {
		before[id] = readReencryptAccount(t, ctx, pool, id)
	}

	buf := &lockedBuf{}
	partial := reencryptRing(t, reencryptV2(), reencryptV3())
	w := newCredentialRewriter(pool, partial, zerolog.New(buf))
	tick, err := w.tick(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if tick.rewritten != 2 || tick.remaining != 2 || tick.old != "1=2" {
		t.Fatalf("tick %+v", tick)
	}
	for _, id := range []int64{1, 2} {
		got := readReencryptAccount(t, ctx, pool, id)
		if got.ver != 1 || !bytes.Equal(got.ct, before[id].ct) {
			t.Fatalf("id %d changed", id)
		}
	}
	for _, id := range []int64{3, 4} {
		got := readReencryptAccount(t, ctx, pool, id)
		assertReencryptSameMeta(t, before[id], got)
		if got.ver != 2 {
			t.Fatalf("id %d version %d", id, got.ver)
		}
		openReencrypt(t, partial, id, got.ct)
	}
	mid := map[int64]reencryptStored{}
	for id := int64(1); id <= 4; id++ {
		mid[id] = readReencryptAccount(t, ctx, pool, id)
	}

	stale := newCredentialRewriter(pool, v1, zerolog.New(buf))
	tick, err = stale.tick(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if tick.rewritten != 0 || tick.remaining != 2 || tick.old != "2=2" {
		t.Fatalf("stale tick %+v", tick)
	}
	for id := int64(1); id <= 4; id++ {
		got := readReencryptAccount(t, ctx, pool, id)
		if got.ver != mid[id].ver || !bytes.Equal(got.ct, mid[id].ct) {
			t.Fatalf("stale replica changed id %d", id)
		}
	}

	restored := reencryptRing(t, reencryptV2(), reencryptV1())
	w = newCredentialRewriter(pool, restored, zerolog.New(buf))
	tick, err = w.tick(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if tick.rewritten != 2 || tick.remaining != 0 || tick.old != "" {
		t.Fatalf("restored tick %+v", tick)
	}
	var blobs [][]byte
	for id := int64(1); id <= 4; id++ {
		got := readReencryptAccount(t, ctx, pool, id)
		blobs = append(blobs, before[id].ct, got.ct)
		assertReencryptSameMeta(t, before[id], got)
		if got.ver != 2 {
			t.Fatalf("id %d version %d", id, got.ver)
		}
		openReencrypt(t, restored, id, got.ct)
		if _, err := v1.Open(id, reencryptKind, got.ct); !errors.Is(err, accountkey.ErrUnknownVersion) {
			t.Fatalf("id %d old key %v", id, err)
		}
	}
	assertReencryptQuiet(t, buf.String(), blobs...)
}

func TestReencryptCorruptDoesNotBlock(t *testing.T) {
	pool := reencryptPool(t)
	ctx := t.Context()
	v1 := reencryptRing(t, reencryptV1())
	rotated := reencryptRing(t, reencryptV2(), reencryptV1())
	var corrupt [][]byte
	for id := int64(1); id <= 20; id++ {
		ct := reencryptCiphertext(t, v1, id)
		ct[len(ct)-1] ^= 0xff
		ver, err := accountkey.HeaderVersion(ct)
		if err != nil || ver != 1 {
			t.Fatalf("header %d %v", ver, err)
		}
		insertReencryptRow(t, ctx, pool, id, ct, 1, false)
		corrupt = append(corrupt, append([]byte(nil), ct...))
	}
	good := insertReencryptAccount(t, ctx, pool, v1, 21, false)

	buf := &lockedBuf{}
	w := newCredentialRewriter(pool, rotated, zerolog.New(buf))
	tick, err := w.tick(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if tick.rewritten != 1 || tick.remaining != 20 || tick.old != "1=20" {
		t.Fatalf("tick %+v", tick)
	}
	for id := int64(1); id <= 20; id++ {
		got := readReencryptAccount(t, ctx, pool, id)
		if got.ver != 1 || !bytes.Equal(got.ct, corrupt[id-1]) || got.rev != 1 {
			t.Fatalf("corrupt id %d changed", id)
		}
	}
	got := readReencryptAccount(t, ctx, pool, 21)
	if got.ver != 2 || bytes.Equal(got.ct, good) || got.rev != 1 {
		t.Fatal("good row was not rewritten")
	}
	openReencrypt(t, rotated, 21, got.ct)
	warns := countReencryptWarns(t, buf.String())
	if warns[1] != 1 || warns[20] != 1 || warnTotal(warns) != 20 {
		t.Fatalf("warns %v", warns)
	}

	tick, err = w.tick(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if tick.rewritten != 0 || tick.remaining != 20 || tick.old != "1=20" {
		t.Fatalf("second tick %+v", tick)
	}
	if again := countReencryptWarns(t, buf.String()); warnTotal(again) != 20 {
		t.Fatalf("warns repeated %v", again)
	}
	blobs := append(corrupt, good, got.ct)
	assertReencryptQuiet(t, buf.String(), blobs...)
}

func warnTotal(m map[int64]int) int {
	n := 0
	for _, c := range m {
		n += c
	}
	return n
}

func countReencryptWarns(t *testing.T, log string) map[int64]int {
	t.Helper()
	out := map[int64]int{}
	for _, line := range strings.Split(log, "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) != nil {
			continue
		}
		if m["message"] != "account credentials were not re-encrypted" {
			continue
		}
		if m["reason"] != "ciphertext does not open" {
			t.Fatalf("reason %v", m["reason"])
		}
		id, ok := m["api_id"].(float64)
		if !ok {
			t.Fatalf("api_id %T", m["api_id"])
		}
		out[int64(id)]++
	}
	return out
}

func TestReencryptLoop(t *testing.T) {
	pool := reencryptPool(t)
	ctx := t.Context()

	buf0 := &lockedBuf{}
	none := startReencryptLoop(t, pool, accountkey.Box{}, buf0)
	none.wg.Wait()
	if !strings.Contains(buf0.String(), "re-encrypt account credentials: no current key") {
		t.Fatalf("log %s", buf0.String())
	}

	v1 := reencryptRing(t, reencryptV1())
	for id := int64(1); id <= 3; id++ {
		insertReencryptAccount(t, ctx, pool, v1, id, id == 2)
	}
	before := map[int64]reencryptStored{}
	var blobs [][]byte
	for id := int64(1); id <= 3; id++ {
		before[id] = readReencryptAccount(t, ctx, pool, id)
		blobs = append(blobs, before[id].ct)
	}

	buf := &lockedBuf{}
	s := startReencryptLoop(t, pool, reencryptRing(t, reencryptV2()), buf)
	until(t, 5*time.Second, "old rows logged", func() error {
		log := buf.String()
		if strings.Contains(log, `"old":"1=3"`) && strings.Contains(log, "account key re-encrypt") {
			return nil
		}
		return errors.New("progress log missing")
	})
	for id := int64(1); id <= 3; id++ {
		got := readReencryptAccount(t, ctx, pool, id)
		if got.ver != 1 || !bytes.Equal(got.ct, before[id].ct) {
			t.Fatalf("id %d changed before the old key was installed", id)
		}
	}
	assertReencryptQuiet(t, buf.String(), blobs...)
	stopped := time.Now()
	s.cancel()
	s.wg.Wait()
	if time.Since(stopped) > 2*time.Second {
		t.Fatal("re-encrypt loop did not stop")
	}

	buf2 := &lockedBuf{}
	s2 := startReencryptLoop(t, pool, reencryptRing(t, reencryptV2(), reencryptV1()), buf2)
	until(t, 10*time.Second, "rows on the current key", func() error {
		n, ok := accountKeyOldCount(context.Background(), pool, 2)
		if !ok || n != 0 {
			return fmt.Errorf("remaining %d ok %v", n, ok)
		}
		return nil
	})
	s2.cancel()
	s2.wg.Wait()
	for id := int64(1); id <= 3; id++ {
		got := readReencryptAccount(t, ctx, pool, id)
		blobs = append(blobs, got.ct)
		assertReencryptSameMeta(t, before[id], got)
		if got.ver != 2 || got.rev != 1 {
			t.Fatalf("id %d version %d revision %d", id, got.ver, got.rev)
		}
		openReencrypt(t, s2.cfg.AccountKeys, id, got.ct)
	}
	log := buf2.String()
	if !strings.Contains(log, "account key re-encrypt") || !strings.Contains(log, `"remaining":0`) {
		t.Fatal("completion was not logged")
	}
	assertReencryptQuiet(t, log, blobs...)
}

func startReencryptLoop(t *testing.T, pool *pgxpool.Pool, keys accountkey.Box, buf *lockedBuf) *Server {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		cfg:    Config{AccountKeys: keys},
		audit:  &auditor{pool: pool},
		log:    zerolog.New(buf).Level(zerolog.InfoLevel),
		ctx:    ctx,
		cancel: cancel,
	}
	s.wg.Add(1)
	go s.runReencrypt()
	t.Cleanup(func() {
		cancel()
		s.wg.Wait()
	})
	return s
}

func TestReencryptNoPool(t *testing.T) {
	buf := &lockedBuf{}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{log: zerolog.New(buf).Level(zerolog.InfoLevel), ctx: ctx, cancel: cancel}
	s.wg.Add(1)
	go s.runReencrypt()
	s.wg.Wait()
	if !strings.Contains(buf.String(), "re-encrypt account credentials") {
		t.Fatalf("log %s", buf.String())
	}
	if strings.Contains(buf.String(), reencryptSecret) {
		t.Fatal("log leaked credential material")
	}
}

func TestAccountKeyOldGauge(t *testing.T) {
	pool := reencryptPool(t)
	ctx := t.Context()
	v1 := reencryptRing(t, reencryptV1())
	rotated := reencryptRing(t, reencryptV2(), reencryptV1())
	insertReencryptAccount(t, ctx, pool, v1, 1, false)
	insertReencryptAccount(t, ctx, pool, v1, 2, true)
	insertReencryptAccount(t, ctx, pool, rotated, 3, false)

	n, ok := accountKeyOldCount(ctx, pool, rotated.Current())
	if !ok || n != 2 {
		t.Fatalf("count %d ok %v", n, ok)
	}
	if _, ok := accountKeyOldCount(ctx, pool, 0); ok {
		t.Fatal("missing current key reported a count")
	}
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok := accountKeyOldCount(cctx, pool, rotated.Current()); ok {
		t.Fatal("cancelled context reported a count")
	}

	with := string(renderMetricsAt(Config{}, time.Unix(0, 0), nil, 1, 0, int(n), nil))
	if !strings.Contains(with, "mftiko_account_key_old 2\n") {
		t.Fatalf("gauge missing:\n%s", with)
	}
	zero := string(renderMetricsAt(Config{}, time.Unix(0, 0), nil, 1, 0, 0, nil))
	if !strings.Contains(zero, "mftiko_account_key_old 0\n") {
		t.Fatalf("zero sample missing:\n%s", zero)
	}
	omitted := string(renderMetricsAt(Config{}, time.Unix(0, 0), nil, 0, 0, -1, nil))
	if !strings.Contains(omitted, "# HELP mftiko_account_key_old ") || strings.Contains(omitted, "\nmftiko_account_key_old ") {
		t.Fatalf("omitted sample:\n%s", omitted)
	}
	s := &Server{audit: &auditor{pool: pool}, cfg: Config{AccountKeys: rotated}}
	if got := accountKeyOldSample(ctx, s); got != 2 {
		t.Fatalf("sample %d", got)
	}
	if got := accountKeyOldSample(ctx, &Server{}); got != -1 {
		t.Fatalf("sample without a pool %d", got)
	}
}
