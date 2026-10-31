package eventlog

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestHeaderAndFrames(t *testing.T) {
	h := Header{Incarnation: 3, N: 2, CreatedNs: 1000, SessionID: "s1", SDKVersion: "v1.2.3", SDKCommit: "abc"}
	b := AppendHeader(nil, h)
	recs := []Record{
		{Kind: KindInit, Seq: 1, RecvNs: 10, TNs: 11, Ref: 7, Body: []byte("init")},
		{Kind: KindMDFeed, Seq: 2, RecvNs: 20, TNs: 21, Body: []byte("md")},
		{Kind: KindHookEnd, Flags: EndPanic, Seq: 3, TNs: 22, Ref: 2, Body: []byte("boom")},
		{Kind: KindTimer, Seq: 4, RecvNs: 30, TNs: 31, Ref: 1},
	}
	for _, r := range recs {
		b = AppendFrame(b, r)
	}
	r, err := NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	h.FormatVersion = FormatVersion
	if r.Header() != h {
		t.Fatalf("header %+v", r.Header())
	}
	got := readAll(t, r)
	for i := range recs {
		if recs[i].Body == nil {
			recs[i].Body = []byte{}
		}
	}
	if !reflect.DeepEqual(got, recs) {
		t.Fatalf("records\n got %+v\nwant %+v", got, recs)
	}
}

// A reader skips a kind it does not know and keeps reading (E20-D008).
func TestUnknownKindSkipped(t *testing.T) {
	b := AppendHeader(nil, Header{SessionID: "s1"})
	b = AppendFrame(b, Record{Kind: KindPost, Seq: 1, Ref: 1})
	b = AppendFrame(b, Record{Kind: 99, Seq: 2, Body: []byte("later")})
	b = AppendFrame(b, Record{Kind: KindPost, Seq: 3, Ref: 2})
	r, err := NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	got := readAll(t, r)
	if len(got) != 2 || got[0].Seq != 1 || got[1].Seq != 3 {
		t.Fatalf("records %+v", got)
	}
}

// A file cut inside its last frame keeps the frames before it.
func TestTruncatedTail(t *testing.T) {
	b := AppendHeader(nil, Header{SessionID: "s1"})
	b = AppendFrame(b, Record{Kind: KindPost, Seq: 1, Ref: 1})
	full := AppendFrame(b, Record{Kind: KindPost, Seq: 2, Ref: 2, Body: []byte("abcd")})
	for cut := len(b) + 1; cut < len(full); cut++ {
		r, err := NewReader(bytes.NewReader(full[:cut]))
		if err != nil {
			t.Fatal(err)
		}
		rec, err := r.Next()
		if err != nil || rec.Seq != 1 {
			t.Fatalf("cut %d: first %+v %v", cut, rec, err)
		}
		if _, err := r.Next(); !errors.Is(err, ErrTruncated) {
			t.Fatalf("cut %d: %v", cut, err)
		}
	}
}

func TestBadHeaderAndFrame(t *testing.T) {
	if _, err := NewReader(bytes.NewReader([]byte("NOTALOG_and_more_bytes_here_padding"))); !errors.Is(err, ErrMagic) {
		t.Fatalf("magic %v", err)
	}
	b := AppendHeader(nil, Header{FormatVersion: 2})
	if _, err := NewReader(bytes.NewReader(b)); !errors.Is(err, ErrVersion) {
		t.Fatalf("version %v", err)
	}
	b = AppendHeader(nil, Header{})
	b = append(b, 1, 0, 0, 0)
	r, err := NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); !errors.Is(err, ErrFrame) {
		t.Fatalf("frame %v", err)
	}
}

func TestFilesOrder(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"10-1.mel", "2-10.mel", "2-2.mel", "2-1.mel", "notes.txt", "2-x.mel", "02-3.mel"} {
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	fs, err := Files(dir)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, f := range fs {
		got = append(got, filepath.Base(f.Path))
	}
	want := []string{"2-1.mel", "2-2.mel", "2-10.mel", "10-1.mel"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("files %v", got)
	}
	if inc, n, ok := ParseName("7-3.mel"); !ok || inc != 7 || n != 3 {
		t.Fatalf("parse %d %d %v", inc, n, ok)
	}
}

func TestBodies(t *testing.T) {
	call := AppendOrderCall(nil, 1, 2, []string{"c1", "c2"})
	c, err := ParseOrderCall(call)
	if err != nil || c.APIID != 1 || c.SubmitGen != 2 || !reflect.DeepEqual(c.Cloids, []string{"c1", "c2"}) {
		t.Fatalf("order_call %+v %v", c, err)
	}
	if _, err := ParseOrderCall(call[:len(call)-1]); err == nil {
		t.Fatal("short order_call")
	}
	recon, err := ParseOrderCall(AppendOrderCall(nil, 1, 0, nil))
	if err != nil || len(recon.Cloids) != 0 {
		t.Fatalf("recon %+v %v", recon, err)
	}

	subject, req, err := ParseReadCall(AppendReadCall(nil, "md.v1.view.x", []byte("req")))
	if err != nil || subject != "md.v1.view.x" || string(req) != "req" {
		t.Fatalf("read_call %q %q %v", subject, req, err)
	}
	ticker, rest, err := ParseResolve(AppendResolve(nil, "Binance_Spot_BTCUSDT", []byte("info")))
	if err != nil || ticker != "Binance_Spot_BTCUSDT" || string(rest) != "info" {
		t.Fatalf("resolve %q %q %v", ticker, rest, err)
	}
	head := TDViewHead(5)
	gen, reply, err := ParseTDView(append(head[:], "view"...))
	if err != nil || gen != 5 || string(reply) != "view" {
		t.Fatalf("td_view %d %q %v", gen, reply, err)
	}
	slotHead := InitHead(9)
	slot, payload, err := ParseInit(append(slotHead[:], "init"...))
	if err != nil || slot != 9 || string(payload) != "init" {
		t.Fatalf("init %d %q %v", slot, payload, err)
	}
	if _, _, err := ParseInit([]byte("short")); err == nil {
		t.Fatal("short init")
	}
	dropped, err := ParseEnd(AppendEnd(nil, 4))
	if err != nil || dropped != 4 {
		t.Fatalf("end %d %v", dropped, err)
	}
}

func TestNames(t *testing.T) {
	if KindOrderCall.String() != "order_call" || Kind(99).String() != "kind_99" || Kind(99).Known() {
		t.Fatal("kind names")
	}
	if !KindMDFeed.Input() || KindOrderCall.Input() || KindHookEnd.Input() {
		t.Fatal("inputs")
	}
	if OpName(OpBatchCancel|FromBatch) != "batch_cancel" || ReadName(ReadKlines) != "klines" || OutcomeName(Timeout) != "timeout" {
		t.Fatal("flag names")
	}
}

func readAll(t *testing.T, r *Reader) []Record {
	t.Helper()
	var out []Record
	for {
		rec, err := r.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, rec)
	}
}
