// Package dump prints the strategy event log as JSON lines (PROTOCOL §12, DECISIONS E20-D002).
//
// The SDK writes raw bytes and decodes nothing on the way in. This package is where a log is
// decoded, for people and scripts: each file is one header line, then one line per frame in the
// order the frames were written. A frame line has rec_seq, kind, the timestamps of its kind under
// their own names (recv_ns, hook_start_ns, hook_end_ns, call_ns, publish_ns, reply_ns and the
// others below), and its body decoded: protobuf as protojson with the proto field names, the md
// envelope as its fields. A body that does not decode is printed in base64 next to the error.
// Frames of a kind the reader does not know are skipped (E20-D008).
//
// It links protojson. A strategy binary links sdk/eventlog only, which decodes nothing.
package dump

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	stsctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/stsctl/v1"
	symv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	"github.com/BullionBear/mftiko/contract/wire/envelope"
	"github.com/BullionBear/mftiko/sdk/eventlog"
)

// Header is the line that opens each file.
type Header struct {
	File          string `json:"file"`
	FormatVersion uint16 `json:"format_version"`
	Incarnation   uint32 `json:"incarnation"`
	N             uint32 `json:"n"`
	CreatedNs     int64  `json:"created_ns"`
	SessionID     string `json:"session_id"`
	SDKVersion    string `json:"sdk_version"`
	SDKCommit     string `json:"sdk_commit"`
}

// Damage is the last line of a file that could not be read to its end: it ends inside a frame,
// which is what a process killed in the middle of a write leaves, or a frame is damaged.
type Damage struct {
	File  string `json:"file"`
	Error string `json:"error"`
}

// Frame is one frame's line. Only the fields of its kind are set; PROTOCOL §12.4 lists them.
type Frame struct {
	RecSeq uint64 `json:"rec_seq"`
	Kind   string `json:"kind"`

	CallNs        int64 `json:"call_ns,omitempty"`
	PublishNs     int64 `json:"publish_ns,omitempty"`
	RecvNs        int64 `json:"recv_ns,omitempty"`
	ReplyNs       int64 `json:"reply_ns,omitempty"`
	HookStartNs   int64 `json:"hook_start_ns,omitempty"`
	HookEndNs     int64 `json:"hook_end_ns,omitempty"`
	AppliedNs     int64 `json:"applied_ns,omitempty"`
	StopNs        int64 `json:"stop_ns,omitempty"`
	OnStopAskedNs int64 `json:"onstop_asked_ns,omitempty"`
	EndNs         int64 `json:"end_ns,omitempty"`

	Input   uint64 `json:"input,omitempty"`
	Ending  string `json:"ending,omitempty"`
	Panic   string `json:"panic,omitempty"`
	Outcome string `json:"outcome,omitempty"`
	Error   string `json:"error,omitempty"`

	Seed        *uint64 `json:"seed,omitempty"`
	CloidSlot   *uint64 `json:"cloid_slot,omitempty"`
	FromCommand bool    `json:"from_command,omitempty"`
	Deliver     bool    `json:"deliver,omitempty"`
	Hung        bool    `json:"hung,omitempty"`
	Dropped     *uint64 `json:"dropped,omitempty"`

	Call      uint64   `json:"call,omitempty"`
	Op        string   `json:"op,omitempty"`
	Batch     bool     `json:"batch,omitempty"`
	APIID     int64    `json:"api_id,omitempty"`
	SubmitGen uint64   `json:"submit_gen,omitempty"`
	Cloids    []string `json:"cloids,omitempty"`
	Gen       uint64   `json:"gen,omitempty"`
	Read      uint64   `json:"read,omitempty"`
	ReadKind  string   `json:"read_kind,omitempty"`
	Subject   string   `json:"subject,omitempty"`
	Timer     uint64   `json:"timer,omitempty"`
	Post      uint64   `json:"post,omitempty"`
	Ticker    string   `json:"ticker,omitempty"`

	MDTsNs    int64  `json:"md_ts_ns,omitempty"`
	ConnEpoch uint64 `json:"conn_epoch,omitempty"`
	Seq       uint64 `json:"seq,omitempty"`

	Payload  json.RawMessage `json:"payload,omitempty"`
	Bindings json.RawMessage `json:"bindings,omitempty"`
	Info     json.RawMessage `json:"info,omitempty"`
	Event    json.RawMessage `json:"event,omitempty"`
	Request  json.RawMessage `json:"request,omitempty"`
	Reply    json.RawMessage `json:"reply,omitempty"`

	Body        []byte `json:"body,omitempty"`
	DecodeError string `json:"decode_error,omitempty"`
}

// Dir prints the log files in dir, oldest first. inc limits the output to one incarnation; 0
// prints every incarnation. A damaged file is reported on its last line and the next file is
// printed; the error returned is the first damage other than a cut last frame.
func Dir(w io.Writer, dir string, inc uint32) error {
	files, err := eventlog.Files(dir)
	if err != nil {
		return err
	}
	d := New(w)
	var first error
	for _, f := range files {
		if inc != 0 && f.Incarnation != inc {
			continue
		}
		if err := d.File(f.Path); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Dumper prints files to one writer. A read_reply is decoded by the read its read_call made, so
// a Dumper remembers the read_calls of the incarnation it is printing.
type Dumper struct {
	enc   *json.Encoder
	inc   uint32
	reads map[uint64]uint16
}

// New is a Dumper that writes to w.
func New(w io.Writer) *Dumper {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return &Dumper{enc: enc, reads: map[uint64]uint16{}}
}

// File prints one file. A file that ends inside a frame is printed up to that frame, with the
// error on its last line, and is not an error.
func (d *Dumper) File(path string) error {
	r, err := eventlog.Open(path)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	defer r.Close()
	return d.Reader(filepath.Base(path), r)
}

// Reader prints the frames of r under the file name name.
func (d *Dumper) Reader(name string, r *eventlog.Reader) error {
	h := r.Header()
	if h.Incarnation != d.inc {
		d.inc = h.Incarnation
		clear(d.reads)
	}
	head := Header{
		File:          name,
		FormatVersion: h.FormatVersion,
		Incarnation:   h.Incarnation,
		N:             h.N,
		CreatedNs:     h.CreatedNs,
		SessionID:     h.SessionID,
		SDKVersion:    h.SDKVersion,
		SDKCommit:     h.SDKCommit,
	}
	if err := d.enc.Encode(head); err != nil {
		return err
	}
	for {
		x, err := r.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			if werr := d.enc.Encode(Damage{File: name, Error: err.Error()}); werr != nil {
				return werr
			}
			if errors.Is(err, eventlog.ErrTruncated) {
				return nil
			}
			return fmt.Errorf("%s: %w", name, err)
		}
		if err := d.enc.Encode(d.Frame(x)); err != nil {
			return err
		}
	}
}

// Frame is the line of one frame.
func (d *Dumper) Frame(x eventlog.Record) Frame {
	f := Frame{RecSeq: x.Seq, Kind: x.Kind.String()}
	var err error
	switch x.Kind {
	case eventlog.KindInit:
		f.RecvNs, f.HookStartNs = x.RecvNs, x.TNs
		seed := x.Ref
		f.Seed = &seed
		var slot uint64
		var payload []byte
		if slot, payload, err = eventlog.ParseInit(x.Body); err == nil {
			f.CloidSlot = &slot
			f.Payload, err = protoJSON(payload, &stsctlv1.InitPayload{})
		}
	case eventlog.KindStart:
		f.RecvNs, f.HookStartNs = x.RecvNs, x.TNs
		f.Payload, err = protoJSON(x.Body, &stsctlv1.StartPayload{})
	case eventlog.KindBindings:
		f.RecvNs, f.AppliedNs = x.RecvNs, x.TNs
		f.FromCommand = x.Flags&eventlog.FromCommand != 0
		f.Bindings, err = protoJSON(x.Body, &stsctlv1.Bindings{})
	case eventlog.KindResolve:
		f.CallNs, f.ReplyNs = x.TNs, x.RecvNs
		f.Outcome = eventlog.OutcomeName(x.Flags)
		var rest []byte
		if f.Ticker, rest, err = eventlog.ParseResolve(x.Body); err == nil {
			if x.Flags == eventlog.OK {
				f.Info, err = protoJSON(rest, &symv1.SymbolInfo{})
			} else {
				f.Error = string(rest)
			}
		}
	case eventlog.KindMDFeed, eventlog.KindMDBook:
		f.RecvNs, f.HookStartNs = x.RecvNs, x.TNs
		var payload []byte
		if f.MDTsNs, f.ConnEpoch, f.Seq, _, payload, err = envelope.Parse(x.Body); err == nil {
			f.Event, err = protoJSON(payload, &mdv1.FeedEvent{})
		}
	case eventlog.KindTDEvent:
		f.RecvNs, f.HookStartNs = x.RecvNs, x.TNs
		f.APIID = int64(x.Ref)
		f.Event, err = protoJSON(x.Body, &tdv1.AccountEvent{})
	case eventlog.KindTDView:
		f.RecvNs, f.HookStartNs = x.RecvNs, x.TNs
		f.APIID = int64(x.Ref)
		var reply []byte
		if f.Gen, reply, err = eventlog.ParseTDView(x.Body); err == nil {
			f.Reply, err = protoJSON(reply, &tdv1.ViewReply{})
		}
	case eventlog.KindOrderReply:
		f.ReplyNs, f.HookStartNs = x.RecvNs, x.TNs
		f.Call = x.Ref
		f.Outcome = eventlog.OutcomeName(x.Flags)
		if x.Flags == eventlog.OK {
			f.Reply, err = protoJSON(x.Body, &tdv1.OrderReply{})
		} else {
			f.Error = string(x.Body)
		}
	case eventlog.KindReadReply:
		f.ReplyNs, f.HookStartNs = x.RecvNs, x.TNs
		f.Read = x.Ref
		f.Outcome = eventlog.OutcomeName(x.Flags)
		kind, known := d.reads[x.Ref]
		if known {
			f.ReadKind = eventlog.ReadName(kind)
		}
		switch {
		case x.Flags != eventlog.OK:
			f.Error = string(x.Body)
		case !known:
			err = errors.New("dump: the read_call of this read is not in the files printed")
		default:
			f.Reply, err = protoJSON(x.Body, readReply(kind))
		}
	case eventlog.KindTimer:
		f.RecvNs, f.HookStartNs = x.RecvNs, x.TNs
		f.Timer = x.Ref
	case eventlog.KindPost:
		f.RecvNs, f.HookStartNs = x.RecvNs, x.TNs
		f.Post = x.Ref
	case eventlog.KindStop:
		f.StopNs = x.RecvNs
		if x.Flags&eventlog.StopHung != 0 {
			f.Hung = true
		} else {
			f.OnStopAskedNs = x.TNs
			f.Deliver = x.Flags&eventlog.StopDeliver != 0
		}
	case eventlog.KindOnStop:
		f.RecvNs, f.HookStartNs = x.RecvNs, x.TNs
	case eventlog.KindHookEnd:
		f.HookEndNs = x.TNs
		f.Input = x.Ref
		switch x.Flags {
		case 0:
			f.Ending = "returned"
		case eventlog.EndPanic:
			f.Ending, f.Panic = "panic", string(x.Body)
		case eventlog.EndError:
			f.Ending, f.Error = "error", string(x.Body)
		default:
			f.Ending = fmt.Sprintf("ending_%d", x.Flags)
		}
	case eventlog.KindOrderCall:
		f.CallNs = x.TNs
		f.Call = x.Ref
		f.Op = eventlog.OpName(x.Flags)
		f.Batch = x.Flags&eventlog.FromBatch != 0
		var c eventlog.OrderCall
		if c, err = eventlog.ParseOrderCall(x.Body); err == nil {
			f.APIID, f.SubmitGen, f.Cloids = c.APIID, c.SubmitGen, c.Cloids
		}
	case eventlog.KindOrderPublish:
		f.PublishNs = x.TNs
		f.Call = x.Ref
		f.Request, err = protoJSON(x.Body, &tdv1.OrderRequest{})
	case eventlog.KindReadCall:
		f.CallNs = x.TNs
		f.Read = x.Ref
		f.ReadKind = eventlog.ReadName(x.Flags)
		d.reads[x.Ref] = x.Flags
		var req []byte
		if f.Subject, req, err = eventlog.ParseReadCall(x.Body); err == nil {
			f.Request, err = protoJSON(req, readRequest(x.Flags))
		}
	case eventlog.KindEnd:
		f.EndNs = x.TNs
		var dropped uint64
		if dropped, err = eventlog.ParseEnd(x.Body); err == nil {
			f.Dropped = &dropped
		}
	}
	if err != nil {
		f.Body = x.Body
		f.DecodeError = err.Error()
	}
	return f
}

var marshal = protojson.MarshalOptions{UseProtoNames: true}

// protoJSON decodes b into m and prints it. A read kind this package does not know has no message.
func protoJSON(b []byte, m proto.Message) (json.RawMessage, error) {
	if m == nil {
		return nil, errors.New("dump: no message for this read kind")
	}
	if err := proto.Unmarshal(b, m); err != nil {
		return nil, err
	}
	out, err := marshal.Marshal(m)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// readRequest is the request message of a read_call: md.view takes a ViewRequest, md.fetch a
// FetchRequest.
func readRequest(kind uint16) proto.Message {
	switch kind {
	case eventlog.ReadTape, eventlog.ReadBookSnapshot:
		return &mdv1.ViewRequest{}
	case eventlog.ReadKlines, eventlog.ReadOpenInterest:
		return &mdv1.FetchRequest{}
	}
	return nil
}

func readReply(kind uint16) proto.Message {
	switch kind {
	case eventlog.ReadTape, eventlog.ReadBookSnapshot:
		return &mdv1.ViewReply{}
	case eventlog.ReadKlines, eventlog.ReadOpenInterest:
		return &mdv1.FetchReply{}
	}
	return nil
}

// Path prints path: a directory as Dir does, a file as one file.
func Path(w io.Writer, path string, inc uint32) error {
	st, err := os.Stat(path)
	if err != nil {
		return err
	}
	if st.IsDir() {
		return Dir(w, path, inc)
	}
	return New(w).File(path)
}
