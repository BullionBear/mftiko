// Package eventlog is the file format of the strategy event log (PROTOCOL §12, DECISIONS E20-D002,
// E20-D008). The Go SDK writes it under VolumeDir()/eventlog when the session spec turns it on.
//
// A file is a header and then frames. A frame is a fixed 40-byte head and the raw bytes it carries:
// the MD envelope, the TD protobuf, the reply as it came off NATS. Nothing is decoded or re-encoded
// on the way in. This package reads the frames; it does not decode the protobuf bodies, so a binary
// that links it does not link a JSON encoder. sdk/eventlog/dump writes a log as JSON lines.
package eventlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
)

const (
	// Magic opens every file.
	Magic = "MFTIKOEL"
	// FormatVersion is the frame layout. A reader refuses a version it does not know. A new kind,
	// or a header with more fields at its end, does not change it (E20-D008).
	FormatVersion uint16 = 1
	// Dir is the directory under the session volume. Files are named {incarnation}-{n}.mel.
	Dir = "eventlog"
	Ext = ".mel"
	// FrameHead is the length of a frame before its body.
	FrameHead = 40
	// MaxFrame bounds a frame's length field. A larger value is damage, not a frame.
	MaxFrame = 64 << 20

	headerFixed = 28
)

// Kind is what a frame holds. Values are only added, never reused or changed; a reader skips a
// kind it does not know (E20-D008).
type Kind uint16

const (
	KindInit         Kind = 1  // InitPayload without nats; ref is the Rand seed
	KindStart        Kind = 2  // StartPayload as received
	KindBindings     Kind = 3  // stsctl.Bindings applied after Start
	KindResolve      Kind = 4  // one Resolve call and its result
	KindMDFeed       Kind = 5  // md envelope and FeedEvent, as delivered to the hook
	KindMDBook       Kind = 6  // orderbook_delta envelope and FeedEvent, as delivered to the book
	KindTDEvent      Kind = 7  // td.evt AccountEvent; ref is api_id
	KindTDView       Kind = 8  // td.view reply; ref is api_id
	KindOrderReply   Kind = 9  // td.order reply; ref is the call id
	KindReadReply    Kind = 10 // md.view or md.fetch reply; ref is the read id
	KindTimer        Kind = 11 // an After or book timer ran; ref is the timer id
	KindPost         Kind = 12 // a Post ran; ref is the post id
	KindStop         Kind = 13 // the dispatcher began the ending
	KindOnStop       Kind = 14 // the strategy's OnStop
	KindHookEnd      Kind = 15 // the hook of the input whose rec_seq is ref returned
	KindOrderCall    Kind = 16 // Submit, Cancel, BatchCancel or RequestRecon; ref is the call id
	KindOrderPublish Kind = 17 // the OrderRequest put on td.order; ref is the call id
	KindReadCall     Kind = 18 // Tape, BookSnapshot, FetchKlines or FetchOpenInterest; ref is the read id
	KindEnd          Kind = 19 // the writer closed
)

var kindNames = [...]string{
	KindInit:         "init",
	KindStart:        "start",
	KindBindings:     "bindings",
	KindResolve:      "resolve",
	KindMDFeed:       "md_feed",
	KindMDBook:       "md_book",
	KindTDEvent:      "td_event",
	KindTDView:       "td_view",
	KindOrderReply:   "order_reply",
	KindReadReply:    "read_reply",
	KindTimer:        "timer",
	KindPost:         "post",
	KindStop:         "stop",
	KindOnStop:       "onstop",
	KindHookEnd:      "hook_end",
	KindOrderCall:    "order_call",
	KindOrderPublish: "order_publish",
	KindReadCall:     "read_call",
	KindEnd:          "end",
}

// Known reports whether this reader knows the kind.
func (k Kind) Known() bool { return k >= KindInit && k <= KindEnd }

func (k Kind) String() string {
	if k.Known() {
		return kindNames[k]
	}
	return "kind_" + strconv.Itoa(int(k))
}

// Input reports whether the kind is a hook's input. Its t_ns is hook_start_ns, and a hook_end
// whose ref is its rec_seq follows unless the hook never returned (E20-D005).
func (k Kind) Input() bool {
	switch k {
	case KindInit, KindStart, KindMDFeed, KindMDBook, KindTDEvent, KindTDView, KindOrderReply,
		KindReadReply, KindTimer, KindPost, KindOnStop:
		return true
	}
	return false
}

// Flags of a bindings frame.
const FromCommand uint16 = 1

// Flags of a stop frame. StopDeliver means td.evt was dropped and the TD buffer is delivered
// before OnStop (DONE and FAILED). Without it the ending delivers nothing (CRASHED).
const StopDeliver uint16 = 1

// Flags of a hook_end frame. The body is the panic value or the error text.
const (
	EndPanic uint16 = 1
	EndError uint16 = 2
)

// Flags of an order_call frame: the operation in the low byte, and FromBatch on each round of a
// BatchCancel.
const (
	OpSubmit      uint16 = 1
	OpCancel      uint16 = 2
	OpBatchCancel uint16 = 3
	OpRecon       uint16 = 4
	OpMask        uint16 = 0xff
	FromBatch     uint16 = 0x100
)

// Flags of a read_call frame.
const (
	ReadTape         uint16 = 1
	ReadBookSnapshot uint16 = 2
	ReadKlines       uint16 = 3
	ReadOpenInterest uint16 = 4
)

// Outcome is the flags of resolve, order_reply and read_reply. OK carries the reply. Failed and
// NotFound carry the error text. The others carry nothing more.
const (
	OK           uint16 = 0
	NoResponders uint16 = 1
	Timeout      uint16 = 2
	Canceled     uint16 = 3
	Failed       uint16 = 4
	NotInInit    uint16 = 5
	NotFound     uint16 = 6
	Stopping     uint16 = 7
)

// OpName is the operation of an order_call's flags.
func OpName(flags uint16) string {
	switch flags & OpMask {
	case OpSubmit:
		return "submit"
	case OpCancel:
		return "cancel"
	case OpBatchCancel:
		return "batch_cancel"
	case OpRecon:
		return "recon"
	}
	return "op_" + strconv.Itoa(int(flags&OpMask))
}

// ReadName is the read of a read_call's flags.
func ReadName(flags uint16) string {
	switch flags {
	case ReadTape:
		return "tape"
	case ReadBookSnapshot:
		return "book_snapshot"
	case ReadKlines:
		return "klines"
	case ReadOpenInterest:
		return "open_interest"
	}
	return "read_" + strconv.Itoa(int(flags))
}

// OutcomeName is the outcome of a resolve, order_reply or read_reply.
func OutcomeName(flags uint16) string {
	switch flags {
	case OK:
		return "ok"
	case NoResponders:
		return "no_responders"
	case Timeout:
		return "timeout"
	case Canceled:
		return "canceled"
	case Failed:
		return "error"
	case NotInInit:
		return "not_in_init"
	case NotFound:
		return "not_found"
	case Stopping:
		return "stopping"
	}
	return "outcome_" + strconv.Itoa(int(flags))
}

// Header opens a file (PROTOCOL §12.2). N counts the files of one incarnation from 1.
type Header struct {
	FormatVersion uint16
	Incarnation   uint32
	N             uint32
	CreatedNs     int64
	SessionID     string
	SDKVersion    string
	SDKCommit     string
}

// AppendHeader encodes h. FormatVersion zero is written as FormatVersion. Each string is cut at
// maxHeaderString bytes.
func AppendHeader(dst []byte, h Header) []byte {
	v := h.FormatVersion
	if v == 0 {
		v = FormatVersion
	}
	start := len(dst)
	dst = append(dst, Magic...)
	dst = binary.LittleEndian.AppendUint16(dst, v)
	dst = binary.LittleEndian.AppendUint16(dst, 0)
	dst = binary.LittleEndian.AppendUint32(dst, h.Incarnation)
	dst = binary.LittleEndian.AppendUint32(dst, h.N)
	dst = binary.LittleEndian.AppendUint64(dst, uint64(h.CreatedNs))
	dst = appendStr(dst, cut(h.SessionID))
	dst = appendStr(dst, cut(h.SDKVersion))
	dst = appendStr(dst, cut(h.SDKCommit))
	binary.LittleEndian.PutUint16(dst[start+10:], uint16(len(dst)-start))
	return dst
}

const maxHeaderString = 1024

func cut(s string) string {
	if len(s) > maxHeaderString {
		return s[:maxHeaderString]
	}
	return s
}

// Record is one frame. Seq is rec_seq: it counts every frame of one incarnation from 1, including
// frames the writer dropped, so a gap means frames are missing.
type Record struct {
	Kind   Kind
	Flags  uint16
	Seq    uint64
	RecvNs int64
	TNs    int64
	Ref    uint64
	Body   []byte
}

// PutFrameHead writes the FrameHead bytes in front of a body of n bytes.
func PutFrameHead(b []byte, kind Kind, flags uint16, seq uint64, recvNs, tNs int64, ref uint64, n int) {
	_ = b[FrameHead-1]
	binary.LittleEndian.PutUint32(b[0:], uint32(FrameHead-4+n))
	binary.LittleEndian.PutUint16(b[4:], uint16(kind))
	binary.LittleEndian.PutUint16(b[6:], flags)
	binary.LittleEndian.PutUint64(b[8:], seq)
	binary.LittleEndian.PutUint64(b[16:], uint64(recvNs))
	binary.LittleEndian.PutUint64(b[24:], uint64(tNs))
	binary.LittleEndian.PutUint64(b[32:], ref)
}

// AppendFrame encodes r as one frame.
func AppendFrame(dst []byte, r Record) []byte {
	var head [FrameHead]byte
	PutFrameHead(head[:], r.Kind, r.Flags, r.Seq, r.RecvNs, r.TNs, r.Ref, len(r.Body))
	dst = append(dst, head[:]...)
	return append(dst, r.Body...)
}

// File is one log file in a directory.
type File struct {
	Path        string
	Incarnation uint32
	N           uint32
}

// Name is the file name of incarnation's n-th file.
func Name(incarnation, n uint32) string {
	return strconv.FormatUint(uint64(incarnation), 10) + "-" + strconv.FormatUint(uint64(n), 10) + Ext
}

// ParseName reads a name Name wrote.
func ParseName(name string) (incarnation, n uint32, ok bool) {
	stem, found := strings.CutSuffix(name, Ext)
	if !found {
		return 0, 0, false
	}
	a, b, found := strings.Cut(stem, "-")
	if !found {
		return 0, 0, false
	}
	inc, err1 := strconv.ParseUint(a, 10, 32)
	num, err2 := strconv.ParseUint(b, 10, 32)
	if err1 != nil || err2 != nil || Name(uint32(inc), uint32(num)) != name {
		return 0, 0, false
	}
	return uint32(inc), uint32(num), true
}

// Files lists the log files in dir, oldest first: by incarnation, then by n. Other names are skipped.
func Files(dir string) ([]File, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []File
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		inc, n, ok := ParseName(e.Name())
		if !ok {
			continue
		}
		out = append(out, File{Path: dir + string(os.PathSeparator) + e.Name(), Incarnation: inc, N: n})
	}
	SortFiles(out)
	return out, nil
}

// SortFiles orders files by incarnation, then n.
func SortFiles(fs []File) {
	slices.SortFunc(fs, func(a, b File) int {
		if a.Incarnation != b.Incarnation {
			return cmpU32(a.Incarnation, b.Incarnation)
		}
		return cmpU32(a.N, b.N)
	})
}

func cmpU32(a, b uint32) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// OrderCall is the body of an order_call: the account, the td.view generation Submit recorded
// (E08-D054), and the client order ids the call covers. RequestRecon lists none.
type OrderCall struct {
	APIID     int64
	SubmitGen uint64
	Cloids    []string
}

// AppendOrderCall encodes an order_call body.
func AppendOrderCall(dst []byte, apiID int64, submitGen uint64, cloids []string) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, uint64(apiID))
	dst = binary.LittleEndian.AppendUint64(dst, submitGen)
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(cloids)))
	for _, id := range cloids {
		dst = appendStr(dst, id)
	}
	return dst
}

// ParseOrderCall decodes an order_call body.
func ParseOrderCall(b []byte) (OrderCall, error) {
	if len(b) < 18 {
		return OrderCall{}, errBody("order_call")
	}
	c := OrderCall{
		APIID:     int64(binary.LittleEndian.Uint64(b)),
		SubmitGen: binary.LittleEndian.Uint64(b[8:]),
	}
	n := int(binary.LittleEndian.Uint16(b[16:]))
	rest := b[18:]
	for range n {
		s, r, ok := cutStr(rest)
		if !ok {
			return OrderCall{}, errBody("order_call")
		}
		c.Cloids = append(c.Cloids, s)
		rest = r
	}
	if len(rest) != 0 {
		return OrderCall{}, errBody("order_call")
	}
	return c, nil
}

// AppendReadCall encodes a read_call body: the subject and the request as sent.
func AppendReadCall(dst []byte, subject string, req []byte) []byte {
	return append(appendStr(dst, subject), req...)
}

// ParseReadCall decodes a read_call body.
func ParseReadCall(b []byte) (subject string, req []byte, err error) {
	s, rest, ok := cutStr(b)
	if !ok {
		return "", nil, errBody("read_call")
	}
	return s, rest, nil
}

// AppendResolve encodes a resolve body: the ticker as the strategy passed it, then SymbolInfo
// when the outcome is OK, or the error text.
func AppendResolve(dst []byte, ticker string, rest []byte) []byte {
	return append(appendStr(dst, ticker), rest...)
}

// ParseResolve decodes a resolve body.
func ParseResolve(b []byte) (ticker string, rest []byte, err error) {
	s, r, ok := cutStr(b)
	if !ok {
		return "", nil, errBody("resolve")
	}
	return s, r, nil
}

// TDViewHead is the bytes a td_view body has before the ViewReply: the request's generation.
func TDViewHead(gen uint64) [8]byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], gen)
	return b
}

// ParseTDView decodes a td_view body.
func ParseTDView(b []byte) (gen uint64, reply []byte, err error) {
	if len(b) < 8 {
		return 0, nil, errBody("td_view")
	}
	return binary.LittleEndian.Uint64(b), b[8:], nil
}

// AppendEnd encodes an end body: how many frames were dropped before it.
func AppendEnd(dst []byte, dropped uint64) []byte {
	return binary.LittleEndian.AppendUint64(dst, dropped)
}

// ParseEnd decodes an end body.
func ParseEnd(b []byte) (dropped uint64, err error) {
	if len(b) != 8 {
		return 0, errBody("end")
	}
	return binary.LittleEndian.Uint64(b), nil
}

func appendStr(dst []byte, s string) []byte {
	if len(s) > 0xffff {
		s = s[:0xffff]
	}
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(s)))
	return append(dst, s...)
}

func cutStr(b []byte) (string, []byte, bool) {
	if len(b) < 2 {
		return "", nil, false
	}
	n := int(binary.LittleEndian.Uint16(b))
	if len(b) < 2+n {
		return "", nil, false
	}
	return string(b[2 : 2+n]), b[2+n:], true
}

func errBody(kind string) error { return fmt.Errorf("eventlog: %s body is malformed", kind) }

var (
	// ErrMagic means the file does not start with Magic.
	ErrMagic = errors.New("eventlog: not an event log")
	// ErrVersion means the file's format version is one this reader does not know.
	ErrVersion = errors.New("eventlog: unknown format version")
	// ErrTruncated means the file ends inside a frame: the writer did not finish it.
	ErrTruncated = errors.New("eventlog: the file ends inside a frame")
	// ErrFrame means a frame's length is impossible.
	ErrFrame = errors.New("eventlog: bad frame length")
)
