package conformance

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sort"
	"strings"
	"time"

	"github.com/BullionBear/mftiko/exchange"
)

// A session fixture is a JSON Lines file under ws/ in the venue's fixture directory: a recorded
// websocket session, one Record per line, the first a meta record. A REST fixture is a JSON file
// under http/: one recorded response (Exchange). The golden files next to them hold what the adapter
// made of them; see the package documentation.

// Record kinds.
const (
	KindMeta  = "meta"   // the first line: which venue, which route, which side
	KindIn    = "ws.in"  // a frame the venue sent
	KindOut   = "ws.out" // a frame the client sent
	KindHTTP  = "http"   // a REST exchange during the session (a book snapshot)
	SideMkt   = "market"
	SidePriv  = "private"
	textCodec = "text"
	b64Codec  = "base64"
)

// Record is one line of a session fixture.
type Record struct {
	Kind string `json:"kind"`
	AtNs int64  `json:"at_ns,omitempty"`

	// meta
	Venue string `json:"venue,omitempty"`
	Side  string `json:"side,omitempty"` // market | private
	Route string `json:"route,omitempty"`
	Note  string `json:"note,omitempty"`

	// ws.in, ws.out
	Encoding string `json:"encoding,omitempty"` // text (default) | base64
	Frame    string `json:"frame,omitempty"`

	// http: Ch names the book channel a depth snapshot belongs to; Check compares the folded book
	// with the snapshot instead of replacing it (the end of a session).
	Ch       string    `json:"ch,omitempty"`
	Check    bool      `json:"check,omitempty"`
	Exchange *Exchange `json:"exchange,omitempty"`
}

// Bytes is the frame's payload.
func (r *Record) Bytes() ([]byte, error) {
	switch r.Encoding {
	case "", textCodec:
		return []byte(r.Frame), nil
	case b64Codec:
		return base64.StdEncoding.DecodeString(r.Frame)
	default:
		return nil, fmt.Errorf("conformance: frame encoding %q", r.Encoding)
	}
}

// FrameRecord is a ws.in or ws.out record of b: text when it is valid UTF-8 JSON-safe text, base64
// otherwise.
func FrameRecord(kind string, at time.Time, b []byte) Record {
	r := Record{Kind: kind, AtNs: at.UnixNano()}
	if isText(b) {
		r.Frame = string(b)
	} else {
		r.Encoding, r.Frame = b64Codec, base64.StdEncoding.EncodeToString(b)
	}
	return r
}

func isText(b []byte) bool {
	for _, c := range string(b) {
		if c == 0xFFFD || (c < 0x20 && c != '\n' && c != '\r' && c != '\t') {
			return false
		}
	}
	return true
}

// Exchange is a REST request and its response, as recorded or as built.
type Exchange struct {
	Request  *Request  `json:"request,omitempty"`
	Response *Response `json:"response,omitempty"`
}

// Header and Cost are exchange.Header and exchange.Cost with lower-case JSON names, like the rest of
// a fixture.
type Header struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type Cost struct {
	Bucket string `json:"bucket"`
	N      int    `json:"n"`
}

func headersOf(hs []exchange.Header) []Header {
	if hs == nil {
		return nil
	}
	out := make([]Header, len(hs))
	for i, h := range hs {
		out[i] = Header{h.Name, h.Value}
	}
	return out
}

func headersFrom(hs []Header) []exchange.Header {
	if hs == nil {
		return nil
	}
	out := make([]exchange.Header, len(hs))
	for i, h := range hs {
		out[i] = exchange.Header{Name: h.Name, Value: h.Value}
	}
	return out
}

// Request is an exchange.HTTPRequest in a fixture: query and body as text.
type Request struct {
	Method  string   `json:"method"`
	Base    string   `json:"base"`
	Path    string   `json:"path"`
	Query   string   `json:"query,omitempty"`
	Header  []Header `json:"header,omitempty"`
	Body    string   `json:"body,omitempty"`
	Cost    []Cost   `json:"cost,omitempty"`
	Mutates bool     `json:"mutates,omitempty"`
}

// RequestOf is r as a fixture records it.
func RequestOf(r *exchange.HTTPRequest) *Request {
	var cost []Cost
	for _, c := range r.Cost {
		cost = append(cost, Cost{c.Bucket, c.N})
	}
	return &Request{
		Method: r.Method, Base: r.Base, Path: r.Path, Query: string(r.Query), Header: headersOf(r.Header),
		Body: string(r.Body), Cost: cost, Mutates: r.Mutates,
	}
}

// Response is an exchange.HTTPResponse in a fixture.
type Response struct {
	Status int             `json:"status"`
	Header []Header        `json:"header,omitempty"`
	Body   json.RawMessage `json:"body,omitempty"` // JSON bodies stay readable
	Text   string          `json:"text,omitempty"` // any other body
}

// ResponseOf is r as a fixture records it.
func ResponseOf(r *exchange.HTTPResponse) *Response {
	out := &Response{Status: r.Status, Header: headersOf(r.Header)}
	if json.Valid(r.Body) {
		out.Body = append(json.RawMessage(nil), r.Body...)
	} else {
		out.Text = string(r.Body)
	}
	return out
}

// HTTP is the response as the adapter receives it.
func (r *Response) HTTP() *exchange.HTTPResponse {
	body := []byte(r.Text)
	if len(r.Body) > 0 {
		body = append([]byte(nil), r.Body...)
	}
	return &exchange.HTTPResponse{Status: r.Status, Header: headersFrom(r.Header), Body: body}
}

// Session is a loaded session fixture.
type Session struct {
	Name    string
	Meta    Record
	Records []Record // everything after the meta line
}

// LoadSession reads ws/<name>.jsonl from fsys.
func LoadSession(fsys fs.FS, name string) (*Session, error) {
	f, err := fsys.Open("ws/" + name + ".jsonl")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	recs, err := ReadRecords(f)
	if err != nil {
		return nil, fmt.Errorf("conformance: ws/%s.jsonl: %w", name, err)
	}
	if len(recs) == 0 || recs[0].Kind != KindMeta {
		return nil, fmt.Errorf("conformance: ws/%s.jsonl: the first line is not a meta record", name)
	}
	m := recs[0]
	if m.Side != SideMkt && m.Side != SidePriv {
		return nil, fmt.Errorf("conformance: ws/%s.jsonl: side %q, want %q or %q", name, m.Side, SideMkt, SidePriv)
	}
	if m.Side == SideMkt && m.Route == "" {
		return nil, fmt.Errorf("conformance: ws/%s.jsonl: a market session names its route", name)
	}
	return &Session{Name: name, Meta: m, Records: recs[1:]}, nil
}

// Sessions lists the session fixtures in fsys, sorted.
func Sessions(fsys fs.FS) ([]string, error) {
	return list(fsys, "ws", ".jsonl", ".golden.jsonl")
}

// ReadRecords reads JSON Lines. Unknown fields are an error: a fixture is data a test trusts.
func ReadRecords(r io.Reader) ([]Record, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 1<<16), 64<<20)
	var out []Record
	for n := 1; sc.Scan(); n++ {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.DisallowUnknownFields()
		var rec Record
		if err := dec.Decode(&rec); err != nil {
			return nil, fmt.Errorf("line %d: %w", n, err)
		}
		out = append(out, rec)
	}
	return out, sc.Err()
}

// LoadExchange reads http/<name>.json from fsys: a recorded response. Missing is fs.ErrNotExist.
func LoadExchange(fsys fs.FS, name string) (*Exchange, error) {
	b, err := fs.ReadFile(fsys, "http/"+name+".json")
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var ex Exchange
	if err := dec.Decode(&ex); err != nil {
		return nil, fmt.Errorf("conformance: http/%s.json: %w", name, err)
	}
	if ex.Response == nil {
		return nil, fmt.Errorf("conformance: http/%s.json: no response", name)
	}
	return &ex, nil
}

func list(fsys fs.FS, dir, suffix, not string) ([]string, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		n := e.Name()
		if e.IsDir() || !strings.HasSuffix(n, suffix) || strings.HasSuffix(n, not) {
			continue
		}
		out = append(out, strings.TrimSuffix(n, suffix))
	}
	sort.Strings(out)
	return out, nil
}
