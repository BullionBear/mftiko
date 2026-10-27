package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
)

// pageLimit is the limit on every automatic page request (E11-D010).
const pageLimit = 1000

var errNotList = errors.New("response is not a list")

// pageMeta is the partial-availability signal on one page (E11-D012).
// Regions are read from a top-level unavailable_regions and from
// freshness.unavailable_regions. controller_absent is read from either place.
type pageMeta struct {
	regions []string
	absent  bool
}

// fetched is every page a read-only list collected. single means --cursor:
// raw is that page's body, unchanged (E11-D011).
type fetched struct {
	items   []json.RawMessage
	regions []string
	absent  bool
	ids     []string
	raw     []byte
	single  bool
	// extra is stable fields kept across automatic pages (E10-D121). Strategy list
	// keeps repository and latest. Other lists leave it nil.
	extra map[string]json.RawMessage
}

// rejectPageFlags refuses paging flags on commands that do not page.
func (a *App) rejectPageFlags() int {
	if a.opt.limitSet || a.opt.cursorSet {
		return a.exit("-limit and -cursor are only for strategy list, account list, session list, and record list")
	}
	return 0
}

// paging resolves E11-D010. --limit caps the total row count. --cursor is one
// page. Passing both is a usage error, exit 1 (E11-D010 supplement).
func (a *App) paging() (single bool, cursor string, total int, code int) {
	if a.opt.limitSet && a.opt.cursorSet {
		return false, "", 0, a.exit("-limit and -cursor cannot both be set")
	}
	if a.opt.cursorSet {
		if a.opt.cursor == "" {
			return false, "", 0, a.exit("-cursor needs a value")
		}
		return true, a.opt.cursor, 0, 0
	}
	if a.opt.limitSet {
		total = a.opt.limit
	}
	return false, "", total, 0
}

func (a *App) preparePaged() (*API, int) {
	if code := a.onlyFlags("limit", "cursor"); code != 0 {
		return nil, code
	}
	if _, _, _, code := a.paging(); code != 0 {
		return nil, code
	}
	return a.api()
}

func (a *App) prepareOne() (*API, int) {
	if code := a.rejectPageFlags(); code != 0 {
		return nil, code
	}
	if code := a.onlyFlags(); code != 0 {
		return nil, code
	}
	return a.api()
}

type fetchFunc func(limit int, cursor string) (Result, error)

// collect follows next_cursor until it is absent (E11-D010). A repeated cursor
// stops the walk. A failed page writes nothing. --limit stops once that many
// rows are in hand, without requesting another page.
func (a *App) collect(fetch fetchFunc) (fetched, int) {
	return a.collectExtra(fetch, nil)
}

// collectExtra is collect, and also keeps the named top-level fields from the
// first page that has them (E10-D121). A null value is not kept.
func (a *App) collectExtra(fetch fetchFunc, keep []string) (fetched, int) {
	single, cursor, total, code := a.paging()
	if code != 0 {
		return fetched{}, code
	}
	seen := map[string]struct{}{}
	var out fetched
	out.single = single
	for {
		if cursor != "" {
			if _, ok := seen[cursor]; ok {
				return fetched{}, a.exitCode(ExitNetwork, "network (2): pagination cursor repeated")
			}
			seen[cursor] = struct{}{}
		}
		res, err := fetch(pageLimit, cursor)
		if code := a.checked(res, err); code != 0 {
			return fetched{}, code
		}
		items, next, meta, err := decodePage(res.Body)
		if err != nil {
			return fetched{}, a.exit(err.Error())
		}
		out.regions = addRegions(out.regions, meta.regions)
		if meta.absent {
			out.absent = true
		}
		if res.RequestID != "" {
			out.ids = append(out.ids, res.RequestID)
		}
		for k, v := range keepFields(res.Body, keep) {
			if out.extra == nil {
				out.extra = map[string]json.RawMessage{}
			}
			if _, ok := out.extra[k]; !ok {
				out.extra[k] = v
			}
		}
		if single {
			out.raw = res.Body
			out.items = items
			return out, 0
		}
		if total > 0 {
			need := total - len(out.items)
			if need < len(items) {
				items = items[:need]
			}
		}
		out.items = append(out.items, items...)
		if total > 0 && len(out.items) >= total {
			return out, 0
		}
		if next == "" {
			return out, 0
		}
		cursor = next
	}
}

func decodePage(body []byte) ([]json.RawMessage, string, pageMeta, error) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(body, &top); err != nil {
		return nil, "", pageMeta{}, errNotList
	}
	raw, ok := top["items"]
	if !ok || string(raw) == "null" {
		return nil, "", pageMeta{}, errNotList
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil || items == nil {
		return nil, "", pageMeta{}, errNotList
	}
	next := ""
	if raw, ok := top["next_cursor"]; ok && string(raw) != "null" {
		if err := json.Unmarshal(raw, &next); err != nil {
			return nil, "", pageMeta{}, errNotList
		}
	}
	return items, next, metaOf(top), nil
}

func keepFields(body []byte, keep []string) map[string]json.RawMessage {
	if len(keep) == 0 {
		return nil
	}
	var top map[string]json.RawMessage
	if json.Unmarshal(body, &top) != nil {
		return nil
	}
	out := map[string]json.RawMessage{}
	for _, k := range keep {
		raw, ok := top[k]
		if !ok || string(raw) == "null" {
			continue
		}
		out[k] = raw
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func readMeta(body []byte) (pageMeta, error) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(body, &top); err != nil {
		return pageMeta{}, err
	}
	return metaOf(top), nil
}

func metaOf(top map[string]json.RawMessage) pageMeta {
	var m pageMeta
	m.regions = addRegions(nil, readRegions(top["unavailable_regions"]))
	if raw, ok := top["controller_absent"]; ok && string(raw) != "null" {
		var v bool
		if json.Unmarshal(raw, &v) == nil && v {
			m.absent = true
		}
	}
	if raw, ok := top["freshness"]; ok && string(raw) != "null" {
		var fr struct {
			UnavailableRegions []string `json:"unavailable_regions"`
			ControllerAbsent   bool     `json:"controller_absent"`
		}
		if json.Unmarshal(raw, &fr) == nil {
			m.regions = addRegions(m.regions, fr.UnavailableRegions)
			if fr.ControllerAbsent {
				m.absent = true
			}
		}
	}
	return m
}

func readRegions(raw json.RawMessage) []string {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var ss []string
	if json.Unmarshal(raw, &ss) != nil {
		return nil
	}
	return ss
}

func addRegions(dst, src []string) []string {
	seen := make(map[string]struct{}, len(dst)+len(src))
	for _, r := range dst {
		seen[r] = struct{}{}
	}
	for _, r := range src {
		if r == "" {
			continue
		}
		if _, ok := seen[r]; ok {
			continue
		}
		seen[r] = struct{}{}
		dst = append(dst, r)
	}
	return dst
}

// mergeJSON is the automatic-page -o json body (E11-D011). Item objects stay
// the bytes the server sent. unavailable_regions is omitted when the union is
// empty. next_cursor and controller_absent are not written.
func mergeJSON(items []json.RawMessage, regions []string, extra map[string]json.RawMessage) []byte {
	var b bytes.Buffer
	b.WriteString(`{"items":[`)
	for i, item := range items {
		if i > 0 {
			b.WriteByte(',')
		}
		b.Write(item)
	}
	b.WriteByte(']')
	if len(extra) > 0 {
		keys := make([]string, 0, len(extra))
		for k := range extra {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			enc, err := json.Marshal(k)
			if err != nil {
				continue
			}
			b.WriteByte(',')
			b.Write(enc)
			b.WriteByte(':')
			b.Write(extra[k])
		}
	}
	if len(regions) > 0 {
		b.WriteString(`,"unavailable_regions":`)
		enc, err := json.Marshal(regions)
		if err != nil {
			enc = []byte("[]")
		}
		b.Write(enc)
	}
	b.WriteString("}\n")
	return b.Bytes()
}

func warnPartial(w io.Writer, regions []string, absent bool, sec secrets) {
	if len(regions) == 0 && !absent {
		return
	}
	msg := "mftiko: warning:"
	if len(regions) > 0 {
		msg += " unavailable regions: " + strings.Join(regions, ", ")
	}
	if absent {
		if len(regions) > 0 {
			msg += ";"
		}
		msg += " controller absent"
	}
	fmt.Fprintln(w, sec.redact(msg))
}

func (a *App) note(ids []string) {
	if !a.Verbose {
		return
	}
	for _, id := range ids {
		if id == "" {
			continue
		}
		fmt.Fprintf(a.Stderr, "X-Request-Id: %s\n", a.sec.redact(id))
	}
}

func (a *App) writeRaw(body []byte) int {
	if _, err := a.Stdout.Write(a.sec.redactBytes(body)); err != nil {
		return a.exit(err.Error())
	}
	return 0
}

func (a *App) finishPaged(f fetched, render func([]json.RawMessage) (string, error)) int {
	if a.JSON {
		body := f.raw
		if !f.single {
			body = mergeJSON(f.items, f.regions, f.extra)
		}
		if code := a.writeRaw(body); code != 0 {
			return code
		}
	} else {
		text, err := render(f.items)
		if err != nil {
			return a.exit(err.Error())
		}
		if _, err := io.WriteString(a.Stdout, a.sec.redact(text)); err != nil {
			return a.exit(err.Error())
		}
	}
	warnPartial(a.Stderr, f.regions, f.absent, a.sec)
	a.note(f.ids)
	return ExitOK
}

func (a *App) finishOne(res Result, render func([]byte) (string, error)) int {
	if a.JSON {
		if code := a.writeRaw(res.Body); code != 0 {
			return code
		}
	} else {
		text, err := render(res.Body)
		if err != nil {
			return a.exit(err.Error())
		}
		if _, err := io.WriteString(a.Stdout, a.sec.redact(text)); err != nil {
			return a.exit(err.Error())
		}
	}
	if meta, err := readMeta(res.Body); err == nil {
		warnPartial(a.Stderr, meta.regions, meta.absent, a.sec)
	}
	a.note([]string{res.RequestID})
	return ExitOK
}
