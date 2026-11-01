package latencyexp

import (
	"slices"
	"strconv"
	"strings"
)

// Prometheus text for one process (E12-D038 writes text by hand; this exporter
// does the same, so a histogram does not pull in a client library).
const metricsType = "text/plain; version=0.0.4; charset=utf-8"

const (
	mMDHook   = "mftiko_md_hook_delay_seconds"
	mHook     = "mftiko_hook_duration_seconds"
	mExchHook = "mftiko_exch_hook_delay_seconds"
	mOrderAck = "mftiko_order_ack_seconds"
	mOrderEx  = "mftiko_order_exchange_seconds"
	mReply    = "mftiko_order_reply_total"
	mRecGap   = "mftiko_eventlog_rec_gap_total"
	mMDGap    = "mftiko_md_seq_gap_total"

	helpMDHook   = "Seconds from an MD frame's recv_ns to its hook start t_ns. kind is md_feed or md_book."
	helpHook     = "Seconds a hook ran, from the input t_ns to hook_end. kind is the input kind."
	helpExchHook = "Seconds from exch_ts_ns to hook start t_ns. Frames with no exch_ts_ns are omitted. kind is md_feed or md_book."
	helpOrderAck = "Seconds from order_call to order_reply recv_ns. outcome is accepted or local_reject."
	helpOrderEx  = "Seconds from order_call to the first exchange OrderUpdate recv_ns for that client_order_id. outcome is new, accepted, or rejected."
	helpReply    = "order_reply results that are counted rather than timed. outcome is timeout or no_responders."
	helpRecGap   = "Event-log frames missing from rec_seq: dropped by the writer, or deleted before they were read."
	helpMDGap    = "MD envelope sequence numbers skipped inside one feed and conn_epoch. kind is md_feed or md_book."
)

// bucketNs are histogram bounds in nanoseconds. 0 collects observations that
// are zero or negative (exchange and local clocks can disagree). The +Inf
// bucket is added when the text is rendered.
var bucketNs = []int64{
	0,
	1_000, 2_000, 5_000,
	10_000, 20_000, 50_000,
	100_000, 200_000, 500_000,
	1_000_000, 2_000_000, 5_000_000,
	10_000_000, 20_000_000, 50_000_000,
	100_000_000, 200_000_000, 500_000_000,
	1_000_000_000, 2_000_000_000, 5_000_000_000,
	10_000_000_000, 30_000_000_000, 60_000_000_000,
}

type family struct {
	name string
	hist bool
	help string
}

var families = []family{
	{mMDHook, true, helpMDHook},
	{mHook, true, helpHook},
	{mExchHook, true, helpExchHook},
	{mOrderAck, true, helpOrderAck},
	{mOrderEx, true, helpOrderEx},
	{mReply, false, helpReply},
	{mRecGap, false, helpRecGap},
	{mMDGap, false, helpMDGap},
}

// mKey is one series. Empty kind or outcome is omitted from the text, so a
// family does not carry a label it does not use (E17-D004).
type mKey struct {
	name, session, kind, outcome string
}

type hist struct {
	bucket []uint64 // one count per bucketNs entry; not cumulative
	sum    int64
	n      uint64
}

type reg struct {
	hist map[mKey]*hist
	cnt  map[mKey]uint64
}

type point struct {
	name, session, kind, outcome string
	hist                         bool
	ns                           int64
	n                            uint64
}

func (r *reg) add(ps []point) {
	for _, p := range ps {
		k := mKey{p.name, p.session, p.kind, p.outcome}
		if p.hist {
			h := r.hist[k]
			if h == nil {
				h = &hist{bucket: make([]uint64, len(bucketNs))}
				if r.hist == nil {
					r.hist = map[mKey]*hist{}
				}
				r.hist[k] = h
			}
			h.n++
			h.sum += p.ns
			for i, b := range bucketNs {
				if p.ns <= b {
					h.bucket[i]++
					break
				}
			}
			continue
		}
		if p.n == 0 {
			continue
		}
		if r.cnt == nil {
			r.cnt = map[mKey]uint64{}
		}
		r.cnt[k] += p.n
	}
}

func (r *reg) text() []byte {
	var b strings.Builder
	for _, f := range families {
		if b.Len() > 0 {
			b.WriteByte('\n')
		}
		typ := "counter"
		if f.hist {
			typ = "histogram"
		}
		b.WriteString("# HELP ")
		b.WriteString(f.name)
		b.WriteByte(' ')
		b.WriteString(f.help)
		b.WriteByte('\n')
		b.WriteString("# TYPE ")
		b.WriteString(f.name)
		b.WriteByte(' ')
		b.WriteString(typ)
		b.WriteByte('\n')
		if f.hist {
			r.writeHist(&b, f.name)
		} else {
			r.writeCnt(&b, f.name)
		}
	}
	return []byte(b.String())
}

func (r *reg) writeHist(b *strings.Builder, name string) {
	var keys []mKey
	for k := range r.hist {
		if k.name == name {
			keys = append(keys, k)
		}
	}
	sortKeys(keys)
	for _, k := range keys {
		h := r.hist[k]
		labels := labelText(k)
		var cum uint64
		for i, bound := range bucketNs {
			cum += h.bucket[i]
			writeSample(b, name+"_bucket", labels, "le", formatSeconds(bound), strconv.FormatUint(cum, 10))
		}
		writeSample(b, name+"_bucket", labels, "le", "+Inf", strconv.FormatUint(h.n, 10))
		writeLine(b, name+"_sum", labels, formatSeconds(h.sum))
		writeLine(b, name+"_count", labels, strconv.FormatUint(h.n, 10))
	}
}

func (r *reg) writeCnt(b *strings.Builder, name string) {
	var keys []mKey
	for k, n := range r.cnt {
		if k.name == name && n > 0 {
			keys = append(keys, k)
		}
	}
	sortKeys(keys)
	for _, k := range keys {
		writeLine(b, name, labelText(k), strconv.FormatUint(r.cnt[k], 10))
	}
}

func sortKeys(keys []mKey) {
	slices.SortFunc(keys, func(a, b mKey) int {
		if c := strings.Compare(a.session, b.session); c != 0 {
			return c
		}
		if c := strings.Compare(a.kind, b.kind); c != 0 {
			return c
		}
		return strings.Compare(a.outcome, b.outcome)
	})
}

func labelText(k mKey) string {
	var parts []string
	if k.session != "" {
		parts = append(parts, `session_id="`+escapeLabel(k.session)+`"`)
	}
	if k.kind != "" {
		parts = append(parts, `kind="`+escapeLabel(k.kind)+`"`)
	}
	if k.outcome != "" {
		parts = append(parts, `outcome="`+escapeLabel(k.outcome)+`"`)
	}
	if len(parts) == 0 {
		return ""
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func writeSample(b *strings.Builder, name, labels, extraName, extraValue, value string) {
	b.WriteString(name)
	if labels == "" {
		b.WriteByte('{')
	} else {
		b.WriteString(labels[:len(labels)-1])
		b.WriteByte(',')
	}
	b.WriteString(extraName)
	b.WriteString(`="`)
	b.WriteString(extraValue)
	b.WriteString(`"} `)
	b.WriteString(value)
	b.WriteByte('\n')
}

func writeLine(b *strings.Builder, name, labels, value string) {
	b.WriteString(name)
	b.WriteString(labels)
	b.WriteByte(' ')
	b.WriteString(value)
	b.WriteByte('\n')
}

func escapeLabel(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '"':
			b.WriteString(`\"`)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// formatSeconds prints ns as a decimal number of seconds. The fraction is the
// nanoseconds, so the text round-trips the integer the histogram stored.
func formatSeconds(ns int64) string {
	if ns < 0 {
		return "-" + formatSeconds(-ns)
	}
	sec := ns / 1_000_000_000
	frac := ns % 1_000_000_000
	if frac == 0 {
		return strconv.FormatInt(sec, 10)
	}
	s := strconv.FormatInt(frac, 10)
	for len(s) < 9 {
		s = "0" + s
	}
	s = strings.TrimRight(s, "0")
	return strconv.FormatInt(sec, 10) + "." + s
}
