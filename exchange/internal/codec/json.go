package codec

import (
	"errors"
	"fmt"
	"sync"
	"unsafe"

	"github.com/bytedance/sonic/decoder"
	"github.com/bytedance/sonic/encoder"

	"github.com/BullionBear/mftiko/contract/wire/decimal"
)

// config is one of the two frozen configurations (D302): sonic decoder options, fixed here. Its decoders come
// from a pool, so a decode allocates nothing of its own; sonic.API would allocate a decoder per call, and its
// Unmarshal copies the whole input first.
type config struct {
	opts decoder.Options
	pool sync.Pool
}

// The two configurations every adapter decodes with. Field names match case-sensitively, because venues give
// different fields keys that differ only in case. Unknown fields are skipped, and what is skipped is still
// validated.
var (
	// borrowed leaves strings pointing into the input: a websocket frame, in the connection's own buffer.
	borrowed = &config{opts: decoder.OptionCaseSensitive}
	// copied copies every string, map keys and the strings inside an any included.
	copied = &config{opts: decoder.OptionCaseSensitive | decoder.OptionCopyString}
)

// DecodeFrame decodes a websocket frame into v without copying it. Strings in v may point into frame and are
// valid only as long as frame is: until Decode returns (D302). Whatever outlives that, the caller copies.
func DecodeFrame(frame []byte, v any) error { return borrowed.decode(frame, v) }

// DecodeBody decodes a REST response body into v. Every string in v is a copy, so v stays valid after the
// transport reuses body.
func DecodeBody(body []byte, v any) error { return copied.decode(body, v) }

func (c *config) decode(in []byte, v any) error {
	d, _ := c.pool.Get().(*decoder.Decoder)
	if d == nil {
		d = new(decoder.Decoder)
	}
	d.Reset(unsafe.String(unsafe.SliceData(in), len(in)))
	d.SetOptions(c.opts)
	err := d.Decode(v)
	if err == nil {
		err = d.CheckTrailings()
	}
	d.Reset("") // a pooled decoder must not keep the caller's buffer alive
	c.pool.Put(d)
	return detach(err)
}

// Marshal encodes v. A request body is marshalled once: those bytes are signed and those bytes are sent
// (D008). NaN and ±Inf are refused; a number goes to a venue as text from decimal.Append.
func Marshal(v any) ([]byte, error) { return encoder.Encode(v, 0) }

// detach fixes err's message now. sonic's syntax errors quote the input, and the caller is about to reuse
// that buffer.
func detach(err error) error {
	if err == nil {
		return nil
	}
	return errors.New("codec: " + err.Error())
}

// Decimal is a number as a venue sends it, quoted ("0.0010") or bare (0.001), read with decimal.ParseBytes
// straight from the input: no string in between (D008). "" and null are decimal.Absent. A field the venue
// leaves out keeps the value it had, so preset Absent where its absence means something.
//
// It is only for reading. Numbers go to a venue with decimal.Append at the instrument's step.
type Decimal float64

// UnmarshalJSON implements json.Unmarshaler, which sonic and encoding/json/v2 both call.
func (d *Decimal) UnmarshalJSON(b []byte) error {
	if n := len(b); n >= 2 && b[0] == '"' && b[n-1] == '"' {
		b = b[1 : n-1]
		if len(b) == 0 {
			*d = Decimal(decimal.Absent())
			return nil
		}
	} else if string(b) == "null" {
		*d = Decimal(decimal.Absent())
		return nil
	}
	v, err := decimal.ParseBytes(b)
	if err != nil {
		return fmt.Errorf("%w: %q", err, b)
	}
	*d = Decimal(v)
	return nil
}
