package codec

import (
	"strconv"

	"github.com/BullionBear/mftiko/contract/wire/decimal"
)

// Query is a URL query built by appending, without url.Values: no map, no sorting, nothing allocated beyond
// the buffer. Pairs stay in the order they were added, which is the order the venue signs them in. Keys and
// values are percent-encoded per RFC 3986: every byte but A–Z a–z 0–9 - . _ ~ becomes %XX in upper case, so
// a space is %20 and a non-ASCII symbol is its UTF-8 bytes, which is what Binance signs.
//
//	q := codec.Query(buf[:0]).Add("symbol", "BTCUSDT").AddInt("timestamp", now.UnixMilli())
type Query []byte

// Add appends key=value.
func (q Query) Add(key, value string) Query { return appendPair(q, key, value) }

// AddBytes appends key=value: a signature, without converting it to a string.
func (q Query) AddBytes(key string, value []byte) Query { return appendPair(q, key, value) }

// AddInt appends key=v in decimal: a timestamp, a limit, an order id.
func (q Query) AddInt(key string, v int64) Query {
	q = q.key(key)
	return strconv.AppendInt(q, v, 10)
}

// AddDecimal appends key=v snapped to step, with the step's decimals and never an exponent (decimal.Append,
// D008). v must be present: decimal.Absent would write NaN.
func (q Query) AddDecimal(key string, v, step float64) Query {
	q = q.key(key)
	return decimal.Append(q, v, step)
}

// key appends the separator, key and '='.
func (q Query) key(key string) Query {
	if len(q) > 0 {
		q = append(q, '&')
	}
	q = escape(q, key)
	return append(q, '=')
}

func appendPair[V string | []byte](q Query, key string, value V) Query {
	return escape(q.key(key), value)
}

const upperHex = "0123456789ABCDEF"

func escape[S string | []byte](dst Query, s S) Query {
	for i := 0; i < len(s); i++ {
		if c := s[i]; unreserved(c) {
			dst = append(dst, c)
		} else {
			dst = append(dst, '%', upperHex[c>>4], upperHex[c&0xF])
		}
	}
	return dst
}

func unreserved(c byte) bool {
	return 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' ||
		c == '-' || c == '.' || c == '_' || c == '~'
}
