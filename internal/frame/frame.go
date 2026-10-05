package frame

const (
	// HeaderLen is the length prefix: 4 bytes, big-endian.
	HeaderLen = 4
	// MaxLen is the largest message body accepted on C3 and C4.
	MaxLen = 1 << 20
)
