package envelope

// Layout v1. Readers always skip to the offset in the Len byte, so later versions may grow the envelope.
const (
	V1    = 1  // value of the version byte
	LenV1 = 32 // envelope length in bytes for v1

	OffVersion   = 0  // uint8
	OffLen       = 1  // uint8: envelope length; the payload starts here
	OffMDTsNs    = 8  // int64 LE: conn worker publish time, MD host clock
	OffConnEpoch = 16 // uint64 LE
	OffSeq       = 24 // uint64 LE: per (feed, conn_epoch), restarts at 1 after a reconnect
)
