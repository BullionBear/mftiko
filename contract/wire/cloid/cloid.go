package cloid

// Layout, most significant bits first: ver(4) | algo(10) | ts_ms(40) | seq(10).
// Sent as the decimal string of the uint64. ver stays in 1..7 so the top bit is 0 and an int64 parse works.
const (
	VerBits  = 4
	AlgoBits = 10
	TsBits   = 40
	SeqBits  = 10

	Version = 1

	// EpochMs is 2026-01-01T00:00:00Z in unix milliseconds. 40 bits of ms last until 2060-11.
	EpochMs int64 = 1767225600000

	// AlgoMin and AlgoMax bound a declared algo_id; 0 is reserved (DECISIONS D25).
	AlgoMin = 1
	AlgoMax = 1<<AlgoBits - 1
)
