package sdk

// Placeholder event types. They become thin wrappers over the generated mftiko.md.v1 and
// mftiko.td.v1 messages once `make proto` has run.
type (
	FeedEvent   struct{}
	ReconState  struct{}
	OrderEvent  struct{}
	FillEvent   struct{}
	RejectEvent struct{}
)
