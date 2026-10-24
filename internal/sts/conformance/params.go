package conformance

// Params is the JSON object written into InitPayload.params (E08-D035, PROTOCOL §9.8).
// The tool fills it. Strategies in every language read the same fields.
type Params struct {
	HookSleepMS   int    `json:"hook_sleep_ms,omitempty"`
	OrderCount    int    `json:"order_count,omitempty"`
	InitSleepMS   int    `json:"init_sleep_ms,omitempty"`
	StartSleepMS  int    `json:"start_sleep_ms,omitempty"`
	OnStopSleepMS int    `json:"on_stop_sleep_ms,omitempty"`
	SelfExit      bool   `json:"self_exit,omitempty"`
	ProbeReads    bool   `json:"probe_reads,omitempty"`
	Scenario      string `json:"scenario,omitempty"`
	UntilSeq      uint64 `json:"until_seq,omitempty"`
	APIID         int64  `json:"api_id,omitempty"`
	AlgoID        uint32 `json:"algo_id,omitempty"`
	UID           uint32 `json:"uid,omitempty"`
}

func baseParams(scenario string) Params {
	return Params{Scenario: scenario, APIID: apiID, AlgoID: algoID, UID: uid}
}
