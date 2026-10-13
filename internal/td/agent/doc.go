// Package agent is the execution-plane agent (D901, D905, D608): one account worker per api_id that
// some session needs, started when the first demand arrives and stopped linger after the last one
// leaves. It writes each demand's ack. It does not talk to an exchange: the account worker does.
//
// It reads two prefixes of its instance: the demand the controller writes,
// /td/{inst}/demand/{session}/{api_id}, and the accounts the API server declares,
// /td/{inst}/account/{api_id}. The owners of an api_id are its demands, a set, not a counter.
//
// An account that is missing or disabled, of an unknown kind, of a kind phase 1 does not run
// (kind_unsupported, D614), or whose sealed credentials do not open with this instance's key
// (sealbox.Reason, D613), is REJECTED and gets no worker. The agent does not check algo_ids: the STS
// agent does, for its own sessions (D616).
//
// At spawn the credentials are unsealed and sent in Init with a NATS user signed for that api_id
// (D611), and the venue hosts this instance uses for the account's kind (D616). The plaintext lives
// only while Init is built.
//
// The ack is READY once the worker reports READY or DEGRADED (D603). A worker that ended for its
// credentials or its position mode is REJECTED and not started again (D608); one the Policy gave up
// on is DEAD (P6). Workers outlive the agent (D102): a new term adopts them.
package agent
