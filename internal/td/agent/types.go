package agent

import (
	"errors"
	"strconv"
	"strings"
)

// Reasons Decode returns. They are written on the ack of a demand key.
var (
	errBadKey     = errors.New("bad key")
	errBadDemand  = errors.New("bad demand")
	errNotAccount = errors.New("not an account demand")
	errBadAccount = errors.New("bad account")
	errMismatch   = errors.New("api_id mismatch")
)

// Entry is one key of the two prefixes the agent reads: a demand or an account declaration.
type Entry struct {
	Demand  *Demand
	Account *Account
}

// Demand is one session's need of one account (PROTOCOL §3.3).
type Demand struct {
	Session string
	APIID   int64
}

// Account is the API server's declaration of one account (PROTOCOL §3.6).
type Account struct {
	APIID               int64
	Kind                string
	Sealed              []byte
	Enabled             bool
	CancelOnDisconnectS uint32
}

// splitDemand splits {session}/{api_id}.
func splitDemand(rest string) (session string, apiID int64, ok bool) {
	session, id, ok := strings.Cut(rest, "/")
	if !ok || session == "" || strings.Contains(id, "/") {
		return "", 0, false
	}
	n, err := strconv.ParseInt(id, 10, 64)
	if err != nil || n <= 0 {
		return "", 0, false
	}
	return session, n, true
}
