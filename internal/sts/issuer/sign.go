package issuer

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"

	"github.com/BullionBear/mftiko/internal/platform/natsdev"
	"github.com/BullionBear/mftiko/internal/sts"
)

// ErrTooManyFeeds is a session spec whose CONNECT argument is longer than
// max_control_line. The server would refuse the connection (D827).
var ErrTooManyFeeds = errors.New("sts: too many feeds")

// clientProtoInfo is nats.go's unexported protocol value (the second iota,
// 1): the client accepts further INFO messages. connectInfo below is that
// client's CONNECT JSON. TestConnectLimit fails when either drifts.
const clientProtoInfo = 1

// connectInfo is the JSON object nats.go writes as the CONNECT argument.
// Field order is the encoding/json order. omitempty matches the client.
// Name has no omitempty, so an empty client name is still present.
type connectInfo struct {
	Verbose      bool   `json:"verbose"`
	Pedantic     bool   `json:"pedantic"`
	UserJWT      string `json:"jwt,omitempty"`
	Nkey         string `json:"nkey,omitempty"`
	Signature    string `json:"sig,omitempty"`
	User         string `json:"user,omitempty"`
	Pass         string `json:"pass,omitempty"`
	Token        string `json:"auth_token,omitempty"`
	TLS          bool   `json:"tls_required"`
	Name         string `json:"name"`
	Lang         string `json:"lang"`
	Version      string `json:"version"`
	Protocol     int    `json:"protocol"`
	Echo         bool   `json:"echo"`
	Headers      bool   `json:"headers"`
	NoResponders bool   `json:"no_responders"`
}

var (
	sigLenOnce sync.Once
	sigLen     int
	sigLenErr  error
)

// nonceSignatureLen is the base64.RawURLEncoding length of a user nkey's
// signature over a nonce. nats.go encodes the CONNECT sig the same way.
// Ed25519 signatures are a fixed length, so the nonce itself does not matter.
func nonceSignatureLen() (int, error) {
	sigLenOnce.Do(func() {
		kp, err := nkeys.CreateUser()
		if err != nil {
			sigLenErr = err
			return
		}
		sig, err := kp.Sign([]byte("nonce"))
		if err != nil {
			sigLenErr = err
			return
		}
		sigLen = len(base64.RawURLEncoding.EncodeToString(sig))
	})
	return sigLen, sigLenErr
}

// connectArg is the CONNECT JSON argument nats.go sends for a session
// connection: user JWT, a nonce signature of the measured length, no client
// name, headers and no_responders on. That is the natsx session path when
// Options.Name is empty (D906, D827). Inbox prefix, reconnect, and handlers
// are not part of this JSON.
//
// The placeholder signature is only as long as a real one. Its bytes are
// not a signature. A user JWT is base64url plus dots, and the placeholder
// is ASCII, so neither is escaped by encoding/json and the length is the
// length of the argument the server measures.
func connectArg(userJWT string) ([]byte, error) {
	n, err := nonceSignatureLen()
	if err != nil {
		return nil, err
	}
	return json.Marshal(connectInfo{
		UserJWT:      userJWT,
		Signature:    strings.Repeat("A", n),
		Lang:         nats.LangString,
		Version:      nats.Version,
		Protocol:     clientProtoInfo,
		Echo:         true,
		Headers:      true,
		NoResponders: true,
	})
}

// SignSession signs the session user for perms and returns the user JWT and
// the user seed. The allow lists are perms.Pub and perms.Sub. There is no
// deny list and no response permission: a session calls, it does not answer
// (D821). The JWT does not expire (D906). The seed is for the worker's
// Init payload; this function does not write it.
//
// The size check is here, not in SessionPermissions. After signing, the
// CONNECT argument is measured as connectArg describes. The server rejects
// a client when that argument is longer than max_control_line
// (len(arg) > limit). The same predicate returns ErrTooManyFeeds, whose
// text says there are too many feeds. A JWT that would not connect is not
// returned.
func SignSession(sessionID string, perms sts.Permissions, account string, signer nkeys.KeyPair) (userJWT, seed string, err error) {
	token, seed, err := encode(sessionID, perms, account, signer)
	if err != nil {
		return "", "", err
	}
	arg, err := connectArg(token)
	if err != nil {
		return "", "", err
	}
	if len(arg) > natsdev.MaxControlLine {
		return "", "", fmt.Errorf("%w: CONNECT argument is %d bytes, max_control_line is %d", ErrTooManyFeeds, len(arg), natsdev.MaxControlLine)
	}
	return token, seed, nil
}

// encode signs the user JWT without the size check. SignSession is the
// function callers use. Tests that must show the server refusing an
// oversized line call encode directly.
func encode(sessionID string, perms sts.Permissions, account string, signer nkeys.KeyPair) (userJWT, seed string, err error) {
	if signer == nil {
		return "", "", errors.New("sts: nil session signing key")
	}
	if account == "" {
		return "", "", errors.New("sts: empty issuer account")
	}
	kp, err := nkeys.CreateUser()
	if err != nil {
		return "", "", err
	}
	pub, err := kp.PublicKey()
	if err != nil {
		return "", "", err
	}
	uc := jwt.NewUserClaims(pub)
	uc.Name = "session-" + sessionID
	uc.IssuerAccount = account
	uc.Pub.Allow.Add(perms.Pub...)
	uc.Sub.Allow.Add(perms.Sub...)
	token, err := uc.Encode(signer)
	if err != nil {
		return "", "", fmt.Errorf("sts: session jwt: %w", err)
	}
	sd, err := kp.Seed()
	if err != nil {
		return "", "", fmt.Errorf("sts: session seed: %w", err)
	}
	return token, string(sd), nil
}
