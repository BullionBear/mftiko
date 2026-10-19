package natsdev

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/nats-io/nkeys"
)

// Signer signs the NATS user of a TD account worker with the MFTIKO account's signing key (D611,
// D414). The TD agent holds one and signs a user at every spawn: a new user key each time, with
// the account worker's allow lists (tdAccountPerms) and the response permission. The user's seed is
// returned for tdctl.InitPayload.nats and is never written anywhere. The JWT does not expire
// (D616): a worker that is spawned again gets a new one.
//
// It is in natsdev because the allow lists are: the dev layout and the check sign the same user.
type Signer struct {
	account string
	key     nkeys.KeyPair
}

// LoadSigner reads the signing key's seed at path, for the account whose public key is account.
// In the dev layout these are Manifest.SigningKeyFile and Manifest.Account.
func LoadSigner(account, path string) (*Signer, error) {
	if !nkeys.IsValidPublicAccountKey(account) {
		return nil, errors.New("natsdev: signer: not an account public key")
	}
	kp, err := readKey(path, keyAccountKind)
	if err != nil {
		return nil, fmt.Errorf("natsdev: signer: %w", err)
	}
	return &Signer{account: account, key: kp}, nil
}

// AccountPublic is the MFTIKO account this signer issues for.
func (s *Signer) AccountPublic() string { return s.account }

// SigningKey is the account signing key. The STS agent signs session users with it (D924).
func (s *Signer) SigningKey() nkeys.KeyPair { return s.key }

// AccountWorker is a new user for the account worker of apiID: its JWT, its seed, and the inbox
// prefix it must use (_INBOX_td_{api_id}).
func (s *Signer) AccountWorker(apiID int64) (userJWT, seed, inbox string, err error) {
	inbox, err = TDAccountInbox(apiID)
	if err != nil {
		return "", "", "", err
	}
	pub, sub, err := tdAccountPerms(apiID)
	if err != nil {
		return "", "", "", err
	}
	kp, err := nkeys.CreateUser()
	if err != nil {
		return "", "", "", err
	}
	userPub, err := kp.PublicKey()
	if err != nil {
		return "", "", "", err
	}
	token, err := encodeUser(userPub, "td-account-"+strconv.FormatInt(apiID, 10), pub, sub, true, s.account, s.key)
	if err != nil {
		return "", "", "", err
	}
	sd, err := kp.Seed()
	if err != nil {
		return "", "", "", err
	}
	return token, string(sd), inbox, nil
}
