// Command mftiko-seal makes a TD instance's key pair and seals account credentials to it (D613).
//
//	mftiko-seal keygen -key PATH                        write a new private key (0600), print the public key
//	mftiko-seal pubkey -key PATH                        print the public key of a private key file
//	mftiko-seal seal -pubkey KEY|@FILE -api-id N -kind K   read credentials as JSON on stdin, print
//	                                                    state.v1.Account.sealed_credentials in base64
//
// The credentials JSON is {"key_type", "key", "secret", "passphrase"}. They come on stdin, never as
// flags, so they stay out of shell history and the process list. Until the API server writes
// accounts (E10), this is how sealed_credentials is made.
package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
	"github.com/BullionBear/mftiko/internal/platform/sealbox"
)

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "mftiko-seal:", err)
		os.Exit(1)
	}
}

const usage = "usage: mftiko-seal keygen|pubkey|seal [flags], or -version"

func run(args []string, stdin io.Reader, stdout io.Writer) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	if args[0] == "-version" || args[0] == "--version" {
		_, err := fmt.Fprintln(stdout, buildinfo.String())
		return err
	}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	keyPath := fs.String("key", "", "private key file")
	pubFlag := fs.String("pubkey", "", "public key in base64, or @FILE")
	apiID := fs.Int64("api-id", 0, "the account's api_id")
	kind := fs.String("kind", "", "the account kind, e.g. BinanceUM")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	switch args[0] {
	case "keygen":
		if *keyPath == "" {
			return errors.New("keygen needs -key")
		}
		k, err := sealbox.GenerateKey()
		if err != nil {
			return err
		}
		if err := sealbox.WriteKeyFile(*keyPath, k); err != nil {
			return err
		}
		_, err = fmt.Fprintln(stdout, k.Public())
		return err
	case "pubkey":
		if *keyPath == "" {
			return errors.New("pubkey needs -key")
		}
		k, err := sealbox.LoadKeyFile(*keyPath)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(stdout, k.Public())
		return err
	case "seal":
		pub, err := readPublic(*pubFlag)
		if err != nil {
			return err
		}
		var in struct {
			KeyType    string `json:"key_type"`
			Key        string `json:"key"`
			Secret     string `json:"secret"`
			Passphrase string `json:"passphrase"`
		}
		dec := json.NewDecoder(stdin)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&in); err != nil {
			// The decoder's error can quote the input; say only that it did not parse.
			return errors.New("seal reads {\"key_type\", \"key\", \"secret\", \"passphrase\"} as JSON on stdin")
		}
		sealed, err := sealbox.SealCredentials(pub, *apiID, *kind, sealbox.Credentials{
			KeyType: in.KeyType, Key: in.Key, Secret: in.Secret, Passphrase: in.Passphrase,
		})
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(stdout, base64.StdEncoding.EncodeToString(sealed))
		return err
	}
	return errors.New(usage)
}

func readPublic(s string) (sealbox.PublicKey, error) {
	if s == "" {
		return sealbox.PublicKey{}, errors.New("seal needs -pubkey")
	}
	if path, ok := strings.CutPrefix(s, "@"); ok {
		b, err := os.ReadFile(path)
		if err != nil {
			return sealbox.PublicKey{}, err
		}
		s = string(b)
	}
	return sealbox.ParsePublicKey(s)
}
