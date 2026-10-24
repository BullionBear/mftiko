package cli

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

const maxCred = 1 << 20

var errCredTooLarge = errors.New("credential is too large")

// credIn is one credential field. raw keeps every byte, which is the ed25519
// PEM. HMAC values drop one trailing newline after the read (E11-D014).
type credIn struct {
	flag  string
	label string
	file  stringOpt
	path  string
	raw   bool
	need  bool
}

// loadCredentials reads the set the kind requires (E11-D014, E11-D015).
// A passphrase file on a kind that does not use one is rejected before any
// secret is read. Off a terminal, or when one field is "-", every required
// field must already be a file: "-" consumes stdin, so nothing else can prompt.
func (a *App) loadCredentials(kind apiv1.AccountKind, kt apiv1.KeyType) (*apiv1.Credentials, int) {
	if a.opt.passFile.set && !kind.Passphrase {
		return nil, a.validation([]apiv1.FieldError{{
			Path: "/credentials/passphrase", Reason: "is not used by this account kind",
		}})
	}
	specs := []credIn{
		{flag: "key-file", label: "Key", file: a.opt.keyFile, path: "/credentials/key", need: true},
		{flag: "secret-file", label: "Secret", file: a.opt.secretFile, path: "/credentials/secret", raw: kt == apiv1.Ed25519, need: true},
		{flag: "passphrase-file", label: "Passphrase", file: a.opt.passFile, path: "/credentials/passphrase", need: kind.Passphrase},
	}
	dashes := 0
	for _, s := range specs {
		if s.file.set && s.file.v == "-" {
			dashes++
		}
	}
	if dashes > 1 {
		return nil, a.exit("at most one credential file can be -")
	}
	if dashes == 1 {
		for _, s := range specs {
			if s.need && !s.file.set {
				return nil, a.exit("- reads stdin; pass the other credential files")
			}
		}
	}
	if !a.tty {
		var missing []apiv1.FieldError
		for _, s := range specs {
			if s.need && !s.file.set {
				missing = append(missing, apiv1.FieldError{Path: s.path, Reason: "required"})
			}
		}
		if len(missing) > 0 {
			return nil, a.validation(missing)
		}
	}

	vals := map[string]string{}
	var pending *credIn
	for i := range specs {
		s := &specs[i]
		if !s.file.set {
			continue
		}
		if s.file.v == "-" {
			pending = s
			continue
		}
		v, code := a.readCred(s, false)
		if code != 0 {
			return nil, code
		}
		vals[s.path] = v
	}
	for i := range specs {
		s := &specs[i]
		if !s.need || s.file.set {
			continue
		}
		v, code := a.readCred(s, true)
		if code != 0 {
			return nil, code
		}
		vals[s.path] = v
	}
	if pending != nil {
		v, code := a.readCred(pending, false)
		if code != 0 {
			return nil, code
		}
		vals[pending.path] = v
	}

	var missing []apiv1.FieldError
	for _, s := range specs {
		if s.need && vals[s.path] == "" {
			missing = append(missing, apiv1.FieldError{Path: s.path, Reason: "required"})
		}
	}
	if len(missing) > 0 {
		return nil, a.validation(missing)
	}
	cred := &apiv1.Credentials{
		KeyType: kt,
		Key:     vals["/credentials/key"],
		Secret:  vals["/credentials/secret"],
	}
	if kind.Passphrase {
		pass := vals["/credentials/passphrase"]
		cred.Passphrase = &pass
	}
	return cred, 0
}

func (a *App) readCred(s *credIn, prompt bool) (string, int) {
	var (
		b   []byte
		err error
	)
	if prompt {
		fmt.Fprintf(a.Stderr, "%s: ", s.label)
		b, err = readHidden(a.Stdin, s.raw)
		fmt.Fprintln(a.Stderr)
	} else {
		if s.file.v == "" {
			return "", a.exit("credential file path is empty")
		}
		b, err = readWhole(s.file.v, a.Stdin)
	}
	if err != nil {
		if errors.Is(err, errCredTooLarge) {
			return "", a.exit("credential is too large")
		}
		if prompt {
			return "", a.exit("cannot read credential from the terminal")
		}
		return "", a.exit("cannot read -" + s.flag)
	}
	return a.acceptCred(b, s.raw), 0
}

// acceptCred remembers the value so a later error cannot echo it (E11-D014).
// Only the value that is sent is stored. A trailing newline that HMAC strips
// is not itself a secret: registering it would redact every newline.
func (a *App) acceptCred(b []byte, raw bool) string {
	out := string(b)
	if !raw {
		out = stripOneNewline(b)
	}
	if strings.TrimSpace(out) == "" {
		return out
	}
	a.sec.add(out)
	if strings.Contains(out, "\n") {
		for _, line := range strings.Split(out, "\n") {
			if len(strings.TrimSpace(line)) >= 8 {
				a.sec.add(line)
			}
		}
	}
	return out
}

// stripOneNewline removes one trailing LF, and a CR immediately before it.
// A second trailing newline stays. PEM does not use this (E11-D014).
func stripOneNewline(b []byte) string {
	if len(b) > 0 && b[len(b)-1] == '\n' {
		b = b[:len(b)-1]
		if len(b) > 0 && b[len(b)-1] == '\r' {
			b = b[:len(b)-1]
		}
	}
	return string(b)
}

func readWhole(path string, stdin io.Reader) ([]byte, error) {
	if path == "-" {
		return readLimited(stdin, true)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return readLimited(f, true)
}

// readHidden reads a credential without echo when r is a terminal.
// untilEOF is the ed25519 secret: the value is the bytes through EOF, including
// newlines. Otherwise one line is read and the caller strips one newline.
func readHidden(r io.Reader, untilEOF bool) ([]byte, error) {
	if f, ok := r.(*os.File); ok && stdinIsTerminal(f) {
		restore, err := withoutEcho(f)
		if err != nil {
			return nil, err
		}
		defer restore()
	}
	return readLimited(r, untilEOF)
}

func readLimited(r io.Reader, untilEOF bool) ([]byte, error) {
	if untilEOF {
		b, err := io.ReadAll(io.LimitReader(r, maxCred+1))
		if err != nil {
			return nil, err
		}
		if len(b) > maxCred {
			return nil, errCredTooLarge
		}
		return b, nil
	}
	var buf []byte
	one := make([]byte, 1)
	for {
		if len(buf) >= maxCred {
			return nil, errCredTooLarge
		}
		n, err := r.Read(one)
		if n == 1 {
			buf = append(buf, one[0])
			if one[0] == '\n' {
				return buf, nil
			}
			continue
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return buf, nil
			}
			return nil, err
		}
		return nil, errors.New("cannot read credential")
	}
}
