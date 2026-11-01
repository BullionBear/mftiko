package artifact

import (
	"bytes"
	"errors"
)

const sdkMarkerPrefix = "mftiko.sdk="

// ErrSDKConflict means the binary embeds two different SDK records (E09-D099).
var ErrSDKConflict = errors.New("artifact: embedded sdk records disagree")

const (
	maxSDKLang    = 32
	maxSDKVersion = 64
)

// embeddedSDK reads the SDK record a non-Go SDK writes into a strategy binary (E09-D099).
// The record is sdkMarkerPrefix, the language, one space, the version, and a newline:
//
//	mftiko.sdk=rust 1.2.0
//
// ok is false when no valid record is present. Identical copies are one record;
// copies that disagree are an error. A prefix not followed by a valid record is skipped.
func embeddedSDK(binary []byte) (sdk SDK, ok bool, err error) {
	prefix := []byte(sdkMarkerPrefix)
	for from := 0; from < len(binary); {
		i := bytes.Index(binary[from:], prefix)
		if i < 0 {
			break
		}
		at := from + i
		if rec, valid := parseSDKRecord(binary[at+len(prefix):]); valid {
			if !ok {
				sdk, ok = rec, true
			} else if rec != sdk {
				return SDK{}, false, ErrSDKConflict
			}
		}
		from = at + 1
	}
	return sdk, ok, nil
}

// parseSDKRecord reads "lang version" up to a newline. lang is a lower-case letter
// followed by lower-case letters and digits. version is letters, digits, '.', '+'
// and '-', and starts with a digit or 'v'. Both are bounded so a stray prefix
// cannot swallow the next string.
func parseSDKRecord(b []byte) (SDK, bool) {
	n := bytes.IndexByte(b[:min(len(b), maxSDKLang+1+maxSDKVersion+1)], '\n')
	if n <= 0 {
		return SDK{}, false
	}
	lang, version, found := bytes.Cut(b[:n], []byte{' '})
	if !found || !validSDKLang(lang) || !validSDKVersion(version) {
		return SDK{}, false
	}
	return SDK{Lang: string(lang), Version: string(version)}, true
}

func validSDKLang(p []byte) bool {
	if len(p) == 0 || len(p) > maxSDKLang || p[0] < 'a' || p[0] > 'z' {
		return false
	}
	for _, c := range p {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}

func validSDKVersion(p []byte) bool {
	if len(p) == 0 || len(p) > maxSDKVersion || !(p[0] >= '0' && p[0] <= '9' || p[0] == 'v') {
		return false
	}
	for _, c := range p {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '.', c == '+', c == '-':
		default:
			return false
		}
	}
	return true
}
