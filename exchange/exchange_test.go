package exchange

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/contract/symbol"
)

func TestCredentialsNeverFormat(t *testing.T) {
	c := Credentials{Type: KeyHMAC, Key: "k-visible", Secret: "s3cret", Passphrase: "pp"}
	wrapped := AccountConfig{APIID: 7, Credentials: c}

	var buf bytes.Buffer
	log := zerolog.New(&buf)
	log.Info().Interface("cfg", wrapped).Interface("c", c).Msg("x")
	j, _ := json.Marshal(wrapped)

	outs := []string{fmt.Sprint(c), fmt.Sprint(&c), string(j), buf.String()}
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d", "%t", "%T"} {
		outs = append(outs, fmt.Sprintf(verb, c), fmt.Sprintf(verb, &c), fmt.Sprintf(verb, wrapped))
	}
	for _, out := range outs {
		for _, secret := range []string{"s3cret", "pp", "k-visible"} {
			if strings.Contains(out, secret) {
				t.Errorf("leaked %q in %s", secret, out)
			}
		}
	}
}

func TestRegistry(t *testing.T) {
	venue := func(name string) Venue {
		return Venue{Venue: symbol.Venue{Name: name, Categories: []symbol.Category{symbol.Spot}}}
	}
	kind := func(name string, trades ...string) AccountKind {
		return AccountKind{Name: name, Trades: trades}
	}
	bad := map[string][]Declaration{
		"duplicate venue": {
			{Exchange: "a", Venues: []Venue{venue("X")}},
			{Exchange: "b", Venues: []Venue{venue("X")}},
		},
		"venue without categories": {{Exchange: "a", Venues: []Venue{{Venue: symbol.Venue{Name: "Y"}}}}},
		"duplicate account kind": {
			{Exchange: "a", Venues: []Venue{venue("X")}, AccountKinds: []AccountKind{kind("K", "X")}},
			{Exchange: "b", AccountKinds: []AccountKind{kind("K", "X")}},
		},
		"kind trading an unknown venue": {{Exchange: "a", AccountKinds: []AccountKind{kind("K", "Nowhere")}}},
		"kind trading nothing":          {{Exchange: "a", AccountKinds: []AccountKind{kind("K")}}},
	}
	for name, decls := range bad {
		if _, err := NewRegistry(decls...); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}

	// A venue and an account kind may share a name: two enums (D301).
	r, err := NewRegistry(
		Declaration{Exchange: "a", Venues: []Venue{venue("X"), venue("W")}, AccountKinds: []AccountKind{kind("X", "X"), kind("XPM", "X", "W")}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(r.Venues(), ","); got != "W,X" {
		t.Fatalf("Venues() = %s", got)
	}
	if got := strings.Join(r.AccountKinds(), ","); got != "X,XPM" {
		t.Fatalf("AccountKinds() = %s", got)
	}
	if _, ok := r.Lookup("XPM"); ok {
		t.Fatal("an account kind resolved as a venue")
	}
	pm, _ := r.AccountKind("XPM")
	if !pm.CanTrade("W") || pm.CanTrade("XPM") {
		t.Fatal("CanTrade")
	}
}
