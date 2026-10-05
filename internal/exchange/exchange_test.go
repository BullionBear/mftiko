package exchange

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/internal/symbol"
)

func TestCredentialsNeverFormat(t *testing.T) {
	c := Credentials{Type: KeyHMAC, Key: "k-visible", Secret: "s3cret", Passphrase: "pp"}
	wrapped := AccountConfig{APIID: 7, Credentials: c}

	var buf bytes.Buffer
	log := zerolog.New(&buf)
	log.Info().Interface("cfg", wrapped).Interface("c", c).Msg("x")
	j, _ := json.Marshal(wrapped)

	outs := []string{
		fmt.Sprint(c), fmt.Sprintf("%v %+v %#v %s", c, c, c, c),
		fmt.Sprintf("%v %+v %#v", wrapped, wrapped, wrapped),
		string(j), buf.String(),
	}
	for _, out := range outs {
		for _, secret := range []string{"s3cret", "pp", "k-visible"} {
			if strings.Contains(out, secret) {
				t.Errorf("leaked %q in %s", secret, out)
			}
		}
	}
}

func TestRegistryRejectsDuplicates(t *testing.T) {
	x := symbol.Venue{Name: "X", Categories: []symbol.Category{symbol.Spot}}
	w := symbol.Venue{Name: "W", Categories: []symbol.Category{symbol.Perp}}
	a := []Venue{{Venue: x, Exchange: "a"}}
	b := []Venue{{Venue: x, Exchange: "b"}}
	if _, err := NewRegistry(a, b); err == nil {
		t.Fatal("duplicate venue accepted")
	}
	if _, err := NewRegistry([]Venue{{Exchange: "a"}}); err == nil {
		t.Fatal("unnamed venue accepted")
	}
	if _, err := NewRegistry([]Venue{{Venue: symbol.Venue{Name: "Y"}, Exchange: "a"}}); err == nil {
		t.Fatal("venue without categories accepted")
	}
	r, err := NewRegistry(a, []Venue{{Venue: w, Exchange: "b"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(r.Names(), ","); got != "W,X" {
		t.Fatalf("Names() = %s", got)
	}
}
