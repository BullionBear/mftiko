package tdsubject

import (
	"errors"
	"testing"
)

func TestSubjects(t *testing.T) {
	order, err := Order("td-a", 7)
	if err != nil {
		t.Fatal(err)
	}
	view, err := View("td-a", 7)
	if err != nil {
		t.Fatal(err)
	}
	evt, err := Evt("td-a", 7)
	if err != nil {
		t.Fatal(err)
	}
	if order != "td.order.td-a.7" || view != "td.view.td-a.7" || evt != "td.evt.td-a.7" {
		t.Fatalf("order %s view %s evt %s", order, view, evt)
	}
}

func TestTokenRefuses(t *testing.T) {
	for _, inst := range []string{"", "td.a", "td*a", "td>a", "td a", "td\ta", "td\u00a0a"} {
		for _, build := range []func(string, int64) (string, error){Order, View, Evt} {
			if _, err := build(inst, 7); !errors.Is(err, ErrInstance) {
				t.Fatalf("inst %q: %v", inst, err)
			}
		}
		if err := Token(inst); !errors.Is(err, ErrInstance) {
			t.Fatalf("Token %q: %v", inst, err)
		}
	}
	if _, err := Order("td-a", 0); !errors.Is(err, ErrAPIID) {
		t.Fatalf("api_id 0: %v", err)
	}
	if _, err := Order("td-a", -1); !errors.Is(err, ErrAPIID) {
		t.Fatalf("api_id -1: %v", err)
	}
}
