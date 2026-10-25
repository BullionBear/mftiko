package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestRetainDays(t *testing.T) {
	if _, err := retainFor(0); err == nil || !strings.Contains(err.Error(), "log retain") {
		t.Fatal(err)
	}
	d, err := retainFor(7)
	if err != nil || d != 7*24*time.Hour {
		t.Fatalf("retainFor(7) = %s, %v", d, err)
	}
	err = run(context.Background(), options{}, zerolog.Nop())
	if err == nil || !strings.Contains(err.Error(), "log retain") {
		t.Fatal(err)
	}
}
