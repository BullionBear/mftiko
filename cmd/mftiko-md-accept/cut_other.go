//go:build !linux

package main

import "errors"

// conns and cut read /proc and use pidfd_getfd. The conn workers run under the shim, which
// needs Linux too.
func conns() error { return errors.New("needs linux") }

func cut() error { return errors.New("needs linux") }
