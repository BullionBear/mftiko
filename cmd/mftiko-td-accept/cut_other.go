//go:build !linux

package main

import "errors"

func conns() error { return errors.New("needs linux") }

func cut() error { return errors.New("needs linux") }
