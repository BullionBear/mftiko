//go:build !linux

package shim

func dupStdio(int) error { return nil }
