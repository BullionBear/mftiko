package agent

import (
	"testing"
)

func TestLogViewReadyAndReuse(t *testing.T) {
	v := newLogView()
	const id = "a1b2c3"
	ch := v.watch(id)
	select {
	case <-ch:
		t.Fatal("watch closed before the first spec list")
	default:
	}
	if _, ready := v.present(id); ready {
		t.Fatal("ready before setSpecs")
	}
	v.setSpecs(map[string]struct{}{id: {}})
	select {
	case <-v.readyCh:
	default:
		t.Fatal("ready channel open")
	}
	select {
	case <-ch:
		t.Fatal("watch closed while the spec is present")
	default:
	}
	present, ready := v.present(id)
	if !present || !ready {
		t.Fatalf("present %v ready %v", present, ready)
	}
	v.setPhase(id, true)
	if !v.terminal(id) {
		t.Fatal("phase did not stick")
	}
	v.setPhase(id, false)
	if v.terminal(id) {
		t.Fatal("terminal is absorbing")
	}
	v.setPhase(id, true)
	v.setSpecs(map[string]struct{}{})
	select {
	case <-ch:
	default:
		t.Fatal("watch stayed open after the spec left")
	}
	if v.terminal(id) {
		t.Fatal("terminal survived the spec leaving")
	}
	gone := v.watch(id)
	select {
	case <-gone:
	default:
		t.Fatal("absent spec returned an open watch")
	}
	v.setSpecs(map[string]struct{}{id: {}})
	if v.terminal(id) {
		t.Fatal("reused session id inherited terminal")
	}
	again := v.watch(id)
	select {
	case <-again:
		t.Fatal("watch of the new spec is already closed")
	default:
	}
	if again == gone {
		t.Fatal("reused the closed watch")
	}
}
