package conn

import "testing"

func TestBusSet(t *testing.T) {
	b := NewBus(true)
	up, known := b.Get()
	if !up || !known {
		t.Fatalf("start %v %v", up, known)
	}
	b.Set(true)
	select {
	case <-b.signal:
		t.Fatal("unchanged set signaled")
	default:
	}
	b.Set(false)
	up, known = b.Get()
	if up || !known {
		t.Fatalf("down %v %v", up, known)
	}
	select {
	case <-b.signal:
	default:
		t.Fatal("change did not signal")
	}
	b.Set(true)
	b.Set(false)
	up, _ = b.Get()
	if up {
		t.Fatal("coalesced value")
	}
}

func TestBindBus(t *testing.T) {
	var disc, recon func()
	b := NewBus(true)
	BindBus(b, func(fn func()) { disc = fn }, func(fn func()) { recon = fn })
	if disc == nil || recon == nil {
		t.Fatal("callbacks")
	}
	disc()
	up, known := b.Get()
	if up || !known {
		t.Fatalf("disconnect %v %v", up, known)
	}
	recon()
	up, known = b.Get()
	if !up || !known {
		t.Fatalf("reconnect %v %v", up, known)
	}
	BindBus(nil, func(func()) {}, func(func()) {})
}
