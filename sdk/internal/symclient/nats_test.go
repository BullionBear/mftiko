package symclient

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	symv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
)

func TestDecodeReply(t *testing.T) {
	var rep symv1.Reply
	if err := decodeReply(encodeReplyError(2, "not_found: uid 9 not found"), &rep); !errors.Is(err, symbol.ErrNotFound) {
		t.Fatalf("not_found: %v", err)
	}
	if err := decodeReply(encodeReplyError(2, "invalid: ticker"), &rep); err == nil || errors.Is(err, symbol.ErrNotFound) {
		t.Fatalf("invalid: %v", err)
	}
	if err := decodeReply([]byte{0x00, 0x02}, &rep); err == nil {
		t.Fatal("short error reply was accepted")
	}
	if err := decodeReply([]byte{0x00, 0x02, 0, 2, 0, 0}, &rep); err == nil {
		t.Fatal("version 2 was accepted")
	}
	okBody, err := proto.Marshal(&symv1.Reply{Result: &symv1.Reply_Info{Info: infoProto(spotBTC(0.1, symbol.Tradable))}})
	if err != nil {
		t.Fatal(err)
	}
	rep = symv1.Reply{}
	if err := decodeReply(okBody, &rep); err != nil {
		t.Fatal(err)
	}
	if rep.GetInfo().GetUid() != 1001 {
		t.Fatalf("info %+v", rep.GetInfo())
	}
}

func TestNATSResolveChangedAndNotFound(t *testing.T) {
	ns := startNATS(t)
	nc := connectNATS(t, ns)
	f := newFake()
	f.seed(spotBTC(0.1, symbol.Tradable))
	serveFake(t, nc, f)

	c := openAt(t, NATS(nc), time.Hour)
	c.AllowResolve(true)
	info, err := c.Resolve(t.Context(), "Bybit_Spot_BTCUSDT")
	if err != nil || info.UID != 1001 || info.Filters[symbol.PriceTick] != 0.1 {
		t.Fatalf("resolve %+v %v", info, err)
	}
	if _, err := c.Resolve(t.Context(), "Bybit_Spot_ETHUSDT"); !errors.Is(err, symbol.ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
	if _, err := c.Info(1002); !errors.Is(err, ErrUnknownUID) {
		t.Fatal(err)
	}

	f.setTick(1001, 0.4)
	rev := f.bump("Bybit", symbol.Spot)
	publishChanged(t, nc, "Bybit", "Spot", rev)
	waitFor(t, func() bool {
		info, err := c.Info(1001)
		return err == nil && info.Filters[symbol.PriceTick] == 0.4
	})

	f.delist(1001, 0.7)
	rev = f.revOf("Bybit", symbol.Spot)
	publishChanged(t, nc, "Bybit", "Spot", rev)
	waitFor(t, func() bool {
		info, err := c.Info(1001)
		return err == nil && info.Status == symbol.Delisted && info.Filters[symbol.PriceTick] == 0.7
	})
}

func (f *fake) revOf(venue string, cat symbol.Category) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rev[revKey(venue, cat)]
}

func serveFake(t *testing.T, nc *nats.Conn, f *fake) {
	t.Helper()
	sub, err := nc.Subscribe(subjectSym, func(m *nats.Msg) {
		if m.Reply == "" {
			return
		}
		var req symv1.Request
		if err := proto.Unmarshal(m.Data, &req); err != nil {
			_ = m.Respond(encodeReplyError(2, "invalid: "+err.Error()))
			return
		}
		var rep symv1.Reply
		err := f.Request(context.Background(), subjectSym, &req, &rep)
		if err != nil {
			word := "unavailable"
			if errors.Is(err, symbol.ErrNotFound) {
				word = "not_found"
			}
			_ = m.Respond(encodeReplyError(2, word+": "+err.Error()))
			return
		}
		b, err := proto.Marshal(&rep)
		if err != nil {
			_ = m.Respond(encodeReplyError(2, "unavailable: "+err.Error()))
			return
		}
		_ = m.Respond(b)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.FlushTimeout(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
}

func publishChanged(t *testing.T, nc *nats.Conn, venue, cat string, rev int64) {
	t.Helper()
	b, err := proto.Marshal(&symv1.Changed{Venue: venue, Category: cat, Revision: rev})
	if err != nil {
		t.Fatal(err)
	}
	subject := "sym.v1.changed." + venue + "." + cat
	if err := nc.Publish(subject, b); err != nil {
		t.Fatal(err)
	}
	if err := nc.FlushTimeout(2 * time.Second); err != nil {
		t.Fatal(err)
	}
}

func encodeReplyError(code uint16, msg string) []byte {
	b := make([]byte, 6+len(msg))
	b[0] = errReplyMark
	b[1] = 1
	binary.BigEndian.PutUint16(b[2:4], code)
	binary.BigEndian.PutUint16(b[4:6], uint16(len(msg)))
	copy(b[6:], msg)
	return b
}

func connectNATS(t *testing.T, url string) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(url, nats.Name("symclient-test"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func startNATS(t *testing.T) string {
	t.Helper()
	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		NoLog:     true,
		NoSigs:    true,
		JetStream: false,
	}
	ns, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		ns.Shutdown()
		t.Fatal("nats-server did not accept connections")
	}
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	addr := ns.Addr()
	if addr == nil {
		t.Fatal("nats-server has no address")
	}
	host, port, err := net.SplitHostPort(addr.String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := strconv.Atoi(port); err != nil {
		t.Fatal(err)
	}
	return "nats://" + host + ":" + port
}
