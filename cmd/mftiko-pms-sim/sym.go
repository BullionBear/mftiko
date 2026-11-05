package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	symv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1"
)

// pmsInbox is the reply prefix the PMS user may subscribe (E21-D009: sym, sym.v1.changed.>, and
// its own inbox). mftiko-md-accept creds -pms signs the same prefix.
const pmsInbox = "_INBOX_pms"

// symClient asks the shared SYM for one instrument by UID, terminal rows included, so a
// delisted instrument still names its exchange ticker (E21-D009).
type symClient struct {
	nc *nats.Conn
}

func dialSym(url, creds string) (*symClient, error) {
	opts := []nats.Option{nats.Name("mftiko-pms-sim"), nats.CustomInboxPrefix(pmsInbox), nats.Timeout(5 * time.Second)}
	if creds != "" {
		opts = append(opts, nats.UserCredentials(creds))
	}
	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, fmt.Errorf("connect to NATS: %w", err)
	}
	return &symClient{nc: nc}, nil
}

func (c *symClient) Close() { c.nc.Close() }

// byUID is SYM's get_by_uid with include_inactive.
func (c *symClient) byUID(ctx context.Context, uid uint32) (*symv1.SymbolInfo, error) {
	req, err := proto.Marshal(&symv1.Request{Req: &symv1.Request_GetByUid{GetByUid: &symv1.GetByUidRequest{Uid: uid, IncludeInactive: true}}})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	msg, err := c.nc.RequestWithContext(ctx, "sym", req)
	if err != nil {
		return nil, fmt.Errorf("sym get_by_uid %d: %w", uid, err)
	}
	if len(msg.Data) > 0 && msg.Data[0] == 0 {
		return nil, fmt.Errorf("sym get_by_uid %d: %s", uid, replyError(msg.Data))
	}
	var rep symv1.Reply
	if err := proto.Unmarshal(msg.Data, &rep); err != nil {
		return nil, fmt.Errorf("sym get_by_uid %d: %w", uid, err)
	}
	info := rep.GetInfo()
	if info == nil {
		return nil, fmt.Errorf("sym get_by_uid %d: no info in the reply", uid)
	}
	return info, nil
}

// replyError reads the natsx error reply: 0x00, version 1, uint16 code, uint16 length, message.
func replyError(b []byte) error {
	if len(b) < 6 || b[1] != 1 || int(binary.BigEndian.Uint16(b[4:6]))+6 != len(b) {
		return errors.New("unreadable error reply")
	}
	return fmt.Errorf("code %d: %s", binary.BigEndian.Uint16(b[2:4]), b[6:])
}

// venueOf is the venue of a universal ticker: the part before the first underscore
// (BinanceUM_Perp_BTCUSDT is BinanceUM).
func venueOf(universal string) string {
	v, _, _ := strings.Cut(universal, "_")
	return v
}
