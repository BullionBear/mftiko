package exchange

import (
	"testing"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
)

// TestEnumsMatchTheContract holds the adapter's enums to the numbers of their common.v1 and td.v1
// namesakes, so the workers convert by a cast (E03-D002). A value added on one side only fails here.
func TestEnumsMatchTheContract(t *testing.T) {
	check := func(kind string, ours []uint8, theirs map[string]int32) {
		t.Helper()
		if len(ours) != len(theirs) {
			t.Errorf("%s: %d values here, %d in the contract", kind, len(ours), len(theirs))
		}
		for i, v := range ours {
			if int(v) != i {
				t.Errorf("%s: value %d at position %d", kind, v, i)
			}
		}
	}
	check("Side", []uint8{uint8(SideUnspecified), uint8(SideBuy), uint8(SideSell)}, commonv1.Side_value)
	check("OrderType", []uint8{uint8(OrderTypeUnspecified), uint8(OrderTypeLimit), uint8(OrderTypeMarket)},
		commonv1.OrderType_value)
	check("TimeInForce", []uint8{uint8(TIFUnspecified), uint8(TIFGTC), uint8(TIFIOC), uint8(TIFFOK), uint8(TIFPostOnly)},
		commonv1.TimeInForce_value)
	check("OrderStatus", []uint8{
		uint8(OrderStatusUnspecified), uint8(OrderStatusPendingNew), uint8(OrderStatusNew), uint8(OrderStatusAccepted),
		uint8(OrderStatusPartiallyFilled), uint8(OrderStatusFilled),
		uint8(OrderStatusCancelled), uint8(OrderStatusRejected), uint8(OrderStatusExpired), uint8(OrderStatusUnknown),
	}, tdv1.OrderStatus_value)

	// And name by name, so two values swapped on one side fail too.
	for _, c := range []struct {
		ours   uint8
		theirs int32
	}{
		{uint8(SideBuy), int32(commonv1.Side_SIDE_BUY)},
		{uint8(SideSell), int32(commonv1.Side_SIDE_SELL)},
		{uint8(OrderTypeLimit), int32(commonv1.OrderType_ORDER_TYPE_LIMIT)},
		{uint8(OrderTypeMarket), int32(commonv1.OrderType_ORDER_TYPE_MARKET)},
		{uint8(TIFGTC), int32(commonv1.TimeInForce_TIME_IN_FORCE_GTC)},
		{uint8(TIFIOC), int32(commonv1.TimeInForce_TIME_IN_FORCE_IOC)},
		{uint8(TIFFOK), int32(commonv1.TimeInForce_TIME_IN_FORCE_FOK)},
		{uint8(TIFPostOnly), int32(commonv1.TimeInForce_TIME_IN_FORCE_POST_ONLY)},
		{uint8(OrderStatusPendingNew), int32(tdv1.OrderStatus_ORDER_STATUS_PENDING_NEW)},
		{uint8(OrderStatusNew), int32(tdv1.OrderStatus_ORDER_STATUS_NEW)},
		{uint8(OrderStatusAccepted), int32(tdv1.OrderStatus_ORDER_STATUS_ACCEPTED)},
		{uint8(OrderStatusPartiallyFilled), int32(tdv1.OrderStatus_ORDER_STATUS_PARTIALLY_FILLED)},
		{uint8(OrderStatusFilled), int32(tdv1.OrderStatus_ORDER_STATUS_FILLED)},
		{uint8(OrderStatusCancelled), int32(tdv1.OrderStatus_ORDER_STATUS_CANCELLED)},
		{uint8(OrderStatusRejected), int32(tdv1.OrderStatus_ORDER_STATUS_REJECTED)},
		{uint8(OrderStatusExpired), int32(tdv1.OrderStatus_ORDER_STATUS_EXPIRED)},
		{uint8(OrderStatusUnknown), int32(tdv1.OrderStatus_ORDER_STATUS_UNKNOWN)},
	} {
		if int32(c.ours) != c.theirs {
			t.Errorf("value %d here is %d in the contract", c.ours, c.theirs)
		}
	}
}

func TestOnBook(t *testing.T) {
	if OnBook(0) != OrderStatusAccepted || OnBook(0.001) != OrderStatusPartiallyFilled {
		t.Fatal("an order on the book is Accepted until something fills")
	}
}
