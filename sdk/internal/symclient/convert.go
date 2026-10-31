package symclient

import (
	"errors"
	"fmt"

	symv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
)

// InfoFromProto reads a SYM row. The event log records Resolve's result as one (E20-D003).
func InfoFromProto(msg *symv1.SymbolInfo) (symbol.Info, error) {
	if msg == nil {
		return symbol.Info{}, errors.New("symclient: empty info")
	}
	t, err := symbol.Parse(msg.GetUniversalTicker())
	if err != nil {
		return symbol.Info{}, fmt.Errorf("symclient: ticker %q: %w", msg.GetUniversalTicker(), err)
	}
	if !symbol.ValidUID(msg.GetUid()) {
		return symbol.Info{}, fmt.Errorf("symclient: uid %d is not a valid uid", msg.GetUid())
	}
	st, err := statusFromProto(msg.GetStatus())
	if err != nil {
		return symbol.Info{}, err
	}
	info := symbol.Info{
		UID:              msg.GetUid(),
		Ticker:           t,
		ExchTicker:       msg.GetExchTicker(),
		Base:             msg.GetBase(),
		Quote:            msg.GetQuote(),
		ContractSize:     msg.GetContractSize(),
		ContractValueCcy: msg.GetContractValueCcy(),
		PriceCcy:         msg.GetPriceCcy(),
		Settle:           msg.GetSettle(),
		ExpiryNs:         msg.GetExpiryNs(),
		Strike:           msg.GetStrike(),
		OptionType:       msg.GetOptionType(),
		Status:           st,
		UpdatedAtNs:      msg.GetUpdatedAtNs(),
	}
	if len(msg.GetFilters()) > 0 {
		info.Filters = make(symbol.Filters, len(msg.GetFilters()))
		for name, b := range msg.GetFilters() {
			if name == "" {
				return symbol.Info{}, errors.New("symclient: filter name is empty")
			}
			// An unset Bound.value is a published restriction with no bound (E00-D008).
			v := decimal.Absent()
			if b != nil && b.Value != nil {
				v = b.GetValue()
			}
			info.Filters[name] = v
		}
	}
	return info, nil
}

// InfoToProto is the SYM row of info. InfoFromProto of it is info again.
func InfoToProto(info symbol.Info) *symv1.SymbolInfo {
	out := &symv1.SymbolInfo{
		Uid:              info.UID,
		UniversalTicker:  info.Ticker.String(),
		ExchTicker:       info.ExchTicker,
		Base:             info.Base,
		Quote:            info.Quote,
		ContractValueCcy: info.ContractValueCcy,
		PriceCcy:         info.PriceCcy,
		ExpiryNs:         info.ExpiryNs,
		OptionType:       info.OptionType,
		Status:           statusToProto(info.Status),
		UpdatedAtNs:      info.UpdatedAtNs,
	}
	if info.ContractSize != 0 {
		v := info.ContractSize
		out.ContractSize = &v
	}
	if info.Settle != "" {
		v := info.Settle
		out.Settle = &v
	}
	if info.Strike != 0 {
		v := info.Strike
		out.Strike = &v
	}
	if len(info.Filters) > 0 {
		out.Filters = make(map[string]*symv1.Bound, len(info.Filters))
		for name, v := range info.Filters {
			b := &symv1.Bound{}
			if decimal.Has(v) {
				x := v
				b.Value = &x
			}
			out.Filters[name] = b
		}
	}
	return out
}

func statusToProto(s symbol.Status) symv1.SymbolStatus {
	switch s {
	case symbol.Prelisting:
		return symv1.SymbolStatus_SYMBOL_STATUS_PRELISTING
	case symbol.Tradable:
		return symv1.SymbolStatus_SYMBOL_STATUS_TRADABLE
	case symbol.Halted:
		return symv1.SymbolStatus_SYMBOL_STATUS_HALTED
	case symbol.Settling:
		return symv1.SymbolStatus_SYMBOL_STATUS_SETTLING
	case symbol.Expired:
		return symv1.SymbolStatus_SYMBOL_STATUS_EXPIRED
	case symbol.Delisted:
		return symv1.SymbolStatus_SYMBOL_STATUS_DELISTED
	case symbol.Unknown:
		return symv1.SymbolStatus_SYMBOL_STATUS_UNKNOWN
	default:
		return symv1.SymbolStatus_SYMBOL_STATUS_UNSPECIFIED
	}
}

func statusFromProto(s symv1.SymbolStatus) (symbol.Status, error) {
	switch s {
	case symv1.SymbolStatus_SYMBOL_STATUS_PRELISTING:
		return symbol.Prelisting, nil
	case symv1.SymbolStatus_SYMBOL_STATUS_TRADABLE:
		return symbol.Tradable, nil
	case symv1.SymbolStatus_SYMBOL_STATUS_HALTED:
		return symbol.Halted, nil
	case symv1.SymbolStatus_SYMBOL_STATUS_SETTLING:
		return symbol.Settling, nil
	case symv1.SymbolStatus_SYMBOL_STATUS_EXPIRED:
		return symbol.Expired, nil
	case symv1.SymbolStatus_SYMBOL_STATUS_DELISTED:
		return symbol.Delisted, nil
	case symv1.SymbolStatus_SYMBOL_STATUS_UNKNOWN:
		return symbol.Unknown, nil
	default:
		return "", fmt.Errorf("symclient: status %q cannot be decoded", s.String())
	}
}
