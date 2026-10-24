package symbol

// margin is how an instrument is grouped for display (E02-D003). The trading path
// does not read it; it reads the three currency fields. This is the only
// definition of the grouping, so a change stays here.
//
// quanto: settlement is neither base nor quote.
// coin (幣本位): settlement equals base. Otherwise, when settlement is set
// and the row is not quanto, it is U-margined (U 本位).
// linear: the linear row and the U-margined option row, which are the same
// three currencies (contract value base, price quote, settlement quote).
// inverse: the inverse row (contract value quote, price quote, settlement base).
// A coin-margined option, whose three currencies are all base, is coin and
// neither linear nor inverse.
type margin struct {
	linear  bool
	inverse bool
	quanto  bool
	coin    bool
	u       bool
}

func classify(base, quote, valueCcy, priceCcy, settle string) margin {
	var m margin
	if settle != "" && settle != base && settle != quote {
		m.quanto = true
		return m
	}
	if settle != "" && settle == base {
		m.coin = true
	} else if settle != "" {
		m.u = true
	}
	if base != "" && quote != "" && settle == quote && valueCcy == base && priceCcy == quote {
		m.linear = true
	}
	if base != "" && quote != "" && base != quote && settle == base && valueCcy == quote && priceCcy == quote {
		m.inverse = true
	}
	return m
}
