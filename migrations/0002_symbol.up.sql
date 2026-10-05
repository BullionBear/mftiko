-- Symbol plane tables (DECISIONS D11, D31), following mftik. Written only by SYM; everything else
-- reads through the sym subject.
--
-- Conventions as in 0001: times are *_ns BIGINT, decimals NUMERIC, venue identifiers TEXT.

-- One instrument, anywhere. Identity is the UniversalTicker, which already carries venue, category
-- and canonical symbol; they are not repeated as columns, so they cannot disagree with it. Listing
-- by venue or by venue and category is a prefix match on it, hence text_pattern_ops.
CREATE TABLE symbol_ticker (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    universal_ticker TEXT    NOT NULL UNIQUE,  -- <Venue>_<Category>_<SYMBOL>
    exch_ticker      TEXT    NOT NULL,         -- the venue's own spelling, what goes on the wire
    base             TEXT    NOT NULL,
    quote            TEXT    NOT NULL,
    contract_size    NUMERIC,                  -- contracts only
    settle           TEXT,                     -- settlement asset, contracts only
    expiry_ns        BIGINT,                   -- dated contracts and options only
    strike           NUMERIC,                  -- options only, as the venue published it
    option_type      TEXT CHECK (option_type IN ('C', 'P')),
    -- False once the venue stops listing it. Rows are never deleted.
    is_active        BOOLEAN NOT NULL DEFAULT TRUE,
    created_at_ns    BIGINT  NOT NULL,
    updated_at_ns    BIGINT  NOT NULL
);

CREATE INDEX symbol_ticker_prefix ON symbol_ticker (universal_ticker text_pattern_ops);
CREATE INDEX symbol_ticker_active ON symbol_ticker (is_active);

-- The reverse lookup (venue's spelling -> ticker) reads one (venue, category) table through the
-- prefix index. exch_ticker is unique within a (venue, category), not within a venue: Bybit's spot
-- and linear books both list "BTCUSDT".

-- One trading restriction per row: venues publish different sets, and a fixed column list would
-- either lose some or be mostly NULL. A row with value NULL means the venue publishes the
-- restriction but sets no bound, which is not the same as no row.
CREATE TABLE symbol_filter (
    ticker_id BIGINT NOT NULL REFERENCES symbol_ticker (id) ON DELETE CASCADE,
    name      TEXT   NOT NULL,  -- price_tick, qty_step, min_qty, max_qty, min_notional, ...
    value     NUMERIC,
    PRIMARY KEY (ticker_id, name)
);
