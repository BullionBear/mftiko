-- Symbol plane tables (DECISIONS D201, D202, D203). Written only by SYM; everything else reads
-- through the sym subject.
--
-- Listings are split by whether the status can still change. The columns of the two tables match.
--   symbol_active:   PRELISTING, TRADABLE, HALTED, SETTLING, UNKNOWN. The Resolver and hot paths
--                    read only this table. Only TRADABLE may trade.
--   symbol_terminal: EXPIRED, DELISTED. The status never changes again. Kept for historical lookup
--                    (old fills and positions, reverse lookup by UID). Not refreshed. Can be archived.
--
-- uid is the primary key of both tables. SYM allocates it: a random integer, not a sequence, stored
-- on the row so a restart or a move keeps it. 0 is illegal. The inclusive range is 1..4294967295
-- (2^32-1). Columns stay BIGINT, with CHECK (uid BETWEEN 1 AND 4294967295). The same ticker
-- delisted and listed again is a new row with a new uid; the old row stays on symbol_terminal, so
-- universal_ticker and (venue, category, exch_ticker) are unique only on symbol_active.
--
-- symbol_uid is the registry: primary key uid, and rows are never deleted. Both listing tables and
-- both filter tables reference it. Allocation inserts into symbol_uid first and retries when that
-- insert reports a unique violation. Deleting or archiving a listing does not delete the registry
-- row, so that uid cannot be drawn again.
--
-- The registry does not stop the same uid sitting on both listing tables at once: both rows would
-- reference the one registry row. symbol_reject_shared_uid raises unique_violation in that case,
-- and SYM draws again. Move deletes the active row and then inserts the terminal row in one
-- transaction, so the uid is never on both tables and the trigger allows the move.
--
-- venue and category are generated from universal_ticker (<Venue>_<Category>_<SYMBOL>, neither of
-- the first two fields contains '_'). They cannot drift from the ticker. The reverse lookup is
-- unique on the active table only: Bybit spot and linear both list "BTCUSDT".
--
-- Strike is NUMERIC with trailing zeros stripped (trim_scale), the numeric half of spell_strike
-- (D203): 6400.0 and 6400 are one value. The ticker's spelling of a fractional strike (6.4 as 6D4)
-- is applied when the ticker is built, not in this column.
--
-- Settlement currency is settle, the contract field name (D203), not settlement_asset.
-- contract_value_ccy is the unit of qty. price_ccy is the unit of price, price_tick and premium.
--
-- symbol_revision is the per-(venue, category) counter change notifications compare (PROTOCOL §7.3).
-- After that revision commits, SYM publishes Changed on sym.changed.{venue}.{category}.
--
-- The status enum is still provisional as a set. It is TEXT plus a CHECK, not a Postgres enum,
-- so adding a value is a change to the check. The contract uses this enum. UNSPECIFIED is not a
-- stored value. Only TRADABLE may trade.
--
-- Conventions as in 0001: times are *_ns BIGINT, decimals NUMERIC, venue identifiers TEXT.

CREATE FUNCTION symbol_normalize_strike() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.strike IS NOT NULL THEN
        NEW.strike := trim_scale(NEW.strike);
    END IF;
    RETURN NEW;
END;
$$;

-- The registry row stays when a listing is deleted, so this is what stops the same uid
-- being on symbol_active and symbol_terminal at the same time.
CREATE FUNCTION symbol_reject_shared_uid() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_TABLE_NAME = 'symbol_active' THEN
        IF EXISTS (SELECT 1 FROM symbol_terminal WHERE uid = NEW.uid) THEN
            RAISE EXCEPTION 'symbol uid % is already on symbol_terminal', NEW.uid
                USING ERRCODE = 'unique_violation', CONSTRAINT = 'symbol_uid_across_tables';
        END IF;
    ELSIF TG_TABLE_NAME = 'symbol_terminal' THEN
        IF EXISTS (SELECT 1 FROM symbol_active WHERE uid = NEW.uid) THEN
            RAISE EXCEPTION 'symbol uid % is already on symbol_active', NEW.uid
                USING ERRCODE = 'unique_violation', CONSTRAINT = 'symbol_uid_across_tables';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

-- Every uid ever allocated. SYM does not delete from this table.
CREATE TABLE symbol_uid (
    uid BIGINT NOT NULL,
    CONSTRAINT symbol_uid_pkey PRIMARY KEY (uid),
    CONSTRAINT symbol_uid_range CHECK (uid BETWEEN 1 AND 4294967295)
);

CREATE TABLE symbol_active (
    uid                BIGINT  NOT NULL REFERENCES symbol_uid (uid),
    universal_ticker   TEXT    NOT NULL,
    exch_ticker        TEXT    NOT NULL,
    base               TEXT    NOT NULL,
    quote              TEXT    NOT NULL,
    contract_size      NUMERIC,
    contract_value_ccy TEXT    NOT NULL,
    price_ccy          TEXT    NOT NULL,
    settle             TEXT,                  -- PnL settlement asset, not the margin asset; NULL for spot
    expiry_ns          BIGINT,
    strike             NUMERIC,               -- trim_scale; options only
    option_type        TEXT,
    status             TEXT    NOT NULL,
    created_at_ns      BIGINT  NOT NULL,
    updated_at_ns      BIGINT  NOT NULL,
    venue              TEXT    GENERATED ALWAYS AS (split_part(universal_ticker, '_', 1)) STORED,
    category           TEXT    GENERATED ALWAYS AS (split_part(universal_ticker, '_', 2)) STORED,
    CONSTRAINT symbol_active_pkey PRIMARY KEY (uid),
    CONSTRAINT symbol_active_ticker UNIQUE (universal_ticker),
    CONSTRAINT symbol_active_exch UNIQUE (venue, category, exch_ticker),
    CONSTRAINT symbol_active_uid CHECK (uid BETWEEN 1 AND 4294967295),
    CONSTRAINT symbol_active_ccy CHECK (contract_value_ccy <> '' AND price_ccy <> ''),
    CONSTRAINT symbol_active_option CHECK (option_type IS NULL OR option_type IN ('C', 'P')),
    CONSTRAINT symbol_active_ticker_shape CHECK (universal_ticker ~ '^[^_]+_[^_]+_[^_]+$'),
    CONSTRAINT symbol_active_status CHECK (status IN ('PRELISTING', 'TRADABLE', 'HALTED', 'SETTLING', 'UNKNOWN'))
);

CREATE TRIGGER symbol_active_strike
    BEFORE INSERT OR UPDATE OF strike ON symbol_active
    FOR EACH ROW EXECUTE FUNCTION symbol_normalize_strike();

CREATE TRIGGER symbol_active_uid_across
    BEFORE INSERT OR UPDATE OF uid ON symbol_active
    FOR EACH ROW EXECUTE FUNCTION symbol_reject_shared_uid();

-- Prefix listing (venue, or venue and category) is a left-anchored match. The unique index above
-- uses the default text operator class, which does not serve that match outside the C locale.
CREATE INDEX symbol_active_prefix ON symbol_active (universal_ticker text_pattern_ops);

CREATE TABLE symbol_terminal (
    uid                BIGINT  NOT NULL REFERENCES symbol_uid (uid),
    universal_ticker   TEXT    NOT NULL,
    exch_ticker        TEXT    NOT NULL,
    base               TEXT    NOT NULL,
    quote              TEXT    NOT NULL,
    contract_size      NUMERIC,
    contract_value_ccy TEXT    NOT NULL,
    price_ccy          TEXT    NOT NULL,
    settle             TEXT,
    expiry_ns          BIGINT,
    strike             NUMERIC,
    option_type        TEXT,
    status             TEXT    NOT NULL,
    created_at_ns      BIGINT  NOT NULL,
    updated_at_ns      BIGINT  NOT NULL,
    venue              TEXT    GENERATED ALWAYS AS (split_part(universal_ticker, '_', 1)) STORED,
    category           TEXT    GENERATED ALWAYS AS (split_part(universal_ticker, '_', 2)) STORED,
    CONSTRAINT symbol_terminal_pkey PRIMARY KEY (uid),
    CONSTRAINT symbol_terminal_uid CHECK (uid BETWEEN 1 AND 4294967295),
    CONSTRAINT symbol_terminal_ccy CHECK (contract_value_ccy <> '' AND price_ccy <> ''),
    CONSTRAINT symbol_terminal_option CHECK (option_type IS NULL OR option_type IN ('C', 'P')),
    CONSTRAINT symbol_terminal_ticker_shape CHECK (universal_ticker ~ '^[^_]+_[^_]+_[^_]+$'),
    CONSTRAINT symbol_terminal_status CHECK (status IN ('EXPIRED', 'DELISTED'))
);

CREATE TRIGGER symbol_terminal_strike
    BEFORE INSERT OR UPDATE OF strike ON symbol_terminal
    FOR EACH ROW EXECUTE FUNCTION symbol_normalize_strike();

CREATE TRIGGER symbol_terminal_uid_across
    BEFORE INSERT OR UPDATE OF uid ON symbol_terminal
    FOR EACH ROW EXECUTE FUNCTION symbol_reject_shared_uid();

-- Historical reverse lookup. Not unique: the old row stays when the ticker is listed again.
CREATE INDEX symbol_terminal_exch ON symbol_terminal (venue, category, exch_ticker);
CREATE INDEX symbol_terminal_ticker ON symbol_terminal (universal_ticker);

-- One trading restriction per row: venues publish different sets, and a fixed column list would
-- either lose some or be mostly NULL. A row with value NULL means the venue publishes the
-- restriction but sets no bound, which is not the same as no row.
-- A terminal move copies these rows with the listing. The uid does not change.
CREATE TABLE symbol_filter_active (
    uid   BIGINT NOT NULL,
    name  TEXT   NOT NULL,  -- price_tick, qty_step, min_qty, max_qty, min_notional, ...
    value NUMERIC,
    PRIMARY KEY (uid, name),
    CONSTRAINT symbol_filter_active_listing FOREIGN KEY (uid) REFERENCES symbol_active (uid) ON DELETE CASCADE,
    CONSTRAINT symbol_filter_active_uid FOREIGN KEY (uid) REFERENCES symbol_uid (uid)
);

CREATE TABLE symbol_filter_terminal (
    uid   BIGINT NOT NULL,
    name  TEXT   NOT NULL,
    value NUMERIC,
    PRIMARY KEY (uid, name),
    CONSTRAINT symbol_filter_terminal_listing FOREIGN KEY (uid) REFERENCES symbol_terminal (uid) ON DELETE CASCADE,
    CONSTRAINT symbol_filter_terminal_uid FOREIGN KEY (uid) REFERENCES symbol_uid (uid)
);

-- One current revision per (venue, category). SYM increments it in the same transaction as the
-- listing change. Clients compare it to notice a missed change notification (PROTOCOL §7.3).
CREATE TABLE symbol_revision (
    venue           TEXT   NOT NULL,
    category        TEXT   NOT NULL,
    revision        BIGINT NOT NULL,
    refreshed_at_ns BIGINT NOT NULL,
    PRIMARY KEY (venue, category)
);
