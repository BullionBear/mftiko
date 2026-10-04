-- History owned by the TD agent (DECISIONS D20, D25). First draft: columns follow what the
-- decisions fix; adjust while implementing.
--
-- Conventions (PROTOCOL §2): times are int64 unix nanoseconds in *_ns columns; decimals are NUMERIC
-- (the wire uses decimal strings); identifiers from venues are TEXT.

-- Terminal orders that have at least one fill. Cancelled, rejected or expired orders without a fill
-- are not stored (D20).
CREATE TABLE orders (
    api_id          BIGINT  NOT NULL,
    order_id        TEXT    NOT NULL,
    client_order_id TEXT,              -- NULL for manual orders
    algo_id         INTEGER,           -- decoded from client_order_id (D25); NULL for manual orders
    ticker          TEXT    NOT NULL,  -- UniversalTicker
    side            TEXT    NOT NULL CHECK (side IN ('buy', 'sell')),
    order_type      TEXT    NOT NULL,
    status          TEXT    NOT NULL,  -- terminal status as reported by the venue
    qty             NUMERIC,
    price           NUMERIC,
    filled_qty      NUMERIC NOT NULL CHECK (filled_qty > 0),
    avg_price       NUMERIC,
    exch_ts_ns      BIGINT  NOT NULL,
    source          TEXT    NOT NULL CHECK (source IN ('provisional', 'confirmed')),
    updated_at_ns   BIGINT  NOT NULL,
    PRIMARY KEY (api_id, order_id)
);

CREATE INDEX orders_algo_ts ON orders (api_id, algo_id, exch_ts_ns);

-- Fills, deduplicated by the venue trade id. A provisional row (from td.evt) never overwrites a
-- confirmed one (from REST); a confirmed row overwrites fee, fee_asset and liquidity (D20).
CREATE TABLE fills (
    api_id          BIGINT  NOT NULL,
    fill_id         TEXT    NOT NULL,  -- venue trade id, identical on websocket and REST
    order_id        TEXT    NOT NULL,
    client_order_id TEXT,              -- REST fills often lack it; attribution goes through orders
    algo_id         INTEGER,
    ticker          TEXT    NOT NULL,
    side            TEXT    NOT NULL CHECK (side IN ('buy', 'sell')),
    price           NUMERIC NOT NULL,
    qty             NUMERIC NOT NULL,
    fee             NUMERIC,
    fee_asset       TEXT,
    liquidity       TEXT,              -- maker | taker, as the venue reports it
    exch_ts_ns      BIGINT  NOT NULL,
    status          TEXT    NOT NULL CHECK (status IN ('provisional', 'confirmed')),
    written_at_ns   BIGINT  NOT NULL,
    PRIMARY KEY (api_id, fill_id)
);

-- Orders still to look up: fills whose order has no terminal row yet (fills LEFT JOIN orders).
CREATE INDEX fills_order ON fills (api_id, order_id);
CREATE INDEX fills_algo_ts ON fills (api_id, algo_id, exch_ts_ns);

-- What backfill reads. A scope is created on the first submit of a ticker; since only moves earlier
-- (LEAST), so a restart never skips history (D20).
CREATE TABLE backfill_scopes (
    api_id   BIGINT NOT NULL,
    ticker   TEXT   NOT NULL,
    since_ns BIGINT NOT NULL,
    PRIMARY KEY (api_id, ticker)
);

-- Where backfill has got to. While catching up it resumes from the venue id (last_id); once caught up
-- it rereads from confirmed_through_ns, which never passes now - SAFETY_LAG (D20). Advanced in the
-- same transaction as the rows it covers.
CREATE TABLE backfill_cursors (
    api_id               BIGINT NOT NULL,
    ticker               TEXT   NOT NULL,
    stream               TEXT   NOT NULL,  -- 'trades' today; cash flows later get their own stream
    last_id              TEXT,
    confirmed_through_ns BIGINT,
    updated_at_ns        BIGINT NOT NULL,
    PRIMARY KEY (api_id, ticker, stream)
);

-- Which session held an algo_id on an account, and when. Resolves (api_id, algo_id, ts_ms) from a
-- client_order_id to a session (D25). At most one open holding per (api_id, algo_id).
CREATE TABLE algo_ownership (
    api_id         BIGINT  NOT NULL,
    algo_id        INTEGER NOT NULL CHECK (algo_id BETWEEN 1 AND 1023),
    session_id     TEXT    NOT NULL,
    acquired_at_ns BIGINT  NOT NULL,
    released_at_ns BIGINT,
    PRIMARY KEY (api_id, algo_id, acquired_at_ns),
    CHECK (released_at_ns IS NULL OR released_at_ns >= acquired_at_ns)
);

CREATE UNIQUE INDEX algo_ownership_open ON algo_ownership (api_id, algo_id) WHERE released_at_ns IS NULL;
