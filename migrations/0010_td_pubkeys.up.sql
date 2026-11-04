-- TD instance public-key fingerprints (E21-D036). Only the API reads and writes this table.
-- The TD agent publishes the key to etcd and does not touch Postgres.

CREATE TABLE td_pubkeys (
    td_instance   TEXT        PRIMARY KEY,
    fingerprint   BYTEA       NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,
    CONSTRAINT td_pubkeys_instance_chk CHECK (td_instance ~ '^[a-z][a-z0-9-]{0,63}$'),
    CONSTRAINT td_pubkeys_fingerprint_len CHECK (octet_length(fingerprint) = 32)
);
