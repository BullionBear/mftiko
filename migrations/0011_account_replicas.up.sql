-- Account replicas (E21-D048, E21-D049). One row per (td_instance, api_id).
-- The TD agent does not read or write this table.
-- Existing accounts move their td_instance and sealed_credentials here so a
-- running session's etcd bytes stay the same. This migration does not touch etcd.

CREATE TABLE account_replicas (
    td_instance        TEXT        NOT NULL,
    api_id             BIGINT      NOT NULL REFERENCES accounts (api_id),
    sealed_credentials BYTEA       NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL,
    updated_at         TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (td_instance, api_id),
    CONSTRAINT account_replicas_instance_chk CHECK (td_instance ~ '^[a-z][a-z0-9-]{0,63}$'),
    CONSTRAINT account_replicas_sealed_len CHECK (octet_length(sealed_credentials) >= 57)
);

INSERT INTO account_replicas (td_instance, api_id, sealed_credentials, created_at, updated_at)
SELECT td_instance, api_id, sealed_credentials, created_at, updated_at
FROM accounts;

ALTER TABLE accounts
    DROP COLUMN td_instance,
    DROP COLUMN sealed_credentials;
