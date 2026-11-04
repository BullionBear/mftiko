-- Reverse of 0011 (E21-D049, E21-D053).
-- The earliest replica of an api_id is the smallest created_at, then the
-- smallest td_instance. An account with no replica is deleted: the restored
-- columns are NOT NULL, and that row exists only after this migration.

ALTER TABLE accounts
    ADD COLUMN td_instance TEXT,
    ADD COLUMN sealed_credentials BYTEA;

UPDATE accounts AS a
SET td_instance = r.td_instance,
    sealed_credentials = r.sealed_credentials
FROM (
    SELECT DISTINCT ON (api_id) api_id, td_instance, sealed_credentials
    FROM account_replicas
    ORDER BY api_id, created_at ASC, td_instance ASC
) AS r
WHERE a.api_id = r.api_id;

DELETE FROM accounts WHERE td_instance IS NULL;

ALTER TABLE accounts
    ALTER COLUMN td_instance SET NOT NULL,
    ALTER COLUMN sealed_credentials SET NOT NULL;

ALTER TABLE accounts
    ADD CONSTRAINT accounts_td_instance_chk CHECK (td_instance ~ '^[a-z][a-z0-9-]{0,63}$'),
    ADD CONSTRAINT accounts_sealed_len CHECK (octet_length(sealed_credentials) >= 57);

DROP TABLE account_replicas;
