-- Accounts (E21-D002, E21-D003, E21-D012, E21-D018–E21-D025). The row is the authority.
-- etcd holds a projection; this migration does not write one. CRUD is E21-03b (#526).
--
-- api_id is drawn by the API in 1..2^53-1 and is the primary key, so it is never reused,
-- including after a soft delete (E21-D002, E21-D023). There is no sequence.
-- name is unique only while the row is live (E21-D023). A soft-deleted name can be registered again.
-- credentials is AEAD ciphertext (XChaCha20-Poly1305), never plaintext. The blob starts with the
-- key version; key_version is that same integer, so a later re-encrypt (#527) can find old rows
-- without opening them (E21-D019). The shortest blob is 4 + 24 + 16 bytes.
-- exch_account_uid is 1 to 64 characters with no leading or trailing space (E21-D025).
-- td_instance stays for the transition and is removed in E21-06 (E21-D002).
-- settings is a JSON object. enabled is stored; the API still forces it true until a later change.

CREATE TABLE accounts (
    api_id           BIGINT      PRIMARY KEY,
    kind             TEXT        NOT NULL,
    name             TEXT        NOT NULL,
    credentials      BYTEA       NOT NULL,
    key_version      BIGINT      NOT NULL,
    settings         JSONB       NOT NULL,
    enabled          BOOLEAN     NOT NULL,
    exch_account_uid TEXT        NOT NULL,
    td_instance      TEXT        NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL,
    updated_at       TIMESTAMPTZ NOT NULL,
    deleted_at       TIMESTAMPTZ,
    CONSTRAINT accounts_api_id_chk CHECK (api_id BETWEEN 1 AND 9007199254740991),
    CONSTRAINT accounts_kind_chk CHECK (kind <> ''),
    CONSTRAINT accounts_name_chk CHECK (char_length(name) BETWEEN 1 AND 128),
    CONSTRAINT accounts_credentials_len CHECK (octet_length(credentials) >= 44),
    CONSTRAINT accounts_key_version_chk CHECK (key_version BETWEEN 1 AND 4294967295),
    CONSTRAINT accounts_settings_obj CHECK (jsonb_typeof(settings) = 'object'),
    CONSTRAINT accounts_uid_chk CHECK (
        char_length(exch_account_uid) BETWEEN 1 AND 64
        AND exch_account_uid = btrim(exch_account_uid)
    ),
    CONSTRAINT accounts_td_instance_chk CHECK (td_instance ~ '^[a-z][a-z0-9-]{0,63}$')
);

CREATE UNIQUE INDEX accounts_name_live
    ON accounts (name)
    WHERE deleted_at IS NULL;
