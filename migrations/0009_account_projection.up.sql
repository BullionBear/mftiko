-- Account CAS token and the etcd projection blob (E21-D027, E21-D028).
-- CRUD is E21-03b (#526). Do not edit 0008: its checksum is already recorded.
--
-- revision is the API's compare-and-swap token. It starts at 1 and increases by 1
-- on a committed update or soft delete. It is not an etcd mod_revision.
-- sealed_credentials is the sealbox blob written to etcd. Reconcile rewrites these
-- exact bytes, so a random nonce does not restart the account worker.
-- The shortest sealbox is 9 bytes of header plus 48 bytes of NaCl anonymous overhead.
-- DEFAULT fills rows created by 0008; new inserts set both columns themselves.

ALTER TABLE accounts
    ADD COLUMN revision BIGINT NOT NULL DEFAULT 1,
    ADD COLUMN sealed_credentials BYTEA NOT NULL DEFAULT decode(repeat('ab', 57), 'hex');

ALTER TABLE accounts
    ADD CONSTRAINT accounts_revision_chk CHECK (revision >= 1),
    ADD CONSTRAINT accounts_sealed_len CHECK (octet_length(sealed_credentials) >= 57);

ALTER TABLE accounts
    ALTER COLUMN revision DROP DEFAULT,
    ALTER COLUMN sealed_credentials DROP DEFAULT;
