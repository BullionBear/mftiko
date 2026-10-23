-- Session history (DECISIONS D1031, D1068, D1073). etcd stays the control plane. This table is the
-- durable record: who created a session, the spec bytes, and how it ended. W5 deletion leaves the
-- row (state closed); that write is #317. state_out_digest is created here and left null until then.
--
-- Idempotency-Key is optional. The partial unique index keeps one live key. Reconcile clears keys
-- older than 24 hours, which is how retention stays inside an index that cannot call now() (D1068).
-- A session_id is unique only while the row is pending or created, so a later session may draw it
-- again (D1007).

CREATE TABLE session_records (
    id               UUID        PRIMARY KEY,
    session_id       TEXT        NOT NULL,
    sts_instance     TEXT        NOT NULL,
    create_revision  BIGINT,
    spec_bytes       BYTEA       NOT NULL,
    spec_sha256      BYTEA       NOT NULL,
    state            TEXT        NOT NULL,
    desired_state    TEXT        NOT NULL,
    end_reason       TEXT,
    created_by       TEXT,
    idempotency_key  TEXT,
    created_at       TIMESTAMPTZ NOT NULL,
    ended_at         TIMESTAMPTZ,
    closed_at        TIMESTAMPTZ,
    state_out_digest TEXT,
    CONSTRAINT session_records_state_chk CHECK (state IN ('pending', 'created', 'abandoned', 'closed')),
    CONSTRAINT session_records_desired_chk CHECK (desired_state IN ('running', 'ended')),
    CONSTRAINT session_records_sha_len CHECK (octet_length(spec_sha256) = 32)
);

CREATE UNIQUE INDEX session_records_open_session_id
    ON session_records (session_id)
    WHERE state IN ('pending', 'created');

CREATE UNIQUE INDEX session_records_idempotency_key
    ON session_records (idempotency_key)
    WHERE idempotency_key IS NOT NULL;
