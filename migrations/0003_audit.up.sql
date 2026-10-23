-- API request audit (DECISIONS D1010, D1049, D1052). One row per /api request, written by
-- mftiko-api when the handler returns. /healthz and /readyz are not rows. A failed insert does
-- not change the HTTP response. Rows are not deleted in this phase (D1010).
--
-- body is the redacted JSON. NULL when the request is not a write, the body is empty, or the
-- body is not a single JSON value (body_len is then the raw byte length). Account credential
-- fields and token fields are removed before this is stored (D1010, D1052). When the redacted JSON
-- exceeds 64 KiB, body_truncated is true and body is
-- {"truncated": true, "len": <redacted bytes>, "prefix": "<head of the redacted JSON>"}.

CREATE TABLE audit (
    id             BIGSERIAL PRIMARY KEY,
    at_ns          BIGINT  NOT NULL,
    user_name      TEXT    NOT NULL DEFAULT '',
    method         TEXT    NOT NULL,
    path           TEXT    NOT NULL,
    query          TEXT    NOT NULL DEFAULT '',
    status         INTEGER NOT NULL,
    duration_ns    BIGINT  NOT NULL,
    request_id     TEXT    NOT NULL,
    remote_addr    TEXT    NOT NULL,
    session_id     TEXT,
    api_id         BIGINT,
    body           JSONB,
    body_len       BIGINT,
    body_truncated BOOLEAN NOT NULL DEFAULT FALSE,
    detail         TEXT    NOT NULL DEFAULT ''
);
