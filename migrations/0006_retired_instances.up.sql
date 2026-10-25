-- Permanent record that an instance was retired (E12-D033). Rows are not deleted.
-- plane is the deployment plane. retired_by is the authenticated API user.
CREATE TABLE retired_instances (
    instance   TEXT PRIMARY KEY,
    plane      TEXT NOT NULL CHECK (plane IN ('sts', 'md', 'td')),
    region     TEXT NOT NULL,
    retired_by TEXT NOT NULL,
    retired_at TIMESTAMPTZ NOT NULL
);
