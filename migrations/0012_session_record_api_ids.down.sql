DROP INDEX IF EXISTS session_records_api_ids;
ALTER TABLE session_records DROP COLUMN IF EXISTS api_ids;
DROP FUNCTION IF EXISTS session_spec_api_ids(bytea);
