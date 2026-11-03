-- Drop the five history tables from 0001_history (E21-D015). Trade history is not stored here (E21-D001).
DROP TABLE IF EXISTS algo_ownership;
DROP TABLE IF EXISTS backfill_cursors;
DROP TABLE IF EXISTS backfill_scopes;
DROP TABLE IF EXISTS fills;
DROP TABLE IF EXISTS orders;
