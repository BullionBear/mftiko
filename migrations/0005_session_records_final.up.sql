-- The session's last status, kept when its row is closed (DECISIONS E10-D031, E10-D117). It is the
-- statev1.SessionStatus proto bytes from etcd at the moment W5 or DELETE closes the row, the same
-- encoding spec_bytes uses. GET /session-records renders it as final. Rows closed before this column
-- existed stay null and show no final.
ALTER TABLE session_records ADD COLUMN final_status BYTEA;
