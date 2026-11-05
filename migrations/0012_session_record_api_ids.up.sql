-- Account filter for session history (E21-D069–E21-D071, #520).
-- spec_bytes is a session.v1.SessionSpec and cannot be indexed. api_ids is the set of
-- AccountDecl.api_id values from that spec: drop 0 (proto3 unset), drop duplicates, sort
-- ascending. SessionSpec.accounts is field 8 (length-delimited). AccountDecl.api_id is
-- field 2 (varint). A row that does not decode becomes '{}' so one bad spec does not
-- stop the migration; that row stays listable and matches no api_id.
--
-- The function stays. mftiko-release switch migrates before it restarts the API, and an
-- old API binary inserts without this column (the default is '{}'). Re-run
-- UPDATE session_records SET api_ids = session_spec_api_ids(spec_bytes);
-- after every replica is on the new binary if any session was created in that window.

ALTER TABLE session_records ADD COLUMN api_ids BIGINT[];

CREATE FUNCTION session_spec_api_ids(spec bytea)
RETURNS bigint[]
LANGUAGE plpgsql
IMMUTABLE
SET search_path = pg_catalog
AS $fn$
DECLARE
  ids bigint[] := ARRAY[]::bigint[];
  n int;
  pos int := 0;
  b int;
  val numeric;
  shift int;
  wt int;
  field numeric;
  mlen int;
  sub_end int;
  spos int;
  swt int;
  sfield numeric;
  api numeric;
  id bigint;
BEGIN
  IF spec IS NULL THEN
    RETURN ids;
  END IF;
  n := octet_length(spec);
  WHILE pos < n LOOP
    val := 0;
    shift := 0;
    LOOP
      IF pos >= n OR shift > 63 THEN
        RETURN ARRAY[]::bigint[];
      END IF;
      b := get_byte(spec, pos);
      pos := pos + 1;
      val := val + (b & 127)::numeric * (2::numeric ^ shift);
      shift := shift + 7;
      EXIT WHEN b < 128;
    END LOOP;
    wt := (val % 8)::int;
    field := (val - wt) / 8;
    IF field = 8 AND wt = 2 THEN
      val := 0;
      shift := 0;
      LOOP
        IF pos >= n OR shift > 28 THEN
          RETURN ARRAY[]::bigint[];
        END IF;
        b := get_byte(spec, pos);
        pos := pos + 1;
        val := val + (b & 127)::numeric * (2::numeric ^ shift);
        shift := shift + 7;
        EXIT WHEN b < 128;
      END LOOP;
      IF val > 2147483647 OR pos + val > n THEN
        RETURN ARRAY[]::bigint[];
      END IF;
      mlen := val::int;
      sub_end := pos + mlen;
      spos := pos;
      api := NULL;
      WHILE spos < sub_end LOOP
        val := 0;
        shift := 0;
        LOOP
          IF spos >= sub_end OR shift > 63 THEN
            RETURN ARRAY[]::bigint[];
          END IF;
          b := get_byte(spec, spos);
          spos := spos + 1;
          val := val + (b & 127)::numeric * (2::numeric ^ shift);
          shift := shift + 7;
          EXIT WHEN b < 128;
        END LOOP;
        swt := (val % 8)::int;
        sfield := (val - swt) / 8;
        IF sfield = 2 AND swt = 0 THEN
          val := 0;
          shift := 0;
          LOOP
            IF spos >= sub_end OR shift > 63 THEN
              RETURN ARRAY[]::bigint[];
            END IF;
            b := get_byte(spec, spos);
            spos := spos + 1;
            val := val + (b & 127)::numeric * (2::numeric ^ shift);
            shift := shift + 7;
            EXIT WHEN b < 128;
          END LOOP;
          api := val % (2::numeric ^ 64);
          IF api >= (2::numeric ^ 63) THEN
            api := api - (2::numeric ^ 64);
          END IF;
        ELSIF swt = 0 THEN
          LOOP
            IF spos >= sub_end THEN
              RETURN ARRAY[]::bigint[];
            END IF;
            b := get_byte(spec, spos);
            spos := spos + 1;
            EXIT WHEN b < 128;
          END LOOP;
        ELSIF swt = 1 THEN
          IF spos + 8 > sub_end THEN
            RETURN ARRAY[]::bigint[];
          END IF;
          spos := spos + 8;
        ELSIF swt = 5 THEN
          IF spos + 4 > sub_end THEN
            RETURN ARRAY[]::bigint[];
          END IF;
          spos := spos + 4;
        ELSIF swt = 2 THEN
          val := 0;
          shift := 0;
          LOOP
            IF spos >= sub_end OR shift > 28 THEN
              RETURN ARRAY[]::bigint[];
            END IF;
            b := get_byte(spec, spos);
            spos := spos + 1;
            val := val + (b & 127)::numeric * (2::numeric ^ shift);
            shift := shift + 7;
            EXIT WHEN b < 128;
          END LOOP;
          IF val > 2147483647 OR spos + val > sub_end THEN
            RETURN ARRAY[]::bigint[];
          END IF;
          spos := spos + val::int;
        ELSE
          RETURN ARRAY[]::bigint[];
        END IF;
      END LOOP;
      IF api IS NOT NULL AND api <> 0 THEN
        id := api::bigint;
        ids := array_append(ids, id);
      END IF;
      pos := sub_end;
    ELSIF wt = 0 THEN
      LOOP
        IF pos >= n THEN
          RETURN ARRAY[]::bigint[];
        END IF;
        b := get_byte(spec, pos);
        pos := pos + 1;
        EXIT WHEN b < 128;
      END LOOP;
    ELSIF wt = 1 THEN
      IF pos + 8 > n THEN
        RETURN ARRAY[]::bigint[];
      END IF;
      pos := pos + 8;
    ELSIF wt = 5 THEN
      IF pos + 4 > n THEN
        RETURN ARRAY[]::bigint[];
      END IF;
      pos := pos + 4;
    ELSIF wt = 2 THEN
      val := 0;
      shift := 0;
      LOOP
        IF pos >= n OR shift > 28 THEN
          RETURN ARRAY[]::bigint[];
        END IF;
        b := get_byte(spec, pos);
        pos := pos + 1;
        val := val + (b & 127)::numeric * (2::numeric ^ shift);
        shift := shift + 7;
        EXIT WHEN b < 128;
      END LOOP;
      IF val > 2147483647 OR pos + val > n THEN
        RETURN ARRAY[]::bigint[];
      END IF;
      pos := pos + val::int;
    ELSE
      RETURN ARRAY[]::bigint[];
    END IF;
  END LOOP;
  RETURN ARRAY(SELECT DISTINCT x FROM unnest(ids) AS t(x) ORDER BY 1);
END;
$fn$;

UPDATE session_records
SET api_ids = COALESCE(session_spec_api_ids(spec_bytes), '{}');

ALTER TABLE session_records
    ALTER COLUMN api_ids SET DEFAULT '{}',
    ALTER COLUMN api_ids SET NOT NULL;

CREATE INDEX session_records_api_ids ON session_records USING GIN (api_ids array_ops);
