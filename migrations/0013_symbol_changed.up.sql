-- Change notifications for SYM's WatchChanges (E22-D009, #554).
-- Every replica only refreshes its own -feeds, so a stream must not depend on which process
-- committed. Postgres sends the notification instead: on commit, in commit order, and not at all
-- for a transaction that rolls back. Refresh (bumpUnit) and Insert/Move (bumpRevision) all write
-- symbol_revision, so the Go write paths do not change.
--
-- Channel sym_changed, payload '<venue>/<category>/<revision>'. A venue and a category have no '/'.
-- An UPDATE that leaves revision as it was sends nothing.
--
-- LISTEN needs a direct session: through a transaction-pooling pgbouncer it hears nothing.

CREATE FUNCTION symbol_revision_notify() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.revision = OLD.revision THEN
        RETURN NULL;
    END IF;
    PERFORM pg_notify('sym_changed', NEW.venue || '/' || NEW.category || '/' || NEW.revision::text);
    RETURN NULL;
END;
$$;

CREATE TRIGGER symbol_revision_notify
AFTER INSERT OR UPDATE ON symbol_revision
FOR EACH ROW EXECUTE FUNCTION symbol_revision_notify();
