-- The media entry a stored sidecar was related to by a name heuristic while
-- the photo it names was already known with other bytes: the decision (an
-- album entry of the known photo, or its new version) waits for the rest of
-- the export, and the entry is kept so that it can be made then.
ALTER TABLE pending_sidecars ADD COLUMN deferred_entry TEXT;
ALTER TABLE pending_sidecars ADD COLUMN deferred_variant TEXT;
ALTER TABLE pending_sidecars ADD COLUMN deferred_still TEXT;
