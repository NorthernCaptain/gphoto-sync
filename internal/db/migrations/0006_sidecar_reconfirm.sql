-- Set on the stored sidecars a part supplied when that part's file is
-- replaced: the re-ingest clears it for every sidecar the replacement still
-- carries, and the rest lose their provenance (their part no longer vouches
-- for them, any later exact sidecar may replace them).
ALTER TABLE sidecars ADD COLUMN reconfirm INTEGER NOT NULL DEFAULT 0;
