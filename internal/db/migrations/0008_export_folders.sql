-- Which album each Takeout album folder of an export was attached to. A
-- folder name recurs across exports for different albums (told apart by
-- title and date); sidecar-only folders have no media occurrence to recover
-- the album from, so the ingest records the choice here.
CREATE TABLE export_folders (
  account_id INTEGER NOT NULL REFERENCES accounts(id),
  export_id  TEXT NOT NULL,
  folder     TEXT NOT NULL,
  album_id   INTEGER NOT NULL REFERENCES albums(id),
  PRIMARY KEY(account_id, export_id, folder)
);
-- Best effort for exports ingested before: the album their media in the
-- folder went to.
INSERT OR IGNORE INTO export_folders(account_id, export_id, folder, album_id)
  SELECT DISTINCT account_id, export_id, folder, album_id FROM occurrences
   WHERE album_id IS NOT NULL AND active = 1 AND state NOT IN ('reset', 'removed');
