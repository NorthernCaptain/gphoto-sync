-- Two repairs for databases that predate the 'removed' occurrence state and
-- that ran the 0002 backfill.
--
-- 1. Entries dropped by a completed part replacement were left inactive in
--    state 'ingested', indistinguishable from album memberships pruning
--    retired. They are told apart by their part's bookkeeping: a part's
--    finished entries list every media entry it still contains, so an
--    inactive occurrence without a finished entry for its folder and name was
--    dropped. The entry is matched the way the ingest classifies it:
--    Takeout/<product>/<folder>/.../<name>, i.e. the third path component is
--    the folder and the last one the name (compared literally, any components
--    in between). Such rows become 'removed': no late sidecar addresses them
--    and a sidecar for that name waits for media elsewhere.
UPDATE occurrences SET state = 'removed'
 WHERE active = 0 AND state NOT IN ('reset', 'removed') AND item_id IS NOT NULL
   AND NOT EXISTS (
     SELECT 1 FROM archives ax JOIN archive_entries ex ON ex.archive_id = ax.id
      WHERE ax.account_id = occurrences.account_id AND ax.export_id = occurrences.export_id AND ax.part_no = occurrences.part_no
        AND ex.status = 'done'
        AND substr(ex.entry_name, 1, 8) = 'Takeout/'
        AND substr(substr(ex.entry_name, 9), instr(substr(ex.entry_name, 9), '/') + 1, length(occurrences.folder) + 1) = occurrences.folder || '/'
        AND substr(ex.entry_name, -length('/' || occurrences.entry_name)) = '/' || occurrences.entry_name);

-- 2. Repair of the byte provenance backfilled by 0002, which only considered
--    active occurrences. Album pruning deactivates occurrences without
--    changing the bytes in the library, so an item whose current bytes
--    arrived with an export whose only occurrence was pruned since was
--    attributed to an older export, to a newer export that delivered other
--    bytes (0002's fallback), or to none; a delayed part could then be judged
--    against the wrong export. Every occurrence that is evidence counts here:
--    active ones and inactive ones pruning retired, never entries a part
--    replacement dropped ('removed') or that await a pending replacement
--    ('reset', it may drop them). An item is re-attributed to the newest
--    export whose occurrence carries its current hash when
--      * it has no provenance,
--      * that export is newer than the recorded one, or
--      * the recorded export has occurrence history for the item (any state,
--        a replacement may have dropped the entry since) but no evidence
--        carrying the current hash (a 0002 fallback, or bytes the export no
--        longer carries), and every part of that export is finished: an
--        unfinished part may have written the provenance itself (an
--        interrupted run, before its occurrence), and its resume settles the
--        record.
--    A recorded export with no occurrence at all is the ingest's own word and
--    is kept unless a newer export proves it stale. An item with no provenance
--    and no matching occurrence falls back to its newest occurrence.
UPDATE items SET bytes_export_id = (
  SELECT o.export_id FROM occurrences o JOIN blobs b ON b.id = items.blob_id
   WHERE o.item_id = items.id AND o.sha256 = b.sha256 AND (o.active = 1 OR o.state NOT IN ('removed', 'reset'))
   ORDER BY substr(o.export_id, 1, instr(o.export_id, '-') - 1) DESC,
            CAST(substr(o.export_id, instr(o.export_id, '-') + 1) AS INTEGER) DESC
   LIMIT 1)
WHERE EXISTS (
  SELECT 1 FROM occurrences o JOIN blobs b ON b.id = items.blob_id
   WHERE o.item_id = items.id AND o.sha256 = b.sha256 AND (o.active = 1 OR o.state NOT IN ('removed', 'reset'))
     AND (items.bytes_export_id IS NULL
          OR substr(o.export_id, 1, instr(o.export_id, '-') - 1) > substr(items.bytes_export_id, 1, instr(items.bytes_export_id, '-') - 1)
          OR (substr(o.export_id, 1, instr(o.export_id, '-') - 1) = substr(items.bytes_export_id, 1, instr(items.bytes_export_id, '-') - 1)
              AND CAST(substr(o.export_id, instr(o.export_id, '-') + 1) AS INTEGER) > CAST(substr(items.bytes_export_id, instr(items.bytes_export_id, '-') + 1) AS INTEGER))
          OR (EXISTS (SELECT 1 FROM occurrences o2 WHERE o2.item_id = items.id AND o2.export_id = items.bytes_export_id)
              AND NOT EXISTS (SELECT 1 FROM occurrences o3 JOIN blobs b3 ON b3.id = items.blob_id
                               WHERE o3.item_id = items.id AND o3.export_id = items.bytes_export_id AND o3.sha256 = b3.sha256 AND (o3.active = 1 OR o3.state NOT IN ('removed', 'reset')))
              AND NOT EXISTS (SELECT 1 FROM archives a WHERE a.account_id = items.account_id AND a.export_id = items.bytes_export_id
                               AND a.status NOT IN ('ingested', 'ingested_with_errors')))));
UPDATE items SET bytes_export_id = (
  SELECT o.export_id FROM occurrences o
   WHERE o.item_id = items.id AND (o.active = 1 OR o.state NOT IN ('removed', 'reset'))
   ORDER BY substr(o.export_id, 1, instr(o.export_id, '-') - 1) DESC,
            CAST(substr(o.export_id, instr(o.export_id, '-') + 1) AS INTEGER) DESC
   LIMIT 1)
WHERE bytes_export_id IS NULL;
