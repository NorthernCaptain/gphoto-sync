-- Which export last supplied an item's bytes and its sidecar. A part of an
-- older export that arrives after a newer one has been ingested must never
-- replace newer content; these columns let the ingest tell the order.
ALTER TABLE items ADD COLUMN bytes_export_id TEXT;
ALTER TABLE sidecars ADD COLUMN export_id TEXT;

-- Best effort for rows created before these columns existed. Export ids are
-- <timestamp>-<sequence>; they are ordered by timestamp, then numeric sequence
-- (lexical order would put sequence 9 after 10). The bytes are attributed to
-- the newest export whose active occurrence carries the item's current hash,
-- else to the newest active occurrence. The sidecar's source stays unknown
-- (NULL): occurrences do not record whether a sidecar came with them, and an
-- unknown source never rejects an arriving sidecar.
UPDATE items SET bytes_export_id = COALESCE(
  (SELECT o.export_id FROM occurrences o JOIN blobs b ON b.id = items.blob_id
    WHERE o.item_id = items.id AND o.active = 1 AND o.sha256 = b.sha256
    ORDER BY substr(o.export_id, 1, instr(o.export_id, '-') - 1) DESC,
             CAST(substr(o.export_id, instr(o.export_id, '-') + 1) AS INTEGER) DESC
    LIMIT 1),
  (SELECT o.export_id FROM occurrences o
    WHERE o.item_id = items.id AND o.active = 1
    ORDER BY substr(o.export_id, 1, instr(o.export_id, '-') - 1) DESC,
             CAST(substr(o.export_id, instr(o.export_id, '-') + 1) AS INTEGER) DESC
    LIMIT 1));
