-- The Google id a stored sidecar names, so that a media file of the same
-- name in the folder only counts as the sidecar's media when it is that
-- photo. Backfilled from the sidecar's url (photos.google.com/photo/<id>).
ALTER TABLE pending_sidecars ADD COLUMN google_id TEXT;
UPDATE pending_sidecars SET google_id = (
  SELECT CASE WHEN instr(u, 'photos.google.com/photo/') > 0
              THEN substr(u, instr(u, 'photos.google.com/photo/') + length('photos.google.com/photo/'))
              ELSE NULL END
    FROM (SELECT json_extract(pending_sidecars.json, '$.url') AS u))
 WHERE json_extract(json, '$.url') IS NOT NULL;
