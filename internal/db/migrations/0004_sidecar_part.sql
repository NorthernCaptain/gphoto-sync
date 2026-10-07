-- The part that supplied each stored sidecar. When that part's file on Drive
-- is replaced and ingested again, an exact sidecar for media that lives in
-- another part may correct the stored metadata. Rows from before this
-- migration keep NULL: no part is known, so they are never corrected this
-- way (a later export that resends the sidecar still updates them).
ALTER TABLE sidecars ADD COLUMN part_no INTEGER;
