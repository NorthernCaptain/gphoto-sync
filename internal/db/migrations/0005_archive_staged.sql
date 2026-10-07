-- When a part's Drive copy was moved into the private staging folder for
-- deletion. Set before the delete is attempted, so a failed delete leaves
-- evidence that a file of this part sits in staging and must be finished
-- there (by sync) before the row may forget the file's id.
ALTER TABLE archives ADD COLUMN staged_at TEXT;
