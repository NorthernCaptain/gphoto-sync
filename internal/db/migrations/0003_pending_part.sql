-- The part that stored each waiting sidecar, so that a part whose file on
-- Drive was replaced can drop its stale rows before being ingested again.
ALTER TABLE pending_sidecars ADD COLUMN part_no INTEGER;

-- Best effort for existing rows: when an export has exactly one ingested
-- part so far, that part stored them. Rows of multi-part exports keep NULL
-- (the source part is not recorded anywhere); a reset of any part of such an
-- export drops them along with that part's own rows.
UPDATE pending_sidecars SET part_no = (
  SELECT MIN(a.part_no) FROM archives a
   WHERE a.account_id = pending_sidecars.account_id AND a.export_id = pending_sidecars.export_id
) WHERE (
  SELECT COUNT(*) FROM archives a
   WHERE a.account_id = pending_sidecars.account_id AND a.export_id = pending_sidecars.export_id
) = 1;
