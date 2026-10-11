-- entries of a part found under a known product folder; NULL for rows
-- written before this was recorded
ALTER TABLE archives ADD COLUMN recognized INTEGER;
