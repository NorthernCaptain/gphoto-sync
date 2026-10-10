-- Set once a stored sidecar was actually consumed: applied to media named
-- after its title, or recorded as an album membership. Only then does the
-- row stop counting as waiting (an exact-name occurrence covers the others).
ALTER TABLE pending_sidecars ADD COLUMN resolved INTEGER NOT NULL DEFAULT 0;
