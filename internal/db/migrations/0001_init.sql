CREATE TABLE accounts (
  id            INTEGER PRIMARY KEY,
  name          TEXT NOT NULL UNIQUE,
  rclone_remote TEXT,
  created_at    TEXT NOT NULL
);

-- one row per distinct content, shared by all accounts; canonical_path is an
-- existing items.lib_path or item_versions.path, relative to data_dir
CREATE TABLE blobs (
  id             INTEGER PRIMARY KEY,
  sha256         TEXT NOT NULL UNIQUE,
  size           INTEGER NOT NULL,
  canonical_path TEXT NOT NULL,
  first_seen     TEXT NOT NULL
);

CREATE TABLE items (
  id                 INTEGER PRIMARY KEY,
  account_id         INTEGER NOT NULL REFERENCES accounts(id),
  blob_id            INTEGER NOT NULL REFERENCES blobs(id),
  google_id          TEXT,
  variant            TEXT NOT NULL,              -- original | edited | motion | alt:<ext>
  identity_key       TEXT NOT NULL,              -- <google_id>#<variant> | occ:<export>#<folder>#<file>
  identity_src       TEXT NOT NULL,              -- google_id | fallback
  filename           TEXT NOT NULL,
  ext                TEXT NOT NULL,
  mime               TEXT,
  width              INTEGER,
  height             INTEGER,
  taken_at           TEXT,
  taken_src          TEXT,
  lib_path           TEXT NOT NULL UNIQUE,       -- <account>/library/YYYY/MM/<name>
  group_id           TEXT NOT NULL,
  archived_export_id TEXT,
  first_seen         TEXT NOT NULL,
  last_seen          TEXT NOT NULL,
  UNIQUE(account_id, identity_key)
);
CREATE INDEX items_google_id ON items(google_id);
CREATE INDEX items_blob ON items(blob_id);
CREATE INDEX items_group ON items(account_id, group_id);
CREATE INDEX items_fallback ON items(account_id, identity_src, filename);

CREATE TABLE sidecars (
  item_id     INTEGER PRIMARY KEY REFERENCES items(id) ON DELETE CASCADE,
  json        BLOB NOT NULL,
  sha256      TEXT NOT NULL,
  description TEXT,
  favorited   INTEGER NOT NULL DEFAULT 0,
  lat         REAL,
  lon         REAL,
  alt         REAL,
  people      TEXT
);

CREATE TABLE albums (
  id             INTEGER PRIMARY KEY,
  account_id     INTEGER NOT NULL REFERENCES accounts(id),
  title          TEXT NOT NULL,
  album_date     TEXT NOT NULL DEFAULT '',
  takeout_folder TEXT NOT NULL,
  kind           TEXT NOT NULL,                  -- own | shared | partner
  path           TEXT NOT NULL UNIQUE,           -- <account>/albums/<name>
  has_meta       INTEGER NOT NULL DEFAULT 0,
  metadata_json  BLOB,
  created_at     TEXT NOT NULL,
  updated_at     TEXT NOT NULL,
  UNIQUE(account_id, title, album_date, takeout_folder)
);
CREATE INDEX albums_folder ON albums(account_id, takeout_folder);

-- one row per (album, item); two independent sources, the row lives while either is set
CREATE TABLE album_items (
  album_id            INTEGER NOT NULL REFERENCES albums(id),
  item_id             INTEGER NOT NULL REFERENCES items(id),
  link_path           TEXT NOT NULL UNIQUE,
  direct              INTEGER NOT NULL DEFAULT 0,
  confirmed_export_id TEXT,
  confirmed_by        TEXT,
  via_group           INTEGER NOT NULL DEFAULT 0,
  group_still         INTEGER,
  PRIMARY KEY(album_id, item_id)
);
CREATE INDEX album_items_item ON album_items(item_id);

CREATE TABLE albums_full (
  album_id          INTEGER PRIMARY KEY REFERENCES albums(id),
  listing_export_id TEXT NOT NULL
);

CREATE TABLE archives (
  id                INTEGER PRIMARY KEY,
  account_id        INTEGER NOT NULL REFERENCES accounts(id),
  remote_id         TEXT UNIQUE,
  name              TEXT NOT NULL,
  export_id         TEXT NOT NULL,
  part_no           INTEGER NOT NULL,
  size              INTEGER,
  md5               TEXT,
  status            TEXT NOT NULL,               -- seen fetching ingesting ingested partial ingested_with_errors failed
  local_path        TEXT,
  remote_deleted_at TEXT,
  started_at        TEXT,
  finished_at       TEXT,
  entries           INTEGER NOT NULL DEFAULT 0,
  ingested          INTEGER NOT NULL DEFAULT 0,
  duplicates        INTEGER NOT NULL DEFAULT 0,
  errors            INTEGER NOT NULL DEFAULT 0,
  UNIQUE(account_id, export_id, part_no)
);

CREATE TABLE archive_entries (
  archive_id INTEGER NOT NULL REFERENCES archives(id),
  entry_name TEXT NOT NULL,
  status     TEXT NOT NULL,                      -- done skipped failed given_up
  attempts   INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  PRIMARY KEY(archive_id, entry_name)
);

CREATE TABLE exports (
  account_id        INTEGER NOT NULL REFERENCES accounts(id),
  export_id         TEXT NOT NULL,
  highest_part      INTEGER NOT NULL DEFAULT 0,
  confirmed_parts   INTEGER,
  last_part_seen_at TEXT,
  complete_at       TEXT,
  authoritative     INTEGER NOT NULL DEFAULT 0,
  albums_pruned_at  TEXT,
  PRIMARY KEY(account_id, export_id)
);

-- one row per media entry ever seen; album membership is derived from active rows
CREATE TABLE occurrences (
  id         INTEGER PRIMARY KEY,
  account_id INTEGER NOT NULL REFERENCES accounts(id),
  export_id  TEXT NOT NULL,
  part_no    INTEGER NOT NULL,
  folder     TEXT NOT NULL,
  entry_name TEXT NOT NULL,
  variant    TEXT NOT NULL,
  sha256     TEXT,
  album_id   INTEGER REFERENCES albums(id),
  item_id    INTEGER REFERENCES items(id),
  state      TEXT NOT NULL,                      -- waiting ingested resolved
  tmp_path   TEXT,
  active     INTEGER NOT NULL DEFAULT 1,
  UNIQUE(account_id, export_id, folder, entry_name)
);
CREATE INDEX occurrences_item ON occurrences(item_id);
CREATE INDEX occurrences_album ON occurrences(album_id, item_id);

CREATE TABLE pending_sidecars (
  account_id INTEGER NOT NULL REFERENCES accounts(id),
  export_id  TEXT NOT NULL,
  folder     TEXT NOT NULL,
  media_key  TEXT NOT NULL,
  json       BLOB NOT NULL,
  PRIMARY KEY(account_id, export_id, folder, media_key)
);

CREATE TABLE item_versions (
  id            INTEGER PRIMARY KEY,
  item_id       INTEGER NOT NULL REFERENCES items(id),
  blob_id       INTEGER NOT NULL REFERENCES blobs(id),
  path          TEXT NOT NULL UNIQUE,
  superseded_at TEXT NOT NULL,
  by_export_id  TEXT
);

CREATE TABLE runs (
  id          INTEGER PRIMARY KEY,
  account_id  INTEGER,
  command     TEXT NOT NULL,
  started_at  TEXT NOT NULL,
  finished_at TEXT,
  exit_code   INTEGER,
  counts      TEXT
);

CREATE TABLE errors (
  id         INTEGER PRIMARY KEY,
  ts         TEXT NOT NULL,
  account_id INTEGER,
  ref        TEXT,
  message    TEXT NOT NULL
);

CREATE TABLE telegram_chats (
  chat_id      INTEGER PRIMARY KEY,
  user_id      INTEGER,
  username     TEXT,
  display_name TEXT,
  paired_at    TEXT NOT NULL,
  revoked_at   TEXT
);

CREATE TABLE telegram_state (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

-- crash recovery for file operations: written before the file calls, marked done after the DB commit
CREATE TABLE fs_journal (
  id    INTEGER PRIMARY KEY,
  op    TEXT NOT NULL,
  steps TEXT NOT NULL,
  state TEXT NOT NULL,                           -- intent | done
  ts    TEXT NOT NULL
);
