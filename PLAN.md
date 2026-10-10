# gphoto-sync — Plan

A service, written in Go and shipped as a Docker image, that mirrors the photos and videos
of one or more Google accounts (yours, your wife's, …) into a local folder tree on Linux,
one subtree per account. It runs as a weekly one-shot container, not a daemon:

```
<data>/
  <email local part>/                        one subtree per Google account, e.g. leo/
    library/YYYY/MM/<original filename>      every item, once
    library/YYYY/MM/<original filename>.json Takeout sidecar kept next to the file
    albums/<album title>/<filename>          own and shared albums (hardlinks into library/)
  <other account>/
    …
```

Status: **plan only, nothing implemented.** Section 7 records all decisions; nothing is
pending.

---

## 1. Why Takeout, and what Takeout gives us

**The Google Photos API cannot do this.** Since 31 March 2025 the Library API only returns
media that the calling app itself uploaded; the scopes that allowed reading a user's library
and the shared-album endpoints were removed. The newer Picker API needs a human to select
items per session, and the Ambient API is a 240-requests-per-day feed for photo frames. Both
strip GPS and transcode video. Browser automation of photos.google.com works but is
unofficial and fragile. All of these were evaluated and rejected; this plan covers only the
one official path that delivers originals: **Google Takeout → Google Drive → rclone → us.**

**Takeout facts the design relies on** (sources at the end):

- **Scheduled, incremental exports** (new since June 2026): with Google Photos as the only
  product selected, Takeout can run on a schedule for 12 months. Google's help page documents
  "every 2 months", the current UI also offers **monthly**. The first export is full; later
  ones can be limited to items "uploaded, backed up, created, or edited since the last
  successful export". Archives are zip or tgz, up to 50 GB per part, delivered to Drive,
  Dropbox, OneDrive, Box, or an e-mail link. Drive delivery writes into a `Takeout` folder
  and **counts against Drive storage**. Not available to accounts in the Advanced Protection
  Program. Deletions are not reported. The baseline for "since the last export" is Google's
  own record of the previous successful job, not the archive files, so ingested parts can
  be deleted from Drive.
- **Archive layout**: `Takeout/Google Photos/Photos from YYYY/…` for every year, plus one
  folder per album named after the album title (`Untitled(1)` for untitled ones) that holds
  a `metadata.json` (`title`, `description`, `access`, `date`, `geoData`, `enrichments`,
  `sharedAlbumComments`) and *copies* of the member files. Every media file has a JSON
  sidecar (`IMG_1.jpg.supplemental-metadata.json`, older exports `IMG_1.jpg.json`) with
  `title` (the original filename), `description`, `photoTakenTime`, `creationTime`,
  `geoData`, `geoDataExif`, `people`, `favorited`, `url`
  (`https://photos.google.com/photo/<id>`), `googlePhotosOrigin`.
- **Known quirks** an ingester must handle, verified on 2026-10-06 against part 1 of the
  real export (33,096 entries, 13,352 media, 19,744 sidecars, 26 year folders, 112 album
  folders, all entries deflate-compressed, 314 non-ASCII names):
  - duplicate names become `IMG_1(1).jpg`; the sidecar is
    `IMG_1.jpg.supplemental-metadata(1).json` in current exports (the `(n)` moves to the
    end of the sidecar name) and `IMG_1.jpg(1).json` in older ones; both styles are matched;
  - sidecar names can be truncated to ~46 characters (none in part 1, still handled);
  - edited versions are `IMG_1-edited.jpg` sharing the original's sidecar (445 in part 1);
  - **splits across parts are the norm, not the exception**: in part 1, 7,336 sidecars
    belong to media stored in another part and 295 media have their sidecar elsewhere; 69
    of the 112 album folders have their `metadata.json` in another part;
  - Pixel motion photos arrive as a pair, `PXL_x.MP` (the motion container, no sidecar of
    its own) plus `PXL_x.MP.jpg` (the still, with sidecar); phone-side duplicates such as
    `PXL_x.MP~2.jpg` keep their `~2`; iPhone Live Photos arrive as HEIC+MOV pairs;
  - `Archive/` and `Trash/` are Google's special folders, not albums; albums can be named
    like years (`2001`) and must not be confused with `Photos from 2001`;
  - album folder names may carry trailing spaces (7 in part 1) and have apostrophes
    replaced by `_`; `metadata.json` `title` holds the real title (`access`, `date`
    present; `date` can be absent in newer exports);
  - item sidecars carry `people`, `url` (the Google photo id) and `googlePhotosOrigin`;
    `geoData` is all zeros when there is no location;
  - extra JSON files to ignore: `shared_album_comments.json`,
    `user-generated-memory-titles.json`, `print-subscriptions.json`, `archive_browser.html`;
  - the same file appears once per album it is in, plus once in its year folder; the
    largest single entry seen is a 1.7 GB video.
- **Shared albums**: content other people added to a shared album is *not* in your export
  (it belongs to their account). Only your own contributions, and items you explicitly
  "saved" to your library, appear. The album folder itself does appear with `metadata.json`.
- **Originals**: Takeout files are the uploaded bytes, EXIF and GPS intact, videos in
  original quality. This is the only Google-supported way to get them.
- **rclone's Drive login** uses rclone's own published OAuth client, so no Google Cloud
  project is needed. Its refresh token has no time limit while it is used; Google revokes it
  only on user revocation, 6 months of disuse, or a security event.

## 2. Architecture (decided 2026-10-05)

**Pipeline:** Google Photos → Takeout scheduled export (monthly, incremental, zip, 50 GB
parts) → Google Drive `Takeout` folder → **rclone** copies each part to local disk →
**gphoto-sync** (Go) ingests the zip, de-duplicates by content hash, lays out `library/` and
`albums/`, records everything in SQLite, deletes the local part → exits.

**Runtime model:** no daemon. A systemd timer starts one container a week with
`docker run --rm … sync`; the app does its work and exits, the container is removed, and
nothing stays resident between runs. A run finds a new export at most a week after it lands.

**Why rclone instead of an in-app Drive client:** it removes the Google Cloud project, the
OAuth consent flow, token storage and the Drive API client from the codebase. The cost is a
staging area for one 50 GB part at a time.

**Accounts:** the service handles several Google accounts in one run. Each account is a
config entry with its own rclone remote, and each person sets up their own Takeout schedule
delivered to their own Drive. Files land under `/data/<account>/`, where the account name is the e-mail local part
(`leo` for leo@…). Accounts are processed one after another, so the 50 GB staging limit
still holds. Identical files in two accounts (partner sharing, items saved from a shared
album) share one inode via hardlink, see 3.3.

**Current state (2026-10-05):** rclone remote `gdrive` configured on the workstation with
full Drive scope, your Drive has 2.7 TiB free, the old `Takeout` folder was emptied, and the
first full Photos export of your account is being generated by Google.

What this means against the original wishlist:

| Wish | Outcome |
|---|---|
| Photos + videos, whole library | Yes |
| Own albums | Yes (Takeout album folders) |
| Albums shared by others, including their photos | Partially: the album folder appears with your own items plus whatever you "saved" to your library. Other people's items are not exported by Google |
| Year/month layout, albums/ | Yes. Shared albums are laid out like own albums (decided), the DB records which were shared |
| Original filenames | Yes: Takeout `title` is the original name even when the on-disk name was truncated or suffixed |
| Metadata preserved, location if possible | Yes: files are originals (EXIF/GPS intact), plus the JSON sidecar is kept |
| SQLite state | Yes |
| Several accounts, subfolder per account | Yes: one Takeout schedule and one rclone remote per account, one `/data/<account>/` subtree each, all handled by the same weekly run |
| Parallel downloads | rclone handles the transfer (multi-threaded per file); hashing, EXIF probing and linking run on a worker pool |
| 24x7, hourly check | Changed: weekly one-shot run from a systemd timer. New data appears monthly via Takeout |
| Download once, link into albums | Yes: hardlinks by default (see 3.3) |
| Go, Docker | Go 1.27 (released 2026-08-19); rclone + the Go binary in one distroless image, amd64 |
| Know what happened | Telegram bot: a report with per-account stats after every run, see 3.9 |

## 3. Design

### 3.1 Components

```
host                                   container (docker run --rm, weekly, exits when done)
┌────────────────────────┐             ┌──────────────────────────────────────────────────┐
│ systemd timer          │── start ──► │ gphoto-sync sync                                 │
│ gphoto-sync.timer      │             │   per account:                                   │
│ gphoto-sync.service    │◄── exit ─── │   1. rclone lsjson <remote>  → new parts?        │
│  (Type=exec)           │   code      │   2. rclone copyto <part> → /state/inbox         │
│                        │             │   3. ingest zip → /data/<acct>/{library,albums}  │
│ /home/leo/gphoto-backup/data   ────┼── /data ──► │   4. SQLite bookkeeping, delete local part       │
│ /home/leo/gphoto-backup/state  ────┼── /state ─► │   5. next part, next account, report, exit       │
│ /home/leo/gphoto-sync/config/rclone ────┼─ /config ─► │  binaries: /usr/local/bin/{gphoto-sync,rclone}   │
└────────────────────────┘             └──────────────────────────────────────────────────┘
```

- **Runner**: the `sync` subcommand. Every writing command (`sync`, `ingest`, `retry`,
  `relink`, `exports confirm`, `telegram pair`) takes an exclusive `flock` on
  `/state/gphoto-sync.lock` before journal recovery and holds it until the process exits;
  a second writer (a manual `ingest` while the timer's `sync` runs) fails fast with exit 1
  instead of sharing temp paths and racing the journaled moves. Dry runs and read-only
  commands (`status`, `verify`, `check-remote`, `telegram chats`/`test`) do not lock, do
  not replay the journal, and open the database without creating or migrating it (they
  fail with a clear message until a writing command has run). The runner then, for each configured
  account (config order, or only `--account <name>`) discovers, fetches and ingests, prints
  a per-account summary, and exits with the worst exit code seen (3.6). Accounts run
  sequentially so staging never holds more than one part.
- **Fetcher**: a thin `os/exec` wrapper around rclone (`lsjson`, `copyto`, `deletefile`).
- **Ingester**: the only writer to `/data` and the DB. Takes (bytes, sidecar?, origin hints)
  and is idempotent, so any re-run is safe. The `ingest` subcommand feeds it from a local
  directory or zip with no rclone involved.
- **Layout + Linker**: computes target paths, resolves collisions, creates album links.
- **Notifier**: sends the run report to every paired Telegram chat (3.9). The exit code and
  journald remain available for systemd.

### 3.2 One run, step by step (per account)

1. **Discover**: `rclone lsjson --hash --files-only <remote>` where `<remote>` is the
   account's `rclone_remote` (for example `gdrive-leo:Takeout`). Each entry gives `ID` (Drive
   file id), `Name`, `Size`, `ModTime`, and `Hashes.MD5`. Keep files matching
   `takeout-<timestamp>-<seq>-<NNN>.zip` (real example:
   `takeout-20261006T035113Z-1-001.zip`). The **export id** is `<timestamp>-<seq>`
   (`20261006T035113Z-1`); it is the key for parts, occurrences, pending sidecars,
   completeness and album listings everywhere below and in the schema (`export_id`), and
   exports are ordered by timestamp then sequence wherever "older" or "newer" is used. The **Drive file id is the key** everywhere; the name is
   only for display and for the local file. Drive allows two files with the same name in one
   folder: if the listing contains duplicate names, those parts are reported and neither is
   fetched or deleted until the duplicate is removed by hand. A part already `ingested`
   in the `archives` table (found by Drive id, else by account + export + part number) is
   skipped only when the listed file is the recorded one: MD5s are compared when both are
   known, else sizes. A file that differs (re-uploaded under a new id, or changed in
   place) is a new ingest of that part: its entry bookkeeping is dropped, the part's
   occurrences are deactivated (the re-ingest reactivates the ones still present), album
   memberships confirmed only by its vanished occurrences fall back to the newest remaining
   evidence, the export's `albums_pruned_at` and the albums' applied-listing marks are
   cleared so the listing is pruned again, and the file goes through the normal fetch and
   ingest; the report notes it. Occurrences the reset deactivated carry state `reset`
   until their part is ingested again, so another part finishing meanwhile leaves their
   memberships alone. Once a part is fully ingested, direct memberships that no
   active occurrence supports any more (the replacement dropped the item from the album,
   and a non-authoritative export is never pruned) lose their link, companions' group
   links are recomputed. A match found by name alone whose
   content cannot be verified (no hash recorded): an `ingested` part is skipped and never
   trashed; an unfinished part is started over, since its finished entries may belong to
   another file. Recording another remote file for a part clears `remote_deleted_at`, so
   the new file's deletion is owed again. For a
   verified `ingested` part, if `delete_remote_after_ingest` is on and `remote_deleted_at`
   is still NULL, its remote deletion is retried now (a failed deletion on an earlier run
   must not leave the part on Drive forever). A part in `partial` state is fetched again
   and only its failed entries are retried. Group by export id
   (one export = N parts).
2. **Settle**: Takeout uploads parts over several hours. Skip any part whose `ModTime` is
   younger than `settle` (default 2h); it is picked up by the next weekly run. Parts are
   ingested independently and idempotently because album folders and their media can land in
   different parts; every media entry gets an `occurrences` row, and sidecars that arrive
   before their media wait in `pending_sidecars`.
3. **Fetch one part at a time**: check free space in the inbox (needs `Size` + 10 %), then
   `rclone backend copyid <remote> <ID> /state/inbox/<account>/<name>` (the Drive backend's
   copy-by-id command, so the exact discovered file is fetched) with
   `--multi-thread-streams`, `--retries`, `--low-level-retries`, JSON logs captured into the
   run log. Verify local size and MD5 against the listing. One part at a time caps staging
   at 50 GB. rclone restarts a file that fails mid-transfer; acceptable once a month.
4. **Open the local zip** with `archive/zip` (random access, zip64). The central directory
   is in memory, so entries are walked in a chosen order: `Photos from YYYY` folders first
   (library items), then album folders (mostly memberships), each group by offset. Classify each entry under `Takeout/Google Photos/`:
   - `*/metadata.json` → album descriptor (title, access, dates). `Photos from YYYY` folders
     are year buckets, not albums. `Archive/` and `Trash/` are Google's special folders:
     Archive items are ingested into `library/` like any other and marked archived by
     setting `items.archived_export_id` to the export that showed them there; a newer
     export that shows the same item in a `Photos from YYYY` folder clears the mark
     (the user unarchived it), and a newer `Archive/` occurrence sets it again, so the
     flag always reflects the newest export that mentioned the item. Trash is skipped
     unless `include_trash: true` (the user deleted those). Any other folder is an
     album; if its `metadata.json` has not been seen yet (it usually sits in another part),
     the album is created with the trimmed folder name as provisional title and updated
     when the descriptor arrives.
   - `*.json` next to media → sidecar. Parse now; if its media occurrence is already
     known, attach it, else stash it in `pending_sidecars` keyed by (account, export,
     folder, normalized media name). The account is part of every key: two accounts
     exporting on the same day with the same filenames must never cross-match.
   - media → step 5.
   - anything else (`archive_browser.html`, `print-subscriptions.json`,
     `user-generated-memory-titles.json`, `shared_album_comments.json`, …) → ignored but
     logged once.
   Entries already recorded for this part (resume after a crash) are skipped by name,
   but their sidecars are still looked up: a sidecar that failed to read on the earlier
   run and parses now is applied to the finished media (promotion of a fallback item, or
   the first sidecar of an item that has none), exactly like a sidecar arriving in a later
   part. Only items without a stored sidecar are touched this way, so the index's fuzzy
   rules (title, stem prefix) can never replace metadata that an exact match supplied.
5. **Ingest a media entry**:
   1. Stream to `/data/<account>/.tmp/<uuid>`, computing SHA-256 while writing.
   2. Match the sidecar using the known naming variants: `X.jpg.supplemental-metadata.json`,
      `X.jpg.json`, truncated `X.jpg.supplemental-metadat.json`-style names, both `(n)`
      styles (`X(1).jpg` ↔ `X.jpg.supplemental-metadata(1).json` or `X.jpg(1).json`),
      `-edited` sharing the original's, and `PXL_x.MP` using the sidecar of `PXL_x.MP.jpg`.
   3. Resolve the **item identity**: `variant` is `edited` for `-edited` files, `motion`
      for a Pixel `PXL_x.MP` companion (identified through its `PXL_x.MP.jpg` still), else
      `original`. With a sidecar whose `url` carries a Google id the key is
      `<google id>#<variant>` (`identity_src = google_id`); the variant is part of the key
      because an edited file shares the original's sidecar and therefore its Google id,
      and both are kept (decided). If an item with this key already exists in this
      account → this is another occurrence of the same item (an album copy, or a
      re-export). Incremental exports re-send items that were *edited*, so the occurrence
      is compared with what is stored: if the sha256 equals the item's blob and the
      sidecar JSON is unchanged, discard the temp file and only record the occurrence
      (membership). If the **sidecar changed** (description, favorite, people, even
      `photoTakenTime`), rewrite the sidecar file and row and refresh the derived fields;
      a changed capture time or title recomputes the path and `rename()`s the file. If
      the **bytes changed** (a re-edited `edited` variant, typically) and the part comes
      from the same or a newer export than the one that supplied the current bytes
      (`items.bytes_export_id`), the item is
      re-pointed to the new blob: the superseded file is kept, renamed in place to
      `<name>~v<first 8 hex of its own sha256>.<ext>` (unique per content, so two changes
      on one day never collide; the rename refuses to replace an existing file unless it
      is already the same inode) and recorded in `item_versions` (backup semantics,
      nothing is deleted), album links are re-created against the new inode,
      and the old blob follows the canonical-path rule (step 10). Parts are not
      guaranteed to arrive in export order (a late part of an older export, a manual
      ingest of an old archive): content from an export **older** than the one that last
      supplied the item's bytes never replaces them; the differing bytes are kept as a
      version under the same `~v<sha8>` naming and only the occurrence is recorded.
      Likewise a sidecar older than the stored one (`sidecars.export_id`) is ignored, and
      an identical sidecar or identical bytes seen in a newer export just move the
      recorded export forward. If no item carries the
      key, a `fallback` item may be **promoted** instead of creating a second item, but
      only on an unambiguous match (step 9).
      Without a usable sidecar the identity is **provisional and per occurrence**:
      `occ:<export id>#<folder>#<on-disk filename>` (`identity_src = fallback`). Two
      unresolved occurrences never share an item, so distinct photos that happen to have
      identical bytes and names cannot be collapsed before their sidecars say they are
      the same. There is no shortcut for album copies either: a sidecar-less
      album-folder occurrence gets its own provisional item (a hardlink to the blob, no
      extra bytes) and is merged into the right item only when its sidecar resolves it,
      which is often a later part: in the real part 1, 295 media had their sidecar in
      another part, so promotion is a routine path, not an edge case. Membership is never
      assigned on byte-and-name similarity alone.
      Provisional items that never resolve are listed in the report as "unresolved
      identity"; they are never lost, at worst the same photo has two library paths
      sharing one blob.
      Otherwise it is a new item, even if its bytes are already known: two distinct Google
      Photos items can be byte-identical (the same picture uploaded twice) and each keeps its
      own path, sidecar, dates and album memberships. Bytes are shared through **blobs**:
      if a blob with this SHA-256 exists (in any account), the new item's path is a
      hardlink to that blob's canonical file and the temp file is discarded; else the temp
      file becomes the blob.
   4. Capture time, first hit wins: sidecar `photoTakenTime` → EXIF `DateTimeOriginal` →
      QuickTime/MP4 creation time → `Photos from YYYY` folder year (month unknown → `YYYY/00`).
      Convert to the configured timezone for the `YYYY/MM` bucket.
   5. Filename: sidecar `title` (the real original name) sanitized for the filesystem,
      falling back to the on-disk name. A `motion` companion keeps its **own** on-disk
      name (`PXL_x.MP`): the still's sidecar supplies its identity and capture time, not
      its name, so the motion bytes land next to the still as `PXL_x.MP`, never under the
      still's `.jpg` name.
   6. Target `library/YYYY/MM/<name>` inside the account's subtree (paths are stored
      relative to `data_dir` and start with the account folder, so they are unique across
      accounts). If that path is already owned by a *different item
      identity* (regardless of whether the bytes match: two uploads of the same picture can
      share name, month and content yet are two items), append `~` plus the first 8 hex
      digits of SHA-256(identity key) before the extension: `IMG_1234~3fa9c1d2.jpg`. The
      suffix is a function of the item identity alone, so it is identical on every re-run
      and differs for every colliding item. Edited versions keep their `-edited` suffix
      next to the original.
   7. `rename()` (new blob) or `link()` (existing blob) into place, atomic on one
      filesystem and journaled (step 11). For a **new blob**, set the file mtime to this item's capture time. For a
      link to an existing blob the mtime is left alone: hardlinks share one inode and
      therefore one mtime, so it stays at the first item's capture time. The authoritative
      per-item capture time is `items.taken_at` and the sidecar, not the mtime, and `verify`
      does not check mtimes. Write the sidecar next to the file as `<name>.json`; insert the
      `items` row (and the `blobs` row for a new blob).
   8. Companion files share the base name and land in the same folder, and the DB records
      the group: HEIC+MOV Live Photos, RAW+JPG, and Pixel motion photos. `PXL_x.MP` has
      no sidecar of its own; it becomes its **own item** with variant `motion` under the
      still's Google id (key `<id>#motion`, so it never collides with the still's
      `<id>#original`), takes its capture time from the still's sidecar, and is tied to
      the still through `group_id`. Album membership for any companion comes from its own
      album-folder occurrences like every other item, plus the group rule in step 9: a
      companion is also linked into every album its still is a direct member of, so a
      motion file or a Live Photo video always follows its still into the album folder.
   9. **Late sidecars and promotion.** Within a part, a media entry whose sidecar has not
      been seen yet waits in `.tmp/` with its `occurrences` row in state `waiting`; when
      the part is finished, any still-unmatched media is ingested with the `fallback`
      identity (capture time from EXIF, else the `Photos from YYYY` folder, name from
      disk) and its occurrence records the resulting `item_id`. Sidecars without media stay
      in `pending_sidecars` for later parts of the same export; once the export is complete, or `export_complete_after`
      has elapsed, orphan sidecars are reported and kept for diagnostics. **Matching a
      late sidecar to a fallback item is exact within an export**: the occurrence key
      (account, export, folder, entry name) identifies it, and the occurrence row names
      the item it was ingested as. **Album membership is derived, never stored on its own.** One `album_items` row per
      `(album, item)` carries two independent source flags. `direct` is true exactly while
      at least one active occurrence links that pair (with `confirmed_export_id` for
      pruning). `via_group` is true exactly while the item shares a `group_id` with a
      still that has `direct = true` in that album (`group_still` names it). The row, and
      the hardlink, exist while either flag is true; a row with both flags false is
      deleted. Each flag is recomputed on its own: `direct` whenever occurrences change
      (ingest, re-attribution, prune), `via_group` whenever any `direct` flag in the group
      changes, and both by `relink`. Pruning only ever clears `direct` on the still; the
      companion's `via_group` follows, and its own `direct`, if it has one, is untouched,
      so a companion with its own occurrence stays in the album after the still leaves. When an occurrence is
      re-pointed to another item, the derived link follows it, and the previous (album,
      item) link is dropped only if no other occurrence supports it. This is attribution
      correction, not pruning, and is allowed at any time. **Across exports**
      (step 3: a sidecar-bearing occurrence arrives in a later export, the earlier one
      never got its sidecar) promotion happens only when exactly one `fallback` item in the
      account has the same `(sha256, on-disk filename, variant)`; if none or several match,
      a new item is created, the fallback items are left untouched, and the report lists
      them under "unresolved identity" for manual review. A promotion is a journaled
      operation (step 11): `google_id` and `identity_key` are set, `taken_at` and `filename` come
      from the sidecar, the target path is recomputed and the file is `rename()`d if it
      changed (album hardlinks point at the inode and stay valid; their `link_path` is
      unaffected), the sidecar JSON is written next to the new path and the old one
      removed, and the occurrences of that item, hence its derived memberships, move with
      it. If
      the promoted key already belongs to another item (the same photo was ingested with
      its sidecar earlier), the fallback item is merged into it. First the fallback's bytes
      and the arriving sidecar are applied to that item under the export-order rule of
      step 8 (a newer export's bytes seen before their sidecar become current, the older
      bytes a kept version; an older export's bytes are kept as a version only), then the
      fallback's memberships are transferred, its path is unlinked and its row deleted,
      subject to the blob rule below.
  10. **Blob canonical path rule.** `blobs.canonical_path` is the path new hardlinks are
      made from. A blob is referenced by item paths (`items.lib_path`) and by kept
      superseded files (`item_versions.path`); both count. Before any referencing path is
      unlinked (merge, prune), if it is the canonical path the blob is re-pointed to
      another surviving referencing path in any account; the blob row is deleted only when
      neither an item nor a version references it. Every journaled rename of a referencing
      path (promotion, metadata-driven rename, supersede) updates `canonical_path` in the
      same journal entry when that path is the canonical one, so the canonical path never
      points at a name that no longer exists. `verify` checks that every canonical path
      exists and shares its inode with every referencing path of that blob.
  11. **Filesystem and database consistency.** SQLite cannot roll back a `rename()`, so
      every multi-step file operation (ingest move, album link, promotion rename,
      supersede rename, merge unlink) is **journaled**: an `fs_journal` row with the
      intended operation and both paths is committed *before* the file call, and marked
      done *after* the matching DB update commits. All file operations are idempotent
      (a rename whose destination already has the source's inode is done; a link that
      already exists is done; an unlink of a missing path is done). On every start, open
      journal rows are replayed or rolled back by inspecting the filesystem, before any
      new work begins, so a crash between a file call and its commit never leaves the
      database pointing at a missing path. A promotion or supersede is one journal entry
      that may contain several steps and is replayed as a unit.
6. **Albums**: an incoming album folder is matched to an existing album in this account
   by `(title, album date, Takeout folder name)`. A different folder name means a
   **different album**, with one narrow exception that needs positive evidence of a
   rename and is only evaluated while applying an export that is *authoritative and
   complete* (3.3: confirmed part count, every part ingested), because an incremental
   export omits unchanged albums and their absence proves nothing: the `metadata.json`
   carries a `date`, exactly one existing album has the same `(title, date)`, and that
   album's recorded folder name occurs nowhere in the complete export; then Google
   renamed the folder (`Title` ↔ `Title(1)`) and the album is re-matched and its folder
   name updated. In an incremental export, or without a `date` (absent in some newer
   exports), there is never a merge; a renamed or same-title album simply gets a distinct
   local path, and the stale one empties out at the next confirmed full export's prune.
   The Takeout folder name is always recorded. Two albums that share title, date *and*
   folder name across exports cannot be told apart by anything Takeout provides; that is
   an accepted limitation. Its
   path is `albums/<sanitized title>/`; if that path already belongs to a *different*
   album identity, the new one gets `albums/<title> (<album date YYYY-MM-DD>)/`, and if
   that is taken too, a short hash of the Takeout folder name is appended. Paths are
   stored in `albums.path` and never change once assigned, so two albums that merely share
   a title, own or shared, never share a folder. Shared albums are laid out exactly like
   own albums; `albums.kind` records that `metadata.json` marked them shared. Each member
   is hardlinked from `library/` as `albums/<album path>/<basename of lib_path>`; if that
   name is already taken in the album by a *different* item (two photos with the same
   original name from different months, say), the identity suffix from step 5.6 is applied
   to the link name as well. The chosen name is stored in `album_items.link_path` and never
   changes. Membership is only ever added here; removals follow the rules in 3.3.
7. **Finish the part**: every entry has a row in `archive_entries` with status `done`,
   `skipped` (non-media noise) or `failed` (+ attempt count, last error). The part becomes
   `ingested` only when no entry is `failed`; otherwise it becomes `partial`, the run's exit
   code is 2, the local copy is kept if there is room (else re-fetched next run), and the
   next run retries just the failed entries. After `max_entry_attempts` (default 3 runs) a
   failed entry becomes `given_up` and the part becomes `ingested_with_errors`. From then on
   **every** run lists the given-up entries in its report and exits with code 4 until they
   are resolved: `gphoto-sync retry --account NAME [--part NAME]` resets them to `failed`
   with zero attempts, the part returns to `partial`, and the next run re-fetches it and
   retries. A part with given-up entries is never deleted remotely and keeps its local copy
   if there is room.
   For an `ingested` part: delete the local copy unless `keep_archives: true`; if
   `delete_remote_after_ingest: true`, re-list the folder, confirm that exactly one file
   carries this name and that its id and MD5 match the `archives` row, then move that
   exact id (`rclone backend moveid`) into `.gphoto-sync-trash/<name>.<id>`, a folder only
   this program writes to, and `rclone deletefile` that unique path (rclone trashes by
   default, recoverable for 30 days); set `remote_deleted_at`. If the check fails or the
   move errors, leave it NULL, report it, and let step 1 retry next run. If the move
   succeeded but the delete failed, the file is no longer in the Takeout listing: at the
   end of every run, ingested parts with `remote_deleted_at` NULL that are absent from the
   listing are looked up by id in the staging folder and deleted from there; one absent
   from both is gone and recorded as deleted. Then fetch the next part.
8. **Report**: print a summary (parts seen/ingested/failed, media added, duplicates,
   links, duration) and send it to Telegram (3.9), on success as well as on failure. If the
   newest export timestamp is older than `expected_interval + grace` (default 30 + 14 days),
   the yearly schedule has probably lapsed: log at error level, exit with code 3, and say
   so in the Telegram message.

Deletions and un-favoriting are **not** visible through Takeout. The local tree is a backup,
not a mirror: nothing is ever deleted locally.

Rejected alternatives for the transfer: `rclone mount` (FUSE needs `--cap-add SYS_ADMIN`
and `/dev/fuse` in the container) and an in-app HTTP-Range zip reader over the Drive API
(needs a Cloud project and OAuth). Both would avoid the 50 GB staging area; neither is worth
the complexity.

### 3.3 Album links: download once, link many

Each item is stored once under `library/` and appears in album folders as a link:

| Mode | Extra space | Survives moving the original | Visible as "a file" to every tool | Caveat |
|---|---|---|---|---|
| `hardlink` (default) | none | yes | yes | Must be the same filesystem, so `library/` and `albums/` live under one mounted volume. Not supported on some SMB/NFS mounts. `rsync` needs `-H`, otherwise backups de-duplicate into copies |
| `symlink` (relative) | ~0 | no (dangling if original moves) | some tools skip symlinks | Works across filesystems |
| `copy` | full | yes | yes | Fallback when neither link works |

Across accounts (decided: on): content is stored once per SHA-256 as a *blob*, and every
item, in whichever account, that has those bytes is a hardlink to it (3.2 step 5). So the
same picture in two accounts, or uploaded twice to one account, costs one copy while each
item keeps its own path, sidecar and album memberships. All accounts therefore live on one
filesystem; moving one account to another disk later turns the shared links into copies at
that moment, nothing is lost. Blobs are global by design; there is no per-account mode.

Linker behaviour: album membership is **additive by default**. An album folder in an
incremental export may contain only the items that changed since the last export, so the
absence of an item there is not evidence that it left the album. Links are only removed
on the basis of an authoritative complete listing, recorded as `exports.authoritative`: an
account's first export is authoritative automatically (it is the full one), and a later full
export can be marked with `gphoto-sync ingest --account NAME --full-albums --parts N
<part>...`, which must be given exactly the N parts of that export (the command refuses,
listing the missing part numbers, otherwise) and sets both `authoritative` and
`confirmed_parts`. A Takeout album folder
can be split
across parts, so a complete listing is the **union of the album folders of all parts of
that export**, and pruning is deferred until the export is *complete*. Part names carry no
total, and a contiguous `-001`…`-N` proves nothing about `-N+1`, so completeness needs the
**confirmed part count**: `exports.confirmed_parts`, set by
`gphoto-sync exports confirm --account NAME --export ID --parts N` (the number Google shows
on the Takeout "Manage your exports" page and in its completion e-mail) or by
`ingest --full-albums --parts N`. An export is complete when `confirmed_parts` is set,
every part `-001`…`-confirmed_parts` is `ingested`, and no further part of that export
exists. When all parts seen are ingested and no new part has appeared for
`export_complete_after` (default 7 days), the Telegram report says "export TS looks
complete with N parts; confirm with …" as a nudge; without the confirmation nothing is ever
pruned, which only delays the removal of stale album links, never loses data. If any part
is `partial` or `ingested_with_errors`, the export never qualifies. Every `album_items` row records the id of the export that last
confirmed it. When a complete listing from export E is applied, only memberships whose
last confirmation is from an export *older than E* (ordered by timestamp, then numeric
sequence) and that are absent from the union are removed; anything
confirmed by a later incremental export is untouched. Removing a
membership marks every occurrence that supports that (album, item) pair and has
an export id older than E as `active = 0`; memberships are derived from **active** occurrences only,
and re-attribution (3.2 step 5.9) only ever moves active occurrences, so a pruned link can
never be re-derived from history. A listing older than the newest
already-applied complete listing for that album is never used for pruning, and an
occurrence from an export older than that listing never creates a membership the
listing does not contain (a delayed part of an old export is recorded as history, not
re-linked into the album); for an album created after the last pruning, the account's
newest applied listing counts. When a sidecar change or a promotion renames a library
file, its album links are renamed with it (symlinks re-pointed). The `relink`
subcommand only rebuilds folders from the current DB state; it never re-applies an old
listing. `sync --dry-run` prints what a pending prune would remove.

### 3.4 Data model (SQLite, pure-Go driver, WAL mode)

```
accounts     (id, name UNIQUE, rclone_remote, created)
blobs        (id, sha256 UNIQUE, size, canonical_path, first_seen)   -- one row per distinct content, all accounts;
                                                                      -- canonical_path = an existing items.lib_path or
                                                                      -- item_versions.path, kept current by every journaled
                                                                      -- rename of that path (3.2 step 5.10)
items        (id, account_id, blob_id, google_id, variant {original,edited,motion}, identity_key,
              identity_src {google_id,fallback}, filename, ext, mime, width, height,
              taken_at, taken_src, lib_path UNIQUE, group_id, archived_export_id NULL,
              bytes_export_id NULL,                -- export that last supplied the bytes at lib_path
              first_seen, last_seen,
              UNIQUE(account_id, identity_key))   -- key = google_id#variant, else occ:export#folder#filename
                                                  -- lib_path, albums.path, link_path, canonical_path: all relative
                                                  -- to data_dir and prefixed with the account folder
sidecars     (item_id, json BLOB, description, favorited, lat, lon, alt, people JSON,
              export_id NULL, part_no NULL,       -- export and part that supplied this sidecar
              reconfirm)                          -- (a replaced part may correct it by exact match; one it
                                                  --  no longer carries keeps its metadata, loses provenance)
albums       (id, account_id, title, album_date, takeout_folder, kind {own,shared,partner},
              path UNIQUE, created, updated)      -- identity (account, title, album_date) or (account, takeout_folder)
occurrences  (id, account_id, export_id, part_no, folder, entry_name, variant, sha256,
              album_id NULL, item_id NULL, state {waiting,ingested,resolved}, tmp_path,
              active)                            -- one row per media entry ever seen; memberships
                                                 -- are derived from active rows; prune deactivates
fs_journal   (id, op {move,link,unlink,promote,supersede}, steps JSON, state {intent,done}, ts)
                                                 -- crash recovery for file operations, 3.2 step 5.11
album_items  (album_id, item_id, link_path, direct, confirmed_export_id, confirmed_by,
              via_group, group_still NULL,       -- two independent sources, row lives while either is set
              PRIMARY KEY(album_id, item_id))
item_versions (item_id, blob_id, path, superseded_at, by_export_id)   -- kept files of re-edited items
albums_full  (album_id, listing_export_id)        -- newest complete listing applied per album
exports_order: export ids compare by timestamp, then numeric sequence; never as strings
archives     (account_id, remote_id UNIQUE, name, export_id, part_no, size, md5, status
              {seen,fetching,ingesting,ingested,partial,ingested_with_errors,failed},
              local_path, remote_deleted_at, started, finished, entries, ingested,
              duplicates, errors)
archive_entries (archive_id, entry_name, status {done,skipped,failed,given_up}, attempts,
              last_error)                        -- resume inside a part, retry failed entries
exports      (account_id, export_id, highest_part, confirmed_parts, last_part_seen_at,
              complete_at, authoritative, albums_pruned_at)   -- completeness + pruning rights, see 3.3
pending_sidecars (account_id, export_id, part_no, folder, media_key, json,
              PRIMARY KEY(account_id, export_id, folder, media_key))   -- sidecars seen before their media;
                                                                      -- part_no: who stored it (dropped when that part is replaced)
runs         (account_id, started, finished, exit_code, counts JSON)
errors       (ts, account_id, ref, message)
telegram_chats (chat_id PRIMARY KEY, user_id, username, display_name, paired_at, revoked_at)
telegram_state (last_update_id)                   -- getUpdates offset, see 3.9
```

Indexes on `items.google_id`, `items(filename, taken_at)`, `blobs.sha256`, `archives.status`. Schema migrations are
embedded SQL files applied at start.

### 3.5 Configuration (`config.yaml`, every key overridable by `GPS_*` env vars)

```yaml
data_dir: /data            # <account>/library and <account>/albums live here (one filesystem)
state_dir: /state          # state.db, inbox/ staging, lock, run logs
timezone: ""               # empty = the container's TZ (decided); set only to override it
workers: 4                 # hashing / exif / linking parallelism
rclone:
  binary: /usr/local/bin/rclone
  config: /config/rclone/rclone.conf   # one file, one [section] per account; mounted read-write
  extra_args: ["--multi-thread-streams", "4", "--retries", "5", "--low-level-retries", "20"]
accounts:                  # key = e-mail local part = folder name under data_dir
  leo:
    rclone_remote: "gdrive-leo:Takeout"
  jane:                                # example for the second account
    rclone_remote: "gdrive-jane:Takeout"
    takeout: { settle: 4h }            # optional override of takeout.* for this account
layout:
  library: "{year}/{month}"          # under data_dir/<account>/library
  albums: "albums"                   # own and shared albums alike
  link_mode: hardlink                # hardlink | symlink | copy  (decided: hardlink)
  keep_sidecar_json: true            # decided: sidecar next to the file
  edited_versions: keep_both         # decided: keep original and -edited
  collision: identity_suffix         # decided: IMG_1234~3fa9c1d2.jpg, hash of the item identity
takeout:
  inbox_dir: /state/inbox
  pattern: "takeout-*.zip"
  settle: 2h                       # ignore parts modified more recently than this
  keep_archives: false             # delete local part after successful ingest
  delete_remote_after_ingest: false   # turn on after the first full export is verified
  expected_interval: 720h          # 30 days; overdue after this + grace
  grace: 336h
  include_trash: false             # Google's Trash/ folder: skipped by default
  include_archive: true            # Google's Archive/ folder: ingested, flagged archived
  max_entry_attempts: 3            # runs before a failing entry is given up
  export_complete_after: 168h      # no new part for 7 days → report asks to confirm the part count
telegram:
  enabled: true
  bot_token_file: /config/telegram.token   # from @BotFather; never in config.yaml or env
  notify_on: [success, partial, unresolved, failure, overdue]
  drain_updates_on_sync: true              # process /stop and advance the offset; pairing stays closed
log:
  level: info
  format: json
```

### 3.6 CLI and exit codes

```
gphoto-sync sync [--account NAME] [--dry-run]              # all accounts by default: discover, fetch, ingest, report, exit
gphoto-sync ingest --account NAME <dir|zip>... [--full-albums --parts N] [--replace]   # local Takeout dump, no rclone
                                                           # a part ingested before under the same name resumes; a zip
                                                           # known to differ (md5, else size) or --replace starts it over
                                                           # (refused while a copy of the part sits in the Drive staging
                                                           #  folder with its deletion unfinished: sync finishes it)
gphoto-sync exports confirm --account NAME --export ID --parts N   # record the part count Google reports
gphoto-sync retry --account NAME [--part NAME]             # reset given-up entries so the next run retries them
gphoto-sync check-remote [--account NAME]                  # list parts rclone can see and their state
gphoto-sync status [--account NAME]                        # counts, last export seen, overdue flag
gphoto-sync verify [--account NAME]                        # re-hash files, report missing/changed, check links
gphoto-sync relink [--account NAME]                        # rebuild album folders from the DB
gphoto-sync telegram pair [--people N] [--timeout 15m]     # prints N single-use codes, waits for "/start <code>"
gphoto-sync telegram chats [--revoke CHAT_ID]              # list / revoke paired chats
gphoto-sync telegram test                                  # send a test message to every paired chat
```

Exit codes: `0` success or nothing to do, `1` fatal (config, lock, DB, rclone missing),
`2` partial (failed entries that will be retried next run), `3` export overdue,
`4` unresolved (given-up entries waiting for `retry`). When several apply, the first in
the order 1, 4, 2, 3 wins and the report lists all of them. systemd records anything
non-zero as a failed unit; the Telegram report carries the detail.

### 3.7 Setup (user-side, documented in README)

1. **One rclone remote per account.** The permission grant is a browser step and happens
   **once per account, never on the server and never inside the container**. The resulting
   refresh token is not tied to a machine, so it is created wherever a browser is and then
   copied. Three ways, pick per person:
   - *On a laptop that has rclone* (what was done for `gdrive` on 2026-10-05):
     `rclone config create gdrive-jane drive scope=drive`. The browser opens; sign in as
     that person (a private window avoids picking the wrong Google account). The new
     `[gdrive-jane]` section with its token is written to the local `rclone.conf`.
   - *Grant on a laptop, create on the server*: run `rclone config` on the server, answer
     "no" to "use web browser to automatically authenticate"; it prints a ready-made
     `rclone authorize "drive" "<base64 options>"` line. Run that line on any machine with a
     browser and rclone, sign in as that person, and paste the token JSON it prints back
     into the server prompt (or use it non-interactively:
     `rclone config create gdrive-jane drive scope=drive token='<that JSON>'`).
   - *SSH port forward*: run `rclone config` on the server, answer "no" to "use web
     browser", it prints a `http://127.0.0.1:53682/...` URL; from the laptop
     `ssh -L 53682:127.0.0.1:53682 server`, open the URL locally, sign in.
   The server's `/home/leo/gphoto-sync/config/rclone/rclone.conf` ends up with one `[section]` per account
   (sections can be concatenated by hand; the existing `[gdrive]` can be renamed to
   `[gdrive-leo]` by editing the header). Owned by the service UID, mode 0600. Treat it as a
   password: it holds every account's refresh token. Verify with
   `rclone --config /home/leo/gphoto-sync/config/rclone/rclone.conf lsd gdrive-jane:`.
2. **One Takeout schedule per account**, created while signed in as that person at
   takeout.google.com (done for your account on 2026-10-05): Google Photos only, monthly for
   one year, incremental after the first export, destination that person's Drive, zip,
   50 GB parts. Each account's Drive needs room for its own full export. Renew after
   12 months; the overdue exit code (per account) is the reminder.
3. **Telegram bot**: create it with @BotFather, save the token to
   `/home/leo/gphoto-sync/config/telegram.token` (service UID, mode 0600), then run
   `docker run --rm -it <mounts> gphoto-sync:local telegram pair --people 2`.
   It prints one single-use code per person; each person opens a private chat with the bot
   and sends `/start <their code>`. `telegram test` confirms delivery.
4. **Host** (checked 2026-10-06): `home-nvr`, `ssh leo@10.19.76.5`, Ubuntu 22.04 x86-64,
   Docker 27.5 + Compose v2.23, user `leo` (uid/gid 1000, docker group, passwordless
   sudo), ext4 root with ~270 GB free: enough for the 182 GB library plus 50 GB staging
   of one account; the extra backup disk should arrive before the second account's full
   export. Layout:
   - `/home/leo/gphoto-sync` — git clone of this repo; the image is built here with
     `docker build -t gphoto-sync:local .`; `config/` holds `config.yaml`,
     `rclone/rclone.conf`, `telegram.token` and is excluded both by `.gitignore` and by
     `.dockerignore`, so credentials never enter the build context or an image layer.
   - `/home/leo/gphoto-backup/data` — all accounts' `library/` and `albums/`; since
     2026-10-09 a symlink to `/media/media_disk/gphoto-backup/data` on the backup HDD
     (ext4, so hardlinks and the same-filesystem `rename()` from `.tmp` keep working; the
     unit has `RequiresMountsFor=/media/media_disk`). `/home/leo/gphoto-backup/state` —
     DB and inbox staging, on the SSD (DB paths are `data_dir`-relative, so the library
     can move again without touching the DB); `/home/leo/gphoto-backup/downloads` —
     manually downloaded parts for `gphoto-sync ingest`.
5. **Install units** from 3.8, `systemctl enable --now gphoto-sync.timer`, and run the first
   pass by hand with `systemctl start gphoto-sync.service` while watching
   `journalctl -fu gphoto-sync` and the Telegram chat.
6. **After the first full export is ingested** and `verify` is clean, set
   `takeout.delete_remote_after_ingest: true` so later parts are trashed on Drive
   automatically (decided).

### 3.8 Docker image and systemd units

**Image**: multi-stage build, `golang:1.27-alpine` compiles a static binary (no cgo, SQLite
via `modernc.org/sqlite`); the final stage is `gcr.io/distroless/static-debian12:nonroot`
with `COPY --from=rclone/rclone:1.72 /usr/local/bin/rclone /usr/local/bin/rclone`. rclone is
also a static Go binary and distroless static ships CA certificates, so nothing else is
needed. Entrypoint `gphoto-sync`, default command `sync`. Built on the server itself
with `docker build -t gphoto-sync:local .` (amd64; no registry needed). The repo ships a
`.dockerignore` that excludes `config/`, `.git/`, and anything not needed for the build,
and the Dockerfile copies only `go.mod`, `go.sum` and the source directories, never the
whole tree; `go vet`,
`staticcheck`, `go test -race`, `govulncheck` run in a `make check` target and, later, CI.

**No daemon, no HTTP**: logs go to stdout/stderr as JSON; `docker run` without `-d` hands them
to systemd, which puts them in the journal. SIGTERM finishes the current entry and exits;
nothing is ever half-written (temp file + rename). Memory during a run is bounded: the zip
central directory of a 50 GB part is a few MB, media are streamed, rclone uses its own
transfer buffers. Between runs nothing is resident.

**Mounts** (`-v host:container`): `/home/leo/gphoto-backup/data:/data`, `/home/leo/gphoto-backup/state:/state`,
`/home/leo/gphoto-sync/config/rclone:/config/rclone` (read-write: rclone persists refreshed access tokens;
read-only would work but log an error every run), `/home/leo/gphoto-sync/config/config.yaml:/config/config.yaml:ro`,
`/home/leo/gphoto-sync/config/telegram.token:/config/telegram.token:ro`.
`--user 1000:1000` (leo, who owns `/home/leo/gphoto-backup`), so files are not root-owned.

**systemd**:

```ini
# /etc/systemd/system/gphoto-sync.service
[Unit]
Description=Google Photos Takeout sync (one-shot container)
Requires=docker.service
After=docker.service network-online.target
Wants=network-online.target

[Service]
Type=exec
TimeoutStopSec=180
ExecStartPre=-/usr/bin/docker rm -f gphoto-sync
ExecStart=/usr/bin/docker run --rm --name gphoto-sync \
    --user 1000:1000 \
    -e TZ=America/Chicago \
    -v /home/leo/gphoto-backup/data:/data \
    -v /home/leo/gphoto-backup/state:/state \
    -v /home/leo/gphoto-sync/config/rclone:/config/rclone \
    -v /home/leo/gphoto-sync/config/config.yaml:/config/config.yaml:ro \
    -v /home/leo/gphoto-sync/config/telegram.token:/config/telegram.token:ro \
    gphoto-sync:local sync
ExecStop=-/usr/bin/docker stop -t 120 gphoto-sync
```

```ini
# /etc/systemd/system/gphoto-sync.timer
[Unit]
Description=Weekly Google Photos Takeout sync

[Timer]
OnCalendar=Sun 03:00
Persistent=true
RandomizedDelaySec=30min

[Install]
WantedBy=timers.target
```

Properties that fall out of this: with `Type=exec` the unit is *active* for as long as
`docker run` is attached to the container, so the timer never starts a second instance
while the first full ingest is still running, and the run can take as long as it needs
(`Type=oneshot` was rejected: it stays "activating" during the run, and `systemctl stop`
then kills `ExecStart` without running `ExecStop`). A non-zero exit puts the unit in
`failed`, visible in `systemctl status`; the Telegram report is the primary alert. `Persistent=true` runs a missed week right
after boot. On `systemctl stop`, `ExecStop` gives the app 120 s to finish the current entry
and `TimeoutStopSec=180` keeps systemd from killing the attached client first; `docker run`
also proxies SIGTERM to the container by default, so both paths lead to a clean exit.
systemd runs `ExecStop` after a normal exit as well, when `--rm` has already removed the
container and `docker stop` would fail, so the command carries the `-` prefix that tells
systemd to ignore its exit status; a successful weekly run therefore stays successful. The
`ExecStartPre` removes a container left behind by a crash or a hard kill. Add
`ExecStartPre=/usr/bin/docker pull …` if the image should self-update. A
`docker compose run --rm gphoto-sync` variant works the same way.

### 3.9 Telegram notifications

One bot, created once with @BotFather, serves every account and every person. The bot
token is a secret (anyone holding it can read the chats) and is mounted as a file, never
written into `config.yaml` or passed as an environment variable visible in `systemctl show`.

**Pairing (part of initial setup).** `gphoto-sync telegram pair --people N` generates N
random single-use codes (8 characters from a 32-symbol alphabet), prints them in the
terminal, and long-polls `getUpdates` until every code is used, the timeout passes, or
Ctrl-C. Codes live only in memory for that session; nothing reusable is stored. Each
person opens a **private** chat with the bot and sends `/start <code>`: on a match the
chat id and user id are stored in `telegram_chats` and the bot replies "paired"; a wrong
or already-used code gets "invalid code" and is logged; `/stop` revokes the chat. Outside
a pairing session `/start` is answered with "pairing is closed, ask the admin to run
telegram pair". Group chats are not supported in v1 (a code posted in a group would be
visible to everyone); each person pairs their own private chat. The weekly `sync` run
drains `getUpdates` only to honour `/stop` and advance `last_update_id`, so no update is
processed twice and pairing never opens by itself.

**Reports.** After every run (`notify_on` filters which outcomes), one message per run goes
to every paired, non-revoked chat via `sendMessage` in HTML parse mode. Every dynamic
value (account names, filenames, error text) is passed through `html.EscapeString`, and
long reports are split on line boundaries into messages of at most 3500 characters so no
tag or entity is cut and Telegram's 4096-character limit is never hit. Content per
account: the parts seen, fetched, ingested, partial or failed; items added, duplicates
skipped, album links created; the newest export date and the overdue flag; exports that
look complete but await `exports confirm`; failed and given-up entries (first few, with the
part name); duration; the exit code. A send failure
is logged and does not change the exit code. `telegram test` sends a canned
message to confirm the wiring.

**Implementation**: plain `net/http` against `https://api.telegram.org/bot<token>/…`
(`getMe`, `getUpdates`, `sendMessage`); no library. Outbound HTTPS only, nothing listens.

### 3.10 Libraries

| Need | Choice | Why |
|---|---|---|
| rclone | `os/exec` wrapper: `lsjson`, `copyto`, `deletefile`, `--use-json-log`; parsed into structs; interface for tests | no library needed, rclone's CLI is the stable surface |
| SQLite | `modernc.org/sqlite` | pure Go, static binary, distroless image |
| Archives | stdlib `archive/zip` (local file, zip64) | stdlib |
| EXIF/HEIC/MP4 dates | `github.com/evanoberholster/imagemeta` (read-only) + a small MP4 `mvhd` reader | pure Go, fast, HEIC-aware |
| Config | `gopkg.in/yaml.v3` + env overlay | small |
| Logging | `log/slog` JSON | stdlib |
| CLI | stdlib `flag` with subcommands | minimal deps |
| Telegram | stdlib `net/http` + `encoding/json` against the Bot API | three endpoints, no library needed |

Originals are never modified (decided): no exiftool pass, no EXIF rewriting. The sidecar
and the file mtime carry the capture date for files whose own EXIF lacks it.

### 3.11 Testing

- Unit: sidecar matcher against a fixture corpus of real Takeout naming quirks; item
  identity (same bytes, different Google ids → two items, one blob; original vs `-edited`
  under one Google id → two items); late-sidecar promotion and merge across parts and
  across exports, including the ambiguous case that must not promote; canonical-path
  re-pointing on merge; re-exported items with changed sidecar or changed bytes
  (superseded file kept); membership derivation from occurrences and re-attribution;
  album matching incl. the rename exception, album path and link-name collisions; path/collision logic; capture-time resolution order; timezone bucketing; link
  modes; rclone JSON parsing; duplicate-name guard.
- Integration: generate a synthetic Takeout zip set with albums, duplicates, edited versions,
  Live Photo pairs, albums split across parts; run `ingest`; assert the tree and the DB.
  Re-run and assert zero changes (idempotency). Kill mid-part and re-run (resume). Inject
  crashes between each journaled file step and its commit, restart, and assert the tree
  and DB agree (journal replay).
- rclone behind an interface with a fake that serves parts from a local directory; one
  opt-in live test against the real remote (`check-remote`).
- Telegram against an `httptest` server: pairing with valid, wrong and reused codes,
  `/start` outside a session, `/stop`, update-offset persistence, HTML escaping, splitting on
  line boundaries.

## 4. Phases

| Phase | Deliverable | Needs from you |
|---|---|---|
| 0 | rclone remote and Drive space: **done 2026-10-05**; server checked and part 1 of the real export available for tests: **done 2026-10-06**. Remaining: recreate your Takeout schedule with destination "Add to Drive" (the first one went to e-mail links), then the same steps per additional account | Takeout fix, one browser login per extra account |
| 1 | `ingest` a local Takeout zip/dir → `library/`, `albums/`, SQLite, hardlinks, sidecars, idempotent re-runs, resume. Testable without rclone | one part of the real export to test against |
| 2 | rclone discovery/fetch loop, `sync`, exit codes, Telegram pairing + reports, Docker image with rclone, systemd units, README | bot token, pairing from each phone |
| 3 | `verify`, `relink`, `status`, `check-remote`, docs | — |

## 5. Risks

- **Takeout format drift**: Google has renamed sidecars and changed folder layouts several
  times. Mitigation: tolerant matcher, fixture corpus, never discard a media file just because
  its sidecar is missing (fall back to EXIF).
- **Incremental export semantics** are new and only partially documented: it is unclear whether
  an album membership change for an old item re-sends the album folder, or whether an export
  that fails mid-way resets the baseline. Mitigation: everything is idempotent and
  hash-deduplicated, so a surprise full export costs time, not correctness.
- **Drive quota, per account**: each export lands in that person's Drive. Yours has 2.7 TiB
  free; the others must be checked (`rclone about gdrive-jane:`). Enabling
  `delete_remote_after_ingest` after the first verified ingest keeps usage flat.
- **Yearly renewal**: the schedule stops after 12 months. Exit code 3 is the reminder.
- **Large part transfer restarts**: rclone restarts a 50 GB file that fails mid-copy.
  Mitigation: retries and multi-thread streams; at one export a month this is tolerable.
- **First full ingest duration**: a few hundred GB may take longer than a week on a slow
  link; the timer simply does not start a second instance until the first exits.
- **Hardlinks and downstream backups**: tools that do not preserve hardlinks will multiply
  storage on the backup target.
- **rclone.conf and the Telegram token are credentials**: full Drive access and the ability
  to read the bot's chats respectively. Both are files with mode 0600, owned by the service
  user, mounted read-only where possible, never baked into the image or put in the unit
  file's environment.
- **Telegram pairing**: single-use codes shown only in the admin's terminal, private chats
  only, pairing closed outside an explicit `telegram pair` session. Anyone with the bot
  token can still read reports, hence the file permissions above.

## 6. Out of scope

Other acquisition paths (Picker API, Ambient API, browser automation; evaluated and rejected,
section 1), a merged "family" view across accounts (the same album title in two accounts stays
two folders), uploading to Google Photos, a web gallery UI, deleting local files to mirror
deletions, a resident daemon mode, rewriting EXIF inside originals, other alert channels
than Telegram.

## 7. Decisions (all taken 2026-10-05)

| Topic | Decision |
|---|---|
| Acquisition path | Takeout → Drive → rclone → gphoto-sync, nothing else |
| Export | Monthly, incremental after the first full export, zip, 50 GB parts, delivered to each person's Drive |
| Run cadence | Weekly one-shot container from a systemd timer; no daemon |
| Accounts | Several; folder and config key = e-mail local part (`leo`, …); one rclone remote and one Takeout schedule each |
| Album links | Hardlinks; `data/` on one local filesystem |
| Cross-account dedup | On, always: identical bytes anywhere share one blob (inode); each item keeps its own path and metadata |
| Collisions | Deterministic suffix from the item identity: `IMG_1234~3fa9c1d2.jpg` |
| Edited versions | Keep both original and `-edited` |
| Sidecars | Takeout JSON kept next to each file |
| Originals | Never modified; no EXIF rewriting |
| Timezone for `YYYY/MM` | The container's `TZ` |
| Shared albums | Laid out like own albums under `albums/<title>`; the DB records they were shared |
| Deletions | Never deleted locally (Takeout cannot report them) |
| Alerts | Telegram bot; report after every run including successes with stats; pairing via single-use codes from `telegram pair`, private chats only |
| Drive cleanup | Auto-trash ingested parts, switched on after the first full export is verified |
| Host | x86-64 Linux with Docker Engine; amd64 image |
| Drive storage | 2.7 TiB free on your account; check the other accounts with `rclone about` |

## Sources

- Google, "Updates to the Google Photos APIs" (why the API path is closed): https://developers.google.com/photos/support/updates
- Google, Takeout help (delivery, sizes, scheduling, Drive quota): https://support.google.com/accounts/answer/3024190
- 9to5Google, incremental scheduled Photos exports (June 2026): https://9to5google.com/2026/06/01/google-photos-schedule-export/
- immich-go (Go, Takeout sidecar parsing reference): https://github.com/simulot/immich-go
- Takeout sidecar quirks: https://metadatafixer.com/learn/google-takeout-json-files-explained , https://github.com/mattwilson1024/google-photos-exif
- rclone Drive backend, `lsjson`, remote setup: https://rclone.org/drive/ , https://rclone.org/commands/rclone_lsjson/ , https://rclone.org/remote_setup/
- systemd timers: https://www.freedesktop.org/software/systemd/man/latest/systemd.timer.html
- Go releases: https://go.dev/doc/devel/release
