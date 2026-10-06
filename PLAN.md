# gphoto-sync — Plan

A 24x7 service, written in Go and shipped as a Docker image, that mirrors the photos and
videos of one Google account into a local folder tree on Linux:

```
<data>/
  library/YYYY/MM/<original filename>        every item, once
  albums/<album title>/<filename>            own albums   (links to library/)
  albums/share/<album title>/<filename>      shared albums (links to library/)
```

Status: **research + plan only, nothing implemented.** Section 7 lists the decisions that
need your answer before coding starts.

---

## 1. The constraint that shapes everything

The obvious design — call the Google Photos Library API every hour, list the library and
albums, download what is new — **has not been possible since 31 March 2025.** Google removed
the `photoslibrary.readonly`, `photoslibrary.sharing` and `photoslibrary` scopes. The
remaining Library API methods (`mediaItems.list/search/get`, `albums.list/get`) only return
content that *our own app uploaded*, and every `sharedAlbums.*` method was deleted outright.
Calls with the old scopes return `403 PERMISSION_DENIED`. This is why rclone is removing its
Google Photos backend and why `gphotos-sync` (Python) stopped working.

Even before the change, the Library API never delivered originals: image downloads kept EXIF
but stripped GPS, and video downloads were transcoded. So "preserve metadata" was never fully
achievable through that API either.

What is left, as of October 2026:

| Path | Lists whole library? | Albums | Shared albums (others' content) | Originals (bytes, GPS, original video) | Automatable 24x7 | Freshness | Risk |
|---|---|---|---|---|---|---|---|
| **A. Library API** | No (app-created only) | No | No | No | Yes | — | Dead end |
| **B. Picker API** | No, user picks items in a Google UI, max 2000 per session | Only by searching album title in the picker | Yes, if the user picks them | No (EXIF minus GPS; video transcoded) | No, needs a human per session | On demand | Official |
| **C. Ambient API** | No, user-curated "media sources" for smart displays | Partial | ? | Unknown, not designed for it | Technically | — | 240 requests/device/day, TV/limited-input OAuth only, meant for slideshows. Dead end for backup |
| **D. Google Takeout → Google Drive → Drive API** | **Yes** | **Yes** (album folders + metadata.json) | **No**: only your own contributions and items you "saved to library" | **Yes** (originals, JSON sidecar with GPS, description, people, favorites) | **Yes**, once the user schedules exports | Every 2 months (schedule lasts 1 year, must be renewed by hand) | Official. Uses Drive storage quota |
| **E. Browser automation** (Chrome DevTools Protocol, "download original" from photos.google.com) | Yes | Yes (best-effort) | Yes (best-effort, `/share/` album URLs) | Yes (same bytes as the web "Download original") | Yes, with a persisted logged-in Chrome profile | Hourly or better | Unofficial. Fragile against UI changes, account may be challenged (2FA, "unusual activity"), arguably against ToS |
| **F. Reverse-engineered mobile app API** | Yes | Yes | Yes | Yes | Yes | Hourly | Unofficial, protobuf, breaks silently, ToS violation. Not considered further |

Key facts behind the table (sources at the end):

- **Picker API**: `sessions.create` → user opens `pickerUri` → app polls → `mediaItems.list`.
  `pickingConfig.maxItemCount` defaults to and is capped at 2000. Returned `mediaFile` has
  `filename`, `mimeType`, `baseUrl`, width/height, camera make/model, exposure data. Base URLs
  live 60 minutes. `=d` gives the image with EXIF *except location*; `=dv` is a transcoded
  video. The picker UI does not show an album list; the user has to search by album title.
- **Ambient API** (new): OAuth type "TVs and Limited Input devices", `CreateDevice`, the user
  chooses media sources in the Photos app settings, then `mediaItems.list` returns a curated
  stream, 100 items per page, 240 requests per device per day. Built for photo frames.
- **Takeout, incremental scheduled export** (new since ~1 June 2026): if Google Photos is the
  *only* product selected, Takeout can run every 2 months for 12 months. The first export is
  full; later ones contain only items "uploaded, backed up, created, or edited since the last
  successful backup". Archives are zip or tgz, up to 50 GB each, delivered to Drive, Dropbox,
  OneDrive, Box, or an e-mail link (links expire after ~7 days, 5 downloads). Drive delivery
  creates a `Takeout` folder and **counts against Drive storage**. Not available to accounts
  in the Advanced Protection Program. Deletions are not reported.
- **Takeout archive layout**: `Takeout/Google Photos/Photos from YYYY/…` for every year, plus
  one folder per album named after the album title (`Untitled(1)` for untitled ones) that
  contains a `metadata.json` (`title`, `description`, `access`, `date`, `geoData`,
  `enrichments`, `sharedAlbumComments`) and *copies* of the member files. Every media file has
  a JSON sidecar (`IMG_1.jpg.supplemental-metadata.json`, older exports `IMG_1.jpg.json`) with
  `title` (the original filename), `description`, `photoTakenTime`, `creationTime`, `geoData`,
  `geoDataExif`, `people`, `favorited`, `url` (`https://photos.google.com/photo/<id>`),
  `googlePhotosOrigin`. Well-known quirks that any ingester must handle: sidecar names are
  truncated to ~46 chars; duplicate names become `IMG_1(1).jpg` with sidecar `IMG_1.jpg(1).json`;
  edited versions are `IMG_1-edited.jpg` sharing the original's sidecar; on-disk names can be
  truncated or de-duplicated while `title` holds the real name; an album folder and the
  matching media may land in different archive parts; Live Photos/motion photos arrive as
  HEIC+MOV pairs or `.MP.jpg`.
- **Shared albums in Takeout**: content other people added to a shared album is *not* in your
  export (it belongs to their account). Only your own contributions, and items you explicitly
  "saved" to your library, appear. The album folder itself does appear with `metadata.json`.
- **Browser automation**: `perkeep/gphotos-cdp` (Go, chromedp) is unmaintained since 2024, but
  two forks (`msfjarvis/gphotos-cdp`, `latetedemelon/gphotos-cdp`) are being pushed to as of
  October 2026 and already support: headless Chromium in a container (`--no-sandbox`,
  `--disable-dev-shm-usage`), persisted login profile, incremental resume via a manifest,
  per-item retry, `-album <id>` and `-album-type share`, reading the capture date from the info
  panel, `YYYY/MM` foldering, mtime stamping. That code is MIT/BSD-style and can be embedded or
  shelled out to.
- **OAuth for Drive**: an OAuth client in "Testing" status gets refresh tokens that expire
  after 7 days, which would kill a 24x7 daemon weekly. The consent screen must be published
  "In production". `drive.readonly` is a *restricted* scope: unverified apps get a scary
  "unverified app" interstitial and a 100-user cap, which is fine for personal use.

## 2. Recommendation

Build the service around a **pluggable `Source` interface** and ship two sources in order:

1. **Takeout-over-Drive (phase 1-2, the backbone).** Official, gives true originals and the
   richest metadata (GPS, descriptions, people, favorites, album membership). The daemon polls
   the Drive `Takeout` folder on the configured interval (hourly by default), downloads new
   archives, stream-extracts them, de-duplicates by content hash, lays out `library/` and
   `albums/`, and records everything in SQLite. The user's only manual duties are to schedule
   the export once and renew it yearly; the daemon alerts when an export is overdue.
2. **Browser automation (phase 4, opt-in).** Closes the two gaps Takeout leaves: hourly
   freshness and other people's content in shared albums. Runs headless Chromium in a sidecar
   container with a persisted profile. Everything it fetches goes through the *same* ingest
   pipeline and is matched to Takeout data by the Google photo id (the `url` in the sidecar
   and the item URL the browser tool records), so a photo that arrives via the browser today and
   via Takeout in two months is stored once.

The Picker API can be added as a small third source ("Add from Google Photos" page served by
the daemon) for ad-hoc manual pulls, but it cannot be automated and delivers lower fidelity, so
it is not on the critical path.

What this means against the original wishlist:

| Wish | Outcome |
|---|---|
| Photos + videos, whole library | Yes (Takeout) |
| Own albums | Yes (Takeout album folders) |
| Albums shared by others, including their photos | Only with the browser source, or if you "save" their items to your library first. Takeout alone exports just the album shell and your own items |
| Year/month layout, albums/, albums/share/ | Yes |
| Original filenames | Yes: Takeout `title` is the original name even when the on-disk name was truncated or suffixed |
| Metadata preserved, location if possible | Yes: Takeout files are originals (EXIF/GPS intact), plus the JSON sidecar is kept. Browser downloads are also originals. Picker downloads lose GPS |
| SQLite state | Yes |
| Parallel downloads | Yes for hashing/linking workers and for multi-part archives; archive download itself is bandwidth-bound, Drive allows it |
| 24x7, hourly check | Yes, but "new data" only *appears* every 2 months via Takeout; hourly freshness needs the browser source |
| Download once, link into albums | Yes: hardlinks by default (see 3.4) |
| Go, Docker | Go 1.27 (released 2026-08-19), distroless image |

## 3. Design

### 3.1 Components

```
┌──────────────────────── gphoto-sync (one binary) ────────────────────────┐
│ scheduler ──► Source.Poll() ──► Fetcher ──► Ingester ──► Layout+Linker   │
│                 │                  │            │              │          │
│   takeout-drive │   drive download │   archive  │   sidecar    │ hardlink │
│   cdp-browser   │   browser files  │   walker   │   matcher    │ symlink  │
│   picker        │   baseUrl GET    │            │   exif probe │ copy     │
│                                                  ▼                        │
│                                         SQLite (state.db)                 │
│ http: /healthz /metrics /picker (optional)                                │
└───────────────────────────────────────────────────────────────────────────┘
```

- **Scheduler**: one loop per source with its own interval, jitter, backoff, and a global
  single-instance lock on the state dir. Runs a full pass on start, then on the interval.
- **Source** (`Poll(ctx) ([]Candidate, error)`, `Fetch(ctx, Candidate) (io.ReadCloser, …)`):
  `takeout-drive`, later `cdp-browser` and `picker`.
- **Ingester**: the only writer to `<data>` and the DB. Takes (bytes, sidecar?, origin
  hints) and is idempotent, so any source can be re-run safely.
- **Layout + Linker**: computes target paths, resolves collisions, creates album links.
- **HTTP**: health, Prometheus metrics, and an optional Picker page.

### 3.2 Takeout-over-Drive source, step by step

1. **Discover**: `files.list` in the Drive folder named `Takeout` (configurable) for files
   matching `takeout-*.zip|.tgz` not yet in the `archives` table. Later: switch to
   `changes.list` with a stored page token to make the hourly poll one cheap call.
2. **Settle**: an export is written as parts `-001`, `-002`, …; parts of one export share the
   timestamp in the name. Wait until no new part with that timestamp has appeared for
   `settle_minutes` (default 30) before ingesting the set. Parts are still ingested
   independently and idempotently, because album folders and their media can be split
   across parts.
3. **Download**: `files.get?alt=media` with `Range` resume, verified against Drive's
   `md5Checksum` and `size`. Two modes:
   - `stream` (default for tgz): decompress straight from the HTTP body, never storing the
     archive. Restart-safe because already-ingested entries are skipped by hash/path.
   - `download` (required for zip, which needs random access): save to `<state>/staging`,
     then extract. Needs up to 50 GB free in staging. The plan recommends choosing **tgz** in
     Takeout.
4. **Walk entries** under `Takeout/Google Photos/`. Classify each entry:
   - `*/metadata.json` → album descriptor (title, access, dates). `Photos from YYYY` folders
     are year buckets, not albums.
   - `*.json` next to media → sidecar. Parse now, stash in `sidecars` table keyed by
     (export, folder, normalized media name) until its media file is seen, and vice versa.
   - media → go to step 5.
   - anything else (`archive_browser.html`, `print-subscriptions.json`, `user-generated-memory-titles.json`, …) → ignored but logged once.
5. **Ingest a media entry**:
   1. Stream to `<data>/.tmp/<uuid>`, computing SHA-256 while writing.
   2. If the hash is already in `media` → this is a duplicate (an album copy of a library
      item, or a re-export). Discard the temp file; only update album membership.
   3. Match the sidecar using the known naming variants (exact, `supplemental-metadata`,
      truncated, `(n)` moved after the extension, `-edited` sharing the original's).
   4. Capture time, first hit wins: sidecar `photoTakenTime` → EXIF `DateTimeOriginal` →
      QuickTime/MP4 creation time → `Photos from YYYY` folder year (month unknown → `YYYY/00`).
      Convert to the configured timezone for the `YYYY/MM` bucket.
   5. Filename: sidecar `title` (the real original name) sanitized for the filesystem,
      falling back to the on-disk name.
   6. Target `library/YYYY/MM/<name>`. On a name collision with *different* content, append a
      short content-hash suffix before the extension (`IMG_1234~3fa9c1d2.jpg`) so re-runs
      are deterministic. Edited versions keep their `-edited` suffix next to the original.
   7. `rename()` into place (same filesystem, atomic), set mtime to the capture time, write
      the sidecar next to it as `<name>.json` (configurable), insert into `media`.
   8. Companion files (HEIC+MOV Live Photos, RAW+JPG, `.MP`) share the base name and land in
      the same folder; the DB records the group.
6. **Albums**: for each album folder create `albums/<title>/` (or `albums/share/<title>/`
   when `metadata.json` marks it shared, see 7.3) and link each member from `library/`.
7. **Finish**: mark the archive ingested with counts; optionally delete it from Drive
   (`drive.delete_after_ingest`, needs the full `drive` scope instead of `drive.readonly`).
8. **Overdue alert**: if no export has been seen for `expected_interval + grace`
   (default 2 months + 2 weeks), log at error level and set a metric/health flag, because the
   yearly schedule probably lapsed.

Deletions and un-favoriting are **not** visible through Takeout. The local tree is a backup,
not a mirror: nothing is ever deleted locally. A `prune` subcommand can be added later for the
browser source, which *can* observe deletions.

### 3.3 Browser-automation source (phase 4, opt-in)

- Chromium runs in a sidecar container (`chromium` image with `--remote-debugging-port`), the
  profile directory is a volume. One interactive login (via noVNC/`xvfb` or by copying a
  profile) is needed; after that it runs headless.
- Go code drives it with `chromedp`, following the proven `gphotos-cdp` approach: walk the
  timeline from the last known item, open each item, press `Shift+D` ("Download original"),
  capture the file from the browser's download dir, read the capture date from the info
  panel, record the photo URL id. Album pass: iterate `/albums`, then each `/album/<id>`;
  shared: `/sharing` → `/share/<id>`.
- Output goes through the same Ingester with `origin = cdp`, `google_id = <id from URL>`.
  Takeout sidecars carry the same id in `url`, so later Takeout data upgrades the record
  (adds GPS/description sidecar) without a second copy.
- Rate limiting (one item every few seconds, nightly full album re-scan at most), randomized
  pauses, and a kill switch when Google shows a challenge page. Documented clearly as
  unofficial.

### 3.4 Album links: download once, link many

Yes, this works, and it is the default. Each item is stored once under `library/` and
appears in album folders as a link:

| Mode | Extra space | Survives moving the original | Visible as "a file" to every tool | Caveat |
|---|---|---|---|---|
| `hardlink` (default) | none | yes | yes | Must be the same filesystem, so `library/` and `albums/` live under one mounted volume. Not supported on some SMB/NFS mounts. `rsync` needs `-H`, otherwise backups de-duplicate into copies |
| `symlink` (relative) | ~0 | no (dangling if original moves) | some tools skip symlinks | Works across filesystems |
| `copy` | full | yes | yes | Fallback when neither link works |

Linker behaviour: `relink` subcommand rebuilds every album folder from the DB; links for
items removed from an album are removed (album membership is the one thing we can mirror
accurately because Takeout re-sends the album folder).

### 3.5 Data model (SQLite, pure-Go driver, WAL mode)

```
media        (id, sha256 UNIQUE, google_id, filename, ext, mime, size, width, height,
              taken_at, taken_src, tz, lib_path UNIQUE, origin, group_id, first_seen, last_seen)
sidecars     (media_id, json BLOB, description, favorited, lat, lon, alt, people JSON)
albums       (id, google_id, title, kind {own,shared,partner}, path, created, updated)
album_items  (album_id, media_id, link_path, PRIMARY KEY(album_id, media_id))
archives     (drive_file_id, name, export_ts, part_no, size, md5, status, started, finished,
              entries, ingested, duplicates, errors)
pending      (export_ts, folder, media_key, sidecar_json | media_tmp_path)   -- unmatched halves
runs         (source, started, finished, result, counts JSON)
errors       (ts, source, ref, message)
```

Indexes on `google_id`, `(filename, taken_at)`, `archives.status`. Schema migrations are
embedded SQL files applied at start.

### 3.6 Configuration (`config.yaml`, every key overridable by `GPS_*` env vars)

```yaml
data_dir: /data            # library/ and albums/ live here (one filesystem)
state_dir: /state          # state.db, oauth token, staging
timezone: Europe/Berlin
interval: 1h               # poll interval for all sources
workers: 4                 # hashing / exif / linking parallelism
layout:
  library: "{year}/{month}"          # under data_dir/library
  albums: "albums"
  shared_albums: "albums/share"
  link_mode: hardlink                # hardlink | symlink | copy
  keep_sidecar_json: true
  edited_versions: keep_both         # keep_both | prefer_edited | prefer_original
  collision: hash_suffix             # hash_suffix | numbered
sources:
  takeout_drive:
    enabled: true
    folder_name: Takeout
    archive_mode: stream             # stream | download
    settle_minutes: 30
    delete_after_ingest: false
    expected_interval: 1440h         # 60 days; overdue alert after this + grace
    grace: 336h
  cdp:
    enabled: false
    chrome_url: http://chromium:9222
    profile_dir: /state/chrome-profile
    albums: true
    shared_albums: true
    min_item_delay: 3s
http:
  listen: ":8080"                    # /healthz, /metrics, /picker (if enabled)
log:
  level: info
  format: json
```

### 3.7 CLI

```
gphoto-sync auth            # OAuth loopback flow for Drive; stores token in state_dir
gphoto-sync run             # daemon: scheduler + http
gphoto-sync sync [--source] # one pass, then exit (good for cron / testing)
gphoto-sync ingest <dir|archive>   # ingest a local Takeout dump; no Google access needed
gphoto-sync status          # counts, last export seen, overdue flag
gphoto-sync verify          # re-hash files, report missing/changed, check links
gphoto-sync relink          # rebuild album folders from the DB
```

### 3.8 Google Cloud / OAuth setup (user-side, documented in README)

1. Create a project, enable **Google Drive API** (and Photos Picker API if phase 5).
2. OAuth consent screen: type External, publishing status **In production** (otherwise
   refresh tokens die after 7 days). Scopes: `drive.readonly` (or `drive` if archives
   should be deleted after ingest). Accept the "unverified app" interstitial.
3. OAuth client type "Desktop app"; put the client JSON in `<state>/client_secret.json`.
4. `gphoto-sync auth` prints a URL; it listens on a loopback port for the redirect. On a
   headless server, run it once on a laptop and copy `<state>/token.json`, or use an SSH port
   forward.
5. In Takeout: deselect all, select only Google Photos, choose "every 2 months for 1 year",
   destination "Add to Drive", format tgz, size 50 GB. Note the Drive quota requirement.

### 3.9 Docker

- Multi-stage build: `golang:1.27-alpine` → `gcr.io/distroless/static-debian12:nonroot`.
  Static binary, no cgo (SQLite via `modernc.org/sqlite`), ~25 MB image.
- Volumes: `/data` (one filesystem for hardlinks), `/state` (DB, tokens, staging), config via
  `/config/config.yaml` or env. Runs as a configurable UID/GID (`PUID/PGID` style) so files
  on the host are owned by the user, not root.
- `HEALTHCHECK` hits `/healthz`. Graceful shutdown on SIGTERM finishes the current entry,
  leaves nothing half-written (temp + rename).
- `docker-compose.yml` with the daemon and an optional `chromium` sidecar for phase 4.
- CI: `go vet`, `staticcheck`, `go test -race`, `govulncheck`, multi-arch image
  (amd64/arm64) via GitHub Actions.

### 3.10 Libraries

| Need | Choice | Why |
|---|---|---|
| OAuth2 | `golang.org/x/oauth2` + `golang.org/x/oauth2/google` | standard |
| Drive | `google.golang.org/api/drive/v3` | official, supports `alt=media` + Range via `http.Client` |
| SQLite | `modernc.org/sqlite` | pure Go, static binary, distroless image |
| Archives | stdlib `archive/zip`, `archive/tar`, `compress/gzip` (+ `klauspost/pgzip` for multi-core gunzip) | stdlib |
| EXIF/HEIC/MP4 dates | `github.com/evanoberholster/imagemeta` (read-only) + a small MP4 `mvhd` reader | pure Go, fast, HEIC-aware |
| Browser (phase 4) | `github.com/chromedp/chromedp` | proven by gphotos-cdp |
| Config | `gopkg.in/yaml.v3` + env overlay | small |
| Logging/metrics | `log/slog`, `prometheus/client_golang` | stdlib / standard |
| CLI | stdlib `flag` with subcommands (or `urfave/cli` if it grows) | minimal deps |

Optional: an `exiftool` pass (separate, larger image variant) to write capture time and GPS
from the sidecar *into* files that lack them (screenshots, some videos). Off by default to keep
originals byte-identical.

### 3.11 Testing

- Unit: sidecar matcher against a fixture corpus of real Takeout naming quirks; path/collision
  logic; capture-time resolution order; timezone bucketing; link modes.
- Integration: generate a synthetic Takeout tgz/zip with albums, duplicates, edited versions,
  Live Photo pairs, split-across-parts albums; run `ingest`; assert the tree and the DB.
  Re-run and assert zero changes (idempotency).
- Drive client behind an interface with an in-memory fake; one opt-in live test with real
  credentials.
- Phase 4: recorded CDP sessions for the matcher; manual acceptance runs.

## 4. Phases

| Phase | Deliverable | Needs from you |
|---|---|---|
| 0 | Decisions in section 7; Google Cloud project; Takeout schedule set up; Drive space checked | answers, ~30 min of console clicking |
| 1 | `ingest` a local Takeout dump → `library/`, `albums/`, SQLite, hardlinks, sidecars, idempotent re-runs. Testable without any Google credentials | a real Takeout export to test against |
| 2 | Drive polling, resumable/streaming download, scheduler, `run` daemon, `/healthz`, Docker image + compose | OAuth client JSON |
| 3 | `verify`, `relink`, `status`, metrics, overdue alerting, docs | — |
| 4 (opt-in) | Browser-automation source + chromium sidecar for hourly freshness and shared albums | one interactive login |
| 5 (opt-in) | Picker page for manual pulls | enable Picker API |

## 5. Risks

- **Takeout format drift**: Google has renamed sidecars and changed folder layouts several
  times. Mitigation: tolerant matcher, fixture corpus, never discard a media file just because
  its sidecar is missing (fall back to EXIF).
- **Incremental export semantics** are new and only partially documented: it is unclear whether
  an album membership change for an old item re-sends the album folder, or whether an export
  that fails mid-way resets the baseline. Mitigation: everything is idempotent and
  hash-deduplicated, so a surprise full export costs time, not correctness.
- **Drive quota**: a full export of a large library can be hundreds of GB; the Drive plan must
  hold at least one full export plus one increment, or the export silently fails.
- **Yearly renewal**: the schedule stops after 12 months. The overdue alert exists for this.
- **Browser source**: UI changes break selectors; Google may require re-login or flag the
  account. Kept optional and isolated.
- **Hardlinks and downstream backups**: tools that do not preserve hardlinks will multiply
  storage on the backup target.

## 6. Out of scope for now

Multiple Google accounts (schema will not prevent it, but phase 1-3 are single-account),
uploading to Google Photos, a web gallery UI, deleting local files to mirror deletions.

## 7. Open questions — please answer before implementation

1. **Acquisition path.** Takeout-over-Drive only (official, originals, new data every ~2
   months), or also the browser-automation source for hourly freshness and shared-album
   content? The plan assumes Takeout first, browser as an opt-in later phase.
2. **Drive storage.** Roughly how big is the library, and how much Drive space does the
   account have? If Drive is too small: Dropbox/OneDrive/Box delivery is possible but means a
   different API client, or e-mail links which cannot be automated.
3. **Shared albums.** Is it acceptable that, without the browser source, `albums/share/` holds
   only the album shells with your own items plus whatever you "saved" to your library? And do
   you want partner-sharing content treated as shared, or merged into `library/` only?
4. **Deletions.** Confirm backup semantics: never delete locally, even if the item disappears
   from Google Photos.
5. **Edited versions.** Keep both original and `-edited` (default), or prefer one?
6. **Collisions.** Hash suffix (`IMG_1234~3fa9c1d2.jpg`, deterministic) vs. numbered
   (`IMG_1234 (2).jpg`)?
7. **Links.** Hardlinks by default, with `library/` and `albums/` on one volume. Is the target
   filesystem local (ext4/btrfs/ZFS/XFS) rather than an SMB/NFS mount? Any downstream backup
   tool that needs to be hardlink-aware?
8. **Sidecars and EXIF writing.** Keep the `.json` sidecar next to each file (default yes)?
   Should an optional `exiftool` pass embed date/GPS into files that lack them (modifies
   bytes), or leave originals untouched?
9. **Timezone** for the `YYYY/MM` bucket: the container's configured zone (default), or UTC?
10. **Archive format**: tgz (streaming ingest, little staging space) is recommended over zip
    (needs up to 50 GB staging). OK?
11. **Host**: which Linux box/NAS, CPU arch (amd64/arm64), and is Docker Compose the target?

## Sources

- Google, "Updates to the Google Photos APIs": https://developers.google.com/photos/support/updates
- Google, Photos API release notes: https://developers.google.com/photos/support/release-notes
- Google, base URLs (`=d`, `=dv`, 60-minute validity): https://developers.google.com/photos/library/guides/access-media-items
- Google, Picker API sessions / mediaItems: https://developers.google.com/photos/picker/reference/rest/v1/sessions , https://developers.google.com/photos/picker/reference/rest/v1/mediaItems , https://developers.google.com/photos/picker/guides/picking-experience
- Google, Ambient API: https://developers.google.com/photos/ambient/guides/get-started , https://developers.google.com/photos/ambient/guides/configure-your-app , https://developers.google.com/photos/ambient/reference/rest/v1/mediaItems/list
- Google, API limits and quotas: https://developers.google.com/photos/overview/api-limits-quotas
- Google, Takeout help (delivery, sizes, scheduling, Drive quota): https://support.google.com/accounts/answer/3024190
- 9to5Google, incremental scheduled Photos exports (June 2026): https://9to5google.com/2026/06/01/google-photos-schedule-export/
- rclone, Google Photos backend removal: https://github.com/rclone/rclone/issues/8434 ; Ambient API discussion: https://forum.rclone.org/t/google-photos-ambient-api/53876
- gphotos-cdp and active forks: https://github.com/perkeep/gphotos-cdp , https://github.com/msfjarvis/gphotos-cdp , https://github.com/latetedemelon/gphotos-cdp
- immich-go (Go, Takeout sidecar parsing reference): https://github.com/simulot/immich-go
- Takeout sidecar quirks: https://metadatafixer.com/learn/google-takeout-json-files-explained , https://github.com/mattwilson1024/google-photos-exif
- OAuth 7-day expiry in Testing status: https://dev.to/ko-hi/googles-oauth-testing-mode-expires-refresh-tokens-in-7-days-publish-the-consent-screen-before-24hm
- Go releases: https://go.dev/doc/devel/release
