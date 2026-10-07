# gphoto-sync

Mirrors the photos and videos of one or more Google accounts into a local folder tree on
Linux, using Google Takeout scheduled exports delivered to Google Drive, fetched with rclone
and ingested by this Go program. Runs as a weekly one-shot Docker container from a systemd
timer. Design and decisions: [PLAN.md](PLAN.md).

```
<data>/
  <account>/
    library/YYYY/MM/<original filename>        every item once, Takeout sidecar next to it as <name>.json
    albums/<album title>/<filename>            own and shared albums, hardlinks into library/
```

## One-time setup

1. **rclone remote per account** (on a machine with a browser, never on the server):
   `rclone config create gdrive-<name> drive scope=drive`, then copy the resulting section(s)
   into `config/rclone/rclone.conf` on the server (mode 0600). See PLAN.md 3.7 for the
   headless variants.
2. **Takeout schedule per account** at takeout.google.com while signed in as that person:
   Google Photos only, monthly for one year, incremental after the first export,
   destination **Add to Drive**, zip, 50 GB parts.
3. **Server layout** (see `deploy/systemd/gphoto-sync.service` for the mounts):
   - `/home/leo/gphoto-sync` — this repo; `config/` is git- and docker-ignored and holds
     `config.yaml` (start from `config/config.example.yaml`), `rclone/rclone.conf`,
     `telegram.token`.
   - `/home/leo/gphoto-backup/data` — the library; `/home/leo/gphoto-backup/state` — SQLite
     database and inbox staging (needs ~60 GB free); `/home/leo/gphoto-backup/downloads` —
     manually downloaded parts for `ingest`.
4. **Build and install**:
   ```sh
   cd /home/leo/gphoto-sync && DOCKER_BUILDKIT=0 docker build -t gphoto-sync:local .
   sudo deploy/install-units.sh
   ```
5. **Telegram** (optional): create a bot with @BotFather, write its token to
   `config/telegram.token` (0600), set `telegram.enabled: true`, then
   `docker run --rm -it <mounts> gphoto-sync:local telegram pair --people 2` and have each
   person send `/start <code>` to the bot in a private chat. `telegram test` confirms delivery.

## Commands

```
sync            [--account NAME] [--dry-run]        weekly run: discover, fetch, ingest, report, exit
ingest          --account NAME <part.zip|dir>...     ingest local parts, no rclone involved
check-remote    [--account NAME]                    what rclone sees on Drive and its state
status | verify [--hash] | relink
retry           --account NAME [--part NAME]        reset given-up entries for the next run
exports confirm --account NAME --export ID --parts N
telegram        pair [--people N] | chats [--revoke ID] | test
```

Exit codes: `0` ok, `1` fatal, `2` partial (retried next run), `3` export overdue,
`4` unresolved entries. A manual run: `systemctl start gphoto-sync.service` and
`journalctl -fu gphoto-sync`.

## Development

```sh
make check            # go vet + go test -race
make image            # docker build -t gphoto-sync:local .
```

The ingester is idempotent: re-running a part changes nothing, an interrupted run resumes,
and every file operation is journaled in SQLite and replayed on start.
