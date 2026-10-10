// Package cli implements the gphoto-sync subcommands.
package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"gphoto-sync/internal/config"
	"gphoto-sync/internal/db"
	"gphoto-sync/internal/fsops"
	"gphoto-sync/internal/ingest"
	"gphoto-sync/internal/store"
)

// Exit codes (PLAN.md 3.6).
const (
	ExitOK         = 0
	ExitFatal      = 1
	ExitPartial    = 2
	ExitOverdue    = 3
	ExitUnresolved = 4
)

// Main runs the CLI and returns the exit code.
func Main(args []string, version string) int {
	if len(args) == 0 {
		usage(os.Stderr)
		return ExitFatal
	}
	cmd, rest := args[0], args[1:]
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	switch cmd {
	case "version":
		fmt.Println("gphoto-sync", version)
		return ExitOK
	case "ingest":
		return cmdIngest(ctx, rest)
	case "status":
		return cmdStatus(ctx, rest)
	case "verify":
		return cmdVerify(ctx, rest)
	case "relink":
		return cmdRelink(ctx, rest)
	case "sync":
		return cmdSync(ctx, rest)
	case "check-remote":
		return cmdCheckRemote(ctx, rest)
	case "exports":
		return cmdExports(ctx, rest)
	case "retry":
		return cmdRetry(ctx, rest)
	case "telegram":
		return cmdTelegram(ctx, rest)
	case "help", "-h", "--help":
		usage(os.Stdout)
		return ExitOK
	}
	fmt.Fprintf(os.Stderr, "unknown command %q\n", cmd)
	usage(os.Stderr)
	return ExitFatal
}

func usage(w io.Writer) {
	fmt.Fprint(w, `usage: gphoto-sync <command> [flags]

  sync          [--account NAME] [--dry-run]        discover parts on Drive via rclone, fetch, ingest, report, exit
  ingest        --account NAME [--max-entries N] [--dry-run] [--full-albums --parts N] [--replace] <part.zip|dir>...
  check-remote  [--account NAME]                    list parts rclone can see and their state
  status        [--account NAME]
  verify        [--account NAME] [--hash]
  relink        [--account NAME]
  retry         --account NAME [--part NAME]        reset given-up entries so the next sync retries them
  exports confirm --account NAME --export ID --parts N
  telegram pair [--people N] [--timeout 15m] | chats [--revoke ID] | test
  version

exit codes: 0 ok, 1 fatal, 2 partial (retried next run), 3 export overdue, 4 unresolved entries

common flags: --config PATH (default $GPS_CONFIG or config/config.yaml)
`)
}

type env struct {
	cfg  *config.Config
	st   *store.Store
	log  *slog.Logger
	ing  *ingest.Ingester
	lock *os.File // writer lock in state_dir, held until the process exits
}

// writerLock pins the lock file for the life of the process: an *os.File that
// becomes unreachable is finalized, which closes the descriptor and with it
// the flock, so the handle must stay referenced even when the env is not.
var writerLock *os.File

func commonFlags(fs *flag.FlagSet) *string {
	def := os.Getenv("GPS_CONFIG")
	if def == "" {
		def = "config/config.yaml"
	}
	return fs.String("config", def, "path to config.yaml")
}

func setup(ctx context.Context, cfgPath string) (*env, error) { return setupMode(ctx, cfgPath, false) }

// setupMode opens everything; readOnly skips the writer lock and journal
// recovery (for dry runs and read-only commands).
func setupMode(ctx context.Context, cfgPath string, readOnly bool) (*env, error) {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil, err
	}
	var level slog.Level
	switch strings.ToLower(cfg.Log.Level) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	var h slog.Handler
	if cfg.Log.Format == "text" {
		h = slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	} else {
		h = slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	}
	log := slog.New(h)
	slog.SetDefault(log)
	// one writer at a time: two ingests would share temp paths and race the
	// journaled moves; the lock is released by process exit. Read-only
	// commands touch nothing on disk, not even the directories
	var lock *os.File
	if !readOnly {
		if err := os.MkdirAll(cfg.StateDir, 0o755); err != nil {
			return nil, err
		}
		if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
			return nil, err
		}
		var err error
		if lock, err = fsops.Lock(filepath.Join(cfg.StateDir, "gphoto-sync.lock")); err != nil {
			return nil, err
		}
		writerLock = lock
	}
	// read-only commands never create or migrate the database
	open := db.Open
	if readOnly {
		open = db.OpenReadOnly
	}
	d, err := open(ctx, filepath.Join(cfg.StateDir, "state.db"))
	if err != nil {
		return nil, err
	}
	st := store.New(d)
	ing := ingest.New(cfg, st, log)
	if !readOnly {
		if err := ing.RecoverJournal(ctx); err != nil {
			return nil, fmt.Errorf("journal recovery: %w", err)
		}
	}
	return &env{cfg: cfg, st: st, log: log, ing: ing, lock: lock}, nil
}

func fatal(err error) int {
	fmt.Fprintln(os.Stderr, "error:", err)
	return ExitFatal
}

func cmdIngest(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("ingest", flag.ContinueOnError)
	cfgPath := commonFlags(fs)
	account := fs.String("account", "", "account name (folder under data_dir)")
	maxEntries := fs.Int("max-entries", 0, "stop after N media entries (testing)")
	dryRun := fs.Bool("dry-run", false, "classify and report only")
	fullAlbums := fs.Bool("full-albums", false, "mark this export authoritative for album pruning (needs --parts)")
	parts := fs.Int("parts", 0, "confirmed number of parts in this export")
	exportID := fs.String("export-id", "", "export id when the file name is not a takeout part name")
	partNo := fs.Int("part", 0, "part number when the file name is not a takeout part name")
	replace := fs.Bool("replace", false, "the file replaces an earlier ingest of this part (a changed zip of the same size is not detected otherwise)")
	if err := fs.Parse(args); err != nil {
		return ExitFatal
	}
	if *account == "" || fs.NArg() == 0 {
		fs.Usage()
		return ExitFatal
	}
	if *fullAlbums && *parts <= 0 {
		return fatal(errors.New("--full-albums requires --parts N"))
	}
	if *fullAlbums {
		// the complete listing needs every part: refuse anything else
		have := map[int]bool{}
		exportIDs := map[string]bool{}
		for _, p := range fs.Args() {
			eid, n, ok := ingest.ParsePartName(filepath.Base(p))
			if !ok {
				return fatal(fmt.Errorf("--full-albums needs takeout part names; %s is not one", filepath.Base(p)))
			}
			if have[n] {
				return fatal(fmt.Errorf("--full-albums: part %d given twice", n))
			}
			have[n] = true
			exportIDs[eid] = true
		}
		if len(exportIDs) != 1 {
			return fatal(errors.New("--full-albums: all parts must belong to the same export"))
		}
		var missing []string
		for n := 1; n <= *parts; n++ {
			if !have[n] {
				missing = append(missing, fmt.Sprint(n))
			}
		}
		if len(missing) > 0 || len(have) != *parts {
			return fatal(fmt.Errorf("--full-albums needs exactly parts 1..%d; missing: %s", *parts, strings.Join(missing, ", ")))
		}
	}
	e, err := setupMode(ctx, *cfgPath, *dryRun)
	if err != nil {
		return fatal(err)
	}
	acc, ok := e.cfg.Accounts[*account]
	if !ok {
		return fatal(fmt.Errorf("account %q is not configured in %s", *account, *cfgPath))
	}
	remote := acc.RcloneRemote
	worst := ExitOK
	for _, p := range fs.Args() {
		name := filepath.Base(p)
		eid, pn, ok := ingest.ParsePartName(name)
		if !ok {
			if *exportID == "" {
				return fatal(fmt.Errorf("%s is not a takeout part name; pass --export-id and --part", name))
			}
			eid, pn = *exportID, *partNo
			if pn == 0 {
				pn = 1
			}
		}
		var src ingest.Source
		if fi, err := os.Stat(p); err != nil {
			return fatal(err)
		} else if fi.IsDir() {
			src, err = ingest.OpenDir(p)
			if err != nil {
				return fatal(err)
			}
		} else {
			src, err = ingest.OpenZip(p)
			if err != nil {
				return fatal(err)
			}
		}
		fi, _ := os.Stat(p)
		meta := ingest.PartMeta{Account: *account, RcloneRemote: remote, ExportID: eid, PartNo: pn, Name: name, LocalPath: p}
		if fi != nil && fi.Mode().IsRegular() {
			meta.Size = fi.Size()
		}
		e.log.Info("ingest start", "account", *account, "part", name, "export", eid, "part_no", pn)
		res, err := e.ing.IngestPart(ctx, src, meta, ingest.Options{MaxEntries: *maxEntries, DryRun: *dryRun, FullAlbums: *fullAlbums, Parts: *parts, Replace: *replace})
		src.Close()
		if err != nil {
			e.log.Error("ingest failed", "part", name, "err", err)
			return ExitFatal
		}
		printResult(res)
		switch res.Status {
		case store.ArchivePartial:
			worst = max(worst, ExitPartial)
		case store.ArchiveWithErrors:
			worst = max(worst, ExitUnresolved)
		}
	}
	if *fullAlbums && worst == ExitOK && !*dryRun {
		eid, _, _ := ingest.ParsePartName(filepath.Base(fs.Args()[0]))
		pres, err := e.ing.Prune(ctx, *account, eid, false)
		if err != nil {
			return fatal(err)
		}
		if pres.Skipped != "" {
			fmt.Println("album pruning skipped:", pres.Skipped)
		} else {
			fmt.Printf("album pruning applied: %d albums checked, %d links removed, %d group links adjusted\n", pres.Albums, pres.Removed, pres.GroupAdjusted)
		}
	}
	return worst
}

func printResult(r *ingest.Result) {
	fmt.Printf("%s: %s\n", r.Part, r.Status)
	if r.Replaced {
		fmt.Println("  replaced an earlier ingest of this part (its entries and occurrences were reset)")
	}
	if r.Unlinked > 0 {
		fmt.Printf("  %d album links removed: no ingested part lists them any more\n", r.Unlinked)
	}
	if r.SidecarsOrphaned > 0 {
		fmt.Printf("  %d sidecars this part supplied before are missing from its replacement (metadata kept, provenance cleared)\n", r.SidecarsOrphaned)
	}
	fmt.Printf("  entries %d, media %d, albums %d, skipped %d\n", r.Entries, r.Media, r.Albums, r.Skipped)
	fmt.Printf("  added %d, duplicates %d, changed %d, promoted %d, merged %d, links %d\n", r.Added, r.Duplicates, r.Changed, r.Promoted, r.Merged, r.Links)
	fmt.Printf("  failed %d, given up %d, unresolved %d, pending sidecars %d, took %s\n", r.Failed, r.GivenUp, r.Unresolved, r.PendingSidecars, r.Duration.Round(time.Second))
	for _, e := range r.Errors {
		fmt.Printf("  ! %s\n", e)
	}
}

func accountsArg(e *env, account string) []string {
	if account != "" {
		return []string{account}
	}
	return e.cfg.AccountNames()
}

func cmdStatus(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	cfgPath := commonFlags(fs)
	account := fs.String("account", "", "account name")
	if err := fs.Parse(args); err != nil {
		return ExitFatal
	}
	e, err := setupMode(ctx, *cfgPath, true)
	if err != nil {
		return fatal(err)
	}
	worst := ExitOK
	for _, name := range accountsArg(e, *account) {
		acct, err := e.st.GetAccount(ctx, name)
		if err != nil {
			return fatal(err)
		}
		fmt.Printf("account %s\n", name)
		if acct == nil {
			fmt.Println("  (nothing ingested yet)")
			continue
		}
		c, err := e.st.Counts(ctx, acct.ID)
		if err != nil {
			return fatal(err)
		}
		fmt.Printf("  items %d (fallback identity %d, archived %d), %.1f GB, albums %d, album links %d\n",
			c["items"], c["items_fallback"], c["items_archived"], float64(c["bytes"])/1e9, c["albums"], c["album_links"])
		fmt.Printf("  parts %d, pending sidecars %d, failed entries %d, given up %d\n", c["archives"], c["pending_sidecars"], c["entries_failed"], c["entries_given_up"])
		if c["entries_given_up"] > 0 {
			worst = max(worst, ExitUnresolved)
		} else if c["entries_failed"] > 0 {
			worst = max(worst, ExitPartial)
		}
		exps, _ := e.st.ListExports(ctx, acct.ID)
		for _, x := range exps {
			fmt.Printf("  export %s: highest part %d, confirmed parts %d, authoritative %v\n", x.ExportID, x.HighestPart, x.ConfirmedParts, x.Authoritative)
		}
		archs, _ := e.st.ListArchives(ctx, acct.ID)
		for _, a := range archs {
			fmt.Printf("    %s: %s (entries %d, ingested %d, duplicates %d, errors %d)\n", a.Name, a.Status, a.Entries, a.Ingested, a.Duplicates, a.Errors)
		}
	}
	return worst
}

func cmdVerify(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	cfgPath := commonFlags(fs)
	account := fs.String("account", "", "account name")
	rehash := fs.Bool("hash", false, "re-hash every file (slow)")
	if err := fs.Parse(args); err != nil {
		return ExitFatal
	}
	e, err := setupMode(ctx, *cfgPath, true)
	if err != nil {
		return fatal(err)
	}
	code := ExitOK
	for _, name := range accountsArg(e, *account) {
		r, err := e.ing.Verify(ctx, name, *rehash)
		if err != nil {
			return fatal(err)
		}
		fmt.Printf("account %s: items %d (missing %d, mismatched %d), kept versions %d (missing %d, mismatched %d), links %d (missing %d, bad %d), canonical paths %d (missing %d)\n",
			name, r.Items, r.MissingFiles, r.HashMismatches, r.Versions, r.MissingVersions, r.VersionMismatches, r.Links, r.MissingLinks, r.BadLinks, r.Canonical, r.MissingCanonical)
		for _, p := range r.Problems {
			fmt.Println("  !", p)
		}
		if r.MissingFiles+r.HashMismatches+r.MissingVersions+r.VersionMismatches+r.MissingLinks+r.BadLinks+r.MissingCanonical > 0 {
			code = ExitFatal
		}
	}
	return code
}

func cmdRelink(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("relink", flag.ContinueOnError)
	cfgPath := commonFlags(fs)
	account := fs.String("account", "", "account name")
	if err := fs.Parse(args); err != nil {
		return ExitFatal
	}
	e, err := setup(ctx, *cfgPath)
	if err != nil {
		return fatal(err)
	}
	for _, name := range accountsArg(e, *account) {
		n, err := e.ing.Relink(ctx, name)
		if err != nil {
			return fatal(err)
		}
		fmt.Printf("account %s: %d links recreated\n", name, n)
	}
	return ExitOK
}
