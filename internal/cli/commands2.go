package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"gphoto-sync/internal/ingest"
	"gphoto-sync/internal/rclone"
	"gphoto-sync/internal/store"
	"gphoto-sync/internal/syncrun"
	"gphoto-sync/internal/telegram"
)

func (e *env) rclone() *rclone.Client {
	return &rclone.Client{Binary: e.cfg.Rclone.Binary, ConfigPath: e.cfg.Rclone.Config, ExtraArgs: e.cfg.Rclone.ExtraArgs, Log: e.log}
}

func (e *env) telegram() (*telegram.Bot, error) {
	if !e.cfg.Telegram.Enabled {
		return nil, nil
	}
	tok, err := telegram.ReadToken(e.cfg.Telegram.BotTokenFile)
	if err != nil {
		return nil, fmt.Errorf("telegram token: %w", err)
	}
	return &telegram.Bot{C: telegram.NewClient(tok, ""), St: e.st, Log: e.log}, nil
}

func cmdSync(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("sync", flag.ContinueOnError)
	cfgPath := commonFlags(fs)
	account := fs.String("account", "", "only this account")
	dryRun := fs.Bool("dry-run", false, "list what would be fetched, change nothing")
	if err := fs.Parse(args); err != nil {
		return ExitFatal
	}
	e, err := setupMode(ctx, *cfgPath, *dryRun)
	if err != nil {
		return fatal(err)
	}
	var bot *telegram.Bot
	if !*dryRun {
		if bot, err = e.telegram(); err != nil {
			return fatal(err)
		}
	}
	var runID int64
	if !*dryRun {
		runID, _ = e.st.BeginRun(ctx, 0, "sync")
	}
	runner := &syncrun.Runner{Cfg: e.cfg, St: e.st, Log: e.log, Ing: e.ing, RC: e.rclone(), Bot: bot}
	var accounts []string
	if *account != "" {
		accounts = []string{*account}
	}
	rep := runner.Run(ctx, accounts, *dryRun)
	fmt.Print(rep.Text())
	if !*dryRun {
		counts, _ := json.Marshal(rep)
		_ = e.st.FinishRun(ctx, runID, rep.Exit, string(counts))
	}
	if bot != nil && wants(e.cfg.Telegram.NotifyOn, syncrun.Outcome(rep.Exit)) {
		if n, err := bot.Broadcast(ctx, rep.HTML()); err != nil {
			e.log.Warn("telegram report partly failed", "sent", n, "err", err)
		} else {
			e.log.Info("telegram report sent", "chats", n)
		}
	}
	return rep.Exit
}

func wants(list []string, outcome string) bool {
	for _, s := range list {
		if s == outcome || s == "all" {
			return true
		}
	}
	return false
}

// humanSize formats a byte count for listings (GB for parts, smaller units
// for stray files such as Takeout's archive_browser.html zip).
func humanSize(n int64) string {
	switch {
	case n >= 1e9:
		return fmt.Sprintf("%.1f GB", float64(n)/1e9)
	case n >= 1e6:
		return fmt.Sprintf("%.1f MB", float64(n)/1e6)
	default:
		return fmt.Sprintf("%.0f KB", float64(n)/1e3)
	}
}

func cmdCheckRemote(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("check-remote", flag.ContinueOnError)
	cfgPath := commonFlags(fs)
	account := fs.String("account", "", "only this account")
	if err := fs.Parse(args); err != nil {
		return ExitFatal
	}
	e, err := setupMode(ctx, *cfgPath, true)
	if err != nil {
		return fatal(err)
	}
	rc := e.rclone()
	if v, err := rc.Version(ctx); err != nil {
		return fatal(err)
	} else {
		fmt.Println(v)
	}
	code := ExitOK
	for _, name := range accountsArg(e, *account) {
		acc := e.cfg.Accounts[name]
		fmt.Printf("account %s (%s)\n", name, acc.RcloneRemote)
		files, err := rc.List(ctx, acc.RcloneRemote)
		if err != nil {
			fmt.Println("  !", err)
			code = ExitFatal
			continue
		}
		if remoteName, _ := rclone.SplitRemote(acc.RcloneRemote); remoteName != "" {
			if about, err := rc.About(ctx, remoteName); err == nil {
				fmt.Printf("  drive: used %.1f GB, free %.1f GB\n", float64(about["used"])/1e9, float64(about["free"])/1e9)
			}
		}
		acctRow, _ := e.st.GetAccount(ctx, name)
		var acctID int64
		if acctRow != nil {
			acctID = acctRow.ID
		}
		for _, f := range files {
			state := "new"
			if a, _ := e.st.GetArchiveByRemoteID(ctx, acctID, f.ID); a != nil {
				state = a.Status
			} else if o, _ := e.st.ArchiveByRemoteIDAnyAccount(ctx, f.ID); o != nil {
				state = "recorded by another account (shared folder?)"
			}
			mark := ""
			if _, _, ok := ingest.ParsePartName(f.Name); !ok {
				mark = " (not a takeout part, ignored)"
			}
			fmt.Printf("  %s  %s  %s  %s%s\n", f.Name, humanSize(f.Size), f.ModTime.Format(time.RFC3339), state, mark)
		}
		if len(files) == 0 {
			fmt.Println("  (empty)")
		}
	}
	return code
}

func cmdExports(ctx context.Context, args []string) int {
	if len(args) == 0 || args[0] != "confirm" {
		fmt.Fprintln(os.Stderr, "usage: gphoto-sync exports confirm --account NAME --export ID --parts N")
		return ExitFatal
	}
	fs := flag.NewFlagSet("exports confirm", flag.ContinueOnError)
	cfgPath := commonFlags(fs)
	account := fs.String("account", "", "account name")
	exportID := fs.String("export", "", "export id, e.g. 20261006T035113Z-1")
	parts := fs.Int("parts", 0, "number of parts Google reports for this export")
	if err := fs.Parse(args[1:]); err != nil {
		return ExitFatal
	}
	if *account == "" || *exportID == "" || *parts <= 0 {
		fs.Usage()
		return ExitFatal
	}
	e, err := setup(ctx, *cfgPath)
	if err != nil {
		return fatal(err)
	}
	acct, err := e.st.GetAccount(ctx, *account)
	if err != nil || acct == nil {
		return fatal(errors.New("unknown account (nothing ingested yet?)"))
	}
	if err := e.st.SetExportConfirmedParts(ctx, acct.ID, *exportID, *parts, false); err != nil {
		return fatal(err)
	}
	fmt.Printf("export %s of %s: confirmed %d parts\n", *exportID, *account, *parts)
	// with every confirmed part ingested the export is complete: stored
	// sidecars still undecided (name collisions with known photos) are settled
	complete, err := e.ing.ExportComplete(ctx, acct.ID, *exportID, 0, time.Now())
	if err != nil {
		return fatal(err)
	}
	if !complete {
		fmt.Println("  not every confirmed part is ingested yet; stored sidecars are settled by the sync that ingests the last one")
		return ExitOK
	}
	if links, promoted, err := e.ing.ResolvePending(ctx, acct, *exportID, true); err != nil {
		return fatal(err)
	} else if links+promoted > 0 {
		fmt.Printf("  %d album links and %d promotions from stored sidecars\n", links, promoted)
	}
	return ExitOK
}

func cmdRetry(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("retry", flag.ContinueOnError)
	cfgPath := commonFlags(fs)
	account := fs.String("account", "", "account name")
	part := fs.String("part", "", "only this part (file name)")
	if err := fs.Parse(args); err != nil {
		return ExitFatal
	}
	if *account == "" {
		fs.Usage()
		return ExitFatal
	}
	e, err := setup(ctx, *cfgPath)
	if err != nil {
		return fatal(err)
	}
	acct, err := e.st.GetAccount(ctx, *account)
	if err != nil || acct == nil {
		return fatal(errors.New("unknown account"))
	}
	n, err := e.st.ResetGivenUp(ctx, acct.ID, *part)
	if err != nil {
		return fatal(err)
	}
	fmt.Printf("%d given-up entries reset; the next sync re-fetches their parts and retries\n", n)
	return ExitOK
}

func cmdTelegram(ctx context.Context, args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: gphoto-sync telegram pair|chats|test [flags]")
		return ExitFatal
	}
	sub := args[0]
	fs := flag.NewFlagSet("telegram "+sub, flag.ContinueOnError)
	cfgPath := commonFlags(fs)
	people := fs.Int("people", 1, "pair: number of single-use codes to issue")
	timeout := fs.Duration("timeout", 15*time.Minute, "pair: how long to wait")
	revoke := fs.Int64("revoke", 0, "chats: revoke this chat id")
	if err := fs.Parse(args[1:]); err != nil {
		return ExitFatal
	}
	// pairing and revocation write telegram state and must not overlap a sync
	// draining the same update stream; listing and test only read
	readOnly := sub != "pair" && !(sub == "chats" && *revoke != 0)
	e, err := setupMode(ctx, *cfgPath, readOnly)
	if err != nil {
		return fatal(err)
	}
	if !e.cfg.Telegram.Enabled {
		return fatal(errors.New("telegram.enabled is false in the config"))
	}
	bot, err := e.telegram()
	if err != nil {
		return fatal(err)
	}
	switch sub {
	case "pair":
		n, err := bot.Pair(ctx, *people, *timeout, os.Stdout)
		if err != nil {
			return fatal(err)
		}
		fmt.Printf("paired %d of %d\n", n, *people)
		if n < *people {
			return ExitPartial
		}
		return ExitOK
	case "chats":
		if *revoke != 0 {
			if err := e.st.RevokeChat(ctx, *revoke); err != nil {
				return fatal(err)
			}
			fmt.Println("revoked", *revoke)
		}
		chats, err := e.st.AllChats(ctx)
		if err != nil {
			return fatal(err)
		}
		for _, c := range chats {
			state := "active"
			if c.RevokedAt != "" {
				state = "revoked " + c.RevokedAt
			}
			fmt.Printf("%d  %s (@%s)  paired %s  %s\n", c.ChatID, c.DisplayName, c.Username, c.PairedAt, state)
		}
		if len(chats) == 0 {
			fmt.Println("no paired chats; run: gphoto-sync telegram pair --people N")
		}
		return ExitOK
	case "test":
		n, err := bot.Broadcast(ctx, "<b>gphoto-sync</b> test message from "+telegram.Esc(hostname())+" at "+telegram.Esc(time.Now().Format(time.RFC3339)))
		if err != nil {
			fmt.Fprintln(os.Stderr, "some sends failed:", err)
		}
		fmt.Printf("sent to %d chat(s)\n", n)
		if err != nil {
			return ExitPartial
		}
		return ExitOK
	}
	fmt.Fprintf(os.Stderr, "unknown telegram subcommand %q\n", sub)
	return ExitFatal
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown host"
	}
	return strings.TrimSpace(h)
}

var _ = store.Now
