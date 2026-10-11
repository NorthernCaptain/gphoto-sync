// Package config loads config.yaml, applies defaults and GPS_* environment
// overrides, and validates the result.
package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"gphoto-sync/internal/takeout"
)

// Config is the whole configuration file. Every duration is a Go duration string.
type Config struct {
	DataDir  string             `yaml:"data_dir"`
	StateDir string             `yaml:"state_dir"`
	Timezone string             `yaml:"timezone"`
	Workers  int                `yaml:"workers"`
	Rclone   Rclone             `yaml:"rclone"`
	Accounts map[string]Account `yaml:"accounts"`
	Layout   Layout             `yaml:"layout"`
	Takeout  Takeout            `yaml:"takeout"`
	Telegram Telegram           `yaml:"telegram"`
	Log      Log                `yaml:"log"`
}

// Rclone describes how the rclone binary is invoked.
type Rclone struct {
	Binary    string   `yaml:"binary"`
	Config    string   `yaml:"config"`
	ExtraArgs []string `yaml:"extra_args"`
}

// Account is one Google account; the map key is the folder name under data_dir.
type Account struct {
	RcloneRemote string          `yaml:"rclone_remote"`
	Takeout      TakeoutOverride `yaml:"takeout"`
}

// TakeoutOverride holds the per-account overrides of Takeout settings.
type TakeoutOverride struct {
	Settle   *time.Duration `yaml:"settle"`
	Language string         `yaml:"language"` // Google language of this account's Takeout; "en" when empty
}

// Layout controls where files are placed and how they are named.
type Layout struct {
	Library         string `yaml:"library"`
	Albums          string `yaml:"albums"`
	LinkMode        string `yaml:"link_mode"`
	KeepSidecarJSON bool   `yaml:"keep_sidecar_json"`
	EditedVersions  string `yaml:"edited_versions"`
	Collision       string `yaml:"collision"`
}

// Takeout controls discovery, fetching and ingest of export parts.
type Takeout struct {
	InboxDir                string        `yaml:"inbox_dir"`
	Pattern                 string        `yaml:"pattern"`
	Settle                  time.Duration `yaml:"settle"`
	KeepArchives            bool          `yaml:"keep_archives"`
	DeleteRemoteAfterIngest bool          `yaml:"delete_remote_after_ingest"`
	ExpectedInterval        time.Duration `yaml:"expected_interval"`
	Grace                   time.Duration `yaml:"grace"`
	IncludeTrash            bool          `yaml:"include_trash"`
	IncludeArchive          bool          `yaml:"include_archive"`
	MaxEntryAttempts        int           `yaml:"max_entry_attempts"`
	ExportCompleteAfter     time.Duration `yaml:"export_complete_after"`
}

// Telegram controls the notification bot.
type Telegram struct {
	Enabled            bool     `yaml:"enabled"`
	BotTokenFile       string   `yaml:"bot_token_file"`
	NotifyOn           []string `yaml:"notify_on"`
	DrainUpdatesOnSync bool     `yaml:"drain_updates_on_sync"`
}

// Log controls logging.
type Log struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// Default returns the built-in defaults.
func Default() *Config {
	return &Config{
		DataDir:  "/data",
		StateDir: "/state",
		Workers:  4,
		Rclone: Rclone{
			Binary:    "rclone",
			Config:    "/config/rclone/rclone.conf",
			ExtraArgs: []string{"--multi-thread-streams", "4", "--retries", "5", "--low-level-retries", "20"},
		},
		Layout: Layout{
			Library:         "{year}/{month}",
			Albums:          "albums",
			LinkMode:        "hardlink",
			KeepSidecarJSON: true,
			EditedVersions:  "keep_both",
			Collision:       "identity_suffix",
		},
		Takeout: Takeout{
			InboxDir:            "/state/inbox",
			Pattern:             "takeout-*.zip",
			Settle:              2 * time.Hour,
			ExpectedInterval:    720 * time.Hour,
			Grace:               336 * time.Hour,
			IncludeArchive:      true,
			MaxEntryAttempts:    3,
			ExportCompleteAfter: 168 * time.Hour,
		},
		Telegram: Telegram{
			BotTokenFile:       "/config/telegram.token",
			NotifyOn:           []string{"success", "partial", "unresolved", "failure", "overdue"},
			DrainUpdatesOnSync: true,
		},
		Log: Log{Level: "info", Format: "json"},
	}
}

// Load reads path (if non-empty), applies environment overrides and validates.
func Load(path string) (*Config, error) {
	c := Default()
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read config: %w", err)
		}
		dec := yaml.NewDecoder(strings.NewReader(string(b)))
		dec.KnownFields(true)
		if err := dec.Decode(c); err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("parse config %s: %w", path, err)
		}
	}
	c.applyEnv()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Config) applyEnv() {
	if v := os.Getenv("GPS_DATA_DIR"); v != "" {
		c.DataDir = v
	}
	if v := os.Getenv("GPS_STATE_DIR"); v != "" {
		c.StateDir = v
	}
	if v := os.Getenv("GPS_TIMEZONE"); v != "" {
		c.Timezone = v
	}
	if v := os.Getenv("GPS_WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.Workers = n
		}
	}
	if v := os.Getenv("GPS_LOG_LEVEL"); v != "" {
		c.Log.Level = v
	}
	if v := os.Getenv("GPS_LOG_FORMAT"); v != "" {
		c.Log.Format = v
	}
	if v := os.Getenv("GPS_RCLONE_CONFIG"); v != "" {
		c.Rclone.Config = v
	}
	if v := os.Getenv("GPS_TELEGRAM_BOT_TOKEN_FILE"); v != "" {
		c.Telegram.BotTokenFile = v
	}
}

// Validate checks the configuration for consistency.
func (c *Config) Validate() error {
	var errs []error
	if c.DataDir == "" || !filepath.IsAbs(c.DataDir) {
		errs = append(errs, errors.New("data_dir must be an absolute path"))
	}
	if c.StateDir == "" || !filepath.IsAbs(c.StateDir) {
		errs = append(errs, errors.New("state_dir must be an absolute path"))
	}
	if c.Workers < 1 {
		errs = append(errs, errors.New("workers must be >= 1"))
	}
	if c.Layout.Library != "{year}/{month}" {
		errs = append(errs, fmt.Errorf("layout.library %q: only \"{year}/{month}\" is supported", c.Layout.Library))
	}
	switch c.Layout.LinkMode {
	case "hardlink", "symlink", "copy":
	default:
		errs = append(errs, fmt.Errorf("layout.link_mode %q: must be hardlink, symlink or copy", c.Layout.LinkMode))
	}
	switch c.Layout.EditedVersions {
	case "keep_both", "prefer_edited", "prefer_original":
	default:
		errs = append(errs, fmt.Errorf("layout.edited_versions %q invalid", c.Layout.EditedVersions))
	}
	switch c.Layout.Collision {
	case "identity_suffix", "numbered":
	default:
		errs = append(errs, fmt.Errorf("layout.collision %q invalid", c.Layout.Collision))
	}
	for name, a := range c.Accounts {
		if name == "" || name != filepath.Base(name) || strings.ContainsAny(name, " /\\") || strings.HasPrefix(name, ".") {
			errs = append(errs, fmt.Errorf("account name %q must be a plain folder name", name))
		}
		if _, err := takeout.ForLanguage(c.LanguageFor(name)); err != nil {
			errs = append(errs, fmt.Errorf("accounts.%s.takeout.language: %w", name, err))
		}
		_ = a
	}
	if c.Timezone != "" {
		if _, err := time.LoadLocation(c.Timezone); err != nil {
			errs = append(errs, fmt.Errorf("timezone %q: %w", c.Timezone, err))
		}
	}
	if c.Takeout.MaxEntryAttempts < 1 {
		errs = append(errs, errors.New("takeout.max_entry_attempts must be >= 1"))
	}
	return errors.Join(errs...)
}

// Location returns the timezone used to bucket photos: the configured zone,
// else the process local zone (TZ environment variable in the container).
func (c *Config) Location() *time.Location {
	if c.Timezone != "" {
		if loc, err := time.LoadLocation(c.Timezone); err == nil {
			return loc
		}
	}
	return time.Local
}

// AccountNames returns the configured account names in sorted order.
func (c *Config) AccountNames() []string {
	names := make([]string, 0, len(c.Accounts))
	for n := range c.Accounts {
		names = append(names, n)
	}
	sortStrings(names)
	return names
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// SettleFor returns the settle duration for an account, honouring overrides.
func (c *Config) SettleFor(account string) time.Duration {
	if a, ok := c.Accounts[account]; ok && a.Takeout.Settle != nil {
		return *a.Takeout.Settle
	}
	return c.Takeout.Settle
}

// LanguageFor returns the Google language of an account's Takeout: the names
// of its folders and files come in it. English when not configured.
func (c *Config) LanguageFor(account string) string {
	if a, ok := c.Accounts[account]; ok && a.Takeout.Language != "" {
		return a.Takeout.Language
	}
	return "en"
}
