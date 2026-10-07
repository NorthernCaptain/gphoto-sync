// Package rclone drives the rclone binary for Drive listing, copy-by-id and
// deletion. rclone's CLI is the stable surface; no library is used.
package rclone

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"time"
)

// Client runs rclone commands with a fixed config file.
type Client struct {
	Binary     string
	ConfigPath string
	ExtraArgs  []string
	Log        *slog.Logger
}

// RemoteFile is one entry of `rclone lsjson`.
type RemoteFile struct {
	ID       string            `json:"ID"`
	Name     string            `json:"Name"`
	Path     string            `json:"Path"`
	Size     int64             `json:"Size"`
	ModTime  time.Time         `json:"ModTime"`
	MimeType string            `json:"MimeType"`
	IsDir    bool              `json:"IsDir"`
	Hashes   map[string]string `json:"Hashes"`
}

// MD5 returns the MD5 hash reported by the remote, if any.
func (f RemoteFile) MD5() string { return f.Hashes["MD5"] }

// SplitRemote splits "gdrive:Takeout" into "gdrive:" and "Takeout".
func SplitRemote(remote string) (name, path string) {
	i := strings.Index(remote, ":")
	if i < 0 {
		return remote + ":", ""
	}
	return remote[:i+1], remote[i+1:]
}

func (c *Client) base(args ...string) []string {
	out := []string{"--config", c.ConfigPath}
	out = append(out, c.ExtraArgs...)
	return append(out, args...)
}

func (c *Client) run(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, c.Binary, c.base(args...)...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if len(msg) > 2000 {
			msg = msg[len(msg)-2000:]
		}
		return nil, fmt.Errorf("rclone %s: %w: %s", strings.Join(args, " "), err, msg)
	}
	return stdout.Bytes(), nil
}

// Version returns the rclone version line.
func (c *Client) Version(ctx context.Context) (string, error) {
	out, err := c.run(ctx, "version")
	if err != nil {
		return "", err
	}
	line, _, _ := strings.Cut(string(out), "\n")
	return strings.TrimSpace(line), nil
}

// List returns the files directly under remote (e.g. "gdrive:Takeout") with hashes.
func (c *Client) List(ctx context.Context, remote string) ([]RemoteFile, error) {
	out, err := c.run(ctx, "lsjson", "--files-only", "--hash", "--no-mimetype", remote)
	if err != nil {
		return nil, err
	}
	var files []RemoteFile
	if err := json.Unmarshal(out, &files); err != nil {
		return nil, fmt.Errorf("parse lsjson: %w", err)
	}
	return files, nil
}

// CopyID downloads the Drive file with the given id to the local path dest,
// streaming rclone's progress lines into the log.
func (c *Client) CopyID(ctx context.Context, remoteName, id, dest string) error {
	args := c.base("backend", "copyid", remoteName, id, dest, "--stats", "60s", "--stats-one-line", "-v")
	cmd := exec.CommandContext(ctx, c.Binary, args...)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	var last string
	sc := bufio.NewScanner(stderr)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		last = line
		if c.Log != nil {
			c.Log.Info("rclone", "line", line)
		}
	}
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("rclone copyid %s: %w: %s", id, err, last)
	}
	return nil
}

// MoveID moves a Drive file by id to destRemotePath (server-side), so a
// later deletion can target a path only this program writes to.
func (c *Client) MoveID(ctx context.Context, remoteName, id, destRemotePath string) error {
	_, err := c.run(ctx, "backend", "moveid", remoteName, id, destRemotePath)
	return err
}

// DeleteFile moves a remote file to trash (Drive's default for rclone).
func (c *Client) DeleteFile(ctx context.Context, remotePath string) error {
	_, err := c.run(ctx, "deletefile", remotePath)
	return err
}

// About returns the remote's quota numbers in bytes.
func (c *Client) About(ctx context.Context, remoteName string) (map[string]int64, error) {
	out, err := c.run(ctx, "about", "--json", remoteName)
	if err != nil {
		return nil, err
	}
	var m map[string]int64
	if err := json.Unmarshal(out, &m); err != nil {
		return nil, fmt.Errorf("parse about: %w", err)
	}
	return m, nil
}
