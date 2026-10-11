package syncrun

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gphoto-sync/internal/config"
	"gphoto-sync/internal/ingest"
	"gphoto-sync/internal/rclone"
	"gphoto-sync/internal/store"
)

const gp = "Takeout/Google Photos/"

func sidecar(title, id string, taken time.Time) []byte {
	return []byte(fmt.Sprintf(`{"title":%q,"description":"","photoTakenTime":{"timestamp":"%d","formatted":"x"},"url":"https://photos.google.com/photo/%s"}`, title, taken.Unix(), id))
}

func writeZip(t *testing.T, path string, files map[string][]byte) {
	t.Helper()
	out, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := zip.NewWriter(out)
	for name, data := range files {
		fw, err := w.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		fw.Write(data)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	out.Close()
}

// A run that ingests a part ends its report with what every configured
// account holds, a never-ingested one included, and their total; a run that
// changes nothing carries no summary. Album links made as copies take bytes
// on disk again.
func TestLibrarySummaryAfterChange(t *testing.T) {
	for _, tc := range []struct {
		mode  string
		bytes int64
	}{
		{"hardlink", 3072},
		{"copy", 5120},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			rc := &fakeRemote{listings: map[string][]rclone.RemoteFile{}, files: map[string]string{}}
			r, _ := newRunner(t, rc)
			r.Cfg.Layout.LinkMode = tc.mode
			r.Cfg.Accounts["bob"] = config.Account{RcloneRemote: "gdrive2:Takeout"}
			now := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
			r.Now = func() time.Time { return now }
			ctx := context.Background()

			rep := r.Run(ctx, []string{"alice"}, false)
			if rep.changed() || rep.Library != nil || strings.Contains(rep.HTML(), "Library") {
				t.Fatalf("idle run reported a change:\n%s", rep.Text())
			}

			taken := time.Date(2020, 6, 21, 13, 0, 0, 0, time.UTC)
			still, video := bytes.Repeat([]byte("a"), 2048), bytes.Repeat([]byte("b"), 1024)
			zipPath := filepath.Join(t.TempDir(), "takeout-20260901T000000Z-1-001.zip")
			writeZip(t, zipPath, map[string][]byte{
				gp + "Photos from 2020/a.jpg":                            still,
				gp + "Photos from 2020/a.jpg.supplemental-metadata.json": sidecar("a.jpg", "AAA", taken),
				gp + "Photos from 2020/b.mp4":                            video,
				gp + "Photos from 2020/b.mp4.supplemental-metadata.json": sidecar("b.mp4", "BBB", taken),
				gp + "Trip/a.jpg":                                        still,
				gp + "Trip/a.jpg.supplemental-metadata.json":             sidecar("a.jpg", "AAA", taken),
			})
			fi, err := os.Stat(zipPath)
			if err != nil {
				t.Fatal(err)
			}
			rc.listings["gdrive:Takeout"] = []rclone.RemoteFile{{ID: "f1", Name: filepath.Base(zipPath), Size: fi.Size(), ModTime: now.Add(-30 * 24 * time.Hour)}}
			rc.files["f1"] = zipPath

			rep = r.Run(ctx, []string{"alice"}, false)
			if rep.Exit != ExitOK {
				t.Fatalf("exit %d:\n%s", rep.Exit, rep.Text())
			}
			if !rep.Accounts[0].Changed || len(rep.Library) != 2 || rep.LibraryTotal == nil {
				t.Fatalf("changed %v, library %+v, total %+v:\n%s", rep.Accounts[0].Changed, rep.Library, rep.LibraryTotal, rep.Text())
			}
			alice := Library{Name: "alice", Photos: 1, Videos: 1, Albums: 1, Links: 1, Bytes: tc.bytes}
			total := alice
			total.Name = "all accounts"
			// configured accounts are listed by name
			if got := *rep.Library[0]; got != alice {
				t.Errorf("alice: got %+v, want %+v", got, alice)
			}
			if got := *rep.Library[1]; got != (Library{Name: "bob"}) {
				t.Errorf("bob: got %+v, want an empty library", got)
			}
			if got := *rep.LibraryTotal; got != total {
				t.Errorf("total: got %+v, want %+v", got, total)
			}
			want := fmt.Sprintf("\nLibrary\nalice: 1 photo, 1 video, 1 album, %d KB on disk\nbob: 0 photos, 0 videos, 0 albums, 0 KB on disk\nall accounts: 1 photo, 1 video, 1 album, %d KB on disk\n", tc.bytes/1024, tc.bytes/1024)
			if text := rep.Text(); !strings.Contains(text, want) {
				t.Errorf("summary missing, want %q in:\n%s", want, text)
			}

			// nothing new on Drive: the part is skipped and the summary stays out
			rep = r.Run(ctx, []string{"alice"}, false)
			if rep.Exit != ExitOK || rep.changed() || rep.Library != nil {
				t.Fatalf("unchanged run: exit %d, changed %v, library %+v:\n%s", rep.Exit, rep.changed(), rep.Library, rep.Text())
			}
		})
	}
}

// The summary lists every configured account and, with several, their total;
// bytes shared between accounts count once.
func TestLibrarySummaryRendering(t *testing.T) {
	rep := &Report{
		Finished: time.Date(2026, 10, 10, 1, 12, 0, 0, time.UTC),
		Accounts: []*AccountReport{{Name: "alice", Changed: true}},
		Library: []*Library{
			{Name: "alice", Photos: 40123, Videos: 3468, Albums: 127, Links: 18995, Bytes: 122_100_000_000},
			{Name: "bob & co", Photos: 1, Videos: 0, Albums: 2, Bytes: 500_000},
		},
		LibraryTotal: &Library{Name: "all accounts", Photos: 40124, Videos: 3468, Albums: 129, Links: 18995, Bytes: 122_100_400_000},
	}
	html := rep.HTML()
	for _, want := range []string{
		"\n<b>Library</b>\n",
		"alice: 40,123 photos, 3,468 videos, 127 albums, 122.1 GB on disk\n",
		"bob &amp; co: 1 photo, 0 videos, 2 albums, 500 KB on disk\n",
		"<b>all accounts</b>: 40,124 photos, 3,468 videos, 129 albums, 122.1 GB on disk\n",
	} {
		if !strings.Contains(html, want) {
			t.Errorf("missing %q in:\n%s", want, html)
		}
	}
	if !strings.HasSuffix(html, "on disk\n") {
		t.Errorf("summary is not the last block:\n%s", html)
	}
}

// A part whose entries are all in another language's layout is refused and
// kept in the inbox; once the names are configured, the next run ingests it
// from the local copy without downloading it again.
func TestUnrecognizedLayoutIsResumedFromLocalCopy(t *testing.T) {
	rc := &fakeRemote{listings: map[string][]rclone.RemoteFile{}, files: map[string]string{}}
	r, st := newRunner(t, rc)
	now := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	r.Now = func() time.Time { return now }
	ctx := context.Background()
	const ru = "Takeout/Google Фото/"
	zipPath := filepath.Join(t.TempDir(), "takeout-20261010T221633Z-1-001.zip")
	writeZip(t, zipPath, map[string][]byte{
		ru + "Фото 2023 г./a.jpg":                            bytes.Repeat([]byte("a"), 2048),
		ru + "Фото 2023 г./a.jpg.supplemental-metadata.json": sidecar("a.jpg", "AAA", time.Date(2023, 1, 1, 13, 0, 0, 0, time.UTC)),
	})
	fi, err := os.Stat(zipPath)
	if err != nil {
		t.Fatal(err)
	}
	sum, err := fileMD5(zipPath)
	if err != nil {
		t.Fatal(err)
	}
	rc.listings["gdrive:Takeout"] = []rclone.RemoteFile{{ID: "f1", Name: filepath.Base(zipPath), Size: fi.Size(), ModTime: now.Add(-30 * 24 * time.Hour), Hashes: map[string]string{"MD5": sum}}}
	rc.files["f1"] = zipPath

	rep := r.Run(ctx, []string{"alice"}, false)
	if rep.Exit != ExitPartial || rc.copies != 1 || !strings.Contains(rep.Text(), "takeout.language") {
		t.Fatalf("first run: exit %d, downloads %d:\n%s", rep.Exit, rc.copies, rep.Text())
	}
	local := filepath.Join(r.Cfg.Takeout.InboxDir, "alice", filepath.Base(zipPath))
	if _, err := os.Stat(local); err != nil {
		t.Fatalf("local copy not kept for the resume: %v", err)
	}
	if rep.Library != nil {
		t.Fatalf("a refused part is not a change:\n%s", rep.Text())
	}

	r.Cfg.Accounts["alice"] = config.Account{RcloneRemote: "gdrive:Takeout", Takeout: config.TakeoutOverride{Language: "ru"}}
	r.Ing = ingest.New(r.Cfg, st, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rep = r.Run(ctx, []string{"alice"}, false)
	if rep.Exit != ExitOK || rc.copies != 1 {
		t.Fatalf("second run: exit %d, downloads %d:\n%s", rep.Exit, rc.copies, rep.Text())
	}
	if len(rep.Library) != 1 || rep.Library[0].Photos != 1 {
		t.Fatalf("second run library: %+v", rep.Library)
	}
	if _, err := os.Stat(local); !os.IsNotExist(err) {
		t.Errorf("local copy still in the inbox after the ingest: %v", err)
	}
}

// A part recorded as ingested without a single recognized entry (a Takeout in
// a language that was not configured at the time, and the local copy already
// removed) is fetched and ingested again once the language is configured.
func TestZeroEntryIngestedPartIsFetchedAgain(t *testing.T) {
	rc := &fakeRemote{listings: map[string][]rclone.RemoteFile{}, files: map[string]string{}}
	r, st := newRunner(t, rc)
	now := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	r.Now = func() time.Time { return now }
	ctx := context.Background()
	const ru = "Takeout/Google Фото/"
	zipPath := filepath.Join(t.TempDir(), "takeout-20261010T221633Z-1-001.zip")
	writeZip(t, zipPath, map[string][]byte{
		ru + "Фото 2023 г./a.jpg":                            bytes.Repeat([]byte("a"), 2048),
		ru + "Фото 2023 г./a.jpg.supplemental-metadata.json": sidecar("a.jpg", "AAA", time.Date(2023, 1, 1, 13, 0, 0, 0, time.UTC)),
	})
	fi, err := os.Stat(zipPath)
	if err != nil {
		t.Fatal(err)
	}
	sum, err := fileMD5(zipPath)
	if err != nil {
		t.Fatal(err)
	}
	rc.listings["gdrive:Takeout"] = []rclone.RemoteFile{{ID: "f1", Name: filepath.Base(zipPath), Size: fi.Size(), ModTime: now.Add(-30 * 24 * time.Hour), Hashes: map[string]string{"MD5": sum}}}
	rc.files["f1"] = zipPath
	acct, err := st.EnsureAccount(ctx, "alice", "gdrive:Takeout")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.UpsertArchive(ctx, &store.Archive{AccountID: acct.ID, RemoteID: "f1", Name: filepath.Base(zipPath), ExportID: "20261010T221633Z-1", PartNo: 1,
		Size: fi.Size(), MD5: sum, Status: store.ArchiveIngested}); err != nil {
		t.Fatal(err)
	}

	r.Cfg.Accounts["alice"] = config.Account{RcloneRemote: "gdrive:Takeout", Takeout: config.TakeoutOverride{Language: "ru"}}
	r.Ing = ingest.New(r.Cfg, st, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rep := r.Run(ctx, []string{"alice"}, false)
	if rep.Exit != ExitOK || rc.copies != 1 || len(rep.Library) != 1 || rep.Library[0].Photos != 1 {
		t.Fatalf("exit %d, downloads %d, library %+v:\n%s", rep.Exit, rc.copies, rep.Library, rep.Text())
	}
	if !strings.Contains(rep.Text(), "without a single recognized entry") {
		t.Errorf("report does not say why the part was ingested again:\n%s", rep.Text())
	}
	archs, _ := st.ListArchives(ctx, acct.ID)
	if len(archs) != 1 || archs[0].Status != store.ArchiveIngested || archs[0].Entries == 0 {
		t.Fatalf("archive after the run: %+v", archs)
	}
}
