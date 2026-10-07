package syncrun

import (
	"testing"

	"gphoto-sync/internal/rclone"
	"gphoto-sync/internal/store"
)

func TestDecide(t *testing.T) {
	md5 := func(h string) map[string]string { return map[string]string{"MD5": h} }
	cases := []struct {
		name  string
		arch  *store.Archive
		f     rclone.RemoteFile
		retry bool
		want  disposition
	}{
		{"unknown part", nil, rclone.RemoteFile{ID: "x"}, false, dispNew},
		{"given up entries, retry requested", &store.Archive{RemoteID: "x", MD5: "aa", Status: store.ArchiveWithErrors}, rclone.RemoteFile{ID: "x", Hashes: md5("aa")}, true, dispResume},
		{"same file, partial", &store.Archive{RemoteID: "x", MD5: "aa", Status: store.ArchivePartial}, rclone.RemoteFile{ID: "x", Hashes: md5("aa")}, false, dispResume},
		{"same file, ingested", &store.Archive{RemoteID: "x", MD5: "aa", Status: store.ArchiveIngested}, rclone.RemoteFile{ID: "x", Hashes: md5("aa")}, false, dispSkipIngested},
		{"same file, given up entries", &store.Archive{RemoteID: "x", MD5: "aa", Status: store.ArchiveWithErrors}, rclone.RemoteFile{ID: "x", Hashes: md5("aa")}, false, dispSkipErrors},
		{"other md5", &store.Archive{RemoteID: "x", MD5: "aa", Status: store.ArchiveIngested}, rclone.RemoteFile{ID: "x", Hashes: md5("bb")}, false, dispReplace},
		{"other size, partial", &store.Archive{RemoteID: "x", Size: 5, Status: store.ArchivePartial}, rclone.RemoteFile{ID: "y", Size: 6}, false, dispReplace},
		{"unprovable, ingested", &store.Archive{Size: 5, Status: store.ArchiveIngested}, rclone.RemoteFile{ID: "y", Size: 5}, false, dispSkipUnverified},
		{"unprovable, partial", &store.Archive{Size: 5, Status: store.ArchivePartial}, rclone.RemoteFile{ID: "y", Size: 5}, false, dispReplace},
		{"same id and size, no hash, partial", &store.Archive{RemoteID: "x", Size: 5, Status: store.ArchivePartial}, rclone.RemoteFile{ID: "x", Size: 5}, false, dispResume},
	}
	for _, c := range cases {
		if got, _ := decide(c.arch, c.f, c.retry); got != c.want {
			t.Errorf("%s: got %d, want %d", c.name, got, c.want)
		}
	}
}

func TestNotDeletable(t *testing.T) {
	md5 := func(h string) map[string]string { return map[string]string{"MD5": h} }
	cases := []struct {
		name string
		arch store.Archive
		f    rclone.RemoteFile
		ok   bool
	}{
		{"same id and md5", store.Archive{RemoteID: "x", MD5: "aa", Size: 5}, rclone.RemoteFile{ID: "x", Size: 5, Hashes: md5("aa")}, true},
		{"other id", store.Archive{RemoteID: "x", MD5: "aa"}, rclone.RemoteFile{ID: "y", Hashes: md5("aa")}, false},
		{"md5 changed", store.Archive{RemoteID: "x", MD5: "aa"}, rclone.RemoteFile{ID: "x", Hashes: md5("bb")}, false},
		{"md5 missing from listing", store.Archive{RemoteID: "x", MD5: "aa"}, rclone.RemoteFile{ID: "x"}, false},
		{"no md5 recorded, same size", store.Archive{RemoteID: "x", Size: 5}, rclone.RemoteFile{ID: "x", Size: 5, Hashes: md5("bb")}, true},
		{"no md5 recorded, other size", store.Archive{RemoteID: "x", Size: 5}, rclone.RemoteFile{ID: "x", Size: 6}, false},
	}
	for _, c := range cases {
		if why := notDeletable(&c.arch, c.f); (why == "") != c.ok {
			t.Errorf("%s: %q", c.name, why)
		}
	}
}

func TestCompareArchive(t *testing.T) {
	md5 := func(h string) map[string]string { return map[string]string{"MD5": h} }
	cases := []struct {
		name     string
		arch     store.Archive
		f        rclone.RemoteFile
		differs  bool
		verified bool
	}{
		{"same id and md5", store.Archive{RemoteID: "x", MD5: "aa", Size: 5}, rclone.RemoteFile{ID: "x", Size: 5, Hashes: md5("aa")}, false, true},
		{"re-upload, same md5", store.Archive{RemoteID: "x", MD5: "aa", Size: 5}, rclone.RemoteFile{ID: "y", Size: 5, Hashes: md5("aa")}, false, true},
		{"re-upload, other md5", store.Archive{RemoteID: "x", MD5: "aa", Size: 5}, rclone.RemoteFile{ID: "y", Size: 5, Hashes: md5("bb")}, true, true},
		{"changed in place", store.Archive{RemoteID: "x", MD5: "aa", Size: 5}, rclone.RemoteFile{ID: "x", Size: 6, Hashes: md5("bb")}, true, true},
		{"no md5 recorded, size differs", store.Archive{RemoteID: "x", Size: 5}, rclone.RemoteFile{ID: "x", Size: 6, Hashes: md5("bb")}, true, true},
		{"no md5 recorded, same id and size", store.Archive{RemoteID: "x", Size: 5}, rclone.RemoteFile{ID: "x", Size: 5, Hashes: md5("bb")}, false, true},
		{"no md5 recorded, other id, same size", store.Archive{RemoteID: "", Size: 5}, rclone.RemoteFile{ID: "y", Size: 5, Hashes: md5("bb")}, false, false},
		{"nothing recorded", store.Archive{}, rclone.RemoteFile{ID: "y", Size: 5}, false, false},
	}
	for _, c := range cases {
		why, verified := compareArchive(&c.arch, c.f)
		if (why != "") != c.differs || verified != c.verified {
			t.Errorf("%s: why=%q verified=%v", c.name, why, verified)
		}
	}
}
