package takeout

import (
	"testing"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		name string
		kind FolderKind
		typ  EntryType
		year int
		ok   bool
	}{
		{"Takeout/Google Photos/Photos from 2003/l-05.jpg", FolderYear, TypeMedia, 2003, true},
		{"Takeout/Google Photos/Photos from 2003/l-05.jpg.supplemental-metadata.json", FolderYear, TypeSidecar, 2003, true},
		{"Takeout/Google Photos/Mortgage payments /metadata.json", FolderAlbum, TypeAlbumMeta, 0, true},
		{"Takeout/Google Photos/2001/IMG.jpg", FolderAlbum, TypeMedia, 0, true},
		{"Takeout/Google Photos/Archive/x.jpg", FolderArchive, TypeMedia, 0, true},
		{"Takeout/Google Photos/Trash/x.jpg", FolderTrash, TypeMedia, 0, true},
		{"Takeout/Google Photos/Album/shared_album_comments.json", FolderAlbum, TypeIgnore, 0, true},
		{"Takeout/archive_browser.html", FolderUnknown, TypeIgnore, 0, true},
		{"Takeout/Drive/Photos from 2003/x.jpg", 0, 0, 0, false},
		{"Takeout/Google Photos/Photos from 2003/x.jpg", FolderYear, TypeMedia, 2003, true},
		{"Takeout/Google Photos/Photos from 2003/", 0, 0, 0, false},
		{"README.txt", 0, 0, 0, false},
	}
	for _, c := range cases {
		e, ok := Classify(c.name, 1, DefaultRules)
		if ok != c.ok {
			t.Errorf("%s: ok=%v want %v", c.name, ok, c.ok)
			continue
		}
		if !ok {
			continue
		}
		if e.FolderKind != c.kind || e.Type != c.typ || e.Year != c.year {
			t.Errorf("%s: got kind=%v type=%v year=%d", c.name, e.FolderKind, e.Type, e.Year)
		}
	}
}

func TestSidecarKeys(t *testing.T) {
	cases := map[string][]string{
		"IMG_1.jpg.supplemental-metadata.json":                     {"IMG_1.jpg"},
		"IMG_1.jpg.json":                                           {"IMG_1.jpg"},
		"IMG_6464.jpg.supplemental-metadata(1).json":               {"IMG_6464(1).jpg"},
		"IMG_1.jpg(1).json":                                        {"IMG_1(1).jpg"},
		"10-MOTION.gif.supplemental-metadata(1).json":              {"10-MOTION(1).gif"},
		"PXL_20221226_195217263.MP.jpg.supplemental-metadata.json": {"PXL_20221226_195217263.MP.jpg"},
		"LongName.jpg.supplemental-metad.json":                     {"LongName.jpg"},
		"LongName.jpg.s.json":                                      {"LongName.jpg"},
	}
	for in, want := range cases {
		got, _ := SidecarKeys(in)
		if len(got) != len(want) || got[0] != want[0] {
			t.Errorf("%s: got %v want %v", in, got, want)
		}
	}
}

func sc(title, url string) *Sidecar {
	s, _ := ParseSidecar([]byte(`{"title":"` + title + `","url":"` + url + `"}`))
	return s
}

func TestIndexLookup(t *testing.T) {
	ix := NewIndex()
	ix.Add("IMG_1.jpg.supplemental-metadata.json", sc("IMG_1.jpg", "https://photos.google.com/photo/AAA"))
	ix.Add("IMG_1.jpg.supplemental-metadata(1).json", sc("IMG_1.jpg", "https://photos.google.com/photo/BBB"))
	ix.Add("PXL_1.MP.jpg.supplemental-metadata.json", sc("PXL_1.MP.jpg", "https://photos.google.com/photo/CCC"))
	ix.Add("IMG_2.HEIC.supplemental-metadata.json", sc("IMG_2.HEIC", "https://photos.google.com/photo/DDD"))
	ix.Add("A_very_long_file_name_that_google_truncat.json", sc("A_very_long_file_name_that_google_truncates_badly.jpg", "https://photos.google.com/photo/EEE"))

	check := func(media, wantID, wantVariant string) {
		t.Helper()
		m, ok := ix.Lookup(media)
		if !ok {
			t.Fatalf("%s: no match", media)
		}
		if m.Sidecar.GoogleID() != wantID || m.Variant != wantVariant {
			t.Errorf("%s: got %s/%s want %s/%s", media, m.Sidecar.GoogleID(), m.Variant, wantID, wantVariant)
		}
	}
	check("IMG_1.jpg", "AAA", VariantOriginal)
	check("IMG_1(1).jpg", "BBB", VariantOriginal)
	check("IMG_1-edited.jpg", "AAA", VariantEdited)
	ix.Add("IMG_3-edited.jpg.supplemental-metadata.json", sc("IMG_3.jpg", "https://photos.google.com/photo/FFF"))
	check("IMG_3-edited.jpg", "FFF", VariantEdited) // own sidecar, still the edited variant
	check("PXL_1.MP.jpg", "CCC", VariantOriginal)
	check("PXL_1.MP", "CCC", VariantMotion)
	check("IMG_2.MOV", "DDD", VariantMotion)
	check("A_very_long_file_name_that_google_truncates_badly.jpg", "EEE", VariantOriginal)
	if _, ok := ix.Lookup("nothing.jpg"); ok {
		t.Error("unexpected match for nothing.jpg")
	}
	if u := ix.Unused(); len(u) != 0 {
		t.Errorf("unused sidecars: %d", len(u))
	}
}

func TestLookupKeys(t *testing.T) {
	keys := LookupKeys("IMG_1-edited.jpg")
	if len(keys) < 2 || keys[0].Key != "IMG_1-edited.jpg" || keys[0].Variant != VariantEdited || keys[1].Key != "IMG_1.jpg" || keys[1].Variant != VariantEdited {
		t.Errorf("edited lookup keys: %+v", keys)
	}
	keys = LookupKeys("PXL_1.MP")
	if keys[0].Variant != VariantOriginal || len(keys) < 2 || keys[1].Variant != VariantMotion {
		t.Errorf("motion lookup keys: %+v", keys)
	}
}

func TestParseAlbumMeta(t *testing.T) {
	m, err := ParseAlbumMeta([]byte(`{"title": "Mortgage payments ", "access": "protected", "date": {"timestamp": "1663091246"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if m.Title != "Mortgage payments" || m.Shared || m.DateKey() == "" {
		t.Errorf("bad parse: %+v", m)
	}
	m, err = ParseAlbumMeta([]byte(`{"albumData": {"title": "Old", "access": "protected"}}`))
	if err != nil || m.Title != "Old" {
		t.Errorf("albumData wrapper: %v %+v", err, m)
	}
	m, _ = ParseAlbumMeta([]byte(`{"title": "S", "sharedAlbumComments": [{"liked": true}]}`))
	if !m.Shared {
		t.Error("shared album not detected")
	}
}

func TestIsEdited(t *testing.T) {
	if o, e := IsEdited("IMG_1-edited.jpg"); !e || o != "IMG_1.jpg" {
		t.Errorf("got %s %v", o, e)
	}
	if o, e := IsEdited("IMG_1-edited(1).jpg"); !e || o != "IMG_1(1).jpg" {
		t.Errorf("got %s %v", o, e)
	}
	if _, e := IsEdited("IMG_1.jpg"); e {
		t.Error("false positive")
	}
}
