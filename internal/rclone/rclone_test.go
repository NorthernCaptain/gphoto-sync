package rclone

import "testing"

func TestSplitRemote(t *testing.T) {
	n, p := SplitRemote("gdrive:Takeout")
	if n != "gdrive:" || p != "Takeout" {
		t.Errorf("got %q %q", n, p)
	}
	n, p = SplitRemote("gdrive")
	if n != "gdrive:" || p != "" {
		t.Errorf("got %q %q", n, p)
	}
}
