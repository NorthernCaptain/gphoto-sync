package config

import (
	"strings"
	"testing"
)

// Each account's Takeout language is one of the built-in ones; English when
// not set.
func TestTakeoutLanguage(t *testing.T) {
	c := Default()
	c.Accounts = map[string]Account{"alice": {RcloneRemote: "r:"}, "jane": {RcloneRemote: "r:", Takeout: TakeoutOverride{Language: "ru"}}}
	if err := c.Validate(); err != nil {
		t.Fatalf("en and ru: %v", err)
	}
	if got := c.LanguageFor("alice"); got != "en" {
		t.Errorf("default language: %v", got)
	}
	c.Accounts["jane"] = Account{RcloneRemote: "r:", Takeout: TakeoutOverride{Language: "xx"}}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "accounts.jane.takeout.language") || !strings.Contains(err.Error(), `"xx"`) {
		t.Fatalf("unknown language: %v", err)
	}
}
