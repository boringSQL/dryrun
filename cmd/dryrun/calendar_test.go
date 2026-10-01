package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/boringsql/dryrun/pkg/diff"
)

func withConfigFile(t *testing.T, body string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "dryrun.toml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	old := flagConfig
	flagConfig = path
	t.Cleanup(func() {
		flagConfig = old
		diff.SetCalendar(diff.DefaultCalendar())
	})
}

func TestApplyCalendar(t *testing.T) {
	t.Run("applies the block", func(t *testing.T) {
		withConfigFile(t, "[calendar]\nweekend = [\"fri\", \"sat\"]\ntimezone = \"Asia/Riyadh\"\n")
		if err := applyCalendar(); err != nil {
			t.Fatal(err)
		}
		if note := diff.CaveatNote(diff.CaveatWeekend); !strings.Contains(note, "Fri/Sat, Asia/Riyadh") {
			t.Errorf("calendar not applied: %s", note)
		}
	})

	t.Run("no block keeps the defaults", func(t *testing.T) {
		withConfigFile(t, "[project]\nid = \"x\"\n")
		if err := applyCalendar(); err != nil {
			t.Fatal(err)
		}
		if note := diff.CaveatNote(diff.CaveatWeekend); !strings.Contains(note, "Sat/Sun, UTC") {
			t.Errorf("default lost: %s", note)
		}
	})

	t.Run("no dryrun.toml keeps the defaults", func(t *testing.T) {
		dir := t.TempDir()
		// stop discovery walking up into a real project
		if err := os.Mkdir(filepath.Join(dir, ".git"), 0o755); err != nil {
			t.Fatal(err)
		}
		t.Chdir(dir)
		old := flagConfig
		flagConfig = ""
		t.Cleanup(func() { flagConfig = old })
		if err := applyCalendar(); err != nil {
			t.Fatalf("a project without dryrun.toml must still diff: %v", err)
		}
	})

	t.Run("a bad zone fails the command", func(t *testing.T) {
		withConfigFile(t, "[calendar]\ntimezone = \"Mars/Olympus\"\n")
		if err := applyCalendar(); err == nil {
			t.Error("want an error")
		}
	})

	t.Run("an unreadable --config fails the command", func(t *testing.T) {
		withConfigFile(t, "[calendar\n")
		if err := applyCalendar(); err == nil {
			t.Error("want an error for invalid TOML named with --config")
		}
	})
}
