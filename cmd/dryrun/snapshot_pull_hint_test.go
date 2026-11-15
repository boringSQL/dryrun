package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A missing dryrun.toml must be reported as missing; one that is present but
// unparseable must not be.
func TestResolveSyncStoreExplainsMissingStore(t *testing.T) {
	for _, tc := range []struct {
		name, toml string
		want       []string
		notWant    string
	}{
		{name: "no config", want: []string{"no dryrun.toml in ", "up to date"}},
		{name: "unparseable config", toml: "[project\nid = broken\n", want: []string{"dryrun.toml: invalid dryrun.toml"}, notWant: "no dryrun.toml in"},
		{name: "config without remote", toml: "[profiles.p]\ndatabase_id = \"d\"\n", want: []string{"pins no remote", "--remote"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetFlags(t)
			dir := t.TempDir()
			if err := os.MkdirAll(filepath.Join(dir, ".git"), 0o755); err != nil {
				t.Fatal(err)
			}
			if tc.toml != "" {
				if err := os.WriteFile(filepath.Join(dir, "dryrun.toml"), []byte(tc.toml), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			withCWD(t, dir)

			for _, push := range []bool{false, true} {
				_, err := resolveSyncStore(context.Background(), "", "", "", push)
				if err == nil {
					t.Fatalf("push=%v: expected an error", push)
				}
				for _, w := range tc.want {
					if !strings.Contains(err.Error(), w) {
						t.Errorf("push=%v: error %q missing %q", push, err, w)
					}
				}
				if tc.notWant != "" && strings.Contains(err.Error(), tc.notWant) {
					t.Errorf("push=%v: error %q must not contain %q", push, err, tc.notWant)
				}
			}
		})
	}
}
