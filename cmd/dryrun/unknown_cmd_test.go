package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// testRoot mirrors main(): a root named "dryrun" carrying the real snapshot and
// profile groups, so the removed-command paths are checked against the actual
// command tree rather than a stub that could drift from it.
func testRoot() *cobra.Command {
	root := &cobra.Command{Use: "dryrun"}
	root.PersistentFlags().StringVar(&flagProfile, "profile", "", "")
	root.AddCommand(snapshotCmd(), profileCmd())
	rejectUnknownSubcommands(root)
	return root
}

// Cobra only rejects unknown subcommands at the root. Below it, `dryrun snapshot
// take` (removed in v0.19) used to print the snapshot help and exit 0, so a cron
// line that was never updated captured nothing and looked healthy. Every case here
// must either error (non-zero exit) or be a legitimate help request.
func TestRejectUnknownSubcommands(t *testing.T) {
	cases := []struct {
		name    string
		args    []string
		wantErr []string // substrings; nil means no error expected
	}{
		{"removed take", []string{"snapshot", "take"},
			[]string{"`dryrun snapshot take` was removed", "dryrun snapshot capture --label primary"}},
		// the removed commands' own flags must not fail parsing first, or the user
		// sees "unknown flag: --push" instead of what to run instead
		{"removed take with its flags", []string{"snapshot", "take", "--push", "--remote", "r"},
			[]string{"`dryrun snapshot take` was removed"}},
		{"removed activity with flags", []string{"snapshot", "activity", "--from", "postgres://x", "--label", "r1"},
			[]string{"`dryrun snapshot activity` was removed", "--streams activity"}},
		{"removed query-stats behind a persistent flag", []string{"--profile", "p", "snapshot", "query-stats", "--from=x"},
			[]string{"`dryrun snapshot query-stats` was removed", "--streams query"}},
		{"typo gets a suggestion", []string{"snapshot", "lsit"},
			[]string{`unknown command "lsit" for "dryrun snapshot"`, "list"}},
		{"unknown under another group", []string{"profile", "nope"},
			[]string{`unknown command "nope" for "dryrun profile"`}},
		{"bare group prints help", []string{"snapshot"}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := testRoot()
			var out bytes.Buffer
			root.SetOut(&out)
			root.SetErr(&out)
			root.SetArgs(tc.args)
			err := root.Execute()
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("want no error, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("want an error, got none; output:\n%s", out.String())
			}
			for _, w := range tc.wantErr {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q missing %q", err, w)
				}
			}
		})
	}
}

// Each removed path must hang off a group that still exists, and must not shadow
// a live subcommand of the same name (which would never be reached by the hint).
func TestRemovedCommandsPointAtLiveGroups(t *testing.T) {
	root := testRoot()
	for path := range removedCommands {
		parts := strings.Fields(strings.TrimPrefix(path, "dryrun "))
		parent, _, err := root.Find(parts[:len(parts)-1])
		if err != nil || parent == root {
			t.Errorf("%s: parent group not found", path)
			continue
		}
		for _, sub := range parent.Commands() {
			if sub.Name() == parts[len(parts)-1] {
				t.Errorf("%s: a live subcommand has this name", path)
			}
		}
	}
}
