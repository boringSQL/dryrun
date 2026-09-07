package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// The `capture` grace period only starts once the notice ships: `take`,
// `activity` and `query-stats` must all announce their replacement, in --help
// and on stderr, in the release that introduces `capture` publicly. Without
// this the aliases can never be removed on schedule.
//
// The replacement string matters as much as the notice. Operators paste it into
// cron, so it has to be runnable as printed and capture the same streams:
//
//   - `activity` also captures query stats best-effort, so its replacement must
//     ask for activity,query or the migration silently stops feeding
//     list_top_queries;
//   - `take` writes under the hardcoded "primary" label, and capture requires
//     --label, so its replacement must name that label or the migration starts
//     a second series for the same physical node;
//   - capture refuses to run without a target, so --from/--label belong in the
//     per-node replacements.
func TestSupersededCommandsCarryDeprecationNotice(t *testing.T) {
	cases := []struct {
		name            string
		wantReplacement string
	}{
		{"take", "dryrun snapshot capture --label primary --streams schema,planner,activity,query"},
		{"activity", "dryrun snapshot capture --from <url> --label <name> --streams activity,query"},
		{"query-stats", "dryrun snapshot capture --from <url> --label <name> --streams query"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := &cobra.Command{Use: "dryrun"}
			root.AddCommand(snapshotCmd())

			snap, _, err := root.Find([]string{"snapshot", tc.name})
			if err != nil {
				t.Fatalf("find snapshot %s: %v", tc.name, err)
			}

			if !strings.HasSuffix(snap.Short, deprecatedSuffix) {
				t.Errorf("Short = %q, want it to end with %q", snap.Short, deprecatedSuffix)
			}
			if !strings.Contains(snap.Long, "removed in v0.18") {
				t.Errorf("Long = %q, want a removal version", snap.Long)
			}
			// help and the runtime notice must not drift apart
			if !strings.Contains(snap.Long, tc.wantReplacement) {
				t.Errorf("Long = %q, want the replacement %q", snap.Long, tc.wantReplacement)
			}

			if snap.PreRun != nil {
				t.Error("PreRun is set; cobra runs PreRunE instead, so it would never fire")
			}
			if snap.PreRunE == nil {
				t.Fatal("no PreRunE, so nothing warns at runtime")
			}
			var stderr bytes.Buffer
			snap.SetErr(&stderr)
			if err := snap.PreRunE(snap, nil); err != nil {
				t.Fatalf("PreRunE: %v", err)
			}
			got := stderr.String()
			if !strings.Contains(got, "is deprecated") || !strings.Contains(got, tc.wantReplacement) {
				t.Errorf("stderr = %q, want a deprecation notice naming %q", got, tc.wantReplacement)
			}
			// CommandPath needs the parent wiring to name the command in full
			if !strings.Contains(got, "dryrun snapshot "+tc.name) {
				t.Errorf("stderr = %q, want the full command path", got)
			}
		})
	}
}

// `capture` is the replacement, not a deprecated alias.
func TestCaptureCarriesNoDeprecationNotice(t *testing.T) {
	root := &cobra.Command{Use: "dryrun"}
	root.AddCommand(snapshotCmd())

	snap, _, err := root.Find([]string{"snapshot", "capture"})
	if err != nil {
		t.Fatalf("find snapshot capture: %v", err)
	}
	if strings.Contains(snap.Short, "deprecated") || strings.Contains(snap.Long, "Deprecated") {
		t.Errorf("capture is marked deprecated: %q / %q", snap.Short, snap.Long)
	}
}

// --help must stay quiet: cobra returns flag.ErrHelp before PreRunE, and the
// deprecated commands must stay listed in `snapshot --help` for the whole grace
// period (which is why cobra's own Deprecated field is not used).
func TestDeprecatedCommandsStayVisibleAndQuietInHelp(t *testing.T) {
	for _, name := range []string{"take", "activity", "query-stats"} {
		root := &cobra.Command{Use: "dryrun"}
		root.AddCommand(snapshotCmd())

		var stdout, stderr bytes.Buffer
		root.SetOut(&stdout)
		root.SetErr(&stderr)
		root.SetArgs([]string{"snapshot", name, "--help"})
		if err := root.Execute(); err != nil {
			t.Fatalf("%s --help: %v", name, err)
		}
		if stderr.Len() != 0 {
			t.Errorf("%s --help wrote to stderr: %q", name, stderr.String())
		}

		snap, _, err := root.Find([]string{"snapshot", name})
		if err != nil {
			t.Fatalf("find snapshot %s: %v", name, err)
		}
		if !snap.IsAvailableCommand() {
			t.Errorf("%s is hidden from `snapshot --help` during its grace period", name)
		}
	}
}

// The notice chains rather than replaces, for either hook shape.
func TestMarkCaptureSupersededKeepsExistingPreRun(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func(*cobra.Command, *bool)
	}{
		{"PreRunE", func(c *cobra.Command, hit *bool) {
			c.PreRunE = func(*cobra.Command, []string) error { *hit = true; return nil }
		}},
		{"PreRun", func(c *cobra.Command, hit *bool) {
			c.PreRun = func(*cobra.Command, []string) { *hit = true }
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var called bool
			cmd := &cobra.Command{Use: "activity", Short: "s"}
			tc.set(cmd, &called)
			markCaptureSuperseded(cmd, "dryrun snapshot capture --streams activity")
			cmd.SetErr(&bytes.Buffer{})
			if err := cmd.PreRunE(cmd, nil); err != nil {
				t.Fatalf("PreRunE: %v", err)
			}
			if !called {
				t.Errorf("original %s was not called", tc.name)
			}
		})
	}
}

// Applying the mark twice must not stutter the Short line.
func TestMarkCaptureSupersededIsIdempotent(t *testing.T) {
	cmd := &cobra.Command{Use: "activity", Short: "Capture activity stats"}
	markCaptureSuperseded(cmd, "dryrun snapshot capture --streams activity")
	first := cmd.Short
	markCaptureSuperseded(cmd, "dryrun snapshot capture --streams activity")
	if cmd.Short != first {
		t.Errorf("Short = %q, want %q", cmd.Short, first)
	}
}
