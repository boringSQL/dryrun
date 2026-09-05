package config

import (
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/history"
)

func parseNodes(t *testing.T, toml string) *ProjectConfig {
	t.Helper()
	cfg, err := Parse(toml)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return cfg
}

func TestResolveNodes(t *testing.T) {
	t.Setenv("PRIMARY_URL", "postgres://u@primary/db")
	t.Setenv("REPLICA_URL", "postgres://u@replica/db")

	cfg := parseNodes(t, `
[project]
id = "acme"

[[node]]
name = "primary"
role = "primary"
url_env = "PRIMARY_URL"
streams = ["planner", "activity", "query"]
interval = "1h"

[[node]]
name = "replica-eu"
role = "standby"
url_env = "REPLICA_URL"
streams = ["activity", "query"]
interval = "30m"

[[node]]
name = "read-pool"
url_env = "REPLICA_URL"
pool = true
`)

	nodes, err := cfg.ResolveNodes()
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 3 {
		t.Fatalf("got %d nodes, want 3", len(nodes))
	}

	t.Run("sorted by name", func(t *testing.T) {
		want := []string{"primary", "read-pool", "replica-eu"}
		for i, n := range nodes {
			if n.Name != want[i] {
				t.Errorf("node %d is %q, want %q", i, n.Name, want[i])
			}
		}
	})

	t.Run("url comes from the environment, not the file", func(t *testing.T) {
		got, err := nodes[0].URL()
		if err != nil {
			t.Fatal(err)
		}
		if got != "postgres://u@primary/db" {
			t.Errorf("url %q", got)
		}
	})

	t.Run("interval parsed", func(t *testing.T) {
		if nodes[0].Interval != time.Hour {
			t.Errorf("interval %s, want 1h", nodes[0].Interval)
		}
		if nodes[2].Interval != 30*time.Minute {
			t.Errorf("interval %s, want 30m", nodes[2].Interval)
		}
	})

	t.Run("role defaults to auto", func(t *testing.T) {
		if nodes[1].Role != "auto" {
			t.Errorf("role %q, want auto when the block omits it", nodes[1].Role)
		}
	})

	t.Run("pool flag survives", func(t *testing.T) {
		if !nodes[1].Pool {
			t.Error("pool = true was dropped")
		}
		if nodes[0].Pool {
			t.Error("pool set on a node that did not declare it")
		}
	})

	t.Run("omitted streams stay nil so capture decides by role", func(t *testing.T) {
		if nodes[1].Streams != nil {
			t.Errorf("streams %v, want nil", nodes[1].Streams)
		}
	})
}

func TestResolveNodes_Errors(t *testing.T) {
	cases := []struct {
		name    string
		toml    string
		wantErr string
	}{
		{
			name:    "no name",
			toml:    "[[node]]\nurl = \"postgres://x\"\n",
			wantErr: "name is required",
		},
		{
			name:    "duplicate name",
			toml:    "[[node]]\nname = \"a\"\nurl = \"postgres://x\"\n\n[[node]]\nname = \"a\"\nurl = \"postgres://y\"\n",
			wantErr: "defined twice",
		},
		{
			name:    "no url at all",
			toml:    "[[node]]\nname = \"a\"\n",
			wantErr: "url or url_env is required",
		},
		{
			name:    "a standby declaring schema",
			toml:    "[[node]]\nname = \"a\"\nurl = \"postgres://x\"\nrole = \"standby\"\nstreams = [\"schema\"]\n",
			wantErr: "a standby cannot originate a schema",
		},
		{
			name:    "both url and url_env",
			toml:    "[[node]]\nname = \"a\"\nurl = \"postgres://x\"\nurl_env = \"SOME_URL\"\n",
			wantErr: "not both",
		},
		{
			name:    "unknown role",
			toml:    "[[node]]\nname = \"a\"\nurl = \"postgres://x\"\nrole = \"leader\"\n",
			wantErr: "want primary, standby, or auto",
		},
		{
			name:    "unknown stream",
			toml:    "[[node]]\nname = \"a\"\nurl = \"postgres://x\"\nstreams = [\"activity\", \"vacuum\"]\n",
			wantErr: "want one of",
		},
		{
			name:    "unparseable interval",
			toml:    "[[node]]\nname = \"a\"\nurl = \"postgres://x\"\ninterval = \"every hour\"\n",
			wantErr: "interval",
		},
		{
			name:    "zero interval",
			toml:    "[[node]]\nname = \"a\"\nurl = \"postgres://x\"\ninterval = \"0s\"\n",
			wantErr: "must be positive",
		},
		{
			// a typo would silently lose the override, so it fails at load
			name:    "per-stream interval for an unknown stream",
			toml:    "[[node]]\nname = \"a\"\nurl = \"postgres://x\"\n[node.intervals]\nvacuum = \"1h\"\n",
			wantErr: "intervals.vacuum: stream \"vacuum\"",
		},
		{
			name:    "unparseable per-stream interval",
			toml:    "[[node]]\nname = \"a\"\nurl = \"postgres://x\"\n[node.intervals]\nquery = \"nightly\"\n",
			wantErr: "intervals.query",
		},
		{
			name:    "zero per-stream interval",
			toml:    "[[node]]\nname = \"a\"\nurl = \"postgres://x\"\n[node.intervals]\nquery = \"0s\"\n",
			wantErr: "must be positive",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := parseNodes(t, tc.toml)
			_, err := cfg.ResolveNodes()
			if err == nil {
				t.Fatalf("want an error containing %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not mention %q", err, tc.wantErr)
			}
		})
	}
}

// A cron host that lost one variable must still capture every other node, so
// the URL is resolved at the point of use rather than when the config loads.
func TestResolveNodes_UnsetURLEnv(t *testing.T) {
	t.Setenv("PRESENT_URL", "postgres://u@h/db")
	cfg := parseNodes(t, `
[[node]]
name = "ok"
url_env = "PRESENT_URL"

[[node]]
name = "broken"
url_env = "DEFINITELY_UNSET_URL"
`)
	nodes, err := cfg.ResolveNodes()
	if err != nil {
		t.Fatalf("one unset variable failed the whole fleet: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("got %d nodes, want both", len(nodes))
	}
	if _, err := nodes[1].URL(); err != nil {
		t.Fatalf("the good node could not resolve: %v", err)
	}
	if _, err := nodes[0].URL(); err == nil || !strings.Contains(err.Error(), "unset") {
		t.Errorf("want an unset-env error from the broken node, got %v", err)
	}
}

func TestResolvedNode_URLExpansion(t *testing.T) {
	t.Setenv("PGURL", "postgres://u@h/db")
	cfg := parseNodes(t, "[[node]]\nname = \"a\"\nurl = \"${PGURL}\"\n")
	nodes, err := cfg.ResolveNodes()
	if err != nil {
		t.Fatal(err)
	}
	got, err := nodes[0].URL()
	if err != nil {
		t.Fatal(err)
	}
	if got != "postgres://u@h/db" {
		t.Errorf("url %q, want the expanded value", got)
	}

	// a reference that expands to nothing is a misconfiguration, not a URL
	cfg2 := parseNodes(t, "[[node]]\nname = \"a\"\nurl = \"${DEFINITELY_UNSET_PGURL}\"\n")
	n2, err := cfg2.ResolveNodes()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := n2[0].URL(); err == nil || !strings.Contains(err.Error(), "expanded to nothing") {
		t.Errorf("want an expansion error, got %v", err)
	}
}

func TestResolveNode_ByName(t *testing.T) {
	t.Setenv("U", "postgres://u@h/db")
	cfg := parseNodes(t, "[[node]]\nname = \"primary\"\nurl_env = \"U\"\n\n[[node]]\nname = \"replica\"\nurl_env = \"U\"\n")

	if n, err := cfg.ResolveNode("replica"); err != nil || n.Name != "replica" {
		t.Fatalf("got %+v err=%v", n, err)
	}
	_, err := cfg.ResolveNode("nope")
	if err == nil {
		t.Fatal("want an error for an unknown node")
	}
	// the error has to list what does exist, or the user has to open the file
	for _, want := range []string{"primary", "replica"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not list %q", err, want)
		}
	}
}

func TestResolveNode_NoBlocks(t *testing.T) {
	cfg := parseNodes(t, "[project]\nid = \"acme\"\n")
	if _, err := cfg.ResolveNode("primary"); err == nil {
		t.Fatal("want an error when no [[node]] blocks are defined")
	}
	nodes, err := cfg.ResolveNodes()
	if err != nil || len(nodes) != 0 {
		t.Errorf("got %v err=%v, want an empty list and no error", nodes, err)
	}
}

func TestDefaultStreamsFor(t *testing.T) {
	// a standby has no schema of its own and its planner stats mirror the
	// primary's, so capturing them there is duplicate work
	if got := DefaultStreamsFor("standby"); hasString(got, "planner") || hasString(got, "schema") {
		t.Errorf("standby defaults %v include primary-only streams", got)
	}
	// a primary captures all four, schema included
	for _, want := range []string{"schema", "planner", "activity", "query"} {
		if !hasString(DefaultStreamsFor("primary"), want) {
			t.Errorf("primary defaults %v omit %s", DefaultStreamsFor("primary"), want)
		}
	}
	// schema writes the hash the others bind to, so it leads
	if got := DefaultStreamsFor("primary"); got[0] != "schema" {
		t.Errorf("primary defaults %v do not lead with schema", got)
	}
}

// A stream config accepts but history cannot resolve aborts the node on --due
// (LastCaptureAt errors) while its attempt marker only logs a warning -- two
// failure modes for one omission. Adding a stream here means adding it to
// history.streamSources in the same change.
func TestKnownStreamsResolveInHistory(t *testing.T) {
	for _, s := range knownStreams {
		if !history.StreamKnown(s) {
			t.Errorf("stream %q is configurable but history has no source table for it", s)
		}
	}
}

// [node.intervals] gives one node two cadences: cheap stats often, expensive
// schema rarely, from a single cron line.
func TestResolveNodes_PerStreamIntervals(t *testing.T) {
	cfg := parseNodes(t, `
[[node]]
name = "primary"
url = "postgres://x"
interval = "5m"
[node.intervals]
query = "1h"
activity = "15m"
`)
	nodes, err := cfg.ResolveNodes()
	if err != nil {
		t.Fatalf("ResolveNodes: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("want 1 node, got %d", len(nodes))
	}
	n := nodes[0]
	if n.Interval != 5*time.Minute {
		t.Errorf("base interval = %s, want 5m", n.Interval)
	}
	want := map[string]time.Duration{"query": time.Hour, "activity": 15 * time.Minute}
	if len(n.Intervals) != len(want) {
		t.Fatalf("intervals = %v, want %v", n.Intervals, want)
	}
	for k, v := range want {
		if n.Intervals[k] != v {
			t.Errorf("intervals[%q] = %s, want %s", k, n.Intervals[k], v)
		}
	}
}

// A block naming none leaves the map nil, so the capture falls through to the
// base interval (and, for schema, the built-in floor).
func TestResolveNodes_NoPerStreamIntervals(t *testing.T) {
	cfg := parseNodes(t, "[[node]]\nname = \"a\"\nurl = \"postgres://x\"\ninterval = \"5m\"\n")
	nodes, err := cfg.ResolveNodes()
	if err != nil {
		t.Fatalf("ResolveNodes: %v", err)
	}
	if nodes[0].Intervals != nil {
		t.Errorf("intervals = %v, want nil", nodes[0].Intervals)
	}
}
