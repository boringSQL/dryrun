package main

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
)

// warnNodeIdentityDrift writes to os.Stderr; swap it for a temp file rather
// than a pipe, whose buffer would deadlock on a long message.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "stderr")
	if err != nil {
		t.Fatal(err)
	}
	orig := os.Stderr
	os.Stderr = f
	defer func() { os.Stderr = orig }()

	fn()

	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// A label names a node, but nothing forces it to keep pointing at one machine.
// Behind a reader endpoint, a k8s Service, or a `kubectl port-forward` that
// reconnects to whichever pod answers, two servers' cumulative counters append
// under a single label and every delta computed across them is wrong.
//
// The guard is a warning, never a failure, because a label may legitimately
// name a read pool where a member change is expected. That makes false
// positives the real risk: a warning that fires on every capture is one the
// operator learns to ignore, which costs more than having no warning at all.
// Most of these cases pin silence rather than output.
func TestWarnNodeIdentityDrift(t *testing.T) {
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}
	started := time.Date(2026, 8, 20, 19, 5, 7, 13394000, time.UTC)

	node := func(at time.Time, addr string) schema.NodeIdentity {
		return schema.NodeIdentity{Source: "n", PostmasterStartTime: &at, ServerAddr: addr}
	}

	t.Run("first capture under a label says nothing", func(t *testing.T) {
		w := &stubWriter{}
		out := captureStderr(t, func() {
			warnNodeIdentityDrift(context.Background(), w, key, "n", node(started, "10.0.0.1"))
		})
		if out != "" {
			t.Errorf("warned with no prior fingerprint: %s", out)
		}
	})

	t.Run("same server says nothing", func(t *testing.T) {
		w := &stubWriter{PrevStart: started.Format(time.RFC3339Nano), PrevAddr: "10.0.0.1"}
		out := captureStderr(t, func() {
			warnNodeIdentityDrift(context.Background(), w, key, "n", node(started, "10.0.0.1"))
		})
		if out != "" {
			t.Errorf("warned on an unchanged server: %s", out)
		}
	})

	// The regression this test exists for. pgx decodes timestamptz into
	// time.Local, so the stored string carries the capturing host's offset. A
	// row pulled from another zone, or a cron run with TZ unset alternating
	// with an interactive one, yields a different STRING for the same INSTANT.
	// Comparing bytes warned on every capture forever; the comparison must be
	// on instants.
	t.Run("same instant in another timezone says nothing", func(t *testing.T) {
		elsewhere := started.In(time.FixedZone("CEST", 2*3600))
		if elsewhere.Format(time.RFC3339Nano) == started.Format(time.RFC3339Nano) {
			t.Fatal("test is not exercising differing offsets")
		}
		w := &stubWriter{PrevStart: elsewhere.Format(time.RFC3339Nano), PrevAddr: "10.0.0.1"}
		out := captureStderr(t, func() {
			warnNodeIdentityDrift(context.Background(), w, key, "n", node(started, "10.0.0.1"))
		})
		if out != "" {
			t.Errorf("warned on the same instant written in another zone: %s", out)
		}
	})

	// A restart moves the start time but not the address. Reporting it in the
	// same words as a real rotation is what teaches operators to dismiss the
	// message, so the two read differently.
	t.Run("same address, new start time reads as a restart", func(t *testing.T) {
		w := &stubWriter{PrevStart: started.Format(time.RFC3339Nano), PrevAddr: "10.0.0.1"}
		out := captureStderr(t, func() {
			warnNodeIdentityDrift(context.Background(), w, key, "n", node(started.Add(time.Hour), "10.0.0.1"))
		})
		if !strings.Contains(out, "restarted") {
			t.Errorf("want a restart message, got: %s", out)
		}
		if strings.Contains(out, "mix two machines") {
			t.Errorf("a restart was reported as two machines: %s", out)
		}
	})

	t.Run("different address warns that counters mix", func(t *testing.T) {
		w := &stubWriter{PrevStart: started.Format(time.RFC3339Nano), PrevAddr: "10.0.0.1"}
		out := captureStderr(t, func() {
			warnNodeIdentityDrift(context.Background(), w, key, "n", node(started.Add(time.Hour), "10.0.0.2"))
		})
		if !strings.Contains(out, "mix two machines") {
			t.Errorf("want the two-machines warning, got: %s", out)
		}
		for _, want := range []string{"10.0.0.1", "10.0.0.2", `"n"`} {
			if !strings.Contains(out, want) {
				t.Errorf("message omits %s, leaving nothing to act on: %s", want, out)
			}
		}
	})

	// An address recorded as empty (Unix socket, or a tunnel that hides it)
	// cannot confirm a restart, so the honest answer is the general warning.
	t.Run("empty recorded address falls back to the general warning", func(t *testing.T) {
		w := &stubWriter{PrevStart: started.Format(time.RFC3339Nano), PrevAddr: ""}
		out := captureStderr(t, func() {
			warnNodeIdentityDrift(context.Background(), w, key, "n", node(started.Add(time.Hour), ""))
		})
		if !strings.Contains(out, "mix two machines") {
			t.Errorf("want the general warning when the address is unknown, got: %s", out)
		}
	})

	t.Run("older capture with no start time says nothing", func(t *testing.T) {
		w := &stubWriter{PrevStart: "", PrevAddr: "10.0.0.1"}
		out := captureStderr(t, func() {
			warnNodeIdentityDrift(context.Background(), w, key, "n", node(started, "10.0.0.2"))
		})
		if out != "" {
			t.Errorf("warned against a row that recorded no fingerprint: %s", out)
		}
	})

	// A corrupt stored value is not evidence the node moved.
	t.Run("unparseable stored value says nothing", func(t *testing.T) {
		w := &stubWriter{PrevStart: "not-a-timestamp", PrevAddr: "10.0.0.1"}
		out := captureStderr(t, func() {
			warnNodeIdentityDrift(context.Background(), w, key, "n", node(started, "10.0.0.2"))
		})
		if out != "" {
			t.Errorf("warned on a corrupt stored timestamp: %s", out)
		}
	})

	// A snapshot decoded from a peer that predates the field.
	t.Run("nil start time on the incoming capture says nothing", func(t *testing.T) {
		w := &stubWriter{PrevStart: started.Format(time.RFC3339Nano), PrevAddr: "10.0.0.1"}
		out := captureStderr(t, func() {
			warnNodeIdentityDrift(context.Background(), w, key, "n",
				schema.NodeIdentity{Source: "n", ServerAddr: "10.0.0.2"})
		})
		if out != "" {
			t.Errorf("warned without an incoming fingerprint: %s", out)
		}
	})

	// The warning is advisory; a store failure must not become capture output.
	t.Run("store error says nothing", func(t *testing.T) {
		w := &stubWriter{PrevFpErr: errors.New("database is locked")}
		out := captureStderr(t, func() {
			warnNodeIdentityDrift(context.Background(), w, key, "n", node(started, "10.0.0.1"))
		})
		if out != "" {
			t.Errorf("turned a store error into a drift warning: %s", out)
		}
	})
}
