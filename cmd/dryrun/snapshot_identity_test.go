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
// pg_postmaster_start_time() also changes on every restart, so a *change* is
// not evidence of rotation. Oscillation is: a fingerprint recurring after a
// different one intervened (A->B->A). Oscillation warns; a one-way change is
// only a notice, because it is equally a restart or a genuine node
// replacement, and rendering the two alike is what teaches operators to
// dismiss both. It never fails: a label may legitimately name a pool.
func TestWarnNodeIdentityDrift(t *testing.T) {
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}
	a := time.Date(2026, 8, 20, 19, 5, 7, 13394000, time.UTC)
	b := a.Add(3 * time.Hour)

	node := func(at time.Time, addr string) schema.NodeIdentity {
		return schema.NodeIdentity{Source: "n", PostmasterStartTime: &at, ServerAddr: addr}
	}
	fp := func(at time.Time, addr string) history.NodeFingerprint {
		return history.NodeFingerprint{StartedAt: at, ServerAddr: addr}
	}
	warn := func(t *testing.T, w *stubWriter, n schema.NodeIdentity) string {
		t.Helper()
		return captureStderr(t, func() {
			warnNodeIdentityDrift(context.Background(), w, key, "n", n, false)
		})
	}

	t.Run("first capture under a label says nothing", func(t *testing.T) {
		if out := warn(t, &stubWriter{}, node(a, "10.0.0.1")); out != "" {
			t.Errorf("warned with no prior fingerprint: %s", out)
		}
	})

	t.Run("same server says nothing", func(t *testing.T) {
		w := &stubWriter{PrevSeen: []history.NodeFingerprint{fp(a, "10.0.0.1")}}
		if out := warn(t, w, node(a, "10.0.0.1")); out != "" {
			t.Errorf("warned on an unchanged server: %s", out)
		}
	})

	// pgx decodes timestamptz into time.Local, so a row pulled from another
	// zone holds a different string for the same instant. Comparing strings
	// warned on every capture forever.
	t.Run("same instant in another timezone says nothing", func(t *testing.T) {
		elsewhere := a.In(time.FixedZone("CEST", 2*3600))
		w := &stubWriter{PrevSeen: []history.NodeFingerprint{fp(elsewhere, "10.0.0.1")}}
		if out := warn(t, w, node(a, "10.0.0.1")); out != "" {
			t.Errorf("warned on the same instant in another zone: %s", out)
		}
	})

	// A->B->A: the rotating endpoint. The only case that warns.
	t.Run("oscillation warns", func(t *testing.T) {
		w := &stubWriter{PrevSeen: []history.NodeFingerprint{
			fp(b, "10.0.0.2"), fp(a, "10.0.0.1"), fp(b, "10.0.0.2"),
		}}
		out := warn(t, w, node(a, "10.0.0.1"))
		if !strings.HasPrefix(out, "warning:") {
			t.Errorf("oscillation must warn, got: %s", out)
		}
		if !strings.Contains(out, "alternating") {
			t.Errorf("message does not name the failure: %s", out)
		}
		if !strings.Contains(out, "2 distinct") {
			t.Errorf("want 2 distinct servers counted, got: %s", out)
		}
	})

	// A->B and never back: a replacement or a restart that moved address.
	// Previously this printed the alarming "mix two machines" warning.
	t.Run("one-way change is a notice, not a warning", func(t *testing.T) {
		w := &stubWriter{PrevSeen: []history.NodeFingerprint{fp(a, "10.0.0.1")}}
		out := warn(t, w, node(b, "10.0.0.2"))
		if !strings.HasPrefix(out, "notice:") {
			t.Errorf("a one-way change must not warn, got: %s", out)
		}
		for _, want := range []string{"10.0.0.1", "10.0.0.2"} {
			if !strings.Contains(out, want) {
				t.Errorf("notice omits %s: %s", want, out)
			}
		}
	})

	t.Run("same address, new start time reads as a restart", func(t *testing.T) {
		w := &stubWriter{PrevSeen: []history.NodeFingerprint{fp(a, "10.0.0.1")}}
		out := warn(t, w, node(b, "10.0.0.1"))
		if !strings.HasPrefix(out, "notice:") || !strings.Contains(out, "restarted") {
			t.Errorf("want a restart notice, got: %s", out)
		}
	})

	// An address is NULL over a Unix socket and identical for every member
	// behind a tunnel, so it never decides identity -- only wording.
	t.Run("unknown address still classifies by start time", func(t *testing.T) {
		w := &stubWriter{PrevSeen: []history.NodeFingerprint{fp(a, "")}}
		out := warn(t, w, node(b, ""))
		if !strings.HasPrefix(out, "notice:") {
			t.Errorf("want a notice, got: %s", out)
		}
		if !strings.Contains(out, "unknown") {
			t.Errorf("want the empty address rendered as unknown: %s", out)
		}
	})

	// Oscillation must be detected on start time alone, even when every
	// member reports the same address (a port-forward hides it).
	t.Run("oscillation behind one address still warns", func(t *testing.T) {
		w := &stubWriter{PrevSeen: []history.NodeFingerprint{
			fp(b, "127.0.0.1"), fp(a, "127.0.0.1"),
		}}
		if out := warn(t, w, node(a, "127.0.0.1")); !strings.HasPrefix(out, "warning:") {
			t.Errorf("want a warning despite one shared address, got: %s", out)
		}
	})

	t.Run("nil start time on the incoming capture says nothing", func(t *testing.T) {
		w := &stubWriter{PrevSeen: []history.NodeFingerprint{fp(a, "10.0.0.1")}}
		out := warn(t, w, schema.NodeIdentity{Source: "n", ServerAddr: "10.0.0.2"})
		if out != "" {
			t.Errorf("warned without an incoming fingerprint: %s", out)
		}
	})

	// the loop over seen[1:] must not match anything here: three servers, no
	// recurrence. A single-row window would never exercise that path.
	t.Run("three distinct servers with no recurrence is a notice", func(t *testing.T) {
		c := b.Add(3 * time.Hour)
		w := &stubWriter{PrevSeen: []history.NodeFingerprint{
			fp(c, "10.0.0.3"), fp(b, "10.0.0.2"),
		}}
		if out := warn(t, w, node(a, "10.0.0.1")); !strings.HasPrefix(out, "notice:") {
			t.Errorf("a one-way A->B->C must not warn, got: %s", out)
		}
	})

	// unchanged since the last capture, even though the label oscillated
	// earlier: nothing new happened, so nothing is said
	t.Run("unchanged head stays silent over an oscillating tail", func(t *testing.T) {
		w := &stubWriter{PrevSeen: []history.NodeFingerprint{
			fp(a, "10.0.0.1"), fp(b, "10.0.0.2"), fp(a, "10.0.0.1"),
		}}
		if out := warn(t, w, node(a, "10.0.0.1")); out != "" {
			t.Errorf("warned when nothing changed since the last capture: %s", out)
		}
	})

	// a label that names a pool: rotation is the expected state
	t.Run("--allow-rotation suppresses the oscillation warning", func(t *testing.T) {
		w := &stubWriter{PrevSeen: []history.NodeFingerprint{
			fp(b, "10.0.0.2"), fp(a, "10.0.0.1"),
		}}
		out := captureStderr(t, func() {
			warnNodeIdentityDrift(context.Background(), w, key, "n", node(a, "10.0.0.1"), true)
		})
		if out != "" {
			t.Errorf("warned on a declared pool: %s", out)
		}
	})

	// it silences rotation, not everything: a replacement is still worth saying
	t.Run("--allow-rotation still notices a one-way change", func(t *testing.T) {
		w := &stubWriter{PrevSeen: []history.NodeFingerprint{fp(a, "10.0.0.1")}}
		out := captureStderr(t, func() {
			warnNodeIdentityDrift(context.Background(), w, key, "n", node(b, "10.0.0.2"), true)
		})
		if !strings.HasPrefix(out, "notice:") {
			t.Errorf("want the one-way notice, got: %s", out)
		}
	})

	t.Run("store error says nothing", func(t *testing.T) {
		w := &stubWriter{PrevFpErr: errors.New("database is locked")}
		if out := warn(t, w, node(a, "10.0.0.1")); out != "" {
			t.Errorf("turned a store error into drift output: %s", out)
		}
	})
}
