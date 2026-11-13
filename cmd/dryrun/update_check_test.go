package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestValidReleaseTag(t *testing.T) {
	tests := map[string]bool{
		"v0.23.0":        true,
		"v10.0.1":        true,
		"0.23.0":         false,
		"v0.23":          false,
		"v0.23.0-rc.1":   false,
		"v0.23.0+build":  false,
		"v0.23.0.1":      false,
		"dev":            false,
		"v0.23.0-0.2025": false,
	}
	for tag, want := range tests {
		if got := validReleaseTag(tag); got != want {
			t.Errorf("validReleaseTag(%q) = %t, want %t", tag, got, want)
		}
	}
}

func TestCompareVersions(t *testing.T) {
	tests := []struct {
		a, b string
		want int
	}{
		{"v0.23.0", "v0.22.0", 1},
		{"v0.22.0", "v0.23.0", -1},
		{"v0.23.0", "v0.23.0", 0},
		{"v0.9.0", "v0.10.0", -1},
		{"v1.0.0", "v0.99.99", 1},
		{"v0.23.1", "v0.23.0", 1},
	}
	for _, tc := range tests {
		if got := compareVersions(tc.a, tc.b); got != tc.want {
			t.Errorf("compareVersions(%q, %q) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestFetchLatest(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		body    string
		want    string
		wantErr bool
	}{
		{"release", http.StatusOK, `{"tag_name":"v0.23.0"}`, "v0.23.0", false},
		{"no releases", http.StatusNotFound, `{"message":"Not Found"}`, "", false},
		{"forbidden", http.StatusForbidden, `{"message":"rate limited"}`, "", true},
		{"malformed", http.StatusOK, `{`, "", true},
		{"prerelease tag", http.StatusOK, `{"tag_name":"v0.23.0-rc.1"}`, "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.Header.Get("Accept"); got != "application/vnd.github+json" {
					t.Errorf("Accept = %q", got)
				}
				if got := r.Header.Get("User-Agent"); !strings.HasPrefix(got, "dryrun/") {
					t.Errorf("User-Agent = %q", got)
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()

			got, err := fetchLatest(context.Background(), srv.URL+"/releases/latest")
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %t", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestUpdateCache(t *testing.T) {
	want := updateCache{
		CheckedAt:    time.Date(2026, 11, 13, 9, 12, 0, 0, time.UTC),
		Latest:       "v0.23.0",
		LastNotified: "v0.22.5",
	}

	t.Run("round trip", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "update-check.json")
		if err := writeUpdateCache(path, want); err != nil {
			t.Fatal(err)
		}
		got := readUpdateCache(path)
		if !got.CheckedAt.Equal(want.CheckedAt) || got.Latest != want.Latest || got.LastNotified != want.LastNotified {
			t.Errorf("got %+v, want %+v", got, want)
		}
	})

	t.Run("missing file", func(t *testing.T) {
		if got := readUpdateCache(filepath.Join(t.TempDir(), "absent.json")); got != (updateCache{}) {
			t.Errorf("got %+v, want empty", got)
		}
	})

	t.Run("corrupt file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "update-check.json")
		if err := os.WriteFile(path, []byte("not json"), 0o644); err != nil {
			t.Fatal(err)
		}
		if got := readUpdateCache(path); got != (updateCache{}) {
			t.Errorf("got %+v, want empty", got)
		}
	})

	t.Run("creates directory", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "a", "b", "update-check.json")
		if err := writeUpdateCache(path, want); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatal(err)
		}
	})
}

func TestUpdateGating(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		version string
		want    bool
	}{
		{"lint", "lint", "v0.22.0", true},
		{"version command", "version", "v0.22.0", true},
		{"snapshot capture", "snapshot capture", "v0.22.0", true},
		{"dev build", "lint", "dev", false},
		{"pseudo version", "lint", "v0.0.0-20251113091200-abcdef123456", false},
		{"bare dryrun", "", "v0.22.0", false},
		{"mcp-serve", "mcp-serve", "v0.22.0", false},
		{"mcp alias", "mcp", "v0.22.0", false},
		{"completion", "completion", "v0.22.0", false},
		{"help", "help", "v0.22.0", false},
		{"__complete", "__complete", "v0.22.0", false},
		{"__completeNoDesc", "__completeNoDesc", "v0.22.0", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := updateNoticeEnabled(tc.path, tc.version); got != tc.want {
				t.Errorf("updateNoticeEnabled(%q, %q) = %t, want %t", tc.path, tc.version, got, tc.want)
			}
		})
	}
}

func TestCheckUpdate(t *testing.T) {
	now := time.Date(2026, 11, 13, 9, 12, 0, 0, time.UTC)
	notice := func(v string) string {
		return "note: dryrun " + v + " is available (you have v0.22.0) - https://github.com/boringSQL/dryrun/releases/tag/" + v + "\n"
	}
	newCachePath := func(t *testing.T, cache updateCache) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "update-check.json")
		if err := writeUpdateCache(path, cache); err != nil {
			t.Fatal(err)
		}
		return path
	}
	noFetch := func(t *testing.T) func(context.Context) (string, error) {
		return func(context.Context) (string, error) {
			t.Fatal("fetch called, want no refresh")
			return "", nil
		}
	}

	t.Run("refresh stale cache and notify", func(t *testing.T) {
		path := newCachePath(t, updateCache{CheckedAt: now.Add(-25 * time.Hour)})
		var out bytes.Buffer
		checkUpdate(context.Background(), &out, updateOpts{
			current:   "v0.22.0",
			cachePath: path,
			now:       now,
			refresh:   true,
			fetch:     func(context.Context) (string, error) { return "v0.23.0", nil },
		})
		if got := out.String(); got != notice("v0.23.0") {
			t.Errorf("output = %q, want %q", got, notice("v0.23.0"))
		}
		got := readUpdateCache(path)
		if !got.CheckedAt.Equal(now) || got.Latest != "v0.23.0" || got.LastNotified != "v0.23.0" {
			t.Errorf("cache = %+v", got)
		}
	})

	t.Run("no refresh within 24h but cached notice prints", func(t *testing.T) {
		path := newCachePath(t, updateCache{CheckedAt: now.Add(-time.Hour), Latest: "v0.23.0"})
		var out bytes.Buffer
		checkUpdate(context.Background(), &out, updateOpts{
			current:   "v0.22.0",
			cachePath: path,
			now:       now,
			refresh:   true,
			fetch:     noFetch(t),
		})
		if got := out.String(); got != notice("v0.23.0") {
			t.Errorf("output = %q, want %q", got, notice("v0.23.0"))
		}
	})

	t.Run("no refresh on other commands", func(t *testing.T) {
		path := newCachePath(t, updateCache{Latest: "v0.23.0"})
		var out bytes.Buffer
		checkUpdate(context.Background(), &out, updateOpts{
			current:   "v0.22.0",
			cachePath: path,
			now:       now,
			fetch:     noFetch(t),
		})
		if got := out.String(); got != notice("v0.23.0") {
			t.Errorf("output = %q, want %q", got, notice("v0.23.0"))
		}
	})

	t.Run("notifies once per version", func(t *testing.T) {
		path := newCachePath(t, updateCache{Latest: "v0.23.0", LastNotified: "v0.23.0"})
		var out bytes.Buffer
		checkUpdate(context.Background(), &out, updateOpts{
			current:   "v0.22.0",
			cachePath: path,
			now:       now,
			fetch:     noFetch(t),
		})
		if out.Len() != 0 {
			t.Errorf("output = %q, want none", out.String())
		}
	})

	t.Run("new version notifies again", func(t *testing.T) {
		path := newCachePath(t, updateCache{Latest: "v0.24.0", LastNotified: "v0.23.0"})
		var out bytes.Buffer
		checkUpdate(context.Background(), &out, updateOpts{
			current:   "v0.22.0",
			cachePath: path,
			now:       now,
			fetch:     noFetch(t),
		})
		if got := out.String(); got != notice("v0.24.0") {
			t.Errorf("output = %q, want %q", got, notice("v0.24.0"))
		}
	})

	t.Run("fetch failure keeps latest and advances checked_at", func(t *testing.T) {
		path := newCachePath(t, updateCache{
			CheckedAt:    now.Add(-48 * time.Hour),
			Latest:       "v0.23.0",
			LastNotified: "v0.23.0",
		})
		var out bytes.Buffer
		checkUpdate(context.Background(), &out, updateOpts{
			current:   "v0.22.0",
			cachePath: path,
			now:       now,
			refresh:   true,
			fetch:     func(context.Context) (string, error) { return "", errors.New("offline") },
		})
		if out.Len() != 0 {
			t.Errorf("output = %q, want none", out.String())
		}
		got := readUpdateCache(path)
		if !got.CheckedAt.Equal(now) || got.Latest != "v0.23.0" || got.LastNotified != "v0.23.0" {
			t.Errorf("cache = %+v", got)
		}
	})

	t.Run("equal or older version does not notify", func(t *testing.T) {
		path := newCachePath(t, updateCache{Latest: "v0.22.0"})
		var out bytes.Buffer
		checkUpdate(context.Background(), &out, updateOpts{
			current:   "v0.22.0",
			cachePath: path,
			now:       now,
			fetch:     noFetch(t),
		})
		if out.Len() != 0 {
			t.Errorf("output = %q, want none", out.String())
		}
	})

	t.Run("nothing written when unchanged", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "update-check.json")
		var out bytes.Buffer
		checkUpdate(context.Background(), &out, updateOpts{
			current:   "v0.22.0",
			cachePath: path,
			now:       now,
			fetch:     noFetch(t),
		})
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("cache written, want untouched: %v", err)
		}
	})
}
