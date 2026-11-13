package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"

	"github.com/boringsql/dryrun/internal/buildinfo"
)

const (
	updateCheckInterval = 24 * time.Hour
	updateCheckTimeout  = 2 * time.Second
	releasesPage        = "https://github.com/boringSQL/dryrun/releases"
	latestReleaseURL    = "https://api.github.com/repos/boringSQL/dryrun/releases/latest"
)

var releaseTag = regexp.MustCompile(`^v\d+\.\d+\.\d+$`)

type updateCache struct {
	CheckedAt    time.Time `json:"checked_at"`
	Latest       string    `json:"latest"`
	LastNotified string    `json:"last_notified"`
}

type updateOpts struct {
	current   string
	cachePath string
	now       time.Time
	refresh   bool
	fetch     func(context.Context) (string, error)
}

func maybeUpdateNotice(cmd *cobra.Command) {
	path := strings.TrimSpace(strings.TrimPrefix(cmd.CommandPath(), cmd.Root().Name()))
	version := buildinfo.Get()
	// config last: only worth reading when the command would show a notice
	if !isatty.IsTerminal(os.Stderr.Fd()) || envUpdateCheckOff() || !updateNoticeEnabled(path, version) || configUpdateCheckOff() {
		return
	}
	cachePath, err := updateCachePath()
	if err != nil {
		slog.Debug("update check: cache path", "error", err)
		return
	}
	checkUpdate(cmd.Context(), cmd.ErrOrStderr(), updateOpts{
		current:   version,
		cachePath: cachePath,
		now:       time.Now().UTC(),
		refresh:   shouldRefreshUpdate(path),
		fetch: func(ctx context.Context) (string, error) {
			return fetchLatest(ctx, latestReleaseURL)
		},
	})
}

func updateNoticeEnabled(commandPath, version string) bool {
	if !validReleaseTag(version) {
		return false
	}
	switch commandPath {
	case "", "mcp-serve", "mcp", "completion", "help":
		return false
	}
	return !strings.HasPrefix(commandPath, "__complete")
}

func shouldRefreshUpdate(commandPath string) bool {
	switch commandPath {
	case "init", "probe", "dump-schema", "drift", "stats apply",
		"snapshot capture", "snapshot push", "snapshot pull":
		return true
	}
	return false
}

func checkUpdate(ctx context.Context, w io.Writer, o updateOpts) {
	cache := readUpdateCache(o.cachePath)
	changed := false

	if o.refresh && (cache.CheckedAt.IsZero() || o.now.Sub(cache.CheckedAt) >= updateCheckInterval) {
		latest, err := o.fetch(ctx)
		// advance either way, so a failing network does not retry on every command
		cache.CheckedAt = o.now
		changed = true
		if err != nil {
			slog.Debug("update check: fetch failed", "error", err)
		} else if latest != "" {
			cache.Latest = latest
		}
	}

	if cache.Latest != "" && cache.Latest != cache.LastNotified && compareVersions(cache.Latest, o.current) > 0 {
		fmt.Fprintf(w, "note: dryrun %s is available (you have %s) - %s/tag/%s\n",
			cache.Latest, o.current, releasesPage, cache.Latest)
		cache.LastNotified = cache.Latest
		changed = true
	}

	if !changed {
		return
	}
	if err := writeUpdateCache(o.cachePath, cache); err != nil {
		slog.Debug("update check: write cache", "error", err)
	}
}

func envUpdateCheckOff() bool {
	return os.Getenv("CI") != "" || os.Getenv("DRYRUN_NO_UPDATE_CHECK") != ""
}

func configUpdateCheckOff() bool {
	_, cfg, err := loadProjectConfig()
	return err == nil && cfg.Update != nil && !*cfg.Update
}

func validReleaseTag(tag string) bool {
	return releaseTag.MatchString(tag)
}

func compareVersions(a, b string) int {
	pa, pb := versionParts(a), versionParts(b)
	for i := range pa {
		switch {
		case pa[i] < pb[i]:
			return -1
		case pa[i] > pb[i]:
			return 1
		}
	}
	return 0
}

func versionParts(v string) [3]int {
	var parts [3]int
	for i, s := range strings.Split(strings.TrimPrefix(v, "v"), ".") {
		if i > 2 {
			break
		}
		parts[i], _ = strconv.Atoi(s)
	}
	return parts
}

func fetchLatest(ctx context.Context, url string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, updateCheckTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "dryrun/"+buildinfo.Get())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return "", nil
	default:
		return "", fmt.Errorf("github returned %s", resp.Status)
	}

	var release struct {
		TagName string `json:"tag_name"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&release); err != nil {
		return "", err
	}
	if !validReleaseTag(release.TagName) {
		return "", fmt.Errorf("unexpected release tag %q", release.TagName)
	}
	return release.TagName, nil
}

func updateCachePath() (string, error) {
	dir, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "dryrun", "update-check.json"), nil
}

func readUpdateCache(path string) updateCache {
	var cache updateCache
	data, err := os.ReadFile(path)
	if err != nil {
		return cache
	}
	// a torn write just costs one refetch
	_ = json.Unmarshal(data, &cache)
	return cache
}

func writeUpdateCache(path string, cache updateCache) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(cache)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}
