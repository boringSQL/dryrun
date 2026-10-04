package history

import (
	"fmt"
	"path"
	"path/filepath"
	"strings"
	"time"
)

// layout <root>/<project>/<database>/<ts>-<hash>.json.zst for FIleSystemStore
const (
	bundleTimeLayout = "20060102T150405Z"
	bundleExtension  = ".json.zst"
)

// shared by BundleDir and the default remote stream so the two can't drift
func StreamSuffix(key SnapshotKey) string {
	return path.Join(string(key.ProjectID), string(key.DatabaseID))
}

func BundleDir(root string, key SnapshotKey) string {
	return filepath.Join(root, filepath.FromSlash(StreamSuffix(key)))
}

// the <ts>-<hash> stem shared by the on-disk filename and the OCI version tag
func formatVersionedName(ts time.Time, contentHash string) string {
	return fmt.Sprintf("%s-%s", ts.UTC().Format(bundleTimeLayout), contentHash)
}

func parseVersionedName(s string) (time.Time, string, bool) {
	i := strings.IndexByte(s, '-')
	if i < 0 || i+1 >= len(s) {
		return time.Time{}, "", false
	}
	ts, err := time.Parse(bundleTimeLayout, s[:i])
	if err != nil {
		return time.Time{}, "", false
	}
	return ts, s[i+1:], true
}

func BundleFilename(ts time.Time, contentHash string) string {
	return formatVersionedName(ts, contentHash) + bundleExtension
}

// inverse of BundleFilename; returns (ts, content_hash, ok)
func ParseBundleFilename(name string) (time.Time, string, bool) {
	if !strings.HasSuffix(name, bundleExtension) {
		return time.Time{}, "", false
	}
	return parseVersionedName(strings.TrimSuffix(name, bundleExtension))
}
