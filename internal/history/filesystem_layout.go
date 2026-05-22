package history

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

// layout <root>/<project>/<database>/<ts>-<hash>.json.zst for FIleSystemStore
const (
	bundleTimeLayout = "20060102T150405Z"
	bundleExtension  = ".json.zst"
)

func BundleDir(root string, key SnapshotKey) string {
	return filepath.Join(root, string(key.ProjectID), string(key.DatabaseID))
}

func BundleFilename(ts time.Time, contentHash string) string {
	return fmt.Sprintf("%s-%s%s", ts.UTC().Format(bundleTimeLayout), contentHash, bundleExtension)
}

func BundlePath(root string, key SnapshotKey, ts time.Time, contentHash string) string {
	return filepath.Join(BundleDir(root, key), BundleFilename(ts, contentHash))
}

// inverse of BundleFilename; returns (ts, content_hash, ok)
func ParseBundleFilename(name string) (time.Time, string, bool) {
	if !strings.HasSuffix(name, bundleExtension) {
		return time.Time{}, "", false
	}
	stem := strings.TrimSuffix(name, bundleExtension)
	i := strings.IndexByte(stem, '-')
	if i < 0 || i+1 >= len(stem) {
		return time.Time{}, "", false
	}
	ts, err := time.Parse(bundleTimeLayout, stem[:i])
	if err != nil {
		return time.Time{}, "", false
	}
	return ts, stem[i+1:], true
}
