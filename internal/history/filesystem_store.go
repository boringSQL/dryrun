package history

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// fsBackend persists each (key, schema) as a zstd-compressed bundle file.
// Planner/activity/query rows live inside the matching bundle keyed by
// schema_ref_hash.
type fsBackend struct {
	root string
}

var _ bundleBackend = (*fsBackend)(nil)

func NewFilesystemStore(root string) (SnapshotStore, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("cannot create filesystem store root: %w", err)
	}
	return &bundleStore{backend: &fsBackend{root: root}}, nil
}

func (f *fsBackend) list(_ context.Context, key SnapshotKey) ([]*Bundle, error) {
	dir := BundleDir(f.root, key)
	entries, err := readBundleEntries(dir)
	if err != nil {
		return nil, err
	}
	out := make([]*Bundle, 0, len(entries))
	for _, e := range entries {
		b, err := readBundle(filepath.Join(dir, e.name))
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, nil
}

func (f *fsBackend) load(_ context.Context, key SnapshotKey, schemaRefHash string) (*Bundle, bool, error) {
	dir := BundleDir(f.root, key)
	entries, err := readBundleEntries(dir)
	if err != nil {
		return nil, false, err
	}
	for _, e := range entries {
		if e.contentHash != schemaRefHash {
			continue
		}
		b, err := readBundle(filepath.Join(dir, e.name))
		if err != nil {
			return nil, false, err
		}
		return b, true, nil
	}
	return nil, false, nil
}

func (f *fsBackend) save(_ context.Context, key SnapshotKey, b *Bundle) error {
	dir := BundleDir(f.root, key)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	return writeBundleAtomic(filepath.Join(dir, BundleFilename(b.Schema.Timestamp, b.Schema.ContentHash)), b)
}

func (f *fsBackend) listKeys(_ context.Context) ([]SnapshotKey, error) {
	projects, err := os.ReadDir(f.root)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	var out []SnapshotKey
	for _, p := range projects {
		if !p.IsDir() {
			continue
		}
		dbs, err := os.ReadDir(filepath.Join(f.root, p.Name()))
		if err != nil {
			return nil, err
		}
		for _, d := range dbs {
			if !d.IsDir() {
				continue
			}
			entries, err := readBundleEntries(filepath.Join(f.root, p.Name(), d.Name()))
			if err != nil {
				return nil, err
			}
			if len(entries) == 0 {
				continue
			}
			out = append(out, SnapshotKey{
				ProjectID:  ProjectId(p.Name()),
				DatabaseID: DatabaseId(d.Name()),
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ProjectID != out[j].ProjectID {
			return out[i].ProjectID < out[j].ProjectID
		}
		return out[i].DatabaseID < out[j].DatabaseID
	})
	return out, nil
}

// internal helpers

type bundleEntry struct {
	name        string
	timestamp   time.Time
	contentHash string
}

func readBundleEntries(dir string) ([]bundleEntry, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var out []bundleEntry
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		ts, hash, ok := ParseBundleFilename(f.Name())
		if !ok {
			continue
		}
		out = append(out, bundleEntry{name: f.Name(), timestamp: ts, contentHash: hash})
	}
	// newest first; sync loops and Latest expect descending order. The hash
	// tiebreak keeps latest~N stable when two bundles share a timestamp.
	sort.SliceStable(out, func(i, j int) bool {
		if !out[i].timestamp.Equal(out[j].timestamp) {
			return out[i].timestamp.After(out[j].timestamp)
		}
		return out[i].contentHash < out[j].contentHash
	})
	return out, nil
}

func readBundle(path string) (*Bundle, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	b, err := DecodeBundle(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return b, nil
}

func writeBundleAtomic(path string, b *Bundle) error {
	compressed, err := EncodeBundle(b)
	if err != nil {
		return err
	}

	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".bundle-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(compressed); err != nil {
		tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		cleanup()
		return err
	}
	return nil
}
