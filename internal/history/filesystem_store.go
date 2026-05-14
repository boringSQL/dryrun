package history

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/boringsql/dryrun/internal/schema"
)

// FilesystemStore persists each (key, schema) as a zstd-compressed bundle
// file. Planner/activity rows live inside the matching bundle keyed by
// schema_ref_hash; cross-store sync uses this wire format end-to-end.
type FilesystemStore struct {
	root string
	mu   sync.Mutex
}

func NewFilesystemStore(root string) (*FilesystemStore, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("cannot create filesystem store root: %w", err)
	}
	return &FilesystemStore{root: root}, nil
}

// Bundle is the on-disk JSON shape. Field names and nullability mirror
// Rust's dry_run_core::history::Bundle so cross-implementation sync stays
// byte-compatible.
type Bundle struct {
	Schema   *schema.SchemaSnapshot                      `json:"schema"`
	Planner  *schema.PlannerStatsSnapshot                `json:"planner"`
	Activity map[string]*schema.ActivityStatsSnapshot    `json:"activity"`
}

var (
	// putting planner or activity without a matching schema bundle is rejected;
	// the bundle is keyed by schema_ref_hash and must exist first.
	ErrOrphanSnapshot = errors.New("no schema bundle matches schema_ref_hash")
)

func (f *FilesystemStore) Put(ctx context.Context, key SnapshotKey, snap StoredSnapshot) (PutOutcome, error) {
	switch {
	case snap.AsSchema() != nil:
		return f.putSchema(ctx, key, snap.AsSchema())
	case snap.AsPlanner() != nil:
		return f.putPlanner(ctx, key, snap.AsPlanner())
	case snap.AsActivity() != nil:
		return f.putActivity(ctx, key, snap.AsActivity())
	}
	return PutInserted, fmt.Errorf("empty StoredSnapshot")
}

func (f *FilesystemStore) putSchema(_ context.Context, key SnapshotKey, snap *schema.SchemaSnapshot) (PutOutcome, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	dir := BundleDir(f.root, key)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return PutInserted, err
	}

	// dedup: a bundle whose filename already carries this content_hash is the
	// same schema (filename embeds the hash, so we can short-circuit without
	// decompressing).
	entries, err := readBundleEntries(dir)
	if err != nil {
		return PutInserted, err
	}
	for _, e := range entries {
		if e.contentHash == snap.ContentHash {
			return PutDeduped, nil
		}
	}

	b := Bundle{Schema: snap, Activity: map[string]*schema.ActivityStatsSnapshot{}}
	if err := writeBundleAtomic(filepath.Join(dir, BundleFilename(snap.Timestamp, snap.ContentHash)), &b); err != nil {
		return PutInserted, err
	}
	return PutInserted, nil
}

func (f *FilesystemStore) putPlanner(_ context.Context, key SnapshotKey, p *schema.PlannerStatsSnapshot) (PutOutcome, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	path, b, err := f.findBundleBySchemaRef(key, p.SchemaRefHash)
	if err != nil {
		return PutInserted, err
	}
	if b.Planner != nil && b.Planner.ContentHash == p.ContentHash {
		return PutDeduped, nil
	}
	b.Planner = p
	if err := writeBundleAtomic(path, b); err != nil {
		return PutInserted, err
	}
	return PutInserted, nil
}

func (f *FilesystemStore) putActivity(_ context.Context, key SnapshotKey, a *schema.ActivityStatsSnapshot) (PutOutcome, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	path, b, err := f.findBundleBySchemaRef(key, a.SchemaRefHash)
	if err != nil {
		return PutInserted, err
	}
	if b.Activity == nil {
		b.Activity = map[string]*schema.ActivityStatsSnapshot{}
	}
	if existing, ok := b.Activity[a.Node.Source]; ok && existing.ContentHash == a.ContentHash {
		return PutDeduped, nil
	}
	b.Activity[a.Node.Source] = a
	if err := writeBundleAtomic(path, b); err != nil {
		return PutInserted, err
	}
	return PutInserted, nil
}

func (f *FilesystemStore) Get(ctx context.Context, key SnapshotKey, kind SnapshotKind, at SnapshotRef) (StoredSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	bundles, err := f.loadBundles(key)
	if err != nil {
		return StoredSnapshot{}, err
	}

	switch kind.Tag {
	case KindSchema:
		b, err := pickSchemaBundle(bundles, at)
		if err != nil {
			return StoredSnapshot{}, err
		}
		return WrapSchema(b.Schema), nil
	case KindPlanner:
		b, err := pickPlannerBundle(bundles, at)
		if err != nil {
			return StoredSnapshot{}, err
		}
		return WrapPlanner(b.Planner), nil
	case KindActivity:
		a, err := pickActivity(bundles, kind.NodeLabel, at)
		if err != nil {
			return StoredSnapshot{}, err
		}
		return WrapActivity(a), nil
	}
	return StoredSnapshot{}, fmt.Errorf("unknown SnapshotKind tag: %d", kind.Tag)
}

func (f *FilesystemStore) List(ctx context.Context, key SnapshotKey, kind SnapshotKind, rng TimeRange) ([]SnapshotSummary, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	bundles, err := f.loadBundles(key)
	if err != nil {
		return nil, err
	}

	var out []SnapshotSummary
	for _, b := range bundles {
		switch kind.Tag {
		case KindSchema:
			s := b.Schema
			if !inRange(s.Timestamp, rng) {
				continue
			}
			out = append(out, SnapshotSummary{
				Kind: SchemaKind(), Timestamp: s.Timestamp,
				ContentHash: s.ContentHash, SchemaRefHash: s.ContentHash,
				Database: s.Database,
			})
		case KindPlanner:
			if b.Planner == nil {
				continue
			}
			if !inRange(b.Planner.Timestamp, rng) {
				continue
			}
			out = append(out, SnapshotSummary{
				Kind: PlannerKind(), Timestamp: b.Planner.Timestamp,
				ContentHash: b.Planner.ContentHash, SchemaRefHash: b.Planner.SchemaRefHash,
				Database: b.Planner.Database,
			})
		case KindActivity:
			for label, a := range b.Activity {
				if kind.NodeLabel != "" && kind.NodeLabel != label {
					continue
				}
				if !inRange(a.Node.Timestamp, rng) {
					continue
				}
				out = append(out, SnapshotSummary{
					Kind: ActivityKind(label), Timestamp: a.Node.Timestamp,
					ContentHash: a.ContentHash, SchemaRefHash: a.SchemaRefHash,
					NodeLabel: label,
				})
			}
		default:
			return nil, fmt.Errorf("unknown SnapshotKind tag: %d", kind.Tag)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp.After(out[j].Timestamp) })
	return out, nil
}

func (f *FilesystemStore) Latest(ctx context.Context, key SnapshotKey, kind SnapshotKind) (*SnapshotSummary, error) {
	list, err := f.List(ctx, key, kind, TimeRange{})
	if err != nil || len(list) == 0 {
		return nil, err
	}
	first := list[0]
	return &first, nil
}

func (f *FilesystemStore) DeleteBefore(ctx context.Context, key SnapshotKey, kind SnapshotKind, cutoff time.Time) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	dir := BundleDir(f.root, key)
	entries, err := readBundleEntries(dir)
	if err != nil {
		return 0, err
	}

	var n int64
	for _, e := range entries {
		path := filepath.Join(dir, e.name)
		b, err := readBundle(path)
		if err != nil {
			return n, err
		}
		switch kind.Tag {
		case KindSchema:
			if b.Schema != nil && b.Schema.Timestamp.Before(cutoff) {
				if err := os.Remove(path); err != nil {
					return n, err
				}
				n++
			}
		case KindPlanner:
			if b.Planner != nil && b.Planner.Timestamp.Before(cutoff) {
				b.Planner = nil
				if err := writeBundleAtomic(path, b); err != nil {
					return n, err
				}
				n++
			}
		case KindActivity:
			before := len(b.Activity)
			for label, a := range b.Activity {
				if kind.NodeLabel != "" && kind.NodeLabel != label {
					continue
				}
				if a.Node.Timestamp.Before(cutoff) {
					delete(b.Activity, label)
				}
			}
			if removed := before - len(b.Activity); removed > 0 {
				if err := writeBundleAtomic(path, b); err != nil {
					return n, err
				}
				n += int64(removed)
			}
		default:
			return n, fmt.Errorf("unknown SnapshotKind tag: %d", kind.Tag)
		}
	}
	return n, nil
}

func (f *FilesystemStore) ListKinds(ctx context.Context, key SnapshotKey) ([]SnapshotKind, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	bundles, err := f.loadBundles(key)
	if err != nil {
		return nil, err
	}

	var hasSchema, hasPlanner bool
	labels := map[string]struct{}{}
	for _, b := range bundles {
		if b.Schema != nil {
			hasSchema = true
		}
		if b.Planner != nil {
			hasPlanner = true
		}
		for label := range b.Activity {
			labels[label] = struct{}{}
		}
	}

	var out []SnapshotKind
	if hasSchema {
		out = append(out, SchemaKind())
	}
	if hasPlanner {
		out = append(out, PlannerKind())
	}
	sortedLabels := make([]string, 0, len(labels))
	for label := range labels {
		sortedLabels = append(sortedLabels, label)
	}
	sort.Strings(sortedLabels)
	for _, label := range sortedLabels {
		out = append(out, ActivityKind(label))
	}
	return out, nil
}

func (f *FilesystemStore) ListKeys(_ context.Context) ([]SnapshotKey, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

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

var _ SnapshotStore = (*FilesystemStore)(nil)

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
	// newest first; sync loops and Latest expect descending order.
	sort.Slice(out, func(i, j int) bool { return out[i].timestamp.After(out[j].timestamp) })
	return out, nil
}

func (f *FilesystemStore) loadBundles(key SnapshotKey) ([]*Bundle, error) {
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

func (f *FilesystemStore) findBundleBySchemaRef(key SnapshotKey, schemaRefHash string) (string, *Bundle, error) {
	dir := BundleDir(f.root, key)
	entries, err := readBundleEntries(dir)
	if err != nil {
		return "", nil, err
	}
	for _, e := range entries {
		if e.contentHash != schemaRefHash {
			continue
		}
		path := filepath.Join(dir, e.name)
		b, err := readBundle(path)
		if err != nil {
			return "", nil, err
		}
		return path, b, nil
	}
	return "", nil, fmt.Errorf("%w: schema_ref=%s", ErrOrphanSnapshot, schemaRefHash)
}

func pickSchemaBundle(bundles []*Bundle, at SnapshotRef) (*Bundle, error) {
	switch at.Kind {
	case RefLatest:
		if len(bundles) == 0 {
			return nil, fmt.Errorf("%w (latest)", ErrSnapshotNotFound)
		}
		return bundles[0], nil
	case RefAt:
		for _, b := range bundles {
			if !b.Schema.Timestamp.After(at.At) {
				return b, nil
			}
		}
		return nil, fmt.Errorf("%w (at-or-before %s)", ErrSnapshotNotFound, at.At.Format(time.RFC3339))
	case RefHash:
		for _, b := range bundles {
			if b.Schema.ContentHash == at.Hash {
				return b, nil
			}
		}
		return nil, fmt.Errorf("%w (hash %s)", ErrSnapshotNotFound, at.Hash)
	}
	return nil, fmt.Errorf("unknown SnapshotRef kind: %d", at.Kind)
}

func pickPlannerBundle(bundles []*Bundle, at SnapshotRef) (*Bundle, error) {
	switch at.Kind {
	case RefLatest:
		for _, b := range bundles {
			if b.Planner != nil {
				return b, nil
			}
		}
		return nil, fmt.Errorf("%w (latest planner)", ErrSnapshotNotFound)
	case RefAt:
		for _, b := range bundles {
			if b.Planner != nil && !b.Planner.Timestamp.After(at.At) {
				return b, nil
			}
		}
		return nil, fmt.Errorf("%w (planner at-or-before %s)", ErrSnapshotNotFound, at.At.Format(time.RFC3339))
	case RefHash:
		for _, b := range bundles {
			if b.Planner != nil && b.Planner.ContentHash == at.Hash {
				return b, nil
			}
		}
		return nil, fmt.Errorf("%w (planner hash %s)", ErrSnapshotNotFound, at.Hash)
	}
	return nil, fmt.Errorf("unknown SnapshotRef kind: %d", at.Kind)
}

func pickActivity(bundles []*Bundle, nodeLabel string, at SnapshotRef) (*schema.ActivityStatsSnapshot, error) {
	switch at.Kind {
	case RefLatest:
		for _, b := range bundles {
			if a := selectActivity(b, nodeLabel); a != nil {
				return a, nil
			}
		}
		return nil, fmt.Errorf("%w (latest activity)", ErrSnapshotNotFound)
	case RefAt:
		for _, b := range bundles {
			a := selectActivity(b, nodeLabel)
			if a != nil && !a.Node.Timestamp.After(at.At) {
				return a, nil
			}
		}
		return nil, fmt.Errorf("%w (activity at-or-before %s)", ErrSnapshotNotFound, at.At.Format(time.RFC3339))
	case RefHash:
		for _, b := range bundles {
			for label, a := range b.Activity {
				if nodeLabel != "" && nodeLabel != label {
					continue
				}
				if a.ContentHash == at.Hash {
					return a, nil
				}
			}
		}
		return nil, fmt.Errorf("%w (activity hash %s)", ErrSnapshotNotFound, at.Hash)
	}
	return nil, fmt.Errorf("unknown SnapshotRef kind: %d", at.Kind)
}

func selectActivity(b *Bundle, nodeLabel string) *schema.ActivityStatsSnapshot {
	if nodeLabel != "" {
		return b.Activity[nodeLabel]
	}
	// any node (used when caller didn't pin a label)
	for _, a := range b.Activity {
		return a
	}
	return nil
}

func inRange(ts time.Time, rng TimeRange) bool {
	if rng.From != nil && ts.Before(*rng.From) {
		return false
	}
	if rng.To != nil && !ts.Before(*rng.To) {
		return false
	}
	return true
}

func readBundle(path string) (*Bundle, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	defer dec.Close()
	plain, err := dec.DecodeAll(raw, nil)
	if err != nil {
		return nil, fmt.Errorf("decompress bundle %s: %w", path, err)
	}
	var b Bundle
	if err := json.Unmarshal(plain, &b); err != nil {
		return nil, fmt.Errorf("parse bundle %s: %w", path, err)
	}
	if b.Activity == nil {
		b.Activity = map[string]*schema.ActivityStatsSnapshot{}
	}
	return &b, nil
}

func writeBundleAtomic(path string, b *Bundle) error {
	raw, err := json.Marshal(b)
	if err != nil {
		return err
	}
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		return err
	}
	defer enc.Close()
	compressed := enc.EncodeAll(raw, nil)

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
