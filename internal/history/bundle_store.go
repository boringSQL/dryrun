package history

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/boringsql/dryrun/internal/schema"
)

// bundleBackend is the transport seam under bundleStore. A backend only reads
// and writes bundles; dedup, orphan detection and selection live above it.
type bundleBackend interface {
	// list returns every versioned bundle for key, newest first.
	list(ctx context.Context, key SnapshotKey) ([]*Bundle, error)
	// load returns the bundle bound to schemaRef, or found=false if none.
	load(ctx context.Context, key SnapshotKey, schemaRef string) (*Bundle, bool, error)
	// save persists b under its schema's version, creating or replacing.
	save(ctx context.Context, key SnapshotKey, b *Bundle) error
	// listKeys returns every key holding at least one bundle.
	listKeys(ctx context.Context) ([]SnapshotKey, error)
}

// bundleStore is the backend-independent half of every SnapshotStore: the four
// put paths, ref selection, summaries and kinds.
type bundleStore struct {
	backend bundleBackend
	mu      sync.Mutex
}

var _ SnapshotStore = (*bundleStore)(nil)

// Bundle is the on-disk JSON shape.
type Bundle struct {
	Schema   *schema.SchemaSnapshot                   `json:"schema"`
	Planner  *schema.PlannerStatsSnapshot             `json:"planner"`
	Activity map[string]*schema.ActivityStatsSnapshot `json:"activity"`
	Query    map[string]*schema.QueryStatsSnapshot    `json:"query,omitempty"`
}

var (
	// putting planner or activity without a matching schema bundle is rejected;
	// the bundle is keyed by schema_ref_hash and must exist first.
	ErrOrphanSnapshot = errors.New("no schema bundle matches schema_ref_hash")
)

// Put serializes read-modify-write: two concurrent merges into one schema
// bundle would otherwise clobber each other's fields.
func (s *bundleStore) Put(ctx context.Context, key SnapshotKey, snap StoredSnapshot) (PutOutcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch {
	case snap.AsSchema() != nil:
		return s.putSchema(ctx, key, snap.AsSchema())
	case snap.AsPlanner() != nil:
		return s.putPlanner(ctx, key, snap.AsPlanner())
	case snap.AsActivity() != nil:
		return s.putActivity(ctx, key, snap.AsActivity())
	case snap.AsQueryStats() != nil:
		return s.putQueryStats(ctx, key, snap.AsQueryStats())
	}
	return PutInserted, fmt.Errorf("empty StoredSnapshot")
}

func (s *bundleStore) putSchema(ctx context.Context, key SnapshotKey, snap *schema.SchemaSnapshot) (PutOutcome, error) {
	// a schema is its content: an existing bundle carrying this hash wins,
	// regardless of when it was captured.
	if _, ok, err := s.backend.load(ctx, key, snap.ContentHash); err != nil {
		return PutInserted, err
	} else if ok {
		return PutDeduped, nil
	}
	b := &Bundle{Schema: snap, Activity: map[string]*schema.ActivityStatsSnapshot{}}
	if err := s.backend.save(ctx, key, b); err != nil {
		return PutInserted, err
	}
	return PutInserted, nil
}

// merge loads the bundle bound to schemaRefHash, applies the change, and saves
// it back. apply reports whether the bundle changed; unchanged is a dedup.
func (s *bundleStore) merge(ctx context.Context, key SnapshotKey, schemaRefHash string, apply func(*Bundle) bool) (PutOutcome, error) {
	b, ok, err := s.backend.load(ctx, key, schemaRefHash)
	if err != nil {
		return PutInserted, err
	}
	if !ok {
		return PutInserted, fmt.Errorf("%w: schema_ref=%s", ErrOrphanSnapshot, schemaRefHash)
	}
	if !apply(b) {
		return PutDeduped, nil
	}
	if err := s.backend.save(ctx, key, b); err != nil {
		return PutInserted, err
	}
	return PutInserted, nil
}

func (s *bundleStore) putPlanner(ctx context.Context, key SnapshotKey, p *schema.PlannerStatsSnapshot) (PutOutcome, error) {
	return s.merge(ctx, key, p.SchemaRefHash, func(b *Bundle) bool {
		if b.Planner != nil && b.Planner.ContentHash == p.ContentHash {
			return false
		}
		b.Planner = p
		return true
	})
}

func (s *bundleStore) putActivity(ctx context.Context, key SnapshotKey, a *schema.ActivityStatsSnapshot) (PutOutcome, error) {
	return s.merge(ctx, key, a.SchemaRefHash, func(b *Bundle) bool {
		if b.Activity == nil {
			b.Activity = map[string]*schema.ActivityStatsSnapshot{}
		}
		if existing, ok := b.Activity[a.Node.Source]; ok && existing.ContentHash == a.ContentHash {
			return false
		}
		b.Activity[a.Node.Source] = a
		return true
	})
}

func (s *bundleStore) putQueryStats(ctx context.Context, key SnapshotKey, q *schema.QueryStatsSnapshot) (PutOutcome, error) {
	return s.merge(ctx, key, q.SchemaRefHash, func(b *Bundle) bool {
		if b.Query == nil {
			b.Query = map[string]*schema.QueryStatsSnapshot{}
		}
		if existing, ok := b.Query[q.Node.Source]; ok && existing.ContentHash == q.ContentHash {
			return false
		}
		b.Query[q.Node.Source] = q
		return true
	})
}

func (s *bundleStore) Get(ctx context.Context, key SnapshotKey, kind SnapshotKind, at SnapshotRef) (StoredSnapshot, error) {
	bundles, err := s.backend.list(ctx, key)
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
	case KindQuery:
		q, err := pickQueryStats(bundles, kind.NodeLabel, at)
		if err != nil {
			return StoredSnapshot{}, err
		}
		return WrapQueryStats(q), nil
	}
	return StoredSnapshot{}, fmt.Errorf("unknown SnapshotKind tag: %d", kind.Tag)
}

func (s *bundleStore) List(ctx context.Context, key SnapshotKey, kind SnapshotKind, rng TimeRange) ([]SnapshotSummary, error) {
	bundles, err := s.backend.list(ctx, key)
	if err != nil {
		return nil, err
	}

	var out []SnapshotSummary
	for _, b := range bundles {
		ss, err := bundleSummaries(b, kind, rng)
		if err != nil {
			return nil, err
		}
		out = append(out, ss...)
	}
	// hash tiebreak: equal timestamps would otherwise make latest~N nondeterministic
	sort.SliceStable(out, func(i, j int) bool {
		if !out[i].Timestamp.Equal(out[j].Timestamp) {
			return out[i].Timestamp.After(out[j].Timestamp)
		}
		return out[i].ContentHash < out[j].ContentHash
	})
	return out, nil
}

func (s *bundleStore) Latest(ctx context.Context, key SnapshotKey, kind SnapshotKind) (*SnapshotSummary, error) {
	list, err := s.List(ctx, key, kind, TimeRange{})
	if err != nil || len(list) == 0 {
		return nil, err
	}
	first := list[0]
	return &first, nil
}

func (s *bundleStore) ListKinds(ctx context.Context, key SnapshotKey) ([]SnapshotKind, error) {
	bundles, err := s.backend.list(ctx, key)
	if err != nil {
		return nil, err
	}
	return bundleKinds(bundles), nil
}

func (s *bundleStore) ListKeys(ctx context.Context) ([]SnapshotKey, error) {
	return s.backend.listKeys(ctx)
}

// per-bundle summaries for one kind; shared by every backend so they report identically
func bundleSummaries(b *Bundle, kind SnapshotKind, rng TimeRange) ([]SnapshotSummary, error) {
	var out []SnapshotSummary
	switch kind.Tag {
	case KindSchema:
		s := b.Schema
		if inRange(s.Timestamp, rng) {
			out = append(out, SnapshotSummary{
				Kind: SchemaKind(), Timestamp: s.Timestamp,
				ContentHash: s.ContentHash, SchemaRefHash: s.ContentHash,
				Database: s.Database,
			})
		}
	case KindPlanner:
		if b.Planner != nil && inRange(b.Planner.Timestamp, rng) {
			out = append(out, SnapshotSummary{
				Kind: PlannerKind(), Timestamp: b.Planner.Timestamp,
				ContentHash: b.Planner.ContentHash, SchemaRefHash: b.Planner.SchemaRefHash,
				Database: b.Planner.Database,
			})
		}
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
	case KindQuery:
		for label, q := range b.Query {
			if kind.NodeLabel != "" && kind.NodeLabel != label {
				continue
			}
			if !inRange(q.Node.Timestamp, rng) {
				continue
			}
			out = append(out, SnapshotSummary{
				Kind: QueryKind(label), Timestamp: q.Node.Timestamp,
				ContentHash: q.ContentHash, SchemaRefHash: q.SchemaRefHash,
				NodeLabel: label,
			})
		}
	default:
		return nil, fmt.Errorf("unknown SnapshotKind tag: %d", kind.Tag)
	}
	return out, nil
}

func bundleKinds(bundles []*Bundle) []SnapshotKind {
	var hasSchema, hasPlanner bool
	labels := map[string]struct{}{}
	queryLabels := map[string]struct{}{}
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
		for label := range b.Query {
			queryLabels[label] = struct{}{}
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
	sortedQueryLabels := make([]string, 0, len(queryLabels))
	for label := range queryLabels {
		sortedQueryLabels = append(sortedQueryLabels, label)
	}
	sort.Strings(sortedQueryLabels)
	for _, label := range sortedQueryLabels {
		out = append(out, QueryKind(label))
	}
	return out
}

func pickSchemaBundle(bundles []*Bundle, at SnapshotRef) (*Bundle, error) {
	return pickBundle(bundles, at, "", func(b *Bundle) (time.Time, string, bool) {
		return b.Schema.Timestamp, b.Schema.ContentHash, true
	})
}

func pickPlannerBundle(bundles []*Bundle, at SnapshotRef) (*Bundle, error) {
	return pickBundle(bundles, at, "planner", func(b *Bundle) (time.Time, string, bool) {
		if b.Planner == nil {
			return time.Time{}, "", false
		}
		return b.Planner.Timestamp, b.Planner.ContentHash, true
	})
}

func pickActivity(bundles []*Bundle, nodeLabel string, at SnapshotRef) (*schema.ActivityStatsSnapshot, error) {
	return pickNode(bundles, nodeLabel, at, "activity",
		func(b *Bundle) map[string]*schema.ActivityStatsSnapshot { return b.Activity },
		func(a *schema.ActivityStatsSnapshot) time.Time { return a.Node.Timestamp },
		func(a *schema.ActivityStatsSnapshot) string { return a.ContentHash })
}

func pickQueryStats(bundles []*Bundle, nodeLabel string, at SnapshotRef) (*schema.QueryStatsSnapshot, error) {
	return pickNode(bundles, nodeLabel, at, "query stats",
		func(b *Bundle) map[string]*schema.QueryStatsSnapshot { return b.Query },
		func(q *schema.QueryStatsSnapshot) time.Time { return q.Node.Timestamp },
		func(q *schema.QueryStatsSnapshot) string { return q.ContentHash })
}

// pickBundle walks bundles newest-first and returns the first whose slot is
// filled and matches at. slot reports a bundle's candidate (ts, hash), or
// ok=false when the bundle carries none (e.g. no planner yet).
func pickBundle(bundles []*Bundle, at SnapshotRef, noun string, slot func(*Bundle) (ts time.Time, hash string, ok bool)) (*Bundle, error) {
	switch at.Kind {
	case RefLatest, RefAt, RefHash:
		for _, b := range bundles {
			ts, hash, ok := slot(b)
			if !ok || (at.Kind == RefAt && ts.After(at.At)) || (at.Kind == RefHash && hash != at.Hash) {
				continue
			}
			return b, nil
		}
	case RefIndex:
		skip := at.Index
		for _, b := range bundles {
			if _, _, ok := slot(b); !ok {
				continue
			}
			if skip == 0 {
				return b, nil
			}
			skip--
		}
	}
	return nil, refMiss(at, noun)
}

// pickNode is pickBundle for the per-node maps (activity, query stats). With
// an empty nodeLabel any node matches; map order then picks it, as before.
func pickNode[T any](bundles []*Bundle, nodeLabel string, at SnapshotRef, noun string,
	snaps func(*Bundle) map[string]T, tsOf func(T) time.Time, hashOf func(T) string) (T, error) {

	selectOne := func(b *Bundle) (T, bool) {
		m := snaps(b)
		if nodeLabel != "" {
			v, ok := m[nodeLabel]
			return v, ok
		}
		for _, v := range m {
			return v, true
		}
		var zero T
		return zero, false
	}

	switch at.Kind {
	case RefLatest, RefAt:
		for _, b := range bundles {
			v, ok := selectOne(b)
			if !ok || (at.Kind == RefAt && tsOf(v).After(at.At)) {
				continue
			}
			return v, nil
		}
	case RefHash:
		for _, b := range bundles {
			for label, v := range snaps(b) {
				if nodeLabel != "" && nodeLabel != label {
					continue
				}
				if hashOf(v) == at.Hash {
					return v, nil
				}
			}
		}
	case RefIndex:
		skip := at.Index
		for _, b := range bundles {
			v, ok := selectOne(b)
			if !ok {
				continue
			}
			if skip == 0 {
				return v, nil
			}
			skip--
		}
	}
	var zero T
	return zero, refMiss(at, noun)
}

// refMiss formats the not-found error; noun is "" for schema, else the kind
// name, keeping each kind's historical message text.
func refMiss(at SnapshotRef, noun string) error {
	switch at.Kind {
	case RefLatest:
		if noun == "" {
			return fmt.Errorf("%w (latest)", ErrSnapshotNotFound)
		}
		return fmt.Errorf("%w (latest %s)", ErrSnapshotNotFound, noun)
	case RefAt:
		if noun == "" {
			return fmt.Errorf("%w (at-or-before %s)", ErrSnapshotNotFound, at.At.Format(time.RFC3339))
		}
		return fmt.Errorf("%w (%s at-or-before %s)", ErrSnapshotNotFound, noun, at.At.Format(time.RFC3339))
	case RefHash:
		if noun == "" {
			return fmt.Errorf("%w (hash %s)", ErrSnapshotNotFound, at.Hash)
		}
		return fmt.Errorf("%w (%s hash %s)", ErrSnapshotNotFound, noun, at.Hash)
	case RefIndex:
		if noun == "" {
			return fmt.Errorf("%w (latest~%d)", ErrSnapshotNotFound, at.Index)
		}
		return fmt.Errorf("%w (%s latest~%d)", ErrSnapshotNotFound, noun, at.Index)
	}
	return fmt.Errorf("unknown SnapshotRef kind: %d", at.Kind)
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
