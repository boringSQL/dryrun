package history

import (
	"context"
	"fmt"
	"time"

	"github.com/boringsql/dryrun/internal/schema"
)

// distinct named types so the compiler catches accidental swaps
type (
	ProjectId  string
	DatabaseId string
)

type SnapshotKey struct {
	ProjectID  ProjectId
	DatabaseID DatabaseId
}

type RefKind int

const (
	RefLatest RefKind = iota
	RefAt
	RefHash
)

// discriminated union: Kind selects which of At/Hash is meaningful
type SnapshotRef struct {
	Kind RefKind
	At   time.Time
	Hash string
}

func NewRefLatest() SnapshotRef        { return SnapshotRef{Kind: RefLatest} }
func NewRefAt(t time.Time) SnapshotRef { return SnapshotRef{Kind: RefAt, At: t} }
func NewRefHash(h string) SnapshotRef  { return SnapshotRef{Kind: RefHash, Hash: h} }

type TimeRange struct {
	From *time.Time
	To   *time.Time
}

type PutOutcome int

const (
	PutInserted PutOutcome = iota
	PutDeduped
)

type SnapshotKindTag int

const (
	KindSchema SnapshotKindTag = iota
	KindPlanner
	KindActivity
)

// Activity rows live per-node; NodeLabel is empty for schema/planner.
type SnapshotKind struct {
	Tag       SnapshotKindTag
	NodeLabel string
}

func SchemaKind() SnapshotKind                { return SnapshotKind{Tag: KindSchema} }
func PlannerKind() SnapshotKind               { return SnapshotKind{Tag: KindPlanner} }
func ActivityKind(label string) SnapshotKind  { return SnapshotKind{Tag: KindActivity, NodeLabel: label} }

func (k SnapshotKind) String() string {
	switch k.Tag {
	case KindSchema:
		return "schema"
	case KindPlanner:
		return "planner"
	case KindActivity:
		if k.NodeLabel != "" {
			return "activity:" + k.NodeLabel
		}
		return "activity"
	}
	return fmt.Sprintf("kind(%d)", k.Tag)
}

// StoredSnapshot is a tagged union over the three concrete snapshot bodies.
// Exactly one of schema/planner/activity is non-nil for a valid instance.
type StoredSnapshot struct {
	schema   *schema.SchemaSnapshot
	planner  *schema.PlannerStatsSnapshot
	activity *schema.ActivityStatsSnapshot
}

func WrapSchema(s *schema.SchemaSnapshot) StoredSnapshot {
	return StoredSnapshot{schema: s}
}
func WrapPlanner(p *schema.PlannerStatsSnapshot) StoredSnapshot {
	return StoredSnapshot{planner: p}
}
func WrapActivity(a *schema.ActivityStatsSnapshot) StoredSnapshot {
	return StoredSnapshot{activity: a}
}

func (s StoredSnapshot) Kind() SnapshotKind {
	switch {
	case s.schema != nil:
		return SchemaKind()
	case s.planner != nil:
		return PlannerKind()
	case s.activity != nil:
		return ActivityKind(s.activity.Node.Source)
	}
	return SnapshotKind{}
}

func (s StoredSnapshot) Timestamp() time.Time {
	switch {
	case s.schema != nil:
		return s.schema.Timestamp
	case s.planner != nil:
		return s.planner.Timestamp
	case s.activity != nil:
		return s.activity.Node.Timestamp
	}
	return time.Time{}
}

func (s StoredSnapshot) ContentHash() string {
	switch {
	case s.schema != nil:
		return s.schema.ContentHash
	case s.planner != nil:
		return s.planner.ContentHash
	case s.activity != nil:
		return s.activity.ContentHash
	}
	return ""
}

// schema bundles join planner/activity via this hash; for schema itself it's
// the content hash (a schema is its own ref).
func (s StoredSnapshot) SchemaRefHash() string {
	switch {
	case s.schema != nil:
		return s.schema.ContentHash
	case s.planner != nil:
		return s.planner.SchemaRefHash
	case s.activity != nil:
		return s.activity.SchemaRefHash
	}
	return ""
}

func (s StoredSnapshot) Database() string {
	switch {
	case s.schema != nil:
		return s.schema.Database
	case s.planner != nil:
		return s.planner.Database
	}
	return ""
}

func (s StoredSnapshot) AsSchema() *schema.SchemaSnapshot           { return s.schema }
func (s StoredSnapshot) AsPlanner() *schema.PlannerStatsSnapshot    { return s.planner }
func (s StoredSnapshot) AsActivity() *schema.ActivityStatsSnapshot  { return s.activity }

type SnapshotStore interface {
	Put(ctx context.Context, key SnapshotKey, snap StoredSnapshot) (PutOutcome, error)
	Get(ctx context.Context, key SnapshotKey, kind SnapshotKind, at SnapshotRef) (StoredSnapshot, error)
	List(ctx context.Context, key SnapshotKey, kind SnapshotKind, rng TimeRange) ([]SnapshotSummary, error)
	Latest(ctx context.Context, key SnapshotKey, kind SnapshotKind) (*SnapshotSummary, error)
	DeleteBefore(ctx context.Context, key SnapshotKey, kind SnapshotKind, cutoff time.Time) (int64, error)
	ListKinds(ctx context.Context, key SnapshotKey) ([]SnapshotKind, error)
	ListKeys(ctx context.Context) ([]SnapshotKey, error)
}
