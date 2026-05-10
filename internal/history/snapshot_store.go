package history

import (
	"context"
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

type SnapshotStore interface {
	Put(ctx context.Context, key SnapshotKey, snap *schema.SchemaSnapshot) (PutOutcome, error)
	Get(ctx context.Context, key SnapshotKey, at SnapshotRef) (*schema.SchemaSnapshot, error)
	List(ctx context.Context, key SnapshotKey, rng TimeRange) ([]SnapshotSummary, error)
	Latest(ctx context.Context, key SnapshotKey) (*SnapshotSummary, error)
	DeleteBefore(ctx context.Context, key SnapshotKey, cutoff time.Time) (int64, error)
	ListKeys(ctx context.Context) ([]SnapshotKey, error)
}
