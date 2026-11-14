package history

import (
	"encoding/json"
	"fmt"

	"github.com/klauspost/compress/zstd"

	"github.com/boringsql/dryrun/internal/schema"
)

// wire format is JSON+zstd; keep encode/decode here so every backend emits identical bytes
func EncodeBundle(b *Bundle) ([]byte, error) {
	raw, err := json.Marshal(b)
	if err != nil {
		return nil, err
	}
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}
	defer enc.Close()
	return enc.EncodeAll(raw, nil), nil
}

// normalize nil Activity to empty so callers can index without a nil check
func DecodeBundle(raw []byte) (*Bundle, error) {
	plain, err := decompressBundle(raw)
	if err != nil {
		return nil, err
	}
	var b Bundle
	if err := json.Unmarshal(plain, &b); err != nil {
		return nil, fmt.Errorf("parse bundle: %w", err)
	}
	if b.Activity == nil {
		b.Activity = map[string]*schema.ActivityStatsSnapshot{}
	}
	return &b, nil
}

func decompressBundle(raw []byte) ([]byte, error) {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	defer dec.Close()
	plain, err := dec.DecodeAll(raw, nil)
	if err != nil {
		return nil, fmt.Errorf("decompress bundle: %w", err)
	}
	return plain, nil
}

type (
	leanBundle struct {
		Schema          *schema.SchemaSnapshot                     `json:"schema"`
		Planner         *leanPlanner                               `json:"planner"`
		Activity        map[string]*schema.ActivityStatsSnapshot   `json:"activity"`
		Query           map[string]*leanQueryStats                 `json:"query,omitempty"`
		ActivityMembers map[string][]*schema.ActivityStatsSnapshot `json:"activity_members,omitempty"`
		QueryMembers    map[string][]*leanQueryStats               `json:"query_members,omitempty"`
	}
	// shadowed fields are not decoded
	leanPlanner struct {
		schema.PlannerStatsSnapshot
		Tables  skipped `json:"tables"`
		Indexes skipped `json:"indexes"`
		Columns skipped `json:"columns"`
	}
	leanQueryStats struct {
		schema.QueryStatsSnapshot
		Queries skipped `json:"queries"`
	}
	skipped struct{}
)

func (*skipped) UnmarshalJSON([]byte) error { return nil }

func decodeLeanBundle(raw []byte) (*Bundle, error) {
	plain, err := decompressBundle(raw)
	if err != nil {
		return nil, err
	}
	var l leanBundle
	if err := json.Unmarshal(plain, &l); err != nil {
		return nil, fmt.Errorf("parse bundle: %w", err)
	}
	b := &Bundle{Schema: l.Schema, Activity: l.Activity, ActivityMembers: l.ActivityMembers}
	if b.Activity == nil {
		b.Activity = map[string]*schema.ActivityStatsSnapshot{}
	}
	if l.Planner != nil {
		p := l.Planner.PlannerStatsSnapshot
		b.Planner = &p
	}
	if l.Query != nil {
		b.Query = make(map[string]*schema.QueryStatsSnapshot, len(l.Query))
		for label, q := range l.Query {
			b.Query[label] = q.unwrap()
		}
	}
	if l.QueryMembers != nil {
		b.QueryMembers = make(map[string][]*schema.QueryStatsSnapshot, len(l.QueryMembers))
		for label, rows := range l.QueryMembers {
			out := make([]*schema.QueryStatsSnapshot, len(rows))
			for i, q := range rows {
				out[i] = q.unwrap()
			}
			b.QueryMembers[label] = out
		}
	}
	return b, nil
}

func (q *leanQueryStats) unwrap() *schema.QueryStatsSnapshot {
	if q == nil {
		return nil
	}
	return &q.QueryStatsSnapshot
}
