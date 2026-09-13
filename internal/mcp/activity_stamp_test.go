package mcp

import (
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/schema"
)

// activity_captured_at is each label's newest capture, the oldest of those. A
// rotating label lists its other servers' older rows after its newest one, and
// those must not drag the stamp back.
func TestOldestActivityStampUsesEachLabelsNewestRow(t *testing.T) {
	t0 := time.Date(2026, 7, 31, 8, 0, 0, 0, time.UTC)
	row := func(source string, at time.Time) schema.NodeActivity {
		return schema.NodeActivity{Node: schema.NodeIdentity{Source: source, Timestamp: at}}
	}
	nodes := []schema.NodeActivity{
		row("primary", t0.Add(-2*time.Hour)),
		row("pool", t0),
		row("pool", t0.Add(-19*24*time.Hour)),
	}
	at, source := oldestActivityStamp(nodes)
	if !at.Equal(t0.Add(-2*time.Hour)) || source != "primary" {
		t.Errorf("got %s from %q, want primary two hours back", at, source)
	}
}
