package schema

import (
	"context"
	"log/slog"
	"time"
)

type phaseTimer struct {
	start  time.Time
	last   time.Time
	fields []any
}

func newPhaseTimer() *phaseTimer {
	now := time.Now()
	return &phaseTimer{start: now, last: now}
}

func (p *phaseTimer) mark(name string) {
	now := time.Now()
	p.fields = append(p.fields, name, now.Sub(p.last).Round(time.Millisecond).String())
	p.last = now
}

func (p *phaseTimer) note(kv ...any) {
	p.fields = append(p.fields, kv...)
}

func measureRTT(ctx context.Context, pool Querier) string {
	if !slog.Default().Enabled(ctx, slog.LevelDebug) {
		return "off"
	}
	start := time.Now()
	var one int
	if err := pool.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		return "err"
	}
	return time.Since(start).Round(time.Millisecond).String()
}

func (p *phaseTimer) log(msg string) {
	if !slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	slog.Debug(msg, append([]any{"total", time.Since(p.start).Round(time.Millisecond).String()}, p.fields...)...)
}
