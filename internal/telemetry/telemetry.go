package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	endpoint    = "https://boringsql.com/i/telemetry"
	httpTimeout = 3 * time.Second
)

type (
	Client struct {
		enabled    bool
		sessionID  uuid.UUID
		appVersion string
		http       *http.Client
		debug      bool
		wg         sync.WaitGroup
	}

	Tracker struct {
		mu     sync.Mutex
		counts map[string]uint64
	}

	StartEvent struct {
		Event         string    `json:"event"`
		SessionID     uuid.UUID `json:"session_id"`
		AppVersion    string    `json:"app_version"`
		Timestamp     time.Time `json:"timestamp"`
		Transport     string    `json:"transport"`
		SchemaLoaded  bool      `json:"schema_loaded"`
		LiveDB        bool      `json:"live_db"`
		TableCount    int       `json:"table_count"`
		PlannerLoaded bool      `json:"planner_loaded"`
		ActivityNodes int       `json:"activity_nodes"`
	}

	SummaryEvent struct {
		Event        string            `json:"event"`
		SessionID    uuid.UUID         `json:"session_id"`
		AppVersion   string            `json:"app_version"`
		Timestamp    time.Time         `json:"timestamp"`
		DurationSecs uint64            `json:"duration_secs"`
		ToolCalls    map[string]uint64 `json:"tool_calls"`
	}
)

func NewClient(enabled bool, sessionID uuid.UUID, appVersion string) *Client {
	if os.Getenv("DO_NOT_TRACK") == "1" {
		enabled = false
	}
	return &Client{
		enabled:    enabled,
		sessionID:  sessionID,
		appVersion: appVersion,
		http:       &http.Client{Timeout: httpTimeout},
		debug:      os.Getenv("DRYRUN_TELEMETRY_DEBUG") == "1",
	}
}

func (c *Client) SessionID() uuid.UUID { return c.sessionID }
func (c *Client) AppVersion() string   { return c.appVersion }
func (c *Client) Enabled() bool        { return c.enabled }

// Background send; never blocks the caller. Wait drains in-flight sends.
func (c *Client) Fire(event any) {
	if !c.enabled {
		return
	}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.send(context.Background(), event)
	}()
}

// Synchronous send with timeout; used at shutdown for the summary event.
func (c *Client) FireWait(event any) {
	if !c.enabled {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), httpTimeout)
	defer cancel()
	c.send(ctx, event)
}

func (c *Client) Wait() { c.wg.Wait() }

func (c *Client) send(ctx context.Context, event any) {
	body, err := json.Marshal(event)
	if err != nil {
		slog.Debug("telemetry: serialization failed", "error", err)
		return
	}
	if c.debug {
		slog.Info("telemetry: sending", "payload", string(body))
	} else {
		slog.Debug("telemetry: sending", "payload", string(body))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		slog.Debug("telemetry: request build failed", "error", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		slog.Debug("telemetry: request failed", "error", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		slog.Debug("telemetry: ok")
		return
	}
	slog.Debug("telemetry: server error", "status", resp.Status)
}

func NewTracker() *Tracker {
	return &Tracker{counts: make(map[string]uint64)}
}

func (t *Tracker) Record(tool string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.counts[tool]++
}

func (t *Tracker) Snapshot() map[string]uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]uint64, len(t.counts))
	for k, v := range t.counts {
		out[k] = v
	}
	return out
}

func NewStartEvent(c *Client, transport string, schemaLoaded, liveDB bool, tableCount int, plannerLoaded bool, activityNodes int) StartEvent {
	return StartEvent{
		Event:         "dryrun_start",
		SessionID:     c.SessionID(),
		AppVersion:    c.AppVersion(),
		Timestamp:     time.Now().UTC(),
		Transport:     transport,
		SchemaLoaded:  schemaLoaded,
		LiveDB:        liveDB,
		TableCount:    tableCount,
		PlannerLoaded: plannerLoaded,
		ActivityNodes: activityNodes,
	}
}

func NewSummaryEvent(c *Client, duration time.Duration, toolCalls map[string]uint64) SummaryEvent {
	return SummaryEvent{
		Event:        "dryrun_summary",
		SessionID:    c.SessionID(),
		AppVersion:   c.AppVersion(),
		Timestamp:    time.Now().UTC(),
		DurationSecs: uint64(duration.Seconds()),
		ToolCalls:    toolCalls,
	}
}
