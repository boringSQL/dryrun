package history

import "time"

// one home for the TEXT timestamp rendering: stored values compare lexically,
// so every write and bind must go through here, where .UTC() is unavoidable
const historyTSLayout = "2006-01-02T15:04:05Z07:00" // = RFC3339, second granularity

func formatHistoryTS(t time.Time) string { return t.UTC().Format(historyTSLayout) }
