package history

import "time"

// one home for the TEXT timestamp rendering: stored values compare lexically,
// so every write and bind must go through here, where .UTC() is unavoidable
const historyTSLayout = "2006-01-02T15:04:05Z07:00" // = RFC3339, second granularity

func formatHistoryTS(t time.Time) string { return t.UTC().Format(historyTSLayout) }

// The read side of the same rule. A value that will not parse is reported as
// unknown, never as the zero date: these strings reach a tool response, where a
// year 1 timestamp reads as a measurement.
func parseHistoryTS(v string) (time.Time, bool) {
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return time.Time{}, false
	}
	return t.UTC(), true
}
