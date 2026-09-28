package query

import (
	"fmt"
	"strings"
)

const (
	dmlFullTableDuration = "proportional to row count"
	lockHeldMarker       = "until COMMIT"
	lockTimeoutMarker    = "No lock_timeout"
)

func holdsAccessExclusive(c MigrationCheck) bool {
	return c.Table != nil && strings.HasPrefix(c.LockType, "ACCESS EXCLUSIVE")
}

func isTableDML(c MigrationCheck) bool {
	switch c.Operation {
	case "INSERT", "UPDATE", "DELETE":
		return c.Table != nil
	}
	return false
}

// DDL's ACCESS EXCLUSIVE is held to COMMIT, so a later write blocks every reader and writer.
func flagLockHeldAcrossDML(checks []MigrationCheck) {
	held := map[string]string{}
	for i := range checks {
		c := &checks[i]
		if holdsAccessExclusive(*c) {
			if _, ok := held[*c.Table]; !ok {
				held[*c.Table] = c.Operation
			}
			continue
		}
		ddl, ok := held[derefString(c.Table)]
		if !ok || !isTableDML(*c) || strings.Contains(c.Recommendation, lockHeldMarker) {
			continue
		}
		reason := fmt.Sprintf("%s runs in the same transaction as the %s above, whose ACCESS EXCLUSIVE lock on %s is held %s: every read and write on the table waits for this statement too. Commit the DDL first and run the write on its own, batched.", c.Operation, ddl, *c.Table, lockHeldMarker)
		if c.LockDuration == dmlFullTableDuration {
			c.Safety = SafetyDangerous
		} else if c.Safety == SafetySafe {
			c.Safety = SafetyCaution
		}
		c.Recommendation = reason + "\n\n" + c.Recommendation
		note := ""
		if c.Rationale != nil {
			note = c.Rationale.Note
		}
		c.Rationale = &Rationale{Reason: reason, Note: note}
	}
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// The lock queue, not the DDL, causes most outages: said once per file.
func flagMissingLockTimeout(checks []MigrationCheck) {
	for i := range checks {
		c := &checks[i]
		if c.Operation == "SET" && strings.Contains(strings.ToLower(c.Statement), "lock_timeout") {
			return
		}
		if !holdsAccessExclusive(*c) {
			continue
		}
		c.LockNote = lockTimeoutMarker + " set before this statement: if it queues behind a long-running transaction, every later query on " + *c.Table + " queues behind it. Run SET lock_timeout = '2s' first and retry on timeout."
		return
	}
}
