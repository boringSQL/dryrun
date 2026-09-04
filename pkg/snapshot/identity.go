package snapshot

import "fmt"

// proof-only: an axis conflicts only when both sides carry it and disagree, so
// an empty axis (pre-feature or permission-limited capture) never false-positives.
// replicas share the primary's system_identifier, so a standby re-point stays clean.
func IdentityConflictWith(prior *SchemaSnapshot, systemIdentifier, database string) (string, bool) {
	if prior == nil {
		return "", false
	}
	if prior.SystemIdentifier != "" && systemIdentifier != "" &&
		prior.SystemIdentifier != systemIdentifier {
		return fmt.Sprintf("cluster system_identifier differs (history %s, capture %s)",
			prior.SystemIdentifier, systemIdentifier), true
	}
	if prior.Database != "" && database != "" &&
		prior.Database != database {
		return fmt.Sprintf("database name differs (history %q, capture %q)",
			prior.Database, database), true
	}
	return "", false
}
