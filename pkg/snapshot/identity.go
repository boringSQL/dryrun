package snapshot

import "fmt"

// proof-only: an axis conflicts only when both sides carry it and disagree, so
// an empty axis (pre-feature or permission-limited capture) never false-positives.
// replicas share the primary's system_identifier, so a standby re-point stays clean.
func IdentityConflict(prior, incoming *SchemaSnapshot) (string, bool) {
	if prior == nil || incoming == nil {
		return "", false
	}
	if prior.SystemIdentifier != "" && incoming.SystemIdentifier != "" &&
		prior.SystemIdentifier != incoming.SystemIdentifier {
		return fmt.Sprintf("cluster system_identifier differs (history %s, capture %s)",
			prior.SystemIdentifier, incoming.SystemIdentifier), true
	}
	if prior.Database != "" && incoming.Database != "" &&
		prior.Database != incoming.Database {
		return fmt.Sprintf("database name differs (history %q, capture %q)",
			prior.Database, incoming.Database), true
	}
	return "", false
}
