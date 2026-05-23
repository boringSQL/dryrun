package datamask

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeMasks drops a data-masking-policy.yml-shaped file into a fresh temp
// directory and hands back its absolute path. Every Load-based test needs a
// real file on disk: masking.LoadSharedMasks is deliberately stdlib + yaml
// only and exposes no in-memory decode entry point, so there is no way to feed
// it a string. t.TempDir() guarantees the fixture is cleaned up and that two
// tests can never collide on the same path even when run concurrently.
func writeMasks(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "data-masking-policy.yml")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write masks fixture: %v", err)
	}
	return path
}

// twoTagMasks is the workhorse fixture: one database ("dev"), three columns
// spread across two tags, and two named policies. It lets a single fixture
// exercise both the "no policy selected" path (everything is sensitive) and
// the "policy selected" path (tag intersection narrows the set). Note the keys
// are unqualified table.column form, so they match in any schema.
const twoTagMasks = `version: 1
databases:
  dev:
    columns:
      users.email:    { expr: "'masked@example.test'", tags: [pii] }
      users.phone:    { expr: "'+10000000000'",        tags: [pii, contact] }
      events.payload: { expr: "'{}'",                  tags: [internal] }
    policies:
      pii:      { include_tags: [pii] }
      internal: { include_tags: [internal] }
`

// mixedKeyMasks pairs a fully-qualified key (schema.table.column) with an
// unqualified one (table.column) inside the same database block, so the two
// matching modes can be tested against the exact same Policy instance.
const mixedKeyMasks = `version: 1
databases:
  dev:
    columns:
      public.users.email: { expr: "x", tags: [pii] }
      orders.total:       { expr: "x", tags: [pii] }
`

// TestLoadTagFilterNarrows verifies the core of policy selection: when the
// caller names a policy, Load must keep only the columns whose tags intersect
// that policy's include_tags, and drop the rest. Selecting "pii" should keep
// users.email and users.phone (both tagged pii) but exclude events.payload
// (tagged internal only). Selecting "internal" must give the mirror image.
// This is the behaviour fixturize relies on to scope masking to, say, just the
// GDPR-relevant columns without touching everything else.
func TestLoadTagFilterNarrows(t *testing.T) {
	path := writeMasks(t, twoTagMasks)

	pii, err := Load(path, "dev", []string{"pii"}, LoadOptions{})
	if err != nil {
		t.Fatalf("Load(pii): %v", err)
	}
	if !pii.IsSensitive("public", "users", "email") {
		t.Error("policy pii should cover users.email")
	}
	if !pii.IsSensitive("public", "users", "phone") {
		t.Error("policy pii should cover users.phone (tagged pii + contact)")
	}
	if pii.IsSensitive("public", "events", "payload") {
		t.Error("policy pii must NOT cover events.payload (internal-tagged only)")
	}

	internal, err := Load(path, "dev", []string{"internal"}, LoadOptions{})
	if err != nil {
		t.Fatalf("Load(internal): %v", err)
	}
	if !internal.IsSensitive("public", "events", "payload") {
		t.Error("policy internal should cover events.payload")
	}
	if internal.IsSensitive("public", "users", "email") {
		t.Error("policy internal must NOT cover users.email")
	}
}

// TestLoadEmptyPolicySelectsAll pins the documented default: when no policy
// name is supplied, Load masks every column listed under the database block,
// regardless of tags. This is the safe default — a freshly-written masks file
// with no policies still protects everything the author bothered to list.
func TestLoadEmptyPolicySelectsAll(t *testing.T) {
	path := writeMasks(t, twoTagMasks)

	all, err := Load(path, "dev", nil, LoadOptions{})
	if err != nil {
		t.Fatalf("Load(nil policies): %v", err)
	}
	for _, c := range []struct{ table, column string }{
		{"users", "email"},
		{"users", "phone"},
		{"events", "payload"},
	} {
		if !all.IsSensitive("public", c.table, c.column) {
			t.Errorf("empty policy list should select %s.%s", c.table, c.column)
		}
	}
}

// TestLoadMissingDatabaseErrors locks the strict default: a masks file with no
// block for the requested database_id (typo, renamed profile, stale file) is a
// hard error. With capture-time masking as the single line of defense, a
// missed dbID would mean raw stats land in history.db permanently.
func TestLoadMissingDatabaseErrors(t *testing.T) {
	path := writeMasks(t, twoTagMasks)

	_, err := Load(path, "prod", nil, LoadOptions{})
	if err == nil {
		t.Fatal("expected error for missing database_id")
	}
	if !strings.Contains(err.Error(), "prod") {
		t.Errorf("error should name the offending database_id, got: %v", err)
	}
}

// TestLoadMissingDatabaseAllowed: the opt-in escape hatch for multi-DB
// projects where only some databases have policies. AllowMissingDatabase=true
// downgrades the missing block to an empty (matches-nothing) Policy.
func TestLoadMissingDatabaseAllowed(t *testing.T) {
	path := writeMasks(t, twoTagMasks)

	pol, err := Load(path, "prod", nil, LoadOptions{AllowMissingDatabase: true})
	if err != nil {
		t.Fatalf("AllowMissingDatabase should downgrade missing dbID: %v", err)
	}
	if pol == nil {
		t.Fatal("Load must return a non-nil Policy in permissive mode")
	}
	if pol.IsSensitive("public", "users", "email") {
		t.Error("permissive empty Policy must match nothing")
	}
}

// TestLoadUnknownPolicyErrors is the opposite of the drift case: a database
// block that exists, but a policy name the caller asked for that does not.
// That is an operator mistake (a typo'd --mask-policy, or a policy that was
// deleted from the file) and must surface as a hard error rather than silently
// masking nothing — silent under-masking is exactly the PII leak this whole
// feature exists to prevent. The error must also echo the bad name.
func TestLoadUnknownPolicyErrors(t *testing.T) {
	path := writeMasks(t, twoTagMasks)

	_, err := Load(path, "dev", []string{"ghost"}, LoadOptions{})
	if err == nil {
		t.Fatal("expected an error for an unknown policy name")
	}
	if !strings.Contains(err.Error(), "ghost") {
		t.Errorf("error should name the missing policy, got: %v", err)
	}
}

// TestLoadQualifiedKeyScopedToSchema proves that a fully-qualified YAML key
// (public.users.email) is bound to exactly that schema. The same table+column
// living in a different schema (audit.users.email) must NOT be treated as
// sensitive — qualified keys are a precision tool for when two schemas hold
// same-named tables with different sensitivity.
func TestLoadQualifiedKeyScopedToSchema(t *testing.T) {
	path := writeMasks(t, mixedKeyMasks)

	pol, err := Load(path, "dev", nil, LoadOptions{})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !pol.IsSensitive("public", "users", "email") {
		t.Error("qualified key public.users.email should match in schema public")
	}
	if pol.IsSensitive("audit", "users", "email") {
		t.Error("qualified key must NOT match the same table in another schema")
	}
}

// TestLoadUnqualifiedKeyMatchesAnySchema is the counterpart: an unqualified
// key (orders.total) is intentionally schema-agnostic and must match the
// column wherever it appears. This is the ergonomic default for the common
// case where a column name is sensitive no matter which schema hosts it.
func TestLoadUnqualifiedKeyMatchesAnySchema(t *testing.T) {
	path := writeMasks(t, mixedKeyMasks)

	pol, err := Load(path, "dev", nil, LoadOptions{})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !pol.IsSensitive("public", "orders", "total") {
		t.Error("unqualified key should match in schema public")
	}
	if !pol.IsSensitive("warehouse", "orders", "total") {
		t.Error("unqualified key should match in schema warehouse too")
	}
}

