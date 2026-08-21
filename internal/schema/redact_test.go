package schema

import (
	"errors"
	"strings"
	"testing"
)

// A connection error quotes whatever it failed to connect with, and that ends
// up in cron logs. Postgres takes a secret in three shapes.
func TestRedactSecrets(t *testing.T) {
	cases := []struct {
		name   string
		in     string
		want   string
		secret string
	}{
		{"url credentials", "postgres://postgres:hunter2@10.0.0.1:5432/db", "postgres://postgres:***@10.0.0.1:5432/db", "hunter2"},
		{"postgresql scheme", "postgresql://u:pw@h/db", "postgresql://u:***@h/db", "pw"},
		// an unencoded @ in the password used to leak everything after it
		{"unencoded at-sign", "postgres://u:p@ss@host/db", "postgres://u:***@host/db", "p@ss"},
		{"password with a slash is bounded by the path", "postgres://u:a/b@host/db", "postgres://u:***@host/db", ""},
		{"no password", "postgres://nopassword@host/db", "postgres://nopassword@host/db", ""},
		{"keyword dsn", "host=h port=5432 password=s3cret dbname=x", "password=***", "s3cret"},
		{"uppercase keyword", "HOST=h PASSWORD=s3cret", "PASSWORD=***", "s3cret"},
		{"space around equals", "password = s3cret host=h", "password = ***", "s3cret"},
		{"single-quoted value", "password='quoted secret' host=h", "password=***", "quoted secret"},
		{"double-quoted value", `password="quoted secret" host=h`, "password=***", "quoted secret"},
		{"pgpassword in an env dump", "PGPASSWORD=s3cret PGHOST=h", "PGPASSWORD=***", "s3cret"},
		// an underscore is a word character, so a \b left context would miss this
		{"prefixed env var", "MYAPP_PASSWORD=s3cret host=h", "MYAPP_PASSWORD=***", "s3cret"},
		{"scheme-less credentials", "u:pw@host/db", "u:***@host/db", ":pw@"},
		{"query parameter", "postgres://u@h/db?password=leaked&sslmode=require", "password=***", "leaked"},
		{"ipv6 host", "postgres://u:pw@[::1]:5432/db", "postgres://u:***@[::1]:5432/db", "pw"},
		{"an address is not a credential", "no url here, owner@example.com", "owner@example.com", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := RedactSecrets(tc.in)
			if !strings.Contains(got, tc.want) {
				t.Errorf("RedactSecrets(%q) = %q, want it to contain %q", tc.in, got, tc.want)
			}
			if tc.secret != "" && strings.Contains(got, tc.secret) {
				t.Errorf("RedactSecrets(%q) = %q still carries the secret", tc.in, got)
			}
		})
	}
}

func TestRedactSecrets_MultipleAndDiagnostics(t *testing.T) {
	// redacting the scheme-less form must not re-match an already-redacted URL
	// and eat its scheme
	t.Run("a redacted url is left alone", func(t *testing.T) {
		got := RedactSecrets("postgres://u:pw@h/db")
		if !strings.HasPrefix(got, "postgres://u:***@") {
			t.Errorf("got %q, want the scheme and user intact", got)
		}
	})

	t.Run("every url in one message", func(t *testing.T) {
		got := RedactSecrets("two: postgres://a:b@h1/db and postgres://c:d@h2/db")
		for _, leaked := range []string{":b@", ":d@"} {
			if strings.Contains(got, leaked) {
				t.Errorf("%q still carries a password", got)
			}
		}
	})

	// over-redaction makes the log useless: the host and mode are why it failed
	t.Run("non-secret parameters survive", func(t *testing.T) {
		got := RedactSecrets("host=db1 user=app sslmode=require dbname=x password=s3cret")
		for _, want := range []string{"host=db1", "user=app", "sslmode=require", "dbname=x"} {
			if !strings.Contains(got, want) {
				t.Errorf("redaction ate %q: %s", want, got)
			}
		}
		if strings.Contains(got, "s3cret") {
			t.Errorf("password survived: %s", got)
		}
	})
}

// Redacting is worthless if the error path does not use it, and testing the
// function alone does not prove the wiring: removing the call from
// classifyConnError left every other test in this file passing.
func TestClassifyConnError_Redacts(t *testing.T) {
	err := classifyConnError(errNoRoute, "postgres://app:hunter2@db1:5432/orders")
	if strings.Contains(err.Error(), "hunter2") {
		t.Errorf("connection error carries the password: %v", err)
	}
	// the host is why it failed; redaction must not eat it
	for _, want := range []string{"db1:5432", "app"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("connection error lost %q: %v", want, err)
		}
	}
}

var errNoRoute = errors.New("no route to host")
