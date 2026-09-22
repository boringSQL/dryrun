package query

import (
	"testing"

	"github.com/boringsql/dryrun/internal/schema"
)

func suggestSchema() *schema.SchemaSnapshot {
	return &schema.SchemaSnapshot{
		PgVersion: "PostgreSQL 17.0",
		Database:  "test",
		Tables: []schema.Table{
			{
				Schema: "auth",
				Name:   "user_account",
				Columns: []schema.Column{
					{Name: "user_id", Ordinal: 1, TypeName: "bigint"},
					{Name: "email", Ordinal: 2, TypeName: "text"},
				},
			},
		},
	}
}

func TestSuggestIndexIgnoresSelectListColumns(t *testing.T) {
	snap := suggestSchema()
	suggestions, err := SuggestIndex("SELECT LOWER(email) FROM auth.user_account WHERE user_id = ANY($1)", snap, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range suggestions {
		for _, c := range s.Columns {
			if c == "email" {
				t.Errorf("suggested index on SELECT-list column: %+v", s)
			}
		}
	}
	found := false
	for _, s := range suggestions {
		if len(s.Columns) == 1 && s.Columns[0] == "user_id" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected a user_id index suggestion, got %+v", suggestions)
	}
}

func TestSuggestIndexNoSuggestionWithoutWhere(t *testing.T) {
	snap := suggestSchema()
	suggestions, err := SuggestIndex("SELECT email FROM auth.user_account", snap, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(suggestions) != 0 {
		t.Errorf("expected no suggestions without a predicate, got %+v", suggestions)
	}
}

func TestSuggestIndexIgnoresHavingColumns(t *testing.T) {
	snap := &schema.SchemaSnapshot{
		PgVersion: "PostgreSQL 17.0",
		Database:  "test",
		Tables: []schema.Table{
			{
				Schema: "public",
				Name:   "sales",
				Columns: []schema.Column{
					{Name: "region", Ordinal: 1, TypeName: "text"},
					{Name: "amount", Ordinal: 2, TypeName: "numeric"},
				},
			},
		},
	}
	suggestions, err := SuggestIndex("SELECT region, sum(amount) FROM sales GROUP BY region HAVING sum(amount) > 100", snap, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(suggestions) != 0 {
		t.Errorf("expected no suggestions from HAVING, got %+v", suggestions)
	}
}
