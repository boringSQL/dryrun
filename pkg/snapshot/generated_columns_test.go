package snapshot

import "testing"

func strp(s string) *string { return &s }

// A generation expression is DDL: ALTER TABLE ... ALTER COLUMN ... SET
// EXPRESSION (PG17) changes what the column computes without touching
// anything else, and a digest that missed it would dedup the new schema away.
func TestDigestCoversGenerationExpr(t *testing.T) {
	build := func(expr string) *SchemaSnapshot {
		return &SchemaSnapshot{
			FormatVersion: FormatVersion,
			Tables: []Table{{
				Schema: "public", Name: "t",
				Columns: []Column{{
					Name: "total", Ordinal: 1, TypeName: "integer",
					Generated: strp("stored"), GenerationExpr: strp(expr),
				}},
			}},
		}
	}
	if DigestFor(build("(a * b)")) == DigestFor(build("(a + b)")) {
		t.Error("two different generation expressions hash the same")
	}
}

// The key is omitted when unset so that adding the field does not move the
// digest of every ordinary column in every existing snapshot.
func TestStructuralOmitsAbsentGenerationExpr(t *testing.T) {
	plain := columnToStructural(&Column{Name: "id", TypeName: "integer"})
	if _, ok := plain["generation_expr"]; ok {
		t.Error("ordinary column carries the key, which would move its digest")
	}

	gen := columnToStructural(&Column{Name: "total", TypeName: "integer", GenerationExpr: strp("(a + b)")})
	if gen["generation_expr"] != "(a + b)" {
		t.Errorf("want the expression in the digest input, got %v", gen["generation_expr"])
	}
}
