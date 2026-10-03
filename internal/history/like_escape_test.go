package history

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Every resolver's LIKE pattern must treat % and _ as literals. Regression:
// with one snapshot stored, "%" resolved to it and "dead_eef" resolved
// "deadbeef"; ResolveSnapshot feeds `snapshot delete`.
func TestResolversEscapeLikeWildcards(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")
	seedSchema(t, store, k, "deadbeef00", time.Now().UTC().Truncate(time.Second))

	resolvers := map[string]func(string) error{
		"ResolveSnapshot":       func(p string) error { _, err := store.ResolveSnapshot(ctx, k, p); return err },
		"ResolveSchemaSnapshot": func(p string) error { _, err := store.ResolveSchemaSnapshot(ctx, k, p); return err },
		"ResolveKind":           func(p string) error { _, err := store.ResolveKind(ctx, k, p); return err },
		"GetSchema":             func(p string) error { _, err := store.GetSchema(ctx, k, NewRefHash(p)); return err },
	}
	for name, resolve := range resolvers {
		for _, prefix := range []string{"%", "dead_eef"} {
			if err := resolve(prefix); !errors.Is(err, ErrSnapshotNotFound) {
				t.Errorf("%s(%q) = %v, want ErrSnapshotNotFound", name, prefix, err)
			}
		}
		if err := resolve("dead"); err != nil {
			t.Errorf("%s(dead) = %v, want resolved", name, err)
		}
	}
}

// A literal underscore in the query must match a literal underscore in the
// stored hash, never an arbitrary character: with escaping off, "a_cd" matched
// both rows and came back ambiguous. Also proves non-hex hashes stored by
// internal callers stay retrievable.
func TestLikeEscapeMatchesUnderscoreLiterally(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")
	base := time.Now().UTC().Truncate(time.Second)
	seedSchema(t, store, k, "a_cd", base)
	seedSchema(t, store, k, "axcd", base.Add(time.Minute))

	got, err := store.ResolveSchemaSnapshot(ctx, k, "a_cd")
	if err != nil {
		t.Fatalf("literal underscore prefix must resolve unambiguously: %v", err)
	}
	if got.ContentHash != "a_cd" {
		t.Errorf("resolved %q, want a_cd", got.ContentHash)
	}
}

// A token that is neither latest[~N] nor hex fails fast at the boundary with a
// validation error instead of a confusing not-found.
func TestResolveTokenRejectsNonHexPrefix(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")
	seedSchema(t, store, k, "deadbeef00", time.Now().UTC().Truncate(time.Second))

	for _, tok := range []string{"%", "dead_eef", "lates"} {
		_, _, err := store.ResolveToken(ctx, k, tok, "schema", "")
		if err == nil || errors.Is(err, ErrSnapshotNotFound) {
			t.Errorf("ResolveToken(%q) = %v, want a validation error", tok, err)
		}
	}

	// uppercase and whitespace are normalized, not rejected
	kind, ref, err := store.ResolveToken(ctx, k, " DEAD ", "schema", "")
	if err != nil {
		t.Fatalf("ResolveToken( DEAD ): %v", err)
	}
	if kind.Tag != KindSchema || ref.Kind != RefHash || ref.Hash != "dead" {
		t.Errorf("ResolveToken( DEAD ) = (%v, %+v), want schema hash dead", kind, ref)
	}
}

func TestNormalizeHashPrefix(t *testing.T) {
	for in, want := range map[string]string{"dead": "dead", "ABCDEF": "abcdef", "  00ff ": "00ff"} {
		got, err := NormalizeHashPrefix(in)
		if err != nil || got != want {
			t.Errorf("NormalizeHashPrefix(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, in := range []string{"", "%", "xyz"} {
		if _, err := NormalizeHashPrefix(in); err == nil {
			t.Errorf("NormalizeHashPrefix(%q) accepted, want error", in)
		}
	}
}
