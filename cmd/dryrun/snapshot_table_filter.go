package main

import (
	"strings"

	"github.com/boringsql/dryrun/pkg/diff"
)

type (
	qualName struct {
		schema, name string
	}
)

// Case-exact (names are stored already folded); no search_path. arg matches
// literally ("a.b" is a legal name) or as schema.name split on the first dot.
func touches(arg string, c diff.Change) bool {
	arg, _, _ = strings.Cut(arg, "(") // a pasted signature means every overload
	qs, qn, qualified := strings.Cut(arg, ".")
	for _, o := range objectNames(c) {
		if o.name == arg || (qualified && o.schema == qs && o.name == qn) {
			return true
		}
	}
	return false
}

func objectNames(c diff.Change) []qualName {
	r := c.Object
	name := r.Name
	if r.Kind == "function" {
		name, _, _ = strings.Cut(name, "(") // overloads share the bare name
	}
	var out []qualName
	switch {
	case r.Schema != nil:
		out = append(out, qualName{*r.Schema, name})
	case r.Kind == "extension":
		out = append(out, qualName{name: name})
	default: // enum, domain, composite_type carry schema.name in Name
		out = append(out, splitQual(name))
	}
	// renames: match both names
	if c.Rename != nil {
		out = append(out, splitQual(c.Rename.FromName), splitQual(c.Rename.ToName))
	}
	return out
}

func splitQual(q string) qualName {
	s, n, ok := strings.Cut(q, ".")
	if !ok {
		return qualName{name: q}
	}
	return qualName{s, n}
}

// Oldest has no diff and drops out; DiffErr stays, hiding it would imply no change.
func filterTable(versions []schemaVersion, arg string) []schemaVersion {
	var out []schemaVersion
	for _, v := range versions {
		if v.DiffErr != nil {
			out = append(out, v)
			continue
		}
		if v.Oldest || v.Delta == nil {
			continue
		}
		var hit []diff.Change
		for _, c := range v.Delta.Changes {
			if touches(arg, c) {
				hit = append(hit, c)
			}
		}
		if len(hit) == 0 {
			continue
		}
		d := *v.Delta
		d.Changes = hit
		v.Delta = &d
		out = append(out, v)
	}
	return out
}
