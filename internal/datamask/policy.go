package datamask

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/boringsql/fixturize/masking"

	"github.com/boringsql/dryrun/internal/history"
)

type Policy struct {
	file        *masking.SharedMasksFile
	dbID        history.DatabaseId
	qualified   map[string]struct{} // "schema.table.column"
	unqualified map[string]struct{} // "table.column", matches any schema
}

func Load(path string, dbID history.DatabaseId, policyNames []string) (*Policy, error) {
	f, err := masking.LoadSharedMasks(path)
	if err != nil {
		return nil, fmt.Errorf("load masks file %s: %w", path, err)
	}

	p := &Policy{
		file:        f,
		dbID:        dbID,
		qualified:   map[string]struct{}{},
		unqualified: map[string]struct{}{},
	}

	// missing dbID is config drift, not fatal: warn and return an empty Policy
	db, ok := f.Databases[string(dbID)]
	if !ok {
		slog.Warn("masks file has no entry for database_id",
			"database_id", string(dbID), "path", path)
		return p, nil
	}

	selected, err := selectColumns(db, policyNames)
	if err != nil {
		return nil, err
	}
	for _, key := range selected {
		p.add(key)
	}
	return p, nil
}

func Discover(startDir string) (string, error) {
	return masking.DiscoverMasksFile(startDir)
}

// qualified keys win; an unqualified table.column key matches any schema
func (p *Policy) IsSensitive(schema, table, column string) bool {
	if p == nil {
		return false
	}
	if _, ok := p.qualified[schema+"."+table+"."+column]; ok {
		return true
	}
	_, ok := p.unqualified[table+"."+column]
	return ok
}

func (p *Policy) add(key string) {
	switch strings.Count(key, ".") {
	case 2:
		p.qualified[key] = struct{}{}
	case 1:
		p.unqualified[key] = struct{}{}
	default:
		slog.Warn("masks file column key is neither table.column nor schema.table.column",
			"key", key, "database_id", string(p.dbID))
	}
}

// empty policyNames = all listed columns; else union by tag intersection
func selectColumns(db masking.SharedDatabase, policyNames []string) ([]string, error) {
	if len(policyNames) == 0 {
		keys := make([]string, 0, len(db.Columns))
		for key := range db.Columns {
			keys = append(keys, key)
		}
		return keys, nil
	}

	tags := map[string]struct{}{}
	for _, name := range policyNames {
		pol, ok := db.Policies[name]
		if !ok {
			return nil, fmt.Errorf("masks file has no policy %q", name)
		}
		for _, t := range pol.IncludeTags {
			tags[t] = struct{}{}
		}
	}

	var keys []string
	for key, col := range db.Columns {
		for _, t := range col.Tags {
			if _, ok := tags[t]; ok {
				keys = append(keys, key)
				break
			}
		}
	}
	return keys, nil
}
