package datamask

import (
	"github.com/boringsql/fixturize/masking"

	"github.com/boringsql/dryrun/internal/history"
)

func Load(path string, dbID history.DatabaseId, policyNames []string) (*masking.Policy, error) {
	return masking.Load(path, string(dbID), policyNames)
}
