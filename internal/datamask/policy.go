package datamask

import (
	"github.com/boringsql/fixturize/masking"

	"github.com/boringsql/dryrun/internal/history"
)

type LoadOptions = masking.LoadOptions

func Load(path string, dbID history.DatabaseId, policyNames []string, opts LoadOptions) (*masking.Policy, error) {
	return masking.Load(path, string(dbID), policyNames, opts)
}
