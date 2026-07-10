package schema

import (
	"context"
	"fmt"
)

// role-safety preflight: prod-reading commands refuse superuser, replication,
// and bypassrls roles before any capture query runs.
type RoleReport struct {
	Rolname     string `json:"rolname"`
	Super       bool   `json:"superuser"`
	Replication bool   `json:"replication"`
	BypassRLS   bool   `json:"bypassrls"`
}

func (d *DryRun) RoleReport(ctx context.Context) (*RoleReport, error) {
	r := &RoleReport{}
	err := d.pool.QueryRow(ctx, `
		SELECT rolname, rolsuper, rolreplication, rolbypassrls
		FROM pg_roles WHERE rolname = current_user`).
		Scan(&r.Rolname, &r.Super, &r.Replication, &r.BypassRLS)
	if err != nil {
		return nil, fmt.Errorf("role preflight: %w", err)
	}
	return r, nil
}

func (r *RoleReport) Privileged() bool {
	return r.Super || r.Replication || r.BypassRLS
}

func (r *RoleReport) Privileges() []string {
	var privs []string
	if r.Super {
		privs = append(privs, "superuser")
	}
	if r.Replication {
		privs = append(privs, "replication")
	}
	if r.BypassRLS {
		privs = append(privs, "bypassrls")
	}
	return privs
}
