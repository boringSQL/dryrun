package snapshot

// version() reports plain "PostgreSQL x.y" on both AlloyDB editions, so flavor
// is detected from extensions and vendor GUCs instead.
type Flavor string

const (
	FlavorPostgres       Flavor = "postgres"
	FlavorAlloyDBOmni    Flavor = "alloydb-omni"
	FlavorAlloyDBManaged Flavor = "alloydb-managed"
)

type FlavorSignals struct {
	HasColumnarEngine bool // google_columnar_engine extension: the AlloyDB family tell
	OmniMarker        bool // alloydb_omni* GUC present: only on self-hosted Omni
}

type Capabilities struct {
	// pre-PG18 direct pg_class/pg_statistic DML; denied on managed. The PG18
	// pg_restore_relation_stats API works on managed and is gated by version.
	CatalogWritable    bool `json:"catalog_writable"`
	StorageInspectable bool `json:"storage_inspectable"`
	FullPageWritesTax  bool `json:"full_page_writes_tax"`
	AdaptiveAutovacuum bool `json:"adaptive_autovacuum"`
	ConfigTunable      bool `json:"config_tunable"`
}

// Fail safe: an unconfirmed AlloyDB reads as managed, so the locked-down
// capabilities apply rather than the permissive ones.
func DetectFlavor(sig FlavorSignals) Flavor {
	if !sig.HasColumnarEngine {
		return FlavorPostgres
	}
	if sig.OmniMarker {
		return FlavorAlloyDBOmni
	}
	return FlavorAlloyDBManaged
}

// Omni runs on stock storage, so only its autovacuum scheduler differs.
func (f Flavor) Capabilities() Capabilities {
	switch f {
	case FlavorAlloyDBOmni:
		return Capabilities{
			CatalogWritable:    true,
			StorageInspectable: true,
			FullPageWritesTax:  true,
			AdaptiveAutovacuum: true,
			ConfigTunable:      true,
		}
	case FlavorAlloyDBManaged:
		return Capabilities{
			CatalogWritable:    false,
			StorageInspectable: false,
			FullPageWritesTax:  false,
			AdaptiveAutovacuum: true,
			ConfigTunable:      false,
		}
	default:
		return Capabilities{
			CatalogWritable:    true,
			StorageInspectable: true,
			FullPageWritesTax:  true,
			AdaptiveAutovacuum: false,
			ConfigTunable:      true,
		}
	}
}

func (f Flavor) Display() string {
	switch f {
	case FlavorAlloyDBOmni:
		return "AlloyDB Omni"
	case FlavorAlloyDBManaged:
		return "AlloyDB (managed)"
	case FlavorPostgres:
		return "PostgreSQL"
	default:
		return string(f)
	}
}
