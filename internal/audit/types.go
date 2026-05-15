package audit

type Config struct {
	DisabledRules       []string `json:"disabled_rules"`
	MaxIndexesPerTable  int      `json:"max_indexes_per_table"`
	NoCommentMinColumns int      `json:"no_comment_min_columns"`
	BloatThreshold      float64  `json:"bloat_threshold"`
}

func DefaultConfig() Config {
	return Config{
		MaxIndexesPerTable:  10,
		NoCommentMinColumns: 5,
		BloatThreshold:      4.0,
	}
}
