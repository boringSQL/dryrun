package schema

import "github.com/boringsql/dryrun/pkg/snapshot"

// Snapshot types.
type (
	SchemaSnapshot           = snapshot.SchemaSnapshot
	Table                    = snapshot.Table
	Column                   = snapshot.Column
	Constraint               = snapshot.Constraint
	ConstraintKind           = snapshot.ConstraintKind
	Index                    = snapshot.Index
	ColumnStats              = snapshot.ColumnStats
	PartitionInfo            = snapshot.PartitionInfo
	PartitionStrategy        = snapshot.PartitionStrategy
	PartitionChild           = snapshot.PartitionChild
	IndexPartitionChild      = snapshot.IndexPartitionChild
	RlsPolicy                = snapshot.RlsPolicy
	Trigger                  = snapshot.Trigger
	EnumType                 = snapshot.EnumType
	DomainType               = snapshot.DomainType
	CompositeType            = snapshot.CompositeType
	CompositeField           = snapshot.CompositeField
	View                     = snapshot.View
	Function                 = snapshot.Function
	Volatility               = snapshot.Volatility
	Extension                = snapshot.Extension
	GucSetting               = snapshot.GucSetting
	Flavor                   = snapshot.Flavor
	Capabilities             = snapshot.Capabilities
	FlavorSignals            = snapshot.FlavorSignals
	StaleStatsEntry          = snapshot.StaleStatsEntry
	QualifiedName            = snapshot.QualifiedName
	TableSizing              = snapshot.TableSizing
	TableActivity            = snapshot.TableActivity
	IndexSizing              = snapshot.IndexSizing
	IndexActivity            = snapshot.IndexActivity
	NodeIdentity             = snapshot.NodeIdentity
	DatabaseActivity         = snapshot.DatabaseActivity
	ReplicationSlotActivity  = snapshot.ReplicationSlotActivity
	CheckpointerActivity     = snapshot.CheckpointerActivity
	TableSizingEntry         = snapshot.TableSizingEntry
	IndexSizingEntry         = snapshot.IndexSizingEntry
	ColumnStatsEntry         = snapshot.ColumnStatsEntry
	TableActivityEntry       = snapshot.TableActivityEntry
	IndexActivityEntry       = snapshot.IndexActivityEntry
	PlannerStatsSnapshot     = snapshot.PlannerStatsSnapshot
	ActivityStatsSnapshot    = snapshot.ActivityStatsSnapshot
	QueryStatsEntry          = snapshot.QueryStatsEntry
	DynamicTagKeyObservation = snapshot.DynamicTagKeyObservation
	QueryStatsMember         = snapshot.QueryStatsMember
	QueryStatsInfo           = snapshot.QueryStatsInfo
	QueryStatsSnapshot       = snapshot.QueryStatsSnapshot
	MaskingInfo              = snapshot.MaskingInfo
	AnnotatedSchema          = snapshot.AnnotatedSchema
	MergedActivity           = snapshot.MergedActivity
	NodeActivity             = snapshot.NodeActivity
)

// Enum values.
const (
	FormatVersion = snapshot.FormatVersion

	ConstraintPrimaryKey = snapshot.ConstraintPrimaryKey
	ConstraintForeignKey = snapshot.ConstraintForeignKey
	ConstraintUnique     = snapshot.ConstraintUnique
	ConstraintCheck      = snapshot.ConstraintCheck
	ConstraintExclusion  = snapshot.ConstraintExclusion

	PartitionRange = snapshot.PartitionRange
	PartitionList  = snapshot.PartitionList
	PartitionHash  = snapshot.PartitionHash

	VolatilityImmutable = snapshot.VolatilityImmutable
	VolatilityStable    = snapshot.VolatilityStable
	VolatilityVolatile  = snapshot.VolatilityVolatile

	FlavorPostgres       = snapshot.FlavorPostgres
	FlavorAlloyDBOmni    = snapshot.FlavorAlloyDBOmni
	FlavorAlloyDBManaged = snapshot.FlavorAlloyDBManaged
)

// Functions.
var (
	ConstraintKindFromPg    = snapshot.ConstraintKindFromPg
	PartitionStrategyFromPg = snapshot.PartitionStrategyFromPg
	VolatilityFromPg        = snapshot.VolatilityFromPg
	DetectStaleStats        = snapshot.DetectStaleStats
	DetectFlavor            = snapshot.DetectFlavor
	RollUpPartitionActivity = snapshot.RollUpPartitionActivity
	RollUpPartitionSizing   = snapshot.RollUpPartitionSizing
	RollUpActivitySnapshot  = snapshot.RollUpActivitySnapshot

	ComputeContentHash           = snapshot.ComputeContentHash
	ComputeStructuralHash        = snapshot.ComputeStructuralHash
	ComputeContentHashV2         = snapshot.ComputeContentHashV2
	DigestFor                    = snapshot.DigestFor
	ComputePlannerContentHash    = snapshot.ComputePlannerContentHash
	ComputeActivityContentHash   = snapshot.ComputeActivityContentHash
	ComputeQueryStatsContentHash = snapshot.ComputeQueryStatsContentHash
)
