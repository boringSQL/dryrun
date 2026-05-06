use pg_introspect::{
    Catalog as PgCatalog, CheckConstraint as PgCheck, Column as PgColumn,
    CompositeType as PgComposite, DomainType as PgDomain, EnumType as PgEnum,
    ExclusionConstraint as PgExclusion, Extension as PgExtension, ForeignKey as PgFk,
    Function as PgFunction, GeneratedKind, IdentityKind, Index as PgIndex,
    PartitionChild as PgPartChild, PartitionInfo as PgPartInfo, PartitionStrategy as PgPartStrat,
    PolicyCommand, PrimaryKey as PgPrimaryKey, RlsPolicy as PgPolicy, Table as PgTable,
    Trigger as PgTrigger, UniqueConstraint as PgUnique, View as PgView, ViewKind,
    Volatility as PgVol,
};

use super::types::*;

// envelope (pg_version, database, gucs, content_hash, ...) is the caller's job
pub fn catalog_to_snapshot_parts(cat: PgCatalog) -> SnapshotParts {
    let mut out = SnapshotParts::default();

    for (_, t) in cat.tables {
        out.tables.push(convert_table(t));
    }
    for (_, v) in cat.views {
        out.views.push(convert_view(v));
    }
    for e in cat.enums {
        out.enums.push(convert_enum(e));
    }
    for d in cat.domains {
        out.domains.push(convert_domain(d));
    }
    for c in cat.composites {
        out.composites.push(convert_composite(c));
    }
    for f in cat.functions {
        out.functions.push(convert_function(f));
    }
    for e in cat.extensions {
        out.extensions.push(convert_extension(e));
    }

    out
}

#[derive(Default)]
pub struct SnapshotParts {
    pub tables: Vec<Table>,
    pub enums: Vec<EnumType>,
    pub domains: Vec<DomainType>,
    pub composites: Vec<CompositeType>,
    pub views: Vec<View>,
    pub functions: Vec<Function>,
    pub extensions: Vec<Extension>,
}

fn convert_table(t: PgTable) -> Table {
    let mut constraints: Vec<Constraint> = Vec::new();
    if let Some(pk) = t.primary_key {
        constraints.push(convert_pk(pk));
    }
    for fk in t.foreign_keys {
        constraints.push(convert_fk(fk));
    }
    for u in t.unique_constraints {
        constraints.push(convert_unique(u));
    }
    for c in t.check_constraints {
        constraints.push(convert_check(c));
    }
    for x in t.exclusion_constraints {
        constraints.push(convert_exclusion(x));
    }

    let mut cols: Vec<Column> = Vec::with_capacity(t.columns.len());
    for (_, c) in t.columns {
        cols.push(convert_column(c));
    }

    Table {
        oid: t.oid,
        schema: t.name.schema,
        name: t.name.name,
        columns: cols,
        constraints,
        indexes: t.indexes.into_iter().map(convert_index).collect(),
        comment: t.comment,
        partition_info: t.partition_info.map(convert_partition_info),
        policies: t.policies.into_iter().map(convert_policy).collect(),
        triggers: t.triggers.into_iter().map(convert_trigger).collect(),
        reloptions: t.reloptions,
        rls_enabled: t.rls_enabled,
    }
}

fn convert_column(c: PgColumn) -> Column {
    // dryrun keeps these as the raw pg_attribute char codes
    let identity = c.identity.map(|k| match k {
        IdentityKind::Always => "a",
        IdentityKind::ByDefault => "d",
    });
    let generated = c.generated.map(|g| match g {
        GeneratedKind::Stored => "s",
        GeneratedKind::Virtual => "v",
    });

    Column {
        name: c.name,
        ordinal: c.ordinal,
        type_name: c.type_name,
        nullable: c.is_nullable,
        default: c.default,
        identity: identity.map(String::from),
        generated: generated.map(String::from),
        comment: c.comment,
        statistics_target: c.statistics_target,
    }
}

fn convert_pk(pk: PgPrimaryKey) -> Constraint {
    Constraint {
        name: pk.name,
        kind: ConstraintKind::PrimaryKey,
        columns: pk.columns,
        definition: Some(pk.definition),
        fk_table: None,
        fk_columns: vec![],
        backing_index: None,
        comment: None,
    }
}

fn convert_fk(fk: PgFk) -> Constraint {
    let target = format!("{}.{}", fk.references.schema, fk.references.name);
    Constraint {
        name: fk.constraint_name,
        kind: ConstraintKind::ForeignKey,
        columns: fk.columns,
        definition: Some(fk.definition),
        fk_table: Some(target),
        fk_columns: fk.references_columns,
        backing_index: None,
        comment: None,
    }
}

fn convert_unique(u: PgUnique) -> Constraint {
    Constraint {
        name: u.name,
        kind: ConstraintKind::Unique,
        columns: u.columns,
        definition: Some(u.definition),
        fk_table: None,
        fk_columns: vec![],
        backing_index: Some(u.index_name),
        comment: None,
    }
}

fn convert_check(c: PgCheck) -> Constraint {
    Constraint {
        name: c.name,
        kind: ConstraintKind::Check,
        columns: c.columns,
        definition: Some(c.definition),
        fk_table: None,
        fk_columns: vec![],
        backing_index: None,
        comment: None,
    }
}

fn convert_exclusion(x: PgExclusion) -> Constraint {
    Constraint {
        name: x.name,
        kind: ConstraintKind::Exclusion,
        columns: x.columns,
        definition: Some(x.definition),
        fk_table: None,
        fk_columns: vec![],
        backing_index: Some(x.index_name),
        comment: None,
    }
}

fn convert_index(i: PgIndex) -> Index {
    Index {
        name: i.name,
        columns: i.columns,
        include_columns: i.included_columns,
        index_type: i.method,
        is_unique: i.is_unique,
        is_primary: i.is_primary,
        predicate: i.predicate,
        definition: i.definition,
        is_valid: i.is_valid,
        backs_constraint: i.backs_constraint,
    }
}

fn convert_partition_info(p: PgPartInfo) -> PartitionInfo {
    PartitionInfo {
        strategy: match p.strategy {
            PgPartStrat::Range => PartitionStrategy::Range,
            PgPartStrat::List => PartitionStrategy::List,
            PgPartStrat::Hash => PartitionStrategy::Hash,
        },
        key: p.key,
        children: p
            .children
            .into_iter()
            .map(convert_partition_child)
            .collect(),
    }
}

fn convert_partition_child(c: PgPartChild) -> PartitionChild {
    PartitionChild {
        schema: c.name.schema,
        name: c.name.name,
        bound: c.bound,
    }
}

fn convert_policy(p: PgPolicy) -> RlsPolicy {
    let cmd = match p.command {
        PolicyCommand::All => "ALL",
        PolicyCommand::Select => "SELECT",
        PolicyCommand::Insert => "INSERT",
        PolicyCommand::Update => "UPDATE",
        PolicyCommand::Delete => "DELETE",
    };
    RlsPolicy {
        name: p.name,
        command: cmd.to_string(),
        permissive: p.permissive,
        roles: p.roles,
        using_expr: p.using_expr,
        with_check_expr: p.with_check_expr,
    }
}

fn convert_trigger(t: PgTrigger) -> Trigger {
    // pg_introspect carries timing/events/orientation separately, but dryrun
    // only stores the rendered definition. drop the rest for now.
    Trigger {
        name: t.name,
        definition: t.definition,
    }
}

fn convert_view(v: PgView) -> View {
    View {
        schema: v.name.schema,
        name: v.name.name,
        definition: v.definition,
        is_materialized: matches!(v.kind, ViewKind::Materialized),
        comment: v.comment,
    }
}

fn convert_enum(e: PgEnum) -> EnumType {
    EnumType {
        schema: e.name.schema,
        name: e.name.name,
        labels: e.labels,
    }
}

fn convert_domain(d: PgDomain) -> DomainType {
    DomainType {
        schema: d.name.schema,
        name: d.name.name,
        base_type: d.base_type,
        nullable: d.is_nullable,
        default: d.default,
        check_constraints: d.constraints,
    }
}

fn convert_composite(c: PgComposite) -> CompositeType {
    let mut fields: Vec<CompositeField> = Vec::with_capacity(c.attributes.len());
    for a in c.attributes {
        fields.push(CompositeField {
            name: a.name,
            type_name: a.type_name,
        });
    }
    CompositeType {
        schema: c.name.schema,
        name: c.name.name,
        fields,
    }
}

fn convert_function(f: PgFunction) -> Function {
    let volatility = match f.volatility {
        PgVol::Immutable => Volatility::Immutable,
        PgVol::Stable => Volatility::Stable,
        PgVol::Volatile => Volatility::Volatile,
    };
    Function {
        schema: f.name.schema,
        name: f.name.name,
        identity_args: f.identity_arguments,
        return_type: f.return_type,
        language: f.language,
        volatility,
        security_definer: f.security_definer,
        comment: f.comment,
    }
}

fn convert_extension(e: PgExtension) -> Extension {
    Extension {
        name: e.name,
        version: e.version,
        schema: e.schema,
    }
}
