use indexmap::IndexMap;
use pg_introspect::{
    Catalog as PgCatalog, CheckConstraint as PgCheck, Column as PgColumn,
    CompositeAttribute as PgCompAttr, CompositeType as PgComposite, DomainType as PgDomain,
    EnumType as PgEnum, ExclusionConstraint as PgExclusion, Extension as PgExtension, FkAction,
    FkMatch, ForeignKey as PgFk, Function as PgFunction, FunctionKind, GeneratedKind, IdentityKind,
    Index as PgIndex, PartitionChild as PgPartChild, PartitionInfo as PgPartInfo,
    PartitionStrategy as PgPartStrat, PolicyCommand, PrimaryKey as PgPrimaryKey, QualifiedName,
    RlsPolicy as PgPolicy, Table as PgTable, Trigger as PgTrigger, TriggerEvent,
    TriggerOrientation, TriggerTiming, UniqueConstraint as PgUnique, View as PgView, ViewKind,
    Volatility as PgVol,
};

use super::super::hash::{HashInput, compute_content_hash};
use super::super::types::{ConstraintKind, PartitionStrategy, Volatility};
use super::*;

fn qn(schema: &str, name: &str) -> QualifiedName {
    QualifiedName {
        schema: schema.into(),
        name: name.into(),
    }
}

fn col(name: &str, ordinal: i16, type_name: &str) -> PgColumn {
    PgColumn {
        name: name.into(),
        type_name: type_name.into(),
        ordinal,
        is_nullable: false,
        is_primary_key: false,
        is_foreign_key: false,
        is_unique: false,
        identity: None,
        generated: None,
        statistics_target: None,
        default: None,
        comment: None,
    }
}

// ── enum / variant mappings ───────────────────────────────────────────────

#[test]
fn identity_kind_maps_to_pg_attribute_codes() {
    let cases: &[(IdentityKind, &str)] =
        &[(IdentityKind::Always, "a"), (IdentityKind::ByDefault, "d")];
    for (kind, expected) in cases {
        let mut c = col("c", 1, "int");
        c.identity = Some(*kind);
        assert_eq!(convert_column(c).identity.as_deref(), Some(*expected));
    }
}

#[test]
fn generated_kind_maps_to_pg_attribute_codes() {
    let cases: &[(GeneratedKind, &str)] =
        &[(GeneratedKind::Stored, "s"), (GeneratedKind::Virtual, "v")];
    for (kind, expected) in cases {
        let mut c = col("c", 1, "int");
        c.generated = Some(*kind);
        assert_eq!(convert_column(c).generated.as_deref(), Some(*expected));
    }
}

#[test]
fn column_without_identity_or_generated_stays_none() {
    let c = convert_column(col("c", 1, "int"));
    assert!(c.identity.is_none());
    assert!(c.generated.is_none());
}

#[test]
fn policy_command_maps_to_uppercase_strings() {
    let cases: &[(PolicyCommand, &str)] = &[
        (PolicyCommand::All, "ALL"),
        (PolicyCommand::Select, "SELECT"),
        (PolicyCommand::Insert, "INSERT"),
        (PolicyCommand::Update, "UPDATE"),
        (PolicyCommand::Delete, "DELETE"),
    ];
    for (cmd, expected) in cases {
        let p = PgPolicy {
            name: "p".into(),
            command: *cmd,
            permissive: true,
            roles: vec!["public".into()],
            using_expr: None,
            with_check_expr: None,
        };
        assert_eq!(convert_policy(p).command, *expected);
    }
}

#[test]
fn volatility_maps_to_internal_enum() {
    let cases: &[(PgVol, Volatility)] = &[
        (PgVol::Immutable, Volatility::Immutable),
        (PgVol::Stable, Volatility::Stable),
        (PgVol::Volatile, Volatility::Volatile),
    ];
    for (pg_vol, expected) in cases {
        let f = PgFunction {
            name: qn("public", "f"),
            kind: FunctionKind::Function,
            language: "sql".into(),
            volatility: *pg_vol,
            security_definer: false,
            arguments: String::new(),
            identity_arguments: String::new(),
            return_type: "int".into(),
            comment: None,
        };
        assert_eq!(convert_function(f).volatility, *expected);
    }
}

#[test]
fn partition_strategy_maps_to_internal_enum() {
    let cases: &[(PgPartStrat, PartitionStrategy)] = &[
        (PgPartStrat::Range, PartitionStrategy::Range),
        (PgPartStrat::List, PartitionStrategy::List),
        (PgPartStrat::Hash, PartitionStrategy::Hash),
    ];
    for (pg_strat, expected) in cases {
        let p = PgPartInfo {
            strategy: *pg_strat,
            key: "k".into(),
            children: vec![],
        };
        assert_eq!(convert_partition_info(p).strategy, *expected);
    }
}

#[test]
fn view_kind_materialized_sets_flag() {
    let mat = PgView {
        oid: 1,
        name: qn("public", "v"),
        kind: ViewKind::Materialized,
        columns: IndexMap::new(),
        definition: "SELECT 1".into(),
        is_updatable: false,
        comment: None,
    };
    assert!(convert_view(mat).is_materialized);

    let plain = PgView {
        oid: 1,
        name: qn("public", "v"),
        kind: ViewKind::View,
        columns: IndexMap::new(),
        definition: "SELECT 1".into(),
        is_updatable: false,
        comment: None,
    };
    assert!(!convert_view(plain).is_materialized);
}

// ── golden fixture catalog ────────────────────────────────────────────────

fn fixture_catalog() -> PgCatalog {
    let mut columns = IndexMap::new();
    let mut id_col = col("id", 1, "int8");
    id_col.identity = Some(IdentityKind::Always);
    columns.insert("id".into(), id_col);

    let mut amount = col("amount", 2, "numeric");
    amount.is_nullable = true;
    columns.insert("amount".into(), amount);

    let mut full_name = col("full_name", 3, "text");
    full_name.generated = Some(GeneratedKind::Stored);
    full_name.default = Some("''".into());
    columns.insert("full_name".into(), full_name);

    let table = PgTable {
        oid: 16384,
        name: qn("public", "orders"),
        columns,
        primary_key: Some(PgPrimaryKey {
            name: "orders_pkey".into(),
            columns: vec!["id".into()],
            definition: "PRIMARY KEY (id)".into(),
        }),
        foreign_keys: vec![PgFk {
            constraint_name: "orders_customer_fk".into(),
            columns: vec!["customer_id".into()],
            references: qn("public", "customers"),
            references_columns: vec!["id".into()],
            is_validated: true,
            is_enforced: true,
            is_deferrable: false,
            is_deferred: false,
            on_update: FkAction::NoAction,
            on_delete: FkAction::Cascade,
            match_type: FkMatch::Simple,
            definition: "FOREIGN KEY (customer_id) REFERENCES public.customers(id) ON DELETE CASCADE".into(),
        }],
        indexes: vec![PgIndex {
            name: "orders_pkey".into(),
            columns: vec!["id".into()],
            included_columns: vec![],
            is_unique: true,
            is_primary: true,
            is_partial: false,
            predicate: None,
            method: "btree".into(),
            definition: "CREATE UNIQUE INDEX orders_pkey ON public.orders (id)".into(),
            is_valid: true,
            backs_constraint: true,
        }],
        unique_constraints: vec![PgUnique {
            name: "orders_external_id_key".into(),
            columns: vec!["external_id".into()],
            index_name: "orders_external_id_key".into(),
            is_validated: true,
            is_deferrable: false,
            is_deferred: false,
            nulls_not_distinct: false,
            definition: "UNIQUE (external_id)".into(),
        }],
        exclusion_constraints: vec![PgExclusion {
            name: "orders_no_overlap".into(),
            columns: vec!["during".into()],
            index_name: "orders_no_overlap".into(),
            definition: "EXCLUDE USING gist (during WITH &&)".into(),
        }],
        check_constraints: vec![PgCheck {
            name: "orders_amount_check".into(),
            definition: "CHECK ((amount > 0))".into(),
            columns: vec!["amount".into()],
            is_no_inherit: false,
        }],
        not_null_constraints: vec![],
        comment: Some("order rows".into()),
        is_partitioned: true,
        is_partition: false,
        partition_info: Some(PgPartInfo {
            strategy: PgPartStrat::Range,
            key: "RANGE (created_at)".into(),
            children: vec![PgPartChild {
                name: qn("public", "orders_2026"),
                bound: "FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')".into(),
            }],
        }),
        reloptions: vec!["fillfactor=80".into()],
        rls_enabled: true,
        policies: vec![PgPolicy {
            name: "orders_owner".into(),
            command: PolicyCommand::Select,
            permissive: true,
            roles: vec!["app".into()],
            using_expr: Some("(owner = current_user)".into()),
            with_check_expr: None,
        }],
        triggers: vec![PgTrigger {
            name: "orders_audit".into(),
            timing: TriggerTiming::After,
            events: vec![TriggerEvent::Insert],
            orientation: TriggerOrientation::Row,
            is_constraint: false,
            is_enabled: true,
            function: qn("public", "audit_log"),
            definition: "CREATE TRIGGER orders_audit AFTER INSERT ON public.orders FOR EACH ROW EXECUTE FUNCTION public.audit_log()".into(),
        }],
    };

    let mat_view = PgView {
        oid: 16500,
        name: qn("public", "orders_summary"),
        kind: ViewKind::Materialized,
        columns: IndexMap::new(),
        definition: "SELECT count(*) FROM orders".into(),
        is_updatable: false,
        comment: None,
    };

    let mut tables = IndexMap::new();
    tables.insert(table.name.clone(), table);
    let mut views = IndexMap::new();
    views.insert(mat_view.name.clone(), mat_view);

    PgCatalog {
        tables,
        views,
        partition_roots: Default::default(),
        dependencies: vec![],
        extensions: vec![PgExtension {
            name: "pgcrypto".into(),
            schema: "public".into(),
            version: "1.3".into(),
        }],
        functions: vec![PgFunction {
            name: qn("public", "audit_log"),
            kind: FunctionKind::Function,
            language: "plpgsql".into(),
            volatility: PgVol::Volatile,
            security_definer: true,
            arguments: String::new(),
            identity_arguments: String::new(),
            return_type: "trigger".into(),
            comment: Some("audit trigger".into()),
        }],
        enums: vec![PgEnum {
            name: qn("public", "order_status"),
            labels: vec!["new".into(), "shipped".into()],
        }],
        domains: vec![PgDomain {
            name: qn("public", "positive_amount"),
            base_type: "numeric".into(),
            is_nullable: false,
            default: None,
            constraints: vec!["CHECK (VALUE > 0)".into()],
        }],
        composites: vec![PgComposite {
            name: qn("public", "address"),
            attributes: vec![
                PgCompAttr {
                    name: "street".into(),
                    type_name: "text".into(),
                },
                PgCompAttr {
                    name: "zip".into(),
                    type_name: "text".into(),
                },
            ],
        }],
    }
}

#[test]
fn fixture_catalog_converts_to_expected_snapshot_parts() {
    let parts = catalog_to_snapshot_parts(fixture_catalog());

    assert_eq!(parts.tables.len(), 1);
    let t = &parts.tables[0];
    assert_eq!(t.schema, "public");
    assert_eq!(t.name, "orders");
    assert_eq!(t.oid, 16384);
    assert_eq!(t.columns.len(), 3);
    assert_eq!(t.columns[0].identity.as_deref(), Some("a"));
    assert_eq!(t.columns[2].generated.as_deref(), Some("s"));
    assert!(t.rls_enabled);
    assert_eq!(t.reloptions, vec!["fillfactor=80".to_string()]);

    // PK + FK + unique + check + exclusion, sorted by name (matches old ORDER BY conname)
    assert_eq!(t.constraints.len(), 5);
    let names: Vec<&str> = t.constraints.iter().map(|c| c.name.as_str()).collect();
    let mut sorted = names.clone();
    sorted.sort();
    assert_eq!(names, sorted, "constraints must be sorted by name");

    let fk = t
        .constraints
        .iter()
        .find(|c| c.kind == ConstraintKind::ForeignKey)
        .expect("fk present");
    assert_eq!(fk.fk_table.as_deref(), Some("public.customers"));
    assert_eq!(fk.fk_columns, vec!["id".to_string()]);

    let unique = t
        .constraints
        .iter()
        .find(|c| c.kind == ConstraintKind::Unique)
        .expect("unique present");
    assert_eq!(
        unique.backing_index.as_deref(),
        Some("orders_external_id_key")
    );

    let p = t.partition_info.as_ref().expect("partition info");
    assert_eq!(p.strategy, PartitionStrategy::Range);
    assert_eq!(p.children.len(), 1);
    assert_eq!(p.children[0].schema, "public");
    assert_eq!(p.children[0].name, "orders_2026");

    assert_eq!(t.policies.len(), 1);
    assert_eq!(t.policies[0].command, "SELECT");
    assert_eq!(t.triggers.len(), 1);
    assert_eq!(t.triggers[0].name, "orders_audit");

    assert_eq!(parts.views.len(), 1);
    assert!(parts.views[0].is_materialized);

    assert_eq!(parts.enums.len(), 1);
    assert_eq!(parts.enums[0].labels, vec!["new", "shipped"]);
    assert_eq!(parts.domains.len(), 1);
    assert_eq!(parts.domains[0].check_constraints.len(), 1);
    assert_eq!(parts.composites.len(), 1);
    assert_eq!(parts.composites[0].fields.len(), 2);
    assert_eq!(parts.functions.len(), 1);
    assert_eq!(parts.functions[0].volatility, Volatility::Volatile);
    assert!(parts.functions[0].security_definer);
    assert_eq!(parts.extensions.len(), 1);
    assert_eq!(parts.extensions[0].name, "pgcrypto");
}

// guards against silent regressions in field ordering, default values, or
// upstream pg_introspect changes that would invalidate snapshots stored in
// users' history.db. update EXPECTED only on intentional snapshot-format changes.
#[test]
fn fixture_catalog_content_hash_is_stable() {
    let parts = catalog_to_snapshot_parts(fixture_catalog());
    let hash = compute_content_hash(&HashInput {
        pg_version: "PostgreSQL 17.0",
        tables: &parts.tables,
        enums: &parts.enums,
        domains: &parts.domains,
        composites: &parts.composites,
        views: &parts.views,
        functions: &parts.functions,
        extensions: &parts.extensions,
    });
    const EXPECTED: &str = "ef118e31e0004baa508665111e32a9c2da964b60b24a900a6a1c654629d32fd6";
    assert_eq!(
        hash, EXPECTED,
        "content_hash drifted; if intentional, update EXPECTED"
    );
}
