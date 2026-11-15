-- name: fetch-dependencies
-- pg_depend edges keyed by pg_identify_object identities. attrdef/rewrite
-- resolve to their column/view; system, temp, internal and extension-internal
-- edges are excluded.
WITH user_objects(classid) AS (
    VALUES
        ('pg_catalog.pg_class'::regclass),
        ('pg_catalog.pg_proc'::regclass),
        ('pg_catalog.pg_type'::regclass),
        ('pg_catalog.pg_namespace'::regclass),
        ('pg_catalog.pg_constraint'::regclass),
        ('pg_catalog.pg_trigger'::regclass),
        ('pg_catalog.pg_rewrite'::regclass),
        ('pg_catalog.pg_attrdef'::regclass),
        ('pg_catalog.pg_extension'::regclass),
        ('pg_catalog.pg_operator'::regclass),
        ('pg_catalog.pg_opclass'::regclass),
        ('pg_catalog.pg_opfamily'::regclass),
        ('pg_catalog.pg_cast'::regclass),
        ('pg_catalog.pg_collation'::regclass),
        ('pg_catalog.pg_statistic_ext'::regclass),
        ('pg_catalog.pg_ts_config'::regclass),
        ('pg_catalog.pg_ts_dict'::regclass),
        ('pg_catalog.pg_ts_parser'::regclass),
        ('pg_catalog.pg_ts_template'::regclass),
        ('pg_catalog.pg_foreign_data_wrapper'::regclass),
        ('pg_catalog.pg_foreign_server'::regclass),
        ('pg_catalog.pg_foreign_table'::regclass),
        ('pg_catalog.pg_user_mapping'::regclass),
        ('pg_catalog.pg_publication'::regclass),
        ('pg_catalog.pg_event_trigger'::regclass)
)
SELECT DISTINCT
       d.deptype::text AS deptype,
       dep.class AS dependent_class,
       dep.type AS dependent_type,
       dep.identity AS dependent_identity,
       d.refclassid::regclass::text AS referenced_class,
       ref.type AS referenced_type,
       ref.identity AS referenced_identity
  FROM pg_catalog.pg_depend d
  LEFT JOIN pg_catalog.pg_attrdef ad
    ON d.classid = 'pg_catalog.pg_attrdef'::regclass AND ad.oid = d.objid
  LEFT JOIN pg_catalog.pg_rewrite rw
    ON d.classid = 'pg_catalog.pg_rewrite'::regclass AND rw.oid = d.objid
  CROSS JOIN LATERAL (
      SELECT CASE WHEN ad.oid IS NOT NULL OR rw.oid IS NOT NULL
                  THEN 'pg_catalog.pg_class'::regclass::text
                  ELSE d.classid::regclass::text
             END AS class,
             x.type AS type,
             x.schema AS schema,
             x.identity AS identity
        FROM pg_catalog.pg_identify_object(
                 CASE WHEN ad.oid IS NOT NULL THEN 'pg_catalog.pg_class'::regclass
                      WHEN rw.oid IS NOT NULL THEN 'pg_catalog.pg_class'::regclass
                      ELSE d.classid
                 END,
                 CASE WHEN ad.oid IS NOT NULL THEN ad.adrelid
                      WHEN rw.oid IS NOT NULL THEN rw.ev_class
                      ELSE d.objid
                 END,
                 CASE WHEN ad.oid IS NOT NULL THEN ad.adnum::int4
                      ELSE d.objsubid
                 END
             ) AS x
  ) dep
  CROSS JOIN LATERAL pg_catalog.pg_identify_object(d.refclassid, d.refobjid, d.refobjsubid) AS ref
 WHERE d.deptype IN ('e', 'i', 'a', 'n', 'x')
   AND d.classid IN (SELECT classid FROM user_objects)
   AND d.refclassid IN (
       SELECT classid FROM user_objects
       UNION ALL
       SELECT 'pg_catalog.pg_language'::regclass
   )
   AND (dep.schema IS NULL OR (
       dep.schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
       AND dep.schema NOT LIKE 'pg_temp_%'
   ))
   AND (ref.schema IS NULL OR (
       ref.schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
       AND ref.schema NOT LIKE 'pg_temp_%'
   ))
   AND NOT (dep.class = d.refclassid::regclass::text AND dep.identity = ref.identity)
   -- Internal triggers are created by constraints.
   AND NOT (
       d.classid = 'pg_catalog.pg_trigger'::regclass
       AND EXISTS (
           SELECT 1
             FROM pg_catalog.pg_trigger t
            WHERE t.oid = d.objid
              AND t.tgisinternal
       )
   )
   -- Keep only the extension edge; CREATE EXTENSION creates the member.
   AND NOT (
       d.deptype <> 'e'
       AND EXISTS (
           SELECT 1
             FROM pg_catalog.pg_depend ed
            WHERE ed.classid = d.classid
              AND ed.objid = d.objid
              AND ed.deptype = 'e'
       )
   )
 ORDER BY dependent_class, dependent_type, dependent_identity,
          referenced_class, referenced_type, referenced_identity, deptype
