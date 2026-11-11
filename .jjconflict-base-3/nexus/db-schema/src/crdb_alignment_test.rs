// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Test: Diesel schema.rs matches CRDB dbinit.sql
//!
//! Validates that the Diesel table definitions in `nexus_db_schema::schema`
//! are consistent with the actual CockroachDB schema produced by `dbinit.sql`.

use camino::Utf8PathBuf;
use omicron_test_utils::dev;
use omicron_test_utils::dev::db::CockroachInstance;
use std::collections::BTreeMap;
use std::collections::HashMap;

// -------------------------------------------------------------------
// Types
// -------------------------------------------------------------------

/// A scalar SQL type (the innermost type, no Nullable/Array wrappers).
#[derive(Debug, Clone, PartialEq, Eq)]
enum ScalarType {
    BigInt,
    Integer,
    SmallInt,
    Double,
    Float,
    Bool,
    Text,
    Uuid,
    Timestamptz,
    Inet,
    Binary,
    Jsonb,
    Interval,
    Numeric,
    Date,
    Timestamp,
    /// An enum type defined in nexus_db_schema::enums.
    /// Contains the full type_name string (e.g.,
    /// "nexus_db_schema::enums::BlockSizeEnum").
    Enum(String),
}

impl std::fmt::Display for ScalarType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            ScalarType::BigInt => "diesel::sql_types::BigInt",
            ScalarType::Integer => "diesel::sql_types::Integer",
            ScalarType::SmallInt => "diesel::sql_types::SmallInt",
            ScalarType::Double => "diesel::sql_types::Double",
            ScalarType::Float => "diesel::sql_types::Float",
            ScalarType::Bool => "diesel::sql_types::Bool",
            ScalarType::Text => "diesel::sql_types::Text",
            ScalarType::Uuid => "diesel::sql_types::Uuid",
            ScalarType::Timestamptz => "diesel::sql_types::Timestamptz",
            ScalarType::Inet => "diesel::sql_types::Inet",
            ScalarType::Binary => "diesel::sql_types::Binary",
            ScalarType::Jsonb => "diesel::sql_types::Jsonb",
            ScalarType::Interval => "diesel::sql_types::Interval",
            ScalarType::Numeric => "diesel::sql_types::Numeric",
            ScalarType::Date => "diesel::sql_types::Date",
            ScalarType::Timestamp => "diesel::sql_types::Timestamp",
            ScalarType::Enum(name) => name.as_str(),
        };
        f.write_str(s)
    }
}

/// A fully described column SQL type: base type + nullability + array-ness.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ColumnType {
    nullable: bool,
    kind: ColumnTypeKind,
}

impl std::fmt::Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.nullable {
            write!(f, "diesel::sql_types::Nullable<{}>", self.kind)
        } else {
            write!(f, "{}", self.kind)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ColumnTypeKind {
    Scalar(ScalarType),
    Array { element: ScalarType, element_nullable: bool },
}

impl std::fmt::Display for ColumnTypeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnTypeKind::Scalar(s) => write!(f, "{s}"),
            ColumnTypeKind::Array { element, element_nullable } => {
                if *element_nullable {
                    write!(
                        f,
                        "diesel::sql_types::Array<\
                         diesel::sql_types::Nullable<{element}>>"
                    )
                } else {
                    write!(f, "diesel::sql_types::Array<{element}>")
                }
            }
        }
    }
}

/// Represents a column from CRDB's information_schema.
#[derive(Debug)]
struct CrdbColumn {
    column_name: String,
    data_type: String,
    is_nullable: bool,
    udt_name: String,
}

// -------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------

/// Path to the "seed" CockroachDB tarball.
fn seed_tar() -> Utf8PathBuf {
    let seed_dir = std::env::var(dev::CRDB_SEED_TAR_ENV).unwrap_or_else(|_| {
        panic!(
            "{} missing -- are you running this test \
             with `cargo nextest run`?",
            dev::CRDB_SEED_TAR_ENV,
        )
    });
    seed_dir.into()
}

/// Parses a normalized type name string into a `ScalarType`.
///
/// Known `diesel::sql_types::*` names map to the corresponding variant;
/// everything else is treated as `ScalarType::Enum`.
fn parse_scalar_type(s: &str) -> Result<ScalarType, String> {
    match s {
        "diesel::sql_types::BigInt" => Ok(ScalarType::BigInt),
        "diesel::sql_types::Integer" => Ok(ScalarType::Integer),
        "diesel::sql_types::SmallInt" => Ok(ScalarType::SmallInt),
        "diesel::sql_types::Double" => Ok(ScalarType::Double),
        "diesel::sql_types::Float" => Ok(ScalarType::Float),
        "diesel::sql_types::Bool" => Ok(ScalarType::Bool),
        "diesel::sql_types::Text" => Ok(ScalarType::Text),
        "diesel::sql_types::Uuid" => Ok(ScalarType::Uuid),
        "diesel::sql_types::Timestamptz" => Ok(ScalarType::Timestamptz),
        "diesel::sql_types::Inet" => Ok(ScalarType::Inet),
        "diesel::sql_types::Binary" => Ok(ScalarType::Binary),
        "diesel::sql_types::Jsonb" => Ok(ScalarType::Jsonb),
        "diesel::sql_types::Interval" => Ok(ScalarType::Interval),
        "diesel::sql_types::Numeric" => Ok(ScalarType::Numeric),
        "diesel::sql_types::Date" => Ok(ScalarType::Date),
        "diesel::sql_types::Timestamp" => Ok(ScalarType::Timestamp),
        _ if s.starts_with("nexus_db_schema::enums::") => {
            Ok(ScalarType::Enum(s.to_string()))
        }
        _ => Err(format!("unknown Diesel type: {s}")),
    }
}

/// Maps a pg-internal `udt_name` value to the corresponding `ScalarType`.
///
/// CockroachDB's `information_schema.columns` provides `udt_name` for every
/// column: scalar columns get unprefixed names (`"int8"`, `"float8"`,
/// `"timestamptz"`), and array columns get `_`-prefixed versions (`"_int8"`).
/// This function handles the unprefixed form; callers strip the `_` for arrays.
fn udt_to_scalar_type(udt_name: &str) -> Option<ScalarType> {
    match udt_name {
        "bool" => Some(ScalarType::Bool),
        "bytea" => Some(ScalarType::Binary),
        "float4" => Some(ScalarType::Float),
        "float8" => Some(ScalarType::Double),
        "inet" => Some(ScalarType::Inet),
        "int2" => Some(ScalarType::SmallInt),
        "int4" => Some(ScalarType::Integer),
        "int8" => Some(ScalarType::BigInt),
        "interval" => Some(ScalarType::Interval),
        "jsonb" => Some(ScalarType::Jsonb),
        "text" | "varchar" => Some(ScalarType::Text),
        "timestamptz" => Some(ScalarType::Timestamptz),
        "uuid" => Some(ScalarType::Uuid),
        "numeric" => Some(ScalarType::Numeric),
        "date" => Some(ScalarType::Date),
        "timestamp" => Some(ScalarType::Timestamp),
        _ => None,
    }
}

/// Returns a map from CRDB enum type name to the `ScalarType`
/// for every enum defined in `nexus_db_schema::enums`.
///
/// For example: `"block_size"` → `ScalarType::Enum("nexus_db_schema::enums::BlockSizeEnum")`.
fn parse_enum_mapping() -> HashMap<String, ScalarType> {
    crate::enums::crdb_to_diesel_enum_type_names()
        .into_iter()
        .map(|(crdb_name, diesel_name)| {
            (crdb_name.to_string(), ScalarType::Enum(diesel_name.to_string()))
        })
        .collect()
}

/// Parses a Diesel `std::any::type_name` string into a `ColumnType`.
///
/// Handles module path normalization (`diesel::pg::types::sql_types::` →
/// `diesel::sql_types::`), then strips outer `Nullable<>`, `Array<>`, and
/// inner `Nullable<>` wrappers to produce a structured representation.
fn normalize_diesel_type(type_name: &str) -> Result<ColumnType, String> {
    let s = type_name
        .replace("diesel::pg::types::sql_types::", "diesel::sql_types::");

    // Strip outer Nullable<...>
    let (nullable, inner) = if let Some(rest) = s
        .strip_prefix("diesel::sql_types::Nullable<")
        .and_then(|r| r.strip_suffix('>'))
    {
        (true, rest)
    } else {
        (false, s.as_str())
    };

    // Check for Array<...>
    let kind = if let Some(array_inner) = inner
        .strip_prefix("diesel::sql_types::Array<")
        .and_then(|r| r.strip_suffix('>'))
    {
        // Check for inner Nullable<...> on the element
        let (element_nullable, elem_str) = if let Some(rest) = array_inner
            .strip_prefix("diesel::sql_types::Nullable<")
            .and_then(|r| r.strip_suffix('>'))
        {
            (true, rest)
        } else {
            (false, array_inner)
        };
        ColumnTypeKind::Array {
            element: parse_scalar_type(elem_str)?,
            element_nullable,
        }
    } else {
        ColumnTypeKind::Scalar(parse_scalar_type(inner)?)
    };

    Ok(ColumnType { nullable, kind })
}

/// Determines the expected `ColumnType` for a CRDB column.
///
/// Given CRDB column metadata (data_type, is_nullable, udt_name) and
/// the enum mapping, returns the expected structured type.
fn expected_diesel_type(
    data_type: &str,
    is_nullable: bool,
    udt_name: &str,
    enum_map: &HashMap<String, ScalarType>,
) -> Result<ColumnType, String> {
    let kind = if data_type == "ARRAY" {
        let elem_udt = udt_name.strip_prefix('_').ok_or_else(|| {
            format!("array udt_name missing '_' prefix: {udt_name}")
        })?;
        let elem = udt_to_scalar_type(elem_udt).ok_or_else(|| {
            format!("unknown array element type: udt_name={udt_name}")
        })?;
        // CRDB doesn't distinguish nullable vs non-nullable array
        // elements, so we default to non-nullable here; the match
        // function ignores element nullability.
        ColumnTypeKind::Array { element: elem, element_nullable: false }
    } else if data_type == "USER-DEFINED" {
        let scalar = enum_map
            .get(udt_name)
            .cloned()
            .ok_or_else(|| format!("unknown enum type: udt_name={udt_name}"))?;
        ColumnTypeKind::Scalar(scalar)
    } else {
        let scalar = udt_to_scalar_type(udt_name)
            .ok_or_else(|| format!("unknown CRDB type: udt_name={udt_name}"))?;
        ColumnTypeKind::Scalar(scalar)
    };

    Ok(ColumnType { nullable: is_nullable, kind })
}

/// Checks whether the Diesel type matches the CRDB-derived type.
///
/// This is more lenient than exact equality because CRDB's
/// information_schema does not distinguish nullable vs non-nullable array
/// elements. Array element nullability is ignored in the comparison.
fn diesel_type_matches_expected(
    diesel_type: &ColumnType,
    crdb_type: &ColumnType,
) -> bool {
    if diesel_type.nullable != crdb_type.nullable {
        return false;
    }
    match (&diesel_type.kind, &crdb_type.kind) {
        (ColumnTypeKind::Scalar(d), ColumnTypeKind::Scalar(c)) => d == c,
        (
            ColumnTypeKind::Array { element: d, .. },
            ColumnTypeKind::Array { element: c, .. },
        ) => {
            // Ignore element_nullable — CRDB can't report it
            d == c
        }
        _ => false,
    }
}

/// Compares two types ignoring top-level nullability.
///
/// Used for views, where information_schema reports all columns as nullable
/// regardless of the underlying query structure. We still check that the
/// base types match.
fn diesel_type_matches_ignoring_nullability(
    diesel_type: &ColumnType,
    crdb_type: &ColumnType,
) -> bool {
    match (&diesel_type.kind, &crdb_type.kind) {
        (ColumnTypeKind::Scalar(d), ColumnTypeKind::Scalar(c)) => d == c,
        (
            ColumnTypeKind::Array { element: d, .. },
            ColumnTypeKind::Array { element: c, .. },
        ) => d == c,
        _ => false,
    }
}

/// Queries CRDB information_schema.columns for all public columns, grouped by
/// table name.
async fn query_crdb_columns(
    crdb: &CockroachInstance,
) -> BTreeMap<String, Vec<CrdbColumn>> {
    let client = crdb.connect().await.expect("failed to connect");
    let rows = client
        .query(
            "SELECT table_name, column_name, is_nullable, data_type, udt_name \
             FROM information_schema.columns \
             WHERE table_schema = 'public' \
             ORDER BY table_name, ordinal_position",
            &[],
        )
        .await
        .expect("failed to query information_schema.columns");
    client.cleanup().await.expect("cleaning up after query");

    let mut result: BTreeMap<String, Vec<CrdbColumn>> = BTreeMap::new();
    for row in rows {
        let table_name: String = row.get(0);
        let column_name: String = row.get(1);
        let is_nullable_str: String = row.get(2);
        let data_type: String = row.get(3);
        let udt_name: String = row.get(4);

        result.entry(table_name).or_default().push(CrdbColumn {
            column_name,
            data_type,
            is_nullable: is_nullable_str == "YES",
            udt_name,
        });
    }
    result
}

/// Queries CRDB information_schema.tables to identify which names are views.
async fn query_crdb_views(
    crdb: &CockroachInstance,
) -> std::collections::HashSet<String> {
    let client = crdb.connect().await.expect("failed to connect");
    let rows = client
        .query(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'public' AND table_type = 'VIEW'",
            &[],
        )
        .await
        .expect("failed to query information_schema.tables");
    client.cleanup().await.expect("cleaning up after query");

    rows.iter().map(|row| row.get::<_, String>(0)).collect()
}

/// Formats a list of items for file comparison.
///
/// Each item on its own line, with a trailing newline. Empty input produces
/// an empty string (which matches an empty file).
fn format_list(items: &[String]) -> String {
    if items.is_empty() { String::new() } else { items.join("\n") + "\n" }
}

// -------------------------------------------------------------------
// Test
// -------------------------------------------------------------------

#[tokio::test]
async fn diesel_schema_matches_crdb_schema() {
    let log = slog::Logger::root(slog::Discard, slog::o!());

    // Spin up CRDB populated with dbinit.sql
    let input_tar = seed_tar();
    let mut crdb = dev::test_setup_database(
        &log,
        dev::StorageSource::CopyFromSeed { input_tar },
    )
    .await;

    // Query CRDB for all column metadata and identify views
    let crdb_tables = query_crdb_columns(&crdb).await;
    let crdb_views = query_crdb_views(&crdb).await;

    crdb.cleanup().await.expect("failed to clean up CRDB");

    // Parse enum mapping from enums.rs
    let enum_map = parse_enum_mapping();

    // Build diesel_tables from the auto-registered DIESEL_TABLES slice
    let diesel_tables: BTreeMap<&str, Vec<(&str, &str)>> = crate::DIESEL_TABLES
        .iter()
        .map(|info| (info.name, (info.columns)()))
        .collect();

    let mut errors: Vec<String> = Vec::new();
    let mut nullable_exceptions: Vec<String> = Vec::new();
    let mut column_order_drift: Vec<String> = Vec::new();
    let mut mismatched_columns: Vec<String> = Vec::new();

    // Check: every Diesel table should exist in CRDB.
    for diesel_table in diesel_tables.keys() {
        if !crdb_tables.contains_key(*diesel_table) {
            errors.push(format!(
                "Table '{diesel_table}' exists in Diesel schema (schema.rs) \
                 but is missing from CRDB (dbinit.sql).",
            ));
        }
    }

    // For each table present in both, compare columns.
    for (table_name, diesel_cols) in &diesel_tables {
        let Some(crdb_cols) = crdb_tables.get(*table_name) else {
            // Diesel-only table, already recorded above.
            continue;
        };

        let is_view = crdb_views.contains(*table_name);

        // Build a map of CRDB columns for this table.
        let crdb_col_map: HashMap<&str, &CrdbColumn> =
            crdb_cols.iter().map(|c| (c.column_name.as_str(), c)).collect();

        // Check each Diesel column exists in CRDB with the right type.
        for (col_name, diesel_type_name) in diesel_cols {
            let key = format!("{table_name}.{col_name}");
            let Some(crdb_col) = crdb_col_map.get(col_name) else {
                mismatched_columns.push(key.clone());
                continue;
            };

            let diesel_type = match normalize_diesel_type(diesel_type_name) {
                Ok(t) => t,
                Err(e) => {
                    errors.push(format!(
                        "{key}: failed to parse Diesel type: {e}",
                    ));
                    continue;
                }
            };

            match expected_diesel_type(
                &crdb_col.data_type,
                crdb_col.is_nullable,
                &crdb_col.udt_name,
                &enum_map,
            ) {
                Ok(crdb_type) => {
                    let matches = if is_view {
                        // For views, information_schema reports all
                        // columns as nullable regardless of the
                        // underlying query. So we compare types after
                        // stripping Nullable<> from both sides.
                        diesel_type_matches_ignoring_nullability(
                            &diesel_type,
                            &crdb_type,
                        )
                    } else {
                        diesel_type_matches_expected(&diesel_type, &crdb_type)
                    };
                    if !matches {
                        // Check if this is a nullable exception: CRDB
                        // says NOT NULL but Diesel says Nullable, and
                        // that explains the mismatch.
                        if !is_view && !crdb_col.is_nullable {
                            let expected_nullable = ColumnType {
                                nullable: true,
                                ..crdb_type.clone()
                            };
                            if diesel_type_matches_expected(
                                &diesel_type,
                                &expected_nullable,
                            ) {
                                nullable_exceptions.push(key.clone());
                                continue;
                            }
                        }
                        mismatched_columns.push(key.clone());
                    }
                }
                Err(e) => {
                    // If expected_diesel_type can't map the CRDB type,
                    // that's drift too (unknown UDT, etc.).
                    mismatched_columns.push(key.clone());
                    eprintln!("  note: {key}: {e}");
                }
            }
        }

        // Check each CRDB column exists in Diesel.
        let diesel_col_names: std::collections::HashSet<&str> =
            diesel_cols.iter().map(|(name, _)| *name).collect();
        for crdb_col in crdb_cols {
            if !diesel_col_names.contains(crdb_col.column_name.as_str()) {
                let key = format!("{table_name}.{}", crdb_col.column_name);
                mismatched_columns.push(key);
            }
        }

        // Check column ordering: the relative order of columns that
        // exist in both Diesel and CRDB must match. We filter to only
        // shared columns so that known existence drift doesn't cause
        // false positives here.
        let diesel_col_order: Vec<&str> = diesel_cols
            .iter()
            .map(|(name, _)| *name)
            .filter(|name| crdb_col_map.contains_key(name))
            .collect();
        let crdb_col_order: Vec<&str> = crdb_cols
            .iter()
            .map(|c| c.column_name.as_str())
            .filter(|name| diesel_col_names.contains(name))
            .collect();
        if diesel_col_order != crdb_col_order {
            column_order_drift.push(table_name.to_string());
        }
    }

    // Sort and compare each anomaly category against its expected file.
    //
    // NOTE: We intentionally do NOT use expectorate here because we don't
    // want EXPECTORATE=overwrite to allow people to accidentally paper over
    // schema drift. When these assertions fail the output should be
    // investigated, not blindly regenerated.

    nullable_exceptions.sort();
    assert!(
        nullable_exceptions.is_empty(),
        "Found columns where CRDB says NOT NULL but Diesel says Nullable. \
         Fix the Diesel schema or CRDB schema rather than adding exceptions: \
         {nullable_exceptions:?}"
    );

    column_order_drift.sort();
    assert!(
        column_order_drift.is_empty(),
        "Column order in schema.rs differs from CRDB for these tables: \
         {column_order_drift:?}\n\n\
         Fix the column order in schema.rs or dbinit.sql so they match."
    );

    mismatched_columns.sort();
    let actual_drift = format_list(&mismatched_columns);
    let expected_drift =
        std::fs::read_to_string("tests/output/schema_known_drift.txt")
            .expect("failed to read schema_known_drift.txt");
    similar_asserts::assert_eq!(
        expected_drift,
        actual_drift,
        "Known drift list doesn't match expected.\n\n\
         This file tracks columns where the Diesel type doesn't match \
         CRDB, or columns that exist in only one side. If this list \
         changed, investigate whether the schemas should be fixed \
         rather than updating the file."
    );

    // Print informational output about drift for visibility.
    if !mismatched_columns.is_empty() {
        eprintln!(
            "Known schema drift ({} item(s), fix when possible):",
            mismatched_columns.len(),
        );
        for entry in &mismatched_columns {
            eprintln!("  {entry}");
        }
    }

    // Hard errors are truly unexpected problems (e.g. type parse failures).
    if !errors.is_empty() {
        errors.sort();
        panic!(
            "Diesel schema.rs does not match CRDB dbinit.sql \
             ({} error(s)):\n\n{}",
            errors.len(),
            errors.join("\n\n"),
        );
    }
}
