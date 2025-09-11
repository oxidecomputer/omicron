// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Run SQL queries against the timeseries database.
//!
//! # Overview
//!
//! `oximeter` collects and stores samples from timeseries. The schema for those
//! samples is defined by applications, using the [`Target`](oximeter::Target)
//! and [`Metric`](oximeter::Metric) traits. Samples from these timeseries are
//! not stored in explicit tables, however. They are "unrolled" into the fields
//! and measurements, which are stored in a table based on their _data type_.
//! For example, `String` fields are stored in the `oximeter.fields_string`
//! table. (See RFD 161 for more details.)
//!
//! This arrangement is flexible and simple, since we can statically define the
//! tables we need, rather than say create a new table for each timeseries
//! schema. However, the drawback of this is that the timeseries data is not
//! easily queried directly. The data is split across many tables, and
//! interleaved with other timeseries, which may not even share a schema.
//!
//! The tools in this module are for making "normal" SQL queries transparently
//! act on the "virtual tables" that are implied by each timeseries. It's
//! effectively a SQL-to-SQL transpiler, converting queries against the
//! timeseries into one or more queries against the actual tables in ClickHouse.

// Copyright 2023 Oxide Computer Company

use crate::DatumType;
use crate::Error as OxdbError;
use crate::FieldType;
use crate::TimeseriesName;
use crate::TimeseriesSchema;
use crate::query::field_table_name;
use crate::query::measurement_table_name;
use indexmap::IndexSet;
use oximeter::MetricsError;
use sqlparser::ast::BinaryOperator;
use sqlparser::ast::Cte;
use sqlparser::ast::Distinct;
use sqlparser::ast::Expr;
use sqlparser::ast::GroupByExpr;
use sqlparser::ast::Ident;
use sqlparser::ast::Join;
use sqlparser::ast::JoinConstraint;
use sqlparser::ast::JoinOperator;
use sqlparser::ast::ObjectName;
use sqlparser::ast::OrderByExpr;
use sqlparser::ast::Query;
use sqlparser::ast::Select;
use sqlparser::ast::SelectItem;
use sqlparser::ast::SetExpr;
use sqlparser::ast::Statement;
use sqlparser::ast::TableAlias;
use sqlparser::ast::TableFactor;
use sqlparser::ast::TableWithJoins;
use sqlparser::ast::Value;
use sqlparser::ast::With;
use sqlparser::dialect::AnsiDialect;
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::ops::ControlFlow;
use std::sync::OnceLock;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("SQL parsing error")]
    Parser(#[from] ParserError),

    #[error("Unsupported SQL: {0}")]
    UnsupportedSql(&'static str),

    #[error("Unsupported function: '{func}'")]
    UnsupportedFunction { func: String },

    #[error("Invalid column '{name}' for timeseries '{timeseries_name}'")]
    InvalidColumn { name: String, timeseries_name: String },

    #[error(
        "Table name '{table_name}' in select query does not match \
        timeseries name '{timeseries_name}'"
    )]
    TableInSelectIsNotTimeseries { table_name: String, timeseries_name: String },

    #[error("Invalid timeseries name: '{name}'")]
    InvalidTimeseriesName { name: String },
}

/// The oximeter timeseries SQL dialect.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct OxdbDialect;

impl Dialect for OxdbDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        AnsiDialect {}.is_identifier_start(ch)
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        AnsiDialect {}.is_identifier_part(ch) || ch == ':'
    }
}

/// A SQL statement that is probably supported.
///
/// There's a big range of statements that are not supported. This is guaranteed
/// to be a single select statement, where all the items being selected FROM
/// are:
///
/// - concrete tables that could be timeseries (valid names)
/// - a subquery against a restricted query
#[derive(Clone, Debug)]
pub struct RestrictedQuery {
    safe_sql: SafeSql,
    query: Query,
    timeseries: IndexSet<TimeseriesName>,
}

impl std::fmt::Display for RestrictedQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.query)
    }
}

macro_rules! unsupported {
    ($msg:literal) => {
        Err(OxdbError::from(Error::UnsupportedSql($msg)))
    };
}

/// A tabular result from a SQL query against a timeseries.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct Table {
    /// The name of each column in the result set.
    pub column_names: Vec<String>,
    /// The rows of the result set, one per column.
    pub rows: Vec<Vec<serde_json::Value>>,
}

/// The full result of running a SQL query against a timeseries.
#[derive(Clone, Debug)]
pub struct QueryResult {
    /// The query as written by the client.
    pub original_query: String,
    /// The rewritten query, run against the JOINed representation of the
    /// timeseries.
    ///
    /// This is the query that is actually run in the database itself.
    pub rewritten_query: String,
    /// Summary of the resource usage of the query.
    pub summary: oxql_types::QuerySummary,
    /// The result of the query, with column names and rows.
    pub table: Table,
}

/// A helper type to preprocess any ClickHouse-specific SQL, and present a
/// known-safe version of it to the main `sqlparser` code.
///
/// This is currently used to handle ASOF JOINs, which are a ClickHouse-specific
/// JOIN that joins rows based on a "closest match" condition. However, a
/// standard SQL parser will take an expression like:
///
/// ```sql
/// SELECT foo ASOF JOIN bar
/// ```
///
/// And interpret the `ASOF` as an alias for `FOO`, as if one had written
///
/// ```sql
/// SELECT foo AS asof JOIN bar
/// ```
///
/// This basically detects and removes a bare `ASOF` in that case, so the parser
/// can run normally.
#[derive(Clone, Debug)]
struct SafeSql {
    original: String,
    safe: String,
}

impl SafeSql {
    fn new(sql: impl AsRef<str>) -> Self {
        // The regex crate doesn't support look-arounds, so we'll have to
        // manually find sequences like `ASOF JOIN`, that are not preceded by
        // `AS`.
        let sql = sql.as_ref().trim().trim_end_matches(';');
        let mut original = Vec::new();
        let mut safe = Vec::new();
        let mut tokens = sql.split_ascii_whitespace().peekable();
        while let Some(token) = tokens.next() {
            // Always push the current token.
            if token.parse::<TimeseriesName>().is_ok() {
                let tok = format!("\"{token}\"");
                safe.push(tok.clone());
                original.push(tok);
            } else {
                safe.push(token.to_string());
                original.push(token.to_string());
            }

            // If the next token is ASOF, and the current is _not_ AS, then this
            // is something like `select foo asof join bar`, and we want to chop
            // out the `asof`. Consume the next token, and break the SQL string,
            // by pushing a new chunk at the end.
            if let Some(next_token) = tokens.peek() {
                if !token.eq_ignore_ascii_case("as")
                    && next_token.eq_ignore_ascii_case("asof")
                {
                    original.push(tokens.next().unwrap().to_string());
                }
            }
        }
        Self { original: original.join(" "), safe: safe.join(" ") }
    }

    fn safe_sql(&self) -> &str {
        &self.safe
    }
}

impl RestrictedQuery {
    /// Construct a new restricted query.
    pub fn new(sql: impl AsRef<str>) -> Result<Self, OxdbError> {
        let safe_sql = SafeSql::new(sql);
        let statements = Parser::parse_sql(&OxdbDialect, &safe_sql.safe_sql())
            .map_err(Error::from)?;
        if statements.len() != 1 {
            return unsupported!("Only a single SQL statement is supported");
        }

        let statement = statements.into_iter().next().unwrap();
        let Statement::Query(mut query) = statement else {
            return unsupported!("Statement must be a SELECT query");
        };

        // Walk the AST before doing any real processing or transformation, and
        // validate any function calls are on the allow-list.
        let maybe_denied_function =
            sqlparser::ast::visit_expressions(&query, |expr| {
                if let Expr::Function(func) = expr {
                    if let Some(name) = func.name.0.first() {
                        if !function_allow_list()
                            .iter()
                            .any(|f| f.name == name.value.as_str())
                        {
                            return ControlFlow::Break(name.value.clone());
                        }
                    }
                }
                ControlFlow::Continue(())
            });
        if let ControlFlow::Break(func) = maybe_denied_function {
            return Err(OxdbError::from(Error::UnsupportedFunction { func }));
        };

        let timeseries = Self::process_query(&mut query)?;
        Ok(Self { safe_sql, query: *query, timeseries })
    }

    /// Convert the original SQL into a query specifically for the `oximeter`
    /// timeseries table organization.
    pub fn to_oximeter_sql(
        &self,
        timeseries_schema: &BTreeMap<TimeseriesName, TimeseriesSchema>,
    ) -> Result<String, OxdbError> {
        self.generate_timeseries_ctes(&timeseries_schema).map(|cte_tables| {
            if cte_tables.is_empty() {
                // The query didn't reference any timeseries at all, let's just
                // return it
                self.safe_sql.original.clone()
            } else {
                // There are some timeseries referenced. Let's return a query
                // constructed by building the CTEs, and then the _original_
                // SQL, which may have `ASOF JOIN`s in it.
                format!(
                    "{} {}",
                    With { recursive: false, cte_tables },
                    self.safe_sql.original,
                )
            }
        })
    }

    // For each timeseries named in `self`, generate a CTE that creates the
    // virtual table for that timeseries by joining all its component parts.
    fn generate_timeseries_ctes(
        &self,
        timeseries_schema: &BTreeMap<TimeseriesName, TimeseriesSchema>,
    ) -> Result<Vec<Cte>, OxdbError> {
        let mut ctes = Vec::with_capacity(self.timeseries.len());
        for timeseries in self.timeseries.iter() {
            let schema =
                timeseries_schema.get(timeseries).ok_or_else(|| {
                    OxdbError::TimeseriesNotFound(
                        timeseries.as_str().to_owned(),
                    )
                })?;
            ctes.push(Self::build_timeseries_cte(schema));
        }
        Ok(ctes)
    }

    // Given a timeseries schema, return a CTE which generates the equivalent
    // virtual table.
    //
    // As timeseries samples are ingested, we "unroll" them in various ways, and
    // store them in a set of normalized tables. These contain the _fields_ (on
    // table per field data type) and the measurements (one table per
    // measurement data type). This method reverses that process, creating a
    // single, virtual table that represents all samples from the timeseries
    // (plural) of the same schema.
    //
    // It generates a CTE like so:
    //
    // ```sql
    // WITH {timeseries_name} AS (
    //   SELECT
    //     timeseries_key,
    //     filter_on_{field_name0}.field_value as {field_name0},
    //     filter_on_{field_name1}.field_value as {field_name1},
    //     ...
    //     measurements.timestamp AS timestamp,
    //     measurements.datum as datum,
    //   FROM
    //     (
    //       SELECT DINSTINCT timeseries_key,
    //         field_value
    //       FROM
    //         fields_{field_type}
    //       WHERE
    //         timeseries_name = '{timeseries_name}'
    //         AND field_name = '{field_name0}
    //     ) AS filter_on_{field_name0}
    //     JOIN (
    //       ... select next field table
    //     ) AS filter_on_{field_name1} ON filter_on_{field_name0}.timeseries_key = filter_on_{field_name1}
    //     ...
    //     JOIN (
    //       SELECT
    //         timeseries_key,
    //         timestamp,
    //         datum,
    //       FROM
    //         measurements_{datum_type}
    //       WHERE
    //         timeseries_name = '{timeseries_name}'
    //     ) AS measurements ON filter_on_fieldN.timeseries_key = measurements.timeseries_key
    //   ORDER BY
    //     timeseries_key,
    //     timestamp
    // )
    // ```
    //
    // In other words, it should generate a CTE that one can query as if the
    // timeseries itself where an actual table in the database, like:
    //
    // ```
    // timeseries_key | field_name0 | field_name1 | ... | timestamp | datum
    // ---------------+-------------+-------------+ ... +-----------+------
    // key0           |  field0_0   |  field0_1   | ... |    t0     |   d0
    // key0           |  field0_0   |  field0_1   | ... |    t1     |   d1
    // key0           |  field0_0   |  field0_1   | ... |    t2     |   d2
    // key0           |  field0_0   |  field0_1   | ... |    t3     |   d3
    // ...
    // key1           |  field1_0   |  field1_1   | ... |    t0     |   d0
    // key1           |  field1_0   |  field1_1   | ... |    t1     |   d1
    // key1           |  field1_0   |  field1_1   | ... |    t2     |   d2
    // key1           |  field1_0   |  field1_1   | ... |    t3     |   d3
    // ...
    // ```
    //
    // In this case, all rows with `key0` are from the "first" timeseries with
    // this schema. `fieldX_Y` indicates the Yth field from timeseries with
    // `key0` as its key.
    fn build_timeseries_cte(schema: &TimeseriesSchema) -> Cte {
        // First build each query against the relevant field tables.
        //
        // These are the `SELECT DISTINCT ... FROM fields_{field_type}`
        // subqueries above.
        let mut field_queries = Vec::with_capacity(schema.field_schema.len());
        for field_schema in schema.field_schema.iter() {
            let field_query = Self::build_field_query(
                &schema.timeseries_name,
                &field_schema.name,
                &field_schema.field_type,
            );
            field_queries.push((field_schema.name.as_str(), field_query));
        }

        // Generate the last measurement query, the last subquery in the main
        // CTE.
        let measurement_query = Self::build_measurement_query(
            &schema.timeseries_name,
            &schema.datum_type,
        );

        // The "top-level" columns are the columns outputted by the CTE itself.
        //
        // These are the aliased columns of the full, reconstructed table
        // representing the timeseries. This makes the timeseries_key available,
        // as well as each field aliased to the actual field name, and the
        // measurements.
        let mut top_level_projections =
            Vec::with_capacity(field_queries.len() + 2);

        // Create the projection of the top-level timeseries_key.
        //
        // This is taken from the first field, which always exists, since
        // timeseries have at least one field. This creates the expression:
        // `filter_{field_name}.timeseries_key AS timeseries_key`
        let timeseries_key_projection = SelectItem::ExprWithAlias {
            expr: Expr::CompoundIdentifier(vec![
                Self::field_subquery_alias(field_queries[0].0),
                Self::str_to_ident("timeseries_key"),
            ]),
            alias: Self::str_to_ident("timeseries_key"),
        };
        top_level_projections.push(timeseries_key_projection);

        // We'll build a big `TableWithJoins` to express the entire JOIN
        // operation between all fields and the measurements. This is the "meat"
        // of the CTE for this timeseries, joining the constituent records into
        // the virtual table for this schema.
        //
        // We select first from the subquery specifying the first field query.
        let mut cte_from = TableWithJoins {
            relation: TableFactor::Derived {
                lateral: false,
                subquery: Self::select_to_query(field_queries[0].1.clone()),
                alias: Some(TableAlias {
                    name: Self::field_subquery_alias(field_queries[0].0),
                    columns: vec![],
                }),
            },
            joins: Vec::with_capacity(field_queries.len()),
        };

        // For all field queries, create a projection for the field_value,
        // aliased as the field name.
        let field_queries: Vec<_> = field_queries.into_iter().collect();
        for (i, (field_name, query)) in field_queries.iter().enumerate() {
            // Select the field_value from this field query, renaming it to the
            // actual field name.
            let projection = SelectItem::ExprWithAlias {
                expr: Expr::CompoundIdentifier(vec![
                    Self::field_subquery_alias(field_name),
                    Self::str_to_ident("field_value"),
                ]),
                alias: Self::str_to_ident(field_name),
            };
            top_level_projections.push(projection);

            // We've inserted the first subquery as the `from.relation` field in
            // the main CTE we're building. We need to skip that one, even
            // though we added its aliased `field_value` column to the top level
            // projections.
            //
            // Any additional field subqueries are inserted in the JOIN portion
            // of the CTE.
            if i == 0 {
                continue;
            }
            let relation = TableFactor::Derived {
                lateral: false,
                subquery: Self::select_to_query(query.clone()),
                alias: Some(TableAlias {
                    name: Self::field_subquery_alias(field_name),
                    columns: vec![],
                }),
            };

            // The join is always INNER, and is on the timeseries_key only.
            // ClickHouse does not support `USING <column>` when using multiple
            // JOINs simultaneously, so we always write this as an `ON`
            // constraint, between the previous field subquery and this one.
            //
            // I.e., `ON filter_foo.timeseries_key = filter_bar.timeseries_key`
            let last_field_name = &field_queries[i - 1].0;
            let constraints = Expr::BinaryOp {
                left: Box::new(Expr::CompoundIdentifier(vec![
                    Self::field_subquery_alias(last_field_name),
                    Self::str_to_ident("timeseries_key"),
                ])),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::CompoundIdentifier(vec![
                    Self::field_subquery_alias(field_name),
                    Self::str_to_ident("timeseries_key"),
                ])),
            };
            let join_operator =
                JoinOperator::Inner(JoinConstraint::On(constraints));
            cte_from.joins.push(Join { relation, join_operator });
        }

        // Finally, we need to project and join in the measurements table.
        let datum_columns = Self::datum_type_to_columns(&schema.datum_type);
        for col in datum_columns.iter() {
            let projection = SelectItem::ExprWithAlias {
                expr: Expr::CompoundIdentifier(vec![
                    Self::str_to_ident("measurements"),
                    Self::str_to_ident(col),
                ]),
                alias: Self::str_to_ident(col),
            };
            top_level_projections.push(projection);
        }
        let relation = TableFactor::Derived {
            lateral: false,
            subquery: Self::select_to_query(measurement_query),
            alias: Some(TableAlias {
                name: Self::str_to_ident("measurements"),
                columns: vec![],
            }),
        };
        let constraints = Expr::BinaryOp {
            left: Box::new(Expr::CompoundIdentifier(vec![
                Self::field_subquery_alias(
                    &schema.field_schema.last().unwrap().name,
                ),
                Self::str_to_ident("timeseries_key"),
            ])),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::CompoundIdentifier(vec![
                Self::str_to_ident("measurements"),
                Self::str_to_ident("timeseries_key"),
            ])),
        };
        let join_operator =
            JoinOperator::Inner(JoinConstraint::On(constraints));
        cte_from.joins.push(Join { relation, join_operator });

        // To build the real virtual table for all the timeseries, we really
        // need to sort the samples as if they were inserted into the table
        // itself. ClickHouse partitions the tables dynamically since we're
        // using a MergeTree engine, which groups and repacks rows in the
        // background.
        //
        // We'll impose a consistent sorting order here. If one does not include
        // this, results are inconsistent, since the different data parts of the
        // measurements tables are not read in order every time.
        let order_by = top_level_projections
            .iter()
            .filter_map(|proj| {
                let SelectItem::ExprWithAlias { alias, .. } = &proj else {
                    unreachable!();
                };
                if alias.value == "timeseries_key"
                    || alias.value == "start_time"
                    || alias.value == "timestamp"
                {
                    Some(OrderByExpr {
                        expr: Expr::Identifier(alias.clone()),
                        asc: None,
                        nulls_first: None,
                    })
                } else {
                    None
                }
            })
            .collect();

        // We now have all the subqueries joined together, plus the columns
        // we're projecting from that join result. We need to build the final
        // CTE that represents the full virtual timeseries table.
        let alias = TableAlias {
            name: Ident {
                value: schema.timeseries_name.to_string(),
                quote_style: Some('"'),
            },
            columns: vec![],
        };
        let top_level_select = Select {
            distinct: None,
            top: None,
            projection: top_level_projections,
            into: None,
            from: vec![cte_from],
            lateral_views: vec![],
            selection: None,
            group_by: GroupByExpr::Expressions(vec![]),
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None,
            named_window: vec![],
            qualify: None,
            value_table_mode: None,
        };
        let mut query = Self::select_to_query(top_level_select);
        query.order_by = order_by;
        Cte { alias, query, from: None, materialized: None }
    }

    // Create a SQL parser `Ident` with a the given name.
    fn str_to_ident(s: &str) -> Ident {
        Ident { value: s.to_string(), quote_style: None }
    }

    // Return an `Ident` alias for a subquery of a specific field table.
    //
    // E.g., the `filter_on_foo` in `(SELECT DISTINCT ... ) AS filter_on_foo`.
    fn field_subquery_alias(field_name: &str) -> Ident {
        Self::str_to_ident(format!("filter_on_{field_name}").as_str())
    }

    // Return the required measurement columns for a specific datum type.
    //
    // Scalar measurements have only a timestamp and datum. Cumulative counters
    // have those plus a start_time. And histograms have those plus the bins,
    // counts, min, max, sum of samples, sum of squares, and quantile arrays.
    fn datum_type_to_columns(
        datum_type: &DatumType,
    ) -> &'static [&'static str] {
        if datum_type.is_histogram() {
            &[
                "start_time",
                "timestamp",
                "bins",
                "counts",
                "min",
                "max",
                "sum_of_samples",
                "squared_mean",
                "p50_marker_heights",
                "p50_marker_positions",
                "p50_desired_marker_positions",
                "p90_marker_heights",
                "p90_marker_positions",
                "p90_desired_marker_positions",
                "p99_marker_heights",
                "p99_marker_positions",
                "p99_desired_marker_positions",
            ]
        } else if datum_type.is_cumulative() {
            &["start_time", "timestamp", "datum"]
        } else {
            &["timestamp", "datum"]
        }
    }

    fn select_to_query(select: Select) -> Box<Query> {
        Box::new(Query {
            with: None,
            body: Box::new(SetExpr::Select(Box::new(select))),
            order_by: vec![],
            limit: None,
            limit_by: vec![],
            offset: None,
            fetch: None,
            locks: vec![],
            for_clause: None,
        })
    }

    // Build a single subquery which selects the unique fields with the provided
    // name. E.g., this creates:
    //
    // ```sql
    // SELECT DISTINCT timeseries_key,
    //   field_value
    // FROM
    //   fields_{field_type}
    // WHERE
    //   timeseries_name = '{timeseries_name}'
    //   AND field_name = '{field_name}'
    // ```
    fn build_field_query(
        timeseries_name: &TimeseriesName,
        field_name: &str,
        field_type: &FieldType,
    ) -> Select {
        // FROM oximeter.fields_{field_type}
        let from = TableWithJoins {
            relation: TableFactor::Table {
                name: ObjectName(vec![Self::str_to_ident(&format!(
                    "oximeter.{}",
                    field_table_name(*field_type)
                ))]),
                alias: None,
                args: None,
                with_hints: vec![],
                version: None,
                partitions: vec![],
            },
            joins: vec![],
        };

        // SELECT timeseries_key, field_value
        let projection = vec![
            SelectItem::UnnamedExpr(Expr::Identifier(Self::str_to_ident(
                "timeseries_key",
            ))),
            SelectItem::UnnamedExpr(Expr::Identifier(Self::str_to_ident(
                "field_value",
            ))),
        ];

        // WHERE timeseries_name = '{timeseries_name}' AND field_name = '{field_name}'
        let selection = Some(Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Self::str_to_ident(
                    "timeseries_name",
                ))),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::SingleQuotedString(
                    timeseries_name.to_string(),
                ))),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Self::str_to_ident(
                    "field_name",
                ))),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::SingleQuotedString(
                    field_name.to_string(),
                ))),
            }),
        });

        Select {
            distinct: Some(Distinct::Distinct),
            top: None,
            projection,
            into: None,
            from: vec![from],
            lateral_views: vec![],
            selection,
            group_by: GroupByExpr::Expressions(vec![]),
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None,
            named_window: vec![],
            qualify: None,
            value_table_mode: None,
        }
    }

    // Build a single subquery which selects the measurements with the provided
    // name. E.g., this creates:
    //
    // ```sql
    // SELECT
    //   timeseries_key,
    //   timestamp,
    //   datum
    // FROM
    //   measurements_{datum_type}
    // WHERE
    //   timeseries_name = '{timeseries_name}'
    // ```
    fn build_measurement_query(
        timeseries_name: &TimeseriesName,
        datum_type: &DatumType,
    ) -> Select {
        // FROM measurements_{datum_type}
        let from = TableWithJoins {
            relation: TableFactor::Table {
                name: ObjectName(vec![Self::str_to_ident(&format!(
                    "oximeter.{}",
                    measurement_table_name(*datum_type)
                ))]),
                alias: None,
                args: None,
                with_hints: vec![],
                version: None,
                partitions: vec![],
            },
            joins: vec![],
        };

        // SELECT timeseries_key, timestamp, [datum type columns]
        let mut projection = vec![SelectItem::UnnamedExpr(Expr::Identifier(
            Self::str_to_ident("timeseries_key"),
        ))];
        let datum_projection = Self::datum_type_to_columns(datum_type);
        projection.extend(datum_projection.iter().map(|name| {
            SelectItem::UnnamedExpr(Expr::Identifier(Self::str_to_ident(name)))
        }));

        // WHERE timeseries_name = '{timeseries_name}'
        let selection = Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Self::str_to_ident(
                "timeseries_name",
            ))),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Value(Value::SingleQuotedString(
                timeseries_name.to_string(),
            ))),
        });

        Select {
            distinct: None,
            top: None,
            projection,
            into: None,
            from: vec![from],
            lateral_views: vec![],
            selection,
            group_by: GroupByExpr::Expressions(vec![]),
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None,
            named_window: vec![],
            qualify: None,
            value_table_mode: None,
        }
    }

    // Verify that the identifier is a single, concrete timeseries name.
    fn extract_timeseries_name(
        from: &[Ident],
    ) -> Result<IndexSet<TimeseriesName>, OxdbError> {
        if from.len() != 1 {
            return unsupported!(
                "Query must select from single named \
                timeseries, with no database"
            );
        }
        from[0]
            .value
            .parse()
            .map(|n| indexmap::indexset! { n })
            .map_err(|_| MetricsError::InvalidTimeseriesName.into())
    }

    // Process a single "table factor", the <object> in `FROM <object>` to
    // extract the names of the timeseries it refers to.
    //
    // Note this is recursive since we do support basic inner joins.
    fn process_table_factor(
        relation: &mut TableFactor,
    ) -> Result<IndexSet<TimeseriesName>, OxdbError> {
        match relation {
            TableFactor::Table { ref mut name, args, with_hints, .. } => {
                if args.is_some() || !with_hints.is_empty() {
                    return unsupported!(
                        "Table functions and hints are not supported"
                    );
                }
                let timeseries_name = Self::extract_timeseries_name(&name.0)?;
                // Rewrite the quote style to be backticks, so that the
                // resulting actual query translates into a valid identifier for
                // ClickHouse, naming the CTE's well generate later.
                name.0[0].quote_style = Some('"');
                Ok(timeseries_name)
            }
            TableFactor::Derived { lateral: false, subquery, .. } => {
                RestrictedQuery::process_query(subquery)
            }
            _ => {
                return unsupported!(
                    "Query must select from concrete tables or subqueries on them"
                );
            }
        }
    }

    // Process a parsed query, returning the named timeseries that it refers to.
    //
    // This is the entry-point for our query processing implementation. We take
    // a parsed query from `sqlparser`, and extract the virtual tables
    // (timeseries names) that we'll need to construct in order to actually run
    // it against our database.
    //
    // Note that we return an _ordered set_ of the timeseries names. This is to
    // produce the CTEs that correspond to each timeseries, but without
    // duplicating the actual CTE.
    fn process_query(
        query: &mut Query,
    ) -> Result<IndexSet<TimeseriesName>, OxdbError> {
        // Some basic checks limiting the scope of the query.
        if query.with.is_some()
            || query.fetch.is_some()
            || !query.locks.is_empty()
        {
            return unsupported!(
                "CTEs, FETCH and LOCKS are not currently supported"
            );
        }
        let SetExpr::Select(select) = &mut *query.body else {
            return unsupported!("Only SELECT queries are currently supported");
        };

        // For each object we're selecting from (a table factor), process that
        // directly, and process any JOINs it also contains.
        let mut timeseries = IndexSet::with_capacity(select.from.len());
        if select.from.len() > 1 {
            return unsupported!(
                "Query must select from a single named table, with no database"
            );
        }
        if let Some(from) = select.from.iter_mut().next() {
            timeseries.extend(Self::process_table_factor(&mut from.relation)?);
            for join in from.joins.iter_mut() {
                let JoinOperator::Inner(op) = &join.join_operator else {
                    return unsupported!(
                        "Only INNER JOINs are supported, using \
                        explicit constraints"
                    );
                };
                if matches!(op, JoinConstraint::Natural) {
                    return unsupported!(
                        "Only INNER JOINs are supported, using \
                        explicit constraints"
                    );
                }
                timeseries
                    .extend(Self::process_table_factor(&mut join.relation)?);
            }
        }
        Ok(timeseries)
    }
}

static CLICKHOUSE_FUNCTION_ALLOW_LIST: OnceLock<BTreeSet<ClickHouseFunction>> =
    OnceLock::new();

#[derive(Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ClickHouseFunction {
    pub name: &'static str,
    pub usage: &'static str,
    pub description: &'static str,
}

impl std::fmt::Display for ClickHouseFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl ClickHouseFunction {
    fn new(usage: &'static str, description: &'static str) -> Self {
        let name = usage.split_once('(').expect("need parentheses").0;
        Self { name, usage, description }
    }
}

/// Return the set of supported ClickHouse SQL functions, with a short help
/// string.
pub fn function_allow_list() -> &'static BTreeSet<ClickHouseFunction> {
    CLICKHOUSE_FUNCTION_ALLOW_LIST.get_or_init(|| {
        let mut out = BTreeSet::new();

        // Core functions
        out.insert(ClickHouseFunction::new("avg(expr)", "Arithmetic mean"));
        out.insert(ClickHouseFunction::new("min(expr)", "Minimum value"));
        out.insert(ClickHouseFunction::new("max(expr)", "Maximum value"));
        out.insert(ClickHouseFunction::new("sum(expr)", "Sum values"));
        out.insert(ClickHouseFunction::new(
            "count(expr)",
            "Count number of rows",
        ));
        out.insert(ClickHouseFunction::new(
            "now()",
            "Return current timestamp",
        ));
        out.insert(ClickHouseFunction::new(
            "first_value(expr)",
            "First value in a partition",
        ));
        out.insert(ClickHouseFunction::new(
            "last_value(expr)",
            "Last value in a partition",
        ));
        out.insert(ClickHouseFunction::new(
            "any(expr)",
            "First non-NULL value",
        ));
        out.insert(ClickHouseFunction::new(
            "topK(k)(expr)",
            "Estimate K most frequent values",
        ));
        out.insert(ClickHouseFunction::new(
            "groupArray(expr)",
            "Create an array from rows",
        ));
        out.insert(ClickHouseFunction::new(
            "argMin(arg, val)",
            "Argument of minimum value",
        ));
        out.insert(ClickHouseFunction::new(
            "argMax(arg, val)",
            "Argument of maximum value",
        ));
        out.insert(ClickHouseFunction::new(
            "quantileExact(quantile)(expr)",
            "Exact quantile of inputs",
        ));

        // To support histograrms, we allow the `-ForEach` combinator functions.
        //
        // See
        // https://clickhouse.com/docs/en/sql-reference/aggregate-functions/combinators#-foreach,
        // but briefly, this allows computing the aggregate function across
        // corresponding array elements.
        out.insert(ClickHouseFunction::new(
            "maxForEach(array expr)",
            "Max of corresponding array elements",
        ));
        out.insert(ClickHouseFunction::new(
            "minForEach(array expr)",
            "Min of corresponding array elements",
        ));
        out.insert(ClickHouseFunction::new(
            "sumForEach(array expr)",
            "Sum of corresponding array elements",
        ));
        out.insert(ClickHouseFunction::new(
            "avgForEach(array expr)",
            "Mean of corresponding array elements",
        ));

        // Type conversions
        //
        // Note that `cast` itself will be difficult to use, because ClickHouse is
        // particular about the capitalization of type names, e.g., it must be
        // `cast(x as String)` not `cast(x as STRING)`.
        out.insert(ClickHouseFunction::new(
            "toString(x)",
            "Convert to a string",
        ));
        out.insert(ClickHouseFunction::new("toInt8(x)", "Convert to an i8"));
        out.insert(ClickHouseFunction::new("toUInt8(x)", "Convert to a u8"));
        out.insert(ClickHouseFunction::new("toInt16(x)", "Convert to an i16"));
        out.insert(ClickHouseFunction::new("toUInt16(x)", "Convert to a u16"));
        out.insert(ClickHouseFunction::new("toInt32(x)", "Convert to an i32"));
        out.insert(ClickHouseFunction::new("toUInt32(x)", "Convert to a u32"));
        out.insert(ClickHouseFunction::new("toInt64(x)", "Convert to an i64"));
        out.insert(ClickHouseFunction::new("toUInt64(x)", "Convert to a u64"));
        out.insert(ClickHouseFunction::new(
            "toFloat32(x)",
            "Convert to an f32",
        ));
        out.insert(ClickHouseFunction::new(
            "toFloat64(x)",
            "Convert to an f64",
        ));
        out.insert(ClickHouseFunction::new(
            "toDate(x)",
            "Convert to a 32-bit date",
        ));
        out.insert(ClickHouseFunction::new(
            "toDateTime(x)",
            "Convert to a 32-bit date and time",
        ));
        out.insert(ClickHouseFunction::new(
            "toDateTime64(x)",
            "Convert to a 64-bit date and time",
        ));
        out.insert(ClickHouseFunction::new(
            "toIntervalYear(x)",
            "Convert to an interval in years",
        ));
        out.insert(ClickHouseFunction::new(
            "toIntervalQuarter(x)",
            "Convert to an interval in quarters",
        ));
        out.insert(ClickHouseFunction::new(
            "toIntervalMonth(x)",
            "Convert to an interval in months",
        ));
        out.insert(ClickHouseFunction::new(
            "toIntervalWeek(x)",
            "Convert to an interval in weeks",
        ));
        out.insert(ClickHouseFunction::new(
            "toIntervalDay(x)",
            "Convert to an interval in days",
        ));
        out.insert(ClickHouseFunction::new(
            "toIntervalHour(x)",
            "Convert to an interval in hours",
        ));
        out.insert(ClickHouseFunction::new(
            "toIntervalMinute(x)",
            "Convert to an interval in minutes",
        ));
        out.insert(ClickHouseFunction::new(
            "toIntervalSecond(x)",
            "Convert to an interval in seconds",
        ));

        // Array functions
        out.insert(ClickHouseFunction::new(
            "arrayMax([func,] arr)",
            "Maximum in source array",
        ));
        out.insert(ClickHouseFunction::new(
            "arrayMin([func,] arr)",
            "Minimum in source array",
        ));
        out.insert(ClickHouseFunction::new(
            "arraySum([func,] arr)",
            "Sum of elements in source array",
        ));
        out.insert(ClickHouseFunction::new(
            "arrayAvg([func,] arr)",
            "Mean of elements in source array",
        ));
        out.insert(ClickHouseFunction::new(
            "arrayMap(func, arr, ...)",
            "Apply function to elements in source array",
        ));
        out.insert(ClickHouseFunction::new(
            "arrayReduce(func, arr, ...)",
            "Aggregate elements in source array with a function",
        ));
        out.insert(ClickHouseFunction::new(
            "arrayFilter(func, arr, ...)",
            "Apply a lambda to source array",
        ));
        out.insert(ClickHouseFunction::new(
            "arrayDifference(arr)",
            "Difference between adjacent elements in source array",
        ));
        out.insert(ClickHouseFunction::new(
            "indexOf(arr, x)",
            "Index of `x` in source array, or 0",
        ));
        out.insert(ClickHouseFunction::new(
            "length(arr)",
            "Length of source array",
        ));

        // Strings
        out.insert(ClickHouseFunction::new(
            "empty(x)",
            "True if array or string is empty",
        ));
        out.insert(ClickHouseFunction::new(
            "lower(x)",
            "Convert a string to lowercase",
        ));
        out.insert(ClickHouseFunction::new(
            "upper(x)",
            "Convert a string to uppercase",
        ));
        out.insert(ClickHouseFunction::new(
            "reverse(x)",
            "Reverse the bytes (not chars) in a string",
        ));
        out.insert(ClickHouseFunction::new(
            "reverseUTF8(x)",
            "Reverse the characters in a string",
        ));
        out.insert(ClickHouseFunction::new(
            "concat(s1, s2, ...)",
            "Concatenate two or more strings",
        ));
        out.insert(ClickHouseFunction::new(
            "concatWithSeparator(sep, s1, s2, ..)",
            "Concatenate two or more strings with a separator",
        ));
        out.insert(ClickHouseFunction::new(
            "substring(s, offset, len)",
            "Return a substring",
        ));
        out.insert(ClickHouseFunction::new(
            "endsWith(s, suffix)",
            "True if `s` ends with `suffix`",
        ));
        out.insert(ClickHouseFunction::new(
            "startsWith(s, prefix)",
            "True if `s` starts with `prefix`",
        ));
        out.insert(ClickHouseFunction::new(
            "splitByChar(sep, s[, limit])",
            "Split on a separator, up to `limit` times",
        ));
        out.insert(ClickHouseFunction::new(
            "splitByString(sep, s[, limit])",
            "Split by a separating string, up to `limit` times",
        ));

        // Time.
        out.insert(ClickHouseFunction::new(
            "tumble(datetime, interval[, tz])",
            "Nonoverlapping time windows of a specified interval",
        ));
        out.insert(ClickHouseFunction::new(
            "toYear(date)",
            "Extract year from date",
        ));
        out.insert(ClickHouseFunction::new(
            "toQuarter(date)",
            "Extract quarter from date",
        ));
        out.insert(ClickHouseFunction::new(
            "toMonth(date)",
            "Extract month from date",
        ));
        out.insert(ClickHouseFunction::new(
            "toDayOfYear(date)",
            "Index of day in its year",
        ));
        out.insert(ClickHouseFunction::new(
            "toDayOfMonth(date)",
            "Index of day in its month",
        ));
        out.insert(ClickHouseFunction::new(
            "toDayOfWeek(date)",
            "Index of day in its week",
        ));
        out.insert(ClickHouseFunction::new(
            "toHour(date)",
            "Extract hour from date",
        ));
        out.insert(ClickHouseFunction::new(
            "toMinute(date)",
            "Extract minute from date",
        ));
        out.insert(ClickHouseFunction::new(
            "toSecond(date)",
            "Extract second from date",
        ));
        out.insert(ClickHouseFunction::new(
            "toUnixTimestamp(date)",
            "Convert to UNIX timestamp",
        ));
        out.insert(ClickHouseFunction::new(
            "toStartOfInterval(date, INTERVAL x UNIT[, tz])",
            "Convert date to the start of the specified interval",
        ));
        out.insert(ClickHouseFunction::new(
            "date_diff('unit', start, end[, tz])",
            "Difference between two dates in the provided unit",
        ));
        out.insert(ClickHouseFunction::new(
            "date_trunc('unit', date[, tz])",
            "Truncate a datetime to the provided unit",
        ));
        out.insert(ClickHouseFunction::new(
            "date_add('unit', count, date)",
            "Add `count` units to `date`",
        ));
        out.insert(ClickHouseFunction::new(
            "date_sub('unit', count, date)",
            "Subtract `count` units from `date`",
        ));

        // Other
        out.insert(ClickHouseFunction::new(
            "generateUUIDv4()",
            "Generate a random UUID v4",
        ));
        out.insert(ClickHouseFunction::new("rand()", "Uniform random u32"));
        out.insert(ClickHouseFunction::new("rand64()", "Uniform random u64"));
        out.insert(ClickHouseFunction::new(
            "runningDifference(arr)",
            "Difference between adjacent values",
        ));
        out.insert(ClickHouseFunction::new(
            "formatReadableSize(x)",
            "Format a byte count for humans",
        ));
        out.insert(ClickHouseFunction::new(
            "formatReadableTimeDelta(x)",
            "Format an interval for humans",
        ));
        out.insert(ClickHouseFunction::new(
            "formatReadableQuantity(x)",
            "Format a quantity for humans",
        ));
        out
    })
}

#[cfg(test)]
mod tests {
    use super::Error;
    use super::OxdbError;
    use super::RestrictedQuery;
    use super::SafeSql;

    #[test]
    fn test_function_allow_list() {
        assert!(RestrictedQuery::new("SELECT bogus()").is_err());
        assert!(matches!(
            RestrictedQuery::new("SELECT bogus()").unwrap_err(),
            OxdbError::Sql(Error::UnsupportedFunction { .. })
        ));
        assert!(RestrictedQuery::new("SELECT now()").is_ok());
    }

    #[test]
    fn test_ctes_are_not_supported() {
        assert!(matches!(
            RestrictedQuery::new("WITH nono AS (SELECT 1) SELECT * FROM NONO")
                .unwrap_err(),
            OxdbError::Sql(Error::UnsupportedSql(_))
        ));
    }

    #[test]
    fn test_multiple_statements_are_not_supported() {
        assert!(matches!(
            RestrictedQuery::new("SELECT 1; SELECT 2;").unwrap_err(),
            OxdbError::Sql(Error::UnsupportedSql(_))
        ));
    }

    #[test]
    fn test_query_must_be_select_statement() {
        for query in [
            "SHOW TABLES",
            "DROP TABLE foo",
            "CREATE TABLE foo (x Int4)",
            "DESCRIBE foo",
            "EXPLAIN SELECT 1",
            "INSERT INTO foo VALUES (1)",
        ] {
            let err = RestrictedQuery::new(query).unwrap_err();
            println!("{err:?}");
            assert!(matches!(err, OxdbError::Sql(Error::UnsupportedSql(_))));
        }
    }

    #[test]
    fn test_cannot_name_database() {
        let err = RestrictedQuery::new("SELECT * FROM dbname.a:a").unwrap_err();
        assert!(matches!(err, OxdbError::Sql(Error::UnsupportedSql(_))));
    }

    #[test]
    fn test_with_comma_join_fails() {
        let err = RestrictedQuery::new("SELECT * FROM a:a, b:b").unwrap_err();
        println!("{err:?}");
        assert!(matches!(err, OxdbError::Sql(Error::UnsupportedSql(_))));
    }

    #[test]
    fn test_join_must_be_inner() {
        let allowed = ["inner", ""];
        let denied =
            ["natural", "cross", "left outer", "right outer", "full outer"];
        for join in allowed.iter() {
            RestrictedQuery::new(format!("SELECT * FROM a:a {join} JOIN b:b"))
                .unwrap_or_else(|_| {
                    panic!("Should be able to use join type '{join}'")
                });
        }
        for join in denied.iter() {
            let sql = format!("SELECT * FROM a:a {join} JOIN b:b");
            println!("{sql}");
            let err = RestrictedQuery::new(&sql).expect_err(
                format!("Should not be able to use join type '{join}'")
                    .as_str(),
            );
            println!("{err:?}");
            assert!(matches!(err, OxdbError::Sql(Error::UnsupportedSql(_))));
        }
    }

    #[test]
    fn test_allow_limit_offset() {
        let sql = "SELECT * FROM a:b LIMIT 10 OFFSET 10;";
        println!("{sql}");
        RestrictedQuery::new(&sql)
            .expect("Should be able to use LIMIT / OFFSET queries");
    }

    #[test]
    fn test_require_table_is_timeseries_name() {
        assert!(RestrictedQuery::new("SELECT * FROM a:b").is_ok());
        let bad = ["table", "db.table", "no:no:no"];
        for each in bad.iter() {
            let sql = format!("SELECT * FROM {each}");
            RestrictedQuery::new(&sql)
                .expect_err("Should have validated timeseries name");
        }
    }

    #[test]
    fn test_allow_subqueries() {
        assert!(RestrictedQuery::new("SELECT * FROM (SELECT 1);").is_ok());
    }

    #[test]
    fn test_query_with_multiple_timeseries_generates_one_cte() {
        let query = "SELECT * FROM a:b JOIN a:b USING (timeseries_key);";
        let res = RestrictedQuery::new(&query).unwrap();
        assert_eq!(res.timeseries.len(), 1);
    }

    #[test]
    fn test_safe_sql_does_not_modify_original_alias() {
        let query = "SELECT * FROM a:b AS ASOF JOIN a:b";
        let query_with_quotes = "SELECT * FROM \"a:b\" AS ASOF JOIN \"a:b\"";
        let safe = SafeSql::new(query);
        let rewritten = safe.safe_sql();
        println!("{query}");
        println!("{query_with_quotes}");
        println!("{rewritten}");

        // Check that we've written out the same query words, ignoring
        // whitespace.
        let words = query_with_quotes
            .split_ascii_whitespace()
            .rev()
            .collect::<Vec<_>>();
        let rewritten_words = rewritten
            .split_ascii_whitespace()
            .rev()
            .take(words.len())
            .collect::<Vec<_>>();
        assert_eq!(words, rewritten_words);
    }
}
