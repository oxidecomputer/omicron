// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Client methods for running OxQL queries against the timeseries database.

// Copyright 2024 Oxide Computer Company

use super::query_summary::QuerySummary;
use crate::client::Client;
use crate::model;
use crate::oxql;
use crate::oxql::ast::table_ops::filter;
use crate::oxql::ast::table_ops::filter::Filter;
use crate::query::field_table_name;
use crate::Error;
use crate::Metric;
use crate::Target;
use crate::TimeseriesKey;
use oximeter::TimeseriesSchema;
use slog::debug;
use slog::trace;
use slog::Logger;
use std::collections::BTreeMap;
use std::time::Duration;
use std::time::Instant;
use uuid::Uuid;

#[usdt::provider(provider = "clickhouse_client")]
mod probes {
    /// Fires when an OxQL query starts, with the query ID and string.
    fn oxql__query__start(_: &usdt::UniqueId, _: &Uuid, query: &str) {}

    /// Fires when an OxQL query ends, either in success or failure.
    fn oxql__query__done(_: &usdt::UniqueId, _: &Uuid) {}

    /// Fires when an OxQL table operation starts, with the query ID and details
    /// of the operation itself.
    fn oxql__table__op__start(_: &usdt::UniqueId, _: &Uuid, op: &str) {}

    /// Fires when an OxQL table operation ends.
    fn oxql__table__op__done(_: &usdt::UniqueId, _: &Uuid) {}
}

/// The full result of an OxQL query.
#[derive(Clone, Debug)]
pub struct OxqlResult {
    /// A query ID assigned to this OxQL query.
    pub query_id: Uuid,

    /// The total duration of the OxQL query.
    ///
    /// This includes the time to run SQL queries against the database, and the
    /// internal processing for each transformation in the query pipeline.
    pub total_duration: Duration,

    /// The summary for each SQL query run against the ClickHouse database.
    ///
    /// Each OxQL query translates into many calls to ClickHouse. We fetch the
    /// fields; count the number of samples; and finally fetch the samples
    /// themselves. In the future, more may be needed as well.
    ///
    /// This returns a list of summaries, one for each SQL query that was run.
    /// It includes the ClickHouse-assigned query ID for correlation and looking
    /// up in the logs.
    pub query_summaries: Vec<QuerySummary>,

    /// The list of OxQL tables returned from the query.
    pub tables: Vec<oxql::Table>,
}

/// The maximum number of data values fetched from the database for an OxQL
/// query.
//
// The `Client::oxql_query()` API is currently unpaginated. It's also not clear
// _how_ to paginate it. The objects contributing to the size of the returned
// value, the actual data points, are nested several layers deep, inside the
// `Timeseries` and `Table`s. A page size is supposed to refer to the top-level
// object, so we'd need to flatten this hierarchy for that to work. That's
// undesirable because it will lead to a huge amount of duplication of the table
// / timeseries-level information, once for each point.
//
// Also, since we cannot use a cursor-based pagination, we're stuck with
// limit-offset. That means we may need to run substantially all of the query,
// just to know how to retrieve the next page, sidestepping one of the main
// goals of pagination (to limit resource usage).
//
// Note that it's also hard or impossible to _predict_ how much data a query
// will use. We need to count the number of rows in the database, for example,
// _and also_ understand how table operations might change that size. For
// example, alignment is allowed to upsample the data (within limits), so the
// number of rows in the database are not the only factor.
//
// This limit here is a crude attempt to limit just the raw data fetched from
// ClickHouse itself. For any OxQL query, we may retrieve many measurements from
// the database. Each time we do so, we increment a counter, and compare it to
// this. If we exceed it, the whole query fails.
pub const MAX_DATABASE_ROWS: u64 = 1_000_000;

impl Client {
    /// Run a OxQL query.
    pub async fn oxql_query(
        &self,
        query: impl AsRef<str>,
    ) -> Result<OxqlResult, Error> {
        // TODO-security: Need a way to implement authz checks for things like
        // viewing resources in another project or silo.
        //
        // I think one way to do that is look at the predicates and make sure
        // they refer to things the user has access to. Another is to add some
        // implicit predicates here, indicating the subset of fields that the
        // query should be able to access.
        //
        // This probably means we'll need to parse the query in Nexus, so that
        // we can attach the other filters ourselves.
        //
        // See https://github.com/oxidecomputer/omicron/issues/5298.
        let query = query.as_ref();
        let parsed_query = oxql::Query::new(query)?;
        let query_id = Uuid::new_v4();
        let query_log =
            self.log.new(slog::o!("query_id" => query_id.to_string()));
        debug!(
            query_log,
            "parsed OxQL query";
            "query" => query,
            "parsed_query" => ?parsed_query,
        );
        let id = usdt::UniqueId::new();
        probes::oxql__query__start!(|| (&id, &query_id, query));
        let mut total_rows_fetched = 0;
        let result = self
            .run_oxql_query(
                &query_log,
                query_id,
                parsed_query,
                &mut total_rows_fetched,
                None,
            )
            .await;
        probes::oxql__query__done!(|| (&id, &query_id));
        result
    }

    fn rewrite_predicate_for_fields(
        schema: &TimeseriesSchema,
        preds: &filter::Filter,
    ) -> Result<Option<String>, Error> {
        // Walk the set of predicates, keeping those which apply to this schema.
        match preds {
            filter::Filter::Atom(atom) => {
                // If the predicate names a field in this timeseries schema,
                // return that predicate printed as a string. If not, we return
                // None.
                let Some(field_schema) =
                    schema.schema_for_field(atom.ident.as_str())
                else {
                    return Ok(None);
                };
                if !atom
                    .expr_type_is_compatible_with_field(field_schema.field_type)
                {
                    return Err(Error::from(anyhow::anyhow!(
                        "Expression for field {} is not compatible with \
                        its type {}",
                        field_schema.name,
                        field_schema.field_type,
                    )));
                }
                Ok(Some(atom.as_db_safe_string()))
            }
            filter::Filter::Expr(expr) => {
                let left_pred =
                    Self::rewrite_predicate_for_fields(schema, &expr.left)?;
                let right_pred =
                    Self::rewrite_predicate_for_fields(schema, &expr.right)?;
                let out = match (left_pred, right_pred) {
                    (Some(left), Some(right)) => Some(format!(
                        "{}({left}, {right})",
                        expr.op.as_db_function_name()
                    )),
                    (Some(single), None) | (None, Some(single)) => Some(single),
                    (None, None) => None,
                };
                Ok(out)
            }
        }
    }

    fn rewrite_predicate_for_measurements(
        schema: &TimeseriesSchema,
        preds: &oxql::ast::table_ops::filter::Filter,
    ) -> Result<Option<String>, Error> {
        // Walk the set of predicates, keeping those which apply to this schema.
        match preds {
            oxql::ast::table_ops::filter::Filter::Atom(atom) => {
                // The relevant columns on which we filter depend on the datum
                // type of the timeseries. All timeseries support "timestamp".
                let ident = atom.ident.as_str();
                if ident == "timestamp" {
                    if matches!(
                        atom.expr,
                        oxql::ast::literal::Literal::Timestamp(_)
                    ) {
                        return Ok(Some(atom.as_db_safe_string()));
                    }
                    return Err(Error::from(anyhow::anyhow!(
                        "Literal cannot be compared with a timestamp"
                    )));
                }

                // We do not currently support filtering in the database on
                // values, only the `timestamp` and possibly `start_time` (if
                // the metric is cumulative).
                if ident == "start_time" {
                    if !schema.datum_type.is_cumulative() {
                        return Err(Error::from(anyhow::anyhow!(
                            "Start time can only be compared if the metric \
                            is cumulative, but found one of type {}",
                            schema.datum_type,
                        )));
                    }
                    if matches!(
                        atom.expr,
                        oxql::ast::literal::Literal::Timestamp(_)
                    ) {
                        return Ok(Some(atom.as_db_safe_string()));
                    }
                    return Err(Error::from(anyhow::anyhow!(
                        "Literal cannot be compared with a timestamp"
                    )));
                }

                // We'll delegate to the actual table op to filter on any of the
                // data columns.
                Ok(None)
            }
            oxql::ast::table_ops::filter::Filter::Expr(expr) => {
                let left_pred = Self::rewrite_predicate_for_measurements(
                    schema, &expr.left,
                )?;
                let right_pred = Self::rewrite_predicate_for_measurements(
                    schema,
                    &expr.right,
                )?;
                let out = match (left_pred, right_pred) {
                    (Some(left), Some(right)) => Some(format!(
                        "{}({left}, {right})",
                        expr.op.as_db_function_name()
                    )),
                    (Some(single), None) | (None, Some(single)) => Some(single),
                    (None, None) => None,
                };
                Ok(out)
            }
        }
    }

    // Run one query.
    //
    // If the query is flat, run it directly. If it's nested, run each of them;
    // concatenate the results; and then apply all the remaining
    // transformations.
    #[async_recursion::async_recursion]
    async fn run_oxql_query(
        &self,
        query_log: &Logger,
        query_id: Uuid,
        query: oxql::Query,
        total_rows_fetched: &mut u64,
        outer_predicates: Option<Filter>,
    ) -> Result<OxqlResult, Error> {
        let split = query.split();
        if let oxql::ast::SplitQuery::Nested { subqueries, transformations } =
            split
        {
            trace!(
                query_log,
                "OxQL query contains subqueries, running recursively"
            );
            // Create the new set of outer predicates to pass in to the
            // subquery, by merging the previous outer predicates with those of
            // the transformation portion of this nested query.
            let new_outer_predicates =
                query.coalesced_predicates(outer_predicates.clone());

            // Run each subquery recursively, and extend the results
            // accordingly.
            let mut query_summaries = Vec::with_capacity(subqueries.len());
            let mut tables = Vec::with_capacity(subqueries.len());
            let query_start = Instant::now();
            for subq in subqueries.into_iter() {
                let res = self
                    .run_oxql_query(
                        query_log,
                        query_id,
                        subq,
                        total_rows_fetched,
                        new_outer_predicates.clone(),
                    )
                    .await?;
                query_summaries.extend(res.query_summaries);
                tables.extend(res.tables);
            }
            for tr in transformations.into_iter() {
                trace!(
                    query_log,
                    "applying query transformation";
                    "transformation" => ?tr,
                );
                let id = usdt::UniqueId::new();
                probes::oxql__table__op__start!(|| (
                    &id,
                    &query_id,
                    format!("{tr:?}")
                ));
                let new_tables = tr.apply(&tables, query.end_time());
                probes::oxql__table__op__done!(|| (&id, &query_id));
                tables = new_tables?;
            }
            let result = OxqlResult {
                query_id,
                total_duration: query_start.elapsed(),
                query_summaries,
                tables,
            };
            return Ok(result);
        }

        // This is a flat query, let's just run it directly. First step is
        // getting the schema itself.
        let query_start = Instant::now();
        let oxql::ast::SplitQuery::Flat(query) = split else {
            unreachable!();
        };
        let name = query.timeseries_name();
        let Some(schema) = self.schema_for_timeseries(name).await? else {
            return Err(Error::TimeseriesNotFound(name.to_string()));
        };
        debug!(
            query_log,
            "running flat OxQL query";
            "query" => ?query,
            "timeseries_name" => %name,
        );

        // Fetch the consistent fields (including keys) for this timeseries,
        // including filtering them based on the predicates in the query
        // that apply to this timeseries in particular. We also need to merge
        // them in with the predicates passed in from a possible outer query.
        let preds = query.coalesced_predicates(outer_predicates.clone());
        debug!(
            query_log,
            "coalesced predicates from flat query";
            "outer_predicates" => ?&outer_predicates,
            "coalesced" => ?&preds,
        );

        // We generally run 2 SQL queries for each OxQL query:
        //
        // 1. Fetch all the fields consistent with the query.
        // 2. Fetch the consistent samples.
        let mut query_summaries = Vec::with_capacity(2);
        let all_fields_query =
            self.all_fields_query(&schema, preds.as_ref())?;
        let (summary, info) = self
            .select_matching_timeseries_info(&all_fields_query, &schema)
            .await?;
        debug!(
            query_log,
            "fetched information for matching timeseries keys";
            "n_keys" => info.len(),
        );
        query_summaries.push(summary);

        // If there are no consistent keys, we can just return an empty table.
        if info.is_empty() {
            let result = OxqlResult {
                query_id,
                total_duration: query_start.elapsed(),
                query_summaries,
                tables: vec![oxql::Table::new(schema.timeseries_name.as_str())],
            };
            return Ok(result);
        }

        // Fetch the consistent measurements for this timeseries.
        //
        // We'll keep track of all the measurements for this timeseries schema,
        // organized by timeseries key. That's because we fetch all consistent
        // samples at once, so we get many concrete _timeseries_ in the returned
        // response, even though they're all from the same schema.
        let (summary, timeseries_by_key) = self
            .select_matching_samples(
                query_log,
                &schema,
                preds.as_ref(),
                &info,
                total_rows_fetched,
            )
            .await?;
        query_summaries.push(summary);

        // At this point, let's construct a set of tables and run the results
        // through the transformation pipeline.
        let mut tables = vec![oxql::Table::from_timeseries(
            schema.timeseries_name.as_str(),
            timeseries_by_key.into_values(),
        )?];

        let transformations = query.transformations();
        debug!(
            query_log,
            "constructed OxQL table, starting transformation pipeline";
            "name" => tables[0].name(),
            "n_timeseries" => tables[0].n_timeseries(),
            "n_transformations" => transformations.len(),
        );
        for tr in transformations {
            trace!(
                query_log,
                "applying query transformation";
                "transformation" => ?tr,
            );
            let id = usdt::UniqueId::new();
            probes::oxql__table__op__start!(|| (
                &id,
                &query_id,
                format!("{tr:?}")
            ));
            let new_tables = tr.apply(&tables, query.end_time());
            probes::oxql__table__op__done!(|| (&id, &query_id));
            tables = new_tables?;
        }
        let result = OxqlResult {
            query_id,
            total_duration: query_start.elapsed(),
            query_summaries,
            tables,
        };
        Ok(result)
    }

    // Select samples matching the set of predicates and consistent keys.
    //
    // Note that this also implements the conversion from cumulative to gauge
    // samples, depending on how data was requested.
    async fn select_matching_samples(
        &self,
        query_log: &Logger,
        schema: &TimeseriesSchema,
        preds: Option<&oxql::ast::table_ops::filter::Filter>,
        info: &BTreeMap<TimeseriesKey, (Target, Metric)>,
        total_rows_fetched: &mut u64,
    ) -> Result<(QuerySummary, BTreeMap<TimeseriesKey, oxql::Timeseries>), Error>
    {
        // We'll create timeseries for each key on the fly. To enable computing
        // deltas, we need to track the last measurement we've seen as well.
        let mut measurements_by_key: BTreeMap<_, Vec<_>> = BTreeMap::new();
        let measurements_query = self.measurements_query(
            schema,
            preds,
            info.keys(),
            total_rows_fetched,
        )?;
        let mut n_measurements: u64 = 0;
        let (summary, body) =
            self.execute_with_body(&measurements_query).await?;
        for line in body.lines() {
            let (key, measurement) =
                model::parse_measurement_from_row(line, schema.datum_type);
            measurements_by_key.entry(key).or_default().push(measurement);
            n_measurements += 1;
        }
        debug!(
            query_log,
            "fetched measurements for OxQL query";
            "n_keys" => measurements_by_key.len(),
            "n_measurements" => n_measurements,
        );

        // At this point, we need to check that we're still within our maximum
        // result size. The measurement query we issued limited the returned
        // result to 1 more than the remainder on our allotment. So if we get
        // exactly that limit, we know that there are more rows than we can
        // allow. We don't know how many more, but we don't care, and we fail
        // the query regardless.
        update_total_rows_and_check(
            query_log,
            total_rows_fetched,
            n_measurements,
        )?;

        // Remove the last measurement, returning just the keys and timeseries.
        let mut out = BTreeMap::new();
        for (key, measurements) in measurements_by_key.into_iter() {
            // Constuct a new timeseries, from the target/metric info.
            let (target, metric) = info.get(&key).unwrap();
            let mut timeseries = oxql::Timeseries::new(
                target
                    .fields
                    .iter()
                    .chain(metric.fields.iter())
                    .map(|field| (field.name.clone(), field.value.clone())),
                oxql::point::DataType::try_from(schema.datum_type)?,
                if schema.datum_type.is_cumulative() {
                    oxql::point::MetricType::Delta
                } else {
                    oxql::point::MetricType::Gauge
                },
            )?;

            // Covert its oximeter measurements into OxQL data types.
            let points = if schema.datum_type.is_cumulative() {
                oxql::point::Points::delta_from_cumulative(&measurements)?
            } else {
                oxql::point::Points::gauge_from_gauge(&measurements)?
            };
            timeseries.points = points;
            debug!(
                query_log,
                "inserted new OxQL timeseries";
                "key" => key,
                "metric_type" => ?timeseries.points.metric_type(),
                "n_points" => timeseries.points.len(),
            );
            out.insert(key, timeseries);
        }
        Ok((summary, out))
    }

    fn measurements_query<'keys>(
        &self,
        schema: &TimeseriesSchema,
        preds: Option<&oxql::ast::table_ops::filter::Filter>,
        consistent_keys: impl ExactSizeIterator<Item = &'keys TimeseriesKey>,
        total_rows_fetched: &mut u64,
    ) -> Result<String, Error> {
        use std::fmt::Write;

        // Filter down the fields to those which apply to the data itself, which
        // includes the timestamps and data values. The supported fields here
        // depend on the datum type.
        let preds_for_measurements = preds
            .map(|p| Self::rewrite_predicate_for_measurements(schema, p))
            .transpose()?
            .flatten();
        let mut query = self.measurements_query_raw(schema.datum_type);
        query.push_str(" WHERE ");
        if let Some(preds) = preds_for_measurements {
            query.push_str(&preds);
            query.push_str(" AND ");
        }
        query.push_str("timeseries_name = '");
        write!(query, "{}", schema.timeseries_name).unwrap();
        query.push('\'');
        if consistent_keys.len() > 0 {
            query.push_str(" AND timeseries_key IN (");
            query.push_str(
                &consistent_keys
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(","),
            );
            query.push(')');
        }

        // Always order by timestamp. This is critical, and the basis for many
        // of the operations downstream, which assume ordered timestamps.
        query.push_str(" ORDER BY timestamp ");

        // Push a limit clause, which restricts the number of records we could
        // return.
        //
        // This is used to ensure that we never go above the limit in
        // `MAX_RESULT_SIZE`. That restricts the _total_ number of rows we want
        // to retch from the database. So we set our limit to be one more than
        // the remainder on our allotment. If we get exactly as many as we set
        // in the limit, then we fail the query because there are more rows that
        // _would_ be returned. We don't know how many more, but there is at
        // least 1 that pushes us over the limit. This prevents tricky
        // TOCTOU-like bugs where we need to check the limit twice, and improves
        // performance, since we don't return much more than we could possibly
        // handle.
        let remainder = MAX_DATABASE_ROWS - *total_rows_fetched;
        query.push_str(" LIMIT ");
        write!(query, "{}", remainder + 1).unwrap();

        // Finally, use JSON format.
        query.push_str(" FORMAT ");
        query.push_str(crate::DATABASE_SELECT_FORMAT);
        Ok(query)
    }

    fn measurements_query_raw(
        &self,
        datum_type: oximeter::DatumType,
    ) -> String {
        let value_columns = if datum_type.is_histogram() {
            "timeseries_key, timestamp, start_time, bins, counts"
        } else if datum_type.is_cumulative() {
            "timeseries_key, timestamp, start_time, datum"
        } else {
            "timeseries_key, timestamp, datum"
        };
        format!(
            "SELECT {} \
            FROM {}.{}",
            value_columns,
            crate::DATABASE_NAME,
            crate::query::measurement_table_name(datum_type),
        )
    }

    fn all_fields_query(
        &self,
        schema: &TimeseriesSchema,
        preds: Option<&oxql::ast::table_ops::filter::Filter>,
    ) -> Result<String, Error> {
        // Filter down the fields to those which apply to this timeseries
        // itself, and rewrite as a DB-safe WHERE clause.
        let preds_for_fields = preds
            .map(|p| Self::rewrite_predicate_for_fields(schema, p))
            .transpose()?
            .flatten();
        let (already_has_where, mut query) = self.all_fields_query_raw(schema);
        if let Some(preds) = preds_for_fields {
            // If the raw field has only a single select query, then we've
            // already added a "WHERE" clause. Simply tack these predicates onto
            // that one.
            if already_has_where {
                query.push_str(" AND ");
            } else {
                query.push_str(" WHERE ");
            }
            query.push_str(&preds);
        }
        query.push_str(" FORMAT ");
        query.push_str(crate::DATABASE_SELECT_FORMAT);
        Ok(query)
    }

    fn all_fields_query_raw(
        &self,
        schema: &TimeseriesSchema,
    ) -> (bool, String) {
        match schema.field_schema.len() {
            0 => unreachable!(),
            1 => {
                let field_schema = schema.field_schema.first().unwrap();
                (
                    true,
                    format!(
                        "SELECT DISTINCT timeseries_key, field_value AS {field_name} \
                        FROM {db_name}.{field_table} \
                        WHERE \
                            timeseries_name = '{timeseries_name}' AND \
                            field_name = '{field_name}'",
                        field_name = field_schema.name,
                        db_name = crate::DATABASE_NAME,
                        field_table = field_table_name(field_schema.field_type),
                        timeseries_name = schema.timeseries_name,
                    )
                )
            }
            _ => {
                let mut top_level_columns =
                    Vec::with_capacity(schema.field_schema.len());
                let mut field_subqueries =
                    Vec::with_capacity(schema.field_schema.len());

                // Select each field value, aliasing it to its field name.
                for field_schema in schema.field_schema.iter() {
                    top_level_columns.push(format!(
                        "filter_on_{}.field_value AS {}",
                        field_schema.name, field_schema.name
                    ));
                    field_subqueries.push((
                        format!(
                            "SELECT DISTINCT timeseries_key, field_value \
                                FROM {db_name}.{field_table} \
                                WHERE \
                                    timeseries_name = '{timeseries_name}' AND \
                                    field_name = '{field_name}' \
                                ",
                            db_name = crate::DATABASE_NAME,
                            field_table =
                                field_table_name(field_schema.field_type),
                            timeseries_name = schema.timeseries_name,
                            field_name = field_schema.name,
                        ),
                        format!("filter_on_{}", field_schema.name),
                    ));
                }

                // Write the top-level select statement, starting by selecting
                // the timeseries key from the first field schema.
                let mut out = format!(
                    "SELECT {}.timeseries_key AS timeseries_key, {} FROM ",
                    field_subqueries[0].1,
                    top_level_columns.join(", "),
                );

                // Then add all the subqueries selecting each field.
                //
                // We need to add these, along with their aliases. The first
                // such subquery has no join conditions, but the later ones all
                // refer to the previous via:
                //
                // `ON <previous_filter_name>.timeseries_key = <current_filter_name>.timeseries_key`
                for (i, (subq, alias)) in field_subqueries.iter().enumerate() {
                    // Push the subquery itself, aliased.
                    out.push('(');
                    out.push_str(subq);
                    out.push_str(") AS ");
                    out.push_str(alias);

                    // Push the join conditions.
                    if i > 0 {
                        let previous_alias = &field_subqueries[i - 1].1;
                        out.push_str(" ON ");
                        out.push_str(alias);
                        out.push_str(".timeseries_key = ");
                        out.push_str(previous_alias);
                        out.push_str(".timeseries_key");
                    }

                    // Push the "INNER JOIN" expression itself, for all but the
                    // last subquery.
                    if i < field_subqueries.len() - 1 {
                        out.push_str(" INNER JOIN ");
                    }
                }
                (false, out)
            }
        }
    }
}

// Helper to update the number of total rows fetched so far, and check it's
// still under the limit.
fn update_total_rows_and_check(
    query_log: &Logger,
    total_rows_fetched: &mut u64,
    count: u64,
) -> Result<(), Error> {
    *total_rows_fetched += count;
    if *total_rows_fetched > MAX_DATABASE_ROWS {
        return Err(Error::from(anyhow::anyhow!(
            "Query requires fetching more than the \
            current limit of {} data points from the \
            timeseries database",
            MAX_DATABASE_ROWS,
        )));
    }
    trace!(
        query_log,
        "verified OxQL measurement query returns few enough results";
        "n_new_measurements" => count,
        "n_total" => *total_rows_fetched,
        "limit" => MAX_DATABASE_ROWS,
    );
    Ok(())
}
