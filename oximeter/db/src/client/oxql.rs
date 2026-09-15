// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Client methods for running OxQL queries against the timeseries database.

use super::Handle;
use crate::Error;
use crate::Metric;
use crate::Target;
use crate::client::Client;
use crate::model::columns;
use crate::model::from_block::FromBlock as _;
use crate::oxql;
use crate::oxql::Query;
use crate::oxql::ast::table_ops::BasicTableOp;
use crate::oxql::ast::table_ops::TableOp;
use crate::oxql::ast::table_ops::align;
use crate::oxql::ast::table_ops::filter;
use crate::oxql::ast::table_ops::filter::Filter;
use crate::oxql::ast::table_ops::limit::Limit;
use crate::oxql::ast::table_ops::limit::LimitKind;
use crate::oxql::query::QueryAuthzScope;
use crate::query::field_table_name;
use chrono::DateTime;
use chrono::TimeDelta;
use chrono::Utc;
use oximeter::Measurement;
use oximeter::TimeseriesSchema;
use oximeter::schema::TimeseriesKey;
use slog::Logger;
use slog::debug;
use slog::trace;
use std::collections::BTreeMap;
use std::collections::HashMap;
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
    pub query_summaries: Vec<oxql_types::QuerySummary>,

    /// The list of OxQL tables returned from the query.
    pub tables: Vec<oxql_types::Table>,
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

// When running an OxQL query, we may need to separately run several field
// queries, to get the consistent keys independently for a range of time.
//
// This type stores the predicates used to generate the keys, and the keys
// consistent with it.
#[derive(Clone, Debug, PartialEq)]
struct ConsistentKeyGroup {
    predicates: Option<Filter>,
    consistent_keys: BTreeMap<TimeseriesKey, (Target, Metric)>,
}

/// Work the database does beyond selecting the rows that match a query.
///
/// The two are mutually exclusive, which is why they share a type. A limit
/// applies to the output of an alignment, so pushing both would have the
/// database apply the limit to the raw samples instead -- taking `last 10`
/// of the samples and aligning those, rather than aligning everything and
/// taking the last 10 periods. `Query::pushable_alignment()` declines an
/// alignment whenever there is a limit, for exactly that reason.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
enum PushedWork {
    /// Just select the rows; everything else happens in Rust.
    #[default]
    Nothing,
    /// Take only the first or last few samples of each timeseries.
    Limit(Limit),
    /// Reduce the samples in each period to a single point.
    Alignment(PushedAlignment),
}

impl PushedWork {
    fn limit(&self) -> Option<Limit> {
        match self {
            PushedWork::Limit(limit) => Some(*limit),
            _ => None,
        }
    }

    fn alignment(&self) -> Option<PushedAlignment> {
        match self {
            PushedWork::Alignment(alignment) => Some(*alignment),
            _ => None,
        }
    }
}

/// An alignment operation being computed in the database rather than in Rust.
///
/// Alignment reduces every sample in a period to a single point, so computing
/// it in ClickHouse is the difference between fetching a day of raw samples
/// and fetching the handful of numbers they reduce to.
#[derive(Clone, Copy, Debug, PartialEq)]
struct PushedAlignment {
    /// The alignment being computed.
    align: align::Align,
    /// The end of the query, which anchors the output periods.
    query_end: DateTime<Utc>,
}

impl PushedAlignment {
    /// Decide whether an alignment can be computed in the database for a
    /// timeseries with this schema.
    ///
    /// The shape of the query is checked separately, by
    /// `Query::pushable_alignment()`. This is the part that depends on what
    /// kind of data is being aligned.
    fn new(
        align: align::Align,
        schema: &TimeseriesSchema,
        query_end: DateTime<Utc>,
    ) -> Option<Self> {
        // A cumulative metric reaches alignment as a delta, and `rate` is the
        // only method defined over one. Anything else is a gauge, where every
        // method is a plain aggregate.
        let supported = if schema.datum_type.is_cumulative() {
            matches!(align.method, align::AlignmentMethod::Rate)
        } else {
            Self::aggregate_for(align.method).is_some()
        };
        if !supported {
            return None;
        }
        // Histograms and the other non-scalar types cannot be aligned at all.
        if !matches!(
            schema.datum_type,
            oximeter::DatumType::I8
                | oximeter::DatumType::U8
                | oximeter::DatumType::I16
                | oximeter::DatumType::U16
                | oximeter::DatumType::I32
                | oximeter::DatumType::U32
                | oximeter::DatumType::I64
                | oximeter::DatumType::U64
                | oximeter::DatumType::F32
                | oximeter::DatumType::F64
                | oximeter::DatumType::CumulativeI64
                | oximeter::DatumType::CumulativeU64
                | oximeter::DatumType::CumulativeF32
                | oximeter::DatumType::CumulativeF64
        ) {
            return None;
        }
        // Zero would make every period identical and divide the grid by zero
        // below. The Rust path loops forever on it, which is its own bug.
        if align.period.is_zero() {
            return None;
        }
        Some(Self { align, query_end })
    }

    /// The ClickHouse aggregate that computes this method over a gauge.
    fn aggregate_for(method: align::AlignmentMethod) -> Option<&'static str> {
        match method {
            // `avg` and ClickHouse's other aggregates skip NULL inputs and
            // return NULL over an empty set, which is exactly how the Rust
            // implementation treats missing points.
            align::AlignmentMethod::MeanWithin => Some("avg"),
            align::AlignmentMethod::Min => Some("min"),
            align::AlignmentMethod::Max => Some("max"),
            // A rate needs the intervals that only a delta carries.
            align::AlignmentMethod::Rate => None,
            align::AlignmentMethod::Interpolate => None,
        }
    }

    /// The SQL expression giving the output period a sample falls in.
    ///
    /// Periods run backwards from the end of the query, so the window holding
    /// a sample at `t` is `(end - (ix + 1) * period, end - ix * period]` for
    /// `ix = floor((end - t) / period)`, and the point it produces is stamped
    /// with that window's end. Reproducing that arithmetic exactly matters
    /// more than it might seem: `toStartOfInterval` would bucket on a grid
    /// anchored at the Unix epoch instead, quietly returning different numbers
    /// from the Rust path for the same query.
    ///
    /// `intDiv` truncates toward zero rather than flooring, so this is only
    /// correct for samples at or before the end of the query. The caller
    /// excludes any later ones, which the Rust path also ignores.
    fn period_expr(&self) -> String {
        format!(
            "fromUnixTimestamp64Nano(toInt64({}), 'UTC')",
            self.period_end_nanos_expr(),
        )
    }

    /// The same expression, as a count of nanoseconds.
    fn period_end_nanos_expr(&self) -> String {
        let end_nanos = self.query_end.timestamp_nanos_opt().unwrap_or(0);
        let period_nanos = self.align.period.as_nanos();
        format!(
            "{end_nanos} - intDiv({end_nanos} - \
            toUnixTimestamp64Nano(timestamp), {period_nanos}) * {period_nanos}"
        )
    }

    /// Fill in the periods the database returned nothing for.
    ///
    /// `GROUP BY` emits a row only for a period that contained samples, while
    /// aligning in Rust walks a fixed grid and emits a missing point for the
    /// empty ones. Rebuild that grid here, so that the two paths return the
    /// same array of points and a query's result does not depend on whether
    /// the alignment happened to be pushed down.
    ///
    /// The grid runs from the earliest period the database returned up to the
    /// end of the query. That is the same extent the Rust path produces, which
    /// stops once a period falls entirely before the first sample -- and the
    /// earliest period holding a sample is exactly the earliest one returned.
    fn restore_empty_periods(
        &self,
        points: &oxql_types::point::Points,
    ) -> Result<oxql_types::point::Points, Error> {
        let Some(&first) = points.timestamps().first() else {
            return Ok(points.clone());
        };
        let period = TimeDelta::from_std(self.align.period).map_err(|_| {
            Error::Database(String::from("period out of range"))
        })?;

        // Walk the grid backwards from the end of the query, the way the Rust
        // implementation does, so the two agree on where the periods fall even
        // if the arithmetic drifts.
        let mut grid = Vec::with_capacity(points.len());
        let mut output_time = self.query_end;
        while output_time >= first {
            grid.push(output_time);
            let Some(next) = output_time.checked_sub_signed(period) else {
                break;
            };
            output_time = next;
        }
        grid.reverse();

        // Both arrays are sorted, so line the returned periods up against the
        // grid in one pass. Anything the grid has and the database did not
        // returned no samples, and becomes a missing point.
        let returned = points.timestamps();
        let mut slots = Vec::with_capacity(grid.len());
        let mut next = 0;
        for timestamp in grid.iter() {
            if returned.get(next) == Some(timestamp) {
                slots.push(Some(next));
                next += 1;
            } else {
                slots.push(None);
            }
        }

        let metric_type = oxql_types::point::MetricType::Gauge;
        let values = match points.values(0) {
            Some(oxql_types::point::ValueArray::Integer(returned)) => {
                oxql_types::point::ValueArray::Integer(
                    slots.iter().map(|s| s.and_then(|s| returned[s])).collect(),
                )
            }
            Some(oxql_types::point::ValueArray::Double(returned)) => {
                oxql_types::point::ValueArray::Double(
                    slots.iter().map(|s| s.and_then(|s| returned[s])).collect(),
                )
            }
            _ => {
                return Err(Error::Oxql(anyhow::anyhow!(
                    "An aligned timeseries must hold numeric points",
                )));
            }
        };
        Ok(oxql_types::point::Points::new(
            None,
            grid,
            vec![oxql_types::point::Values { values, metric_type }],
        ))
    }

    /// Wrap a query selecting raw samples in one that aligns them.
    ///
    /// The result has a `timeseries_key`, a `timestamp` and a `datum`, and no
    /// `start_time`. That is the shape of a gauge, which is what alignment
    /// produces, so it is parsed by the same code that reads raw gauge
    /// samples.
    fn wrap(
        &self,
        inner: String,
        total_rows_fetched: &mut u64,
    ) -> Result<String, Error> {
        // The row budget applies to what crosses the wire, which is now the
        // aligned points rather than the samples behind them. That is the
        // whole point of computing this in the database.
        let remainder = MAX_DATABASE_ROWS - *total_rows_fetched;
        let end_nanos = self.query_end.timestamp_nanos_opt().unwrap_or(0);

        // Samples after the end of the query are excluded here rather than
        // left to the predicates, both because the Rust path ignores them and
        // because `intDiv` truncates toward zero, so the period arithmetic is
        // only correct for samples at or before it.
        let inner = format!(
            "{inner} AND timestamp <= \
            fromUnixTimestamp64Nano(toInt64({end_nanos}), 'UTC')"
        );
        let body = match self.align.method {
            align::AlignmentMethod::Rate => self.rate_body(inner),
            _ => self.aggregate_body(inner),
        };
        Ok(format!(
            "{body} GROUP BY timeseries_key, timestamp \
            ORDER BY timeseries_key, timestamp LIMIT {}",
            remainder + 1,
        ))
    }

    /// Reduce a gauge's samples with a plain aggregate.
    fn aggregate_body(&self, inner: String) -> String {
        let aggregate = Self::aggregate_for(self.align.method)
            .expect("checked when the alignment was accepted");
        let period = self.period_expr();
        format!(
            "SELECT timeseries_key, {period} AS timestamp, \
            {aggregate}(datum) AS datum FROM ({inner})"
        )
    }

    /// Reduce a counter's samples to the rate it advanced in each period.
    ///
    /// This has to reproduce two steps at once: the conversion from cumulative
    /// samples to deltas that `Points::from_cumulative()` does, and the
    /// reduction that `rate_in_window()` does over the result.
    ///
    /// The conversion is a difference against the previous sample, taken
    /// within an epoch -- a run of samples sharing a start time. A counter
    /// resets when the producer restarts, which begins a new epoch, and since
    /// the epoch is the partition the reset cannot leak into a difference. The
    /// first sample of an epoch has no predecessor, and its own value is the
    /// increase since the epoch began.
    ///
    /// Two different notions of "previous" are needed, and they differ only
    /// when a sample is missing. The value is differenced against the last
    /// sample that *had* one, so that a gap in collection is counted rather
    /// than lost -- `anyLast` skips nulls, which is what makes that work. The
    /// interval, though, runs back to the immediately preceding sample
    /// whatever its value, which is what `lagInFrame` gives. That mirrors
    /// `Points::from_cumulative()`, which advances timestamps across missing
    /// samples while carrying the last real datum forward.
    ///
    /// Summing the differences in a period telescopes back to
    /// `last - first`, so the result is the increase over the period divided
    /// by the span it was observed over, which is what the Rust path computes.
    fn rate_body(&self, inner: String) -> String {
        let period = self.period_expr();
        let period_end_nanos = self.period_end_nanos_expr();
        let period_nanos = self.align.period.as_nanos();
        let epoch =
            "PARTITION BY timeseries_key, start_time ORDER BY timestamp";

        format!(
            // A period can span no time at all -- the first sample of a
            // series carries its own start time, and a producer sampled the
            // instant it started counting has the two equal. There is no rate
            // to report over zero duration, so `nullIf` turns the divisor into
            // NULL and the whole point becomes missing, which is what the Rust
            // path does when the span is not positive.
            "SELECT timeseries_key, period AS timestamp, \
            sum(increase) / nullIf(toFloat64(max(end_nanos) - \
            min(start_nanos)) / 1000000000, 0) AS datum \
            FROM (\
                SELECT timeseries_key, period, period_start_nanos, \
                series_row, epoch_start_nanos, end_nanos, \
                if(previous_end_nanos IS NULL, epoch_start_nanos, \
                    previous_end_nanos) AS start_nanos, \
                if(previous_datum IS NULL, toFloat64(datum), \
                    toFloat64(datum) - toFloat64(previous_datum)) AS increase \
                FROM (\
                    SELECT timeseries_key, datum, \
                    {period} AS period, \
                    {period_end_nanos} - {period_nanos} AS period_start_nanos, \
                    toUnixTimestamp64Nano(timestamp) AS end_nanos, \
                    toUnixTimestamp64Nano(start_time) AS epoch_start_nanos, \
                    row_number() OVER (PARTITION BY timeseries_key \
                        ORDER BY start_time, timestamp) AS series_row, \
                    anyLast(datum) OVER ({epoch} ROWS BETWEEN UNBOUNDED \
                        PRECEDING AND 1 PRECEDING) AS previous_datum, \
                    lagInFrame(toNullable(toUnixTimestamp64Nano(timestamp))) \
                        OVER ({epoch} ROWS BETWEEN 1 PRECEDING AND CURRENT \
                        ROW) AS previous_end_nanos \
                    FROM ({inner})\
                )\
            ) \
            WHERE NOT (series_row = 1 AND epoch_start_nanos < \
                period_start_nanos)"
        )
    }
}

impl Client {
    /// Build a query plan for the OxQL query.
    pub async fn plan_oxql_query(
        &self,
        query: impl AsRef<str>,
    ) -> Result<oxql::plan::Plan, Error> {
        let query = query.as_ref();
        let parsed_query = oxql::Query::new(query)?;
        self.build_query_plan(&parsed_query).await
    }

    /// Build a query plan for the OxQL query.
    async fn build_query_plan(
        &self,
        query: &Query,
    ) -> Result<oxql::plan::Plan, Error> {
        let referenced_timeseries = query.all_timeseries_names();
        let mut schema = BTreeMap::new();
        for name in referenced_timeseries.into_iter() {
            let Some(sch) = self.schema_for_timeseries(name).await? else {
                return Err(Error::TimeseriesNotFound(name.to_string()));
            };
            schema.insert(name.clone(), sch);
        }
        let plan =
            oxql::plan::Plan::new(query.parsed_query().clone(), &schema)?;
        Ok(plan)
    }

    /// Run a OxQL query.
    pub async fn oxql_query(
        &self,
        query: impl AsRef<str>,
        scope: QueryAuthzScope,
    ) -> Result<OxqlResult, Error> {
        let query = query.as_ref();
        let parsed_query = oxql::Query::new(query)?;
        let filtered_query = parsed_query.insert_authz_filters(scope);

        let plan = self.build_query_plan(&filtered_query).await?;
        if plan.requires_full_table_scan() {
            return Err(Error::Oxql(anyhow::anyhow!(
                "This query requires at least one full table scan. \
                Please rewrite the query to filter either the fields \
                or timestamps, in order to reduce the amount of data \
                fetched from the database."
            )));
        }
        let query_id = Uuid::new_v4();
        let query_log =
            self.log.new(slog::o!("query_id" => query_id.to_string()));
        debug!(
            query_log,
            "parsed OxQL query";
            "query" => query,
            "parsed_query" => ?parsed_query,
            "filtered_query" => ?filtered_query,
        );
        let id = usdt::UniqueId::new();
        probes::oxql__query__start!(|| (&id, &query_id, query));
        let mut total_rows_fetched = 0;
        let result = self
            .run_oxql_query(
                &query_log,
                &mut self.claim_connection().await?,
                query_id,
                filtered_query,
                &mut total_rows_fetched,
                None,
                None,
            )
            .await;
        probes::oxql__query__done!(|| (&id, &query_id));
        result
    }

    /// Rewrite the predicates from an OxQL query so that they apply only to the
    /// field tables.
    fn rewrite_predicate_for_fields(
        schema: &TimeseriesSchema,
        preds: &filter::Filter,
    ) -> Result<Option<String>, Error> {
        // Potentially negate the predicate.
        let maybe_not = if preds.negated { "NOT " } else { "" };

        // Walk the set of predicates, keeping those which apply to this schema.
        match &preds.expr {
            filter::FilterExpr::Simple(inner) => {
                // If the predicate names a field in this timeseries schema,
                // return that predicate printed as a string. If not, we return
                // None.
                let Some(field_schema) =
                    schema.schema_for_field(inner.ident.as_str())
                else {
                    return Ok(None);
                };
                if !inner.value_type_is_compatible_with_field(
                    field_schema.field_type,
                ) {
                    return Err(Error::from(anyhow::anyhow!(
                        "Expression for field {} is not compatible with \
                        its type {}",
                        field_schema.name,
                        field_schema.field_type,
                    )));
                }
                Ok(Some(format!("{}{}", maybe_not, inner.as_db_safe_string())))
            }
            filter::FilterExpr::Compound(inner) => {
                let left_pred =
                    Self::rewrite_predicate_for_fields(schema, &inner.left)?;
                let right_pred =
                    Self::rewrite_predicate_for_fields(schema, &inner.right)?;
                let out = match (left_pred, right_pred) {
                    (Some(left), Some(right)) => Some(format!(
                        "{}{}({left}, {right})",
                        maybe_not,
                        inner.op.as_db_function_name()
                    )),
                    (Some(single), None) | (None, Some(single)) => Some(single),
                    (None, None) => None,
                };
                Ok(out)
            }
        }
    }

    /// Rewrite the predicates from an OxQL query so that they apply only to the
    /// measurement table.
    fn rewrite_predicate_for_measurements(
        schema: &TimeseriesSchema,
        preds: &oxql::ast::table_ops::filter::Filter,
    ) -> Result<Option<String>, Error> {
        // Potentially negate the predicate.
        let maybe_not = if preds.negated { "NOT " } else { "" };

        // Walk the set of predicates, keeping those which apply to this schema.
        match &preds.expr {
            filter::FilterExpr::Simple(inner) => {
                // The relevant columns on which we filter depend on the datum
                // type of the timeseries. All timeseries support "timestamp".
                let ident = inner.ident.as_str();
                if ident == "timestamp" {
                    if matches!(
                        inner.value,
                        oxql::ast::literal::Literal::Timestamp(_)
                    ) {
                        return Ok(Some(format!(
                            "{}{}",
                            maybe_not,
                            inner.as_db_safe_string()
                        )));
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
                        inner.value,
                        oxql::ast::literal::Literal::Timestamp(_)
                    ) {
                        return Ok(Some(format!(
                            "{}{}",
                            maybe_not,
                            inner.as_db_safe_string()
                        )));
                    }
                    return Err(Error::from(anyhow::anyhow!(
                        "Literal cannot be compared with a timestamp"
                    )));
                }

                // We'll delegate to the actual table op to filter on any of the
                // data columns.
                Ok(None)
            }
            filter::FilterExpr::Compound(inner) => {
                let left_pred = Self::rewrite_predicate_for_measurements(
                    schema,
                    &inner.left,
                )?;
                let right_pred = Self::rewrite_predicate_for_measurements(
                    schema,
                    &inner.right,
                )?;
                let out = match (left_pred, right_pred) {
                    (Some(left), Some(right)) => Some(format!(
                        "{}{}({left}, {right})",
                        maybe_not,
                        inner.op.as_db_function_name()
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
    #[allow(clippy::too_many_arguments)]
    async fn run_oxql_query(
        &self,
        query_log: &Logger,
        handle: &mut Handle,
        query_id: Uuid,
        query: oxql::Query,
        total_rows_fetched: &mut u64,
        outer_predicates: Option<Filter>,
        outer_limit: Option<Limit>,
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
            let new_outer_limit = query.coalesced_limits(outer_limit);

            // Run each subquery recursively, and extend the results
            // accordingly.
            let mut query_summaries = Vec::with_capacity(subqueries.len());
            let mut tables = Vec::with_capacity(subqueries.len());
            let query_start = Instant::now();
            for subq in subqueries.into_iter() {
                let res = self
                    .run_oxql_query(
                        query_log,
                        handle,
                        query_id,
                        subq,
                        total_rows_fetched,
                        new_outer_predicates.clone(),
                        new_outer_limit,
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
        let limit = query.coalesced_limits(outer_limit);
        debug!(
            query_log,
            "coalesced limit operations from flat query";
            "outer_limit" => ?&outer_limit,
            "coalesced" => ?&limit,
        );

        // Decide what else the database can do for us. The shape of the query
        // has to allow the alignment to be pushed, and so does the kind of
        // data; failing either, we fall back to the limit, which is what was
        // pushed before alignment ever was.
        let alignment = query.pushable_alignment(limit).and_then(|align| {
            PushedAlignment::new(align, &schema, *query.end_time())
        });
        let pushed = match (alignment, limit) {
            (Some(alignment), _) => {
                debug!(
                    query_log,
                    "pushing alignment into the database";
                    "method" => %alignment.align.method,
                    "period" => ?alignment.align.period,
                );
                PushedWork::Alignment(alignment)
            }
            (None, Some(limit)) => PushedWork::Limit(limit),
            (None, None) => PushedWork::Nothing,
        };

        // We generally run a few SQL queries for each OxQL query:
        //
        // - Some number of queries to fetch the timeseries keys that are
        // consistent with it.
        // - Fetch the consistent samples.
        //
        // Note that there are often 2 or more queries needed for the first
        // case. In particular, there is one query required for each independent
        // time range in the query (including when a time range isn't
        // specified).
        //
        // For example, consider the filter operation:
        //
        // ```
        // filter some_predicate || (timestamp > @now() - 1m && other_predicate)
        // ```
        //
        // That is, we return all timepoints for things where `some_predicate`
        // is true, and only the last minute for those satisfying
        // `other_predicate`. If we simply drop the timestamp filter, and run
        // the two predicates conjoined, we would erroneously return only the
        // last minute for everything, including those satisfying
        // `some_predicate`.
        //
        // So instead, we need to run one query for each of those, fetch the
        // keys associated with it, and then independently select the
        // measurements satisfying both the time range and key-consistency
        // constraints. Thankfully that can be done in one query, albeit a
        // complicated one.
        //
        // Convert any outer predicates to DNF, and split into disjoint key
        // groups for the measurement queries.
        let disjoint_predicates = if let Some(preds) = preds.as_ref() {
            let simplified = preds.simplify_to_dnf()?;
            debug!(
                query_log,
                "simplified filtering predicates to disjunctive normal form";
                "original" => %preds,
                "DNF" => %simplified,
            );
            simplified
                .flatten_disjunctions()
                .into_iter()
                .map(Option::Some)
                .collect()
        } else {
            // There are no outer predicates, so we have 1 disjoint key group,
            // with no predicates.
            vec![None]
        };

        // Run each query group indepdendently, keeping the predicates and the
        // timeseries keys corresponding to it.
        let mut consistent_key_groups =
            Vec::with_capacity(1 + disjoint_predicates.len());
        let mut query_summaries =
            Vec::with_capacity(1 + disjoint_predicates.len());
        for predicates in disjoint_predicates.into_iter() {
            debug!(
                query_log,
                "running disjoint query predicate";
                "predicate" => predicates.as_ref().map(|s| s.to_string()).unwrap_or("none".into()),
            );
            let all_fields_query =
                self.all_fields_query(&schema, predicates.as_ref())?;
            let (summary, consistent_keys) = self
                .select_matching_timeseries_info(
                    handle,
                    &all_fields_query,
                    &schema,
                )
                .await?;
            debug!(
                query_log,
                "fetched information for matching timeseries keys";
                "n_keys" => consistent_keys.len(),
            );
            query_summaries.push(summary);

            // If there are no consistent keys, move to the next independent
            // query chunk.
            if consistent_keys.is_empty() {
                continue;
            }

            // Push the disjoint filter itself, plus the keys consistent with
            // it.
            consistent_key_groups
                .push(ConsistentKeyGroup { predicates, consistent_keys });
        }

        // If there are no consistent keys _at all_, we can just return an empty
        // table.
        if consistent_key_groups.is_empty() {
            let result = OxqlResult {
                query_id,
                total_duration: query_start.elapsed(),
                query_summaries,
                tables: vec![oxql_types::Table::new(
                    schema.timeseries_name.as_str(),
                )],
            };
            return Ok(result);
        }

        // Fetch the consistent measurements for this timeseries, by key group.
        //
        // We'll keep track of all the measurements for this timeseries schema,
        // organized by timeseries key. That's because we fetch all consistent
        // samples at once, so we get many concrete _timeseries_ in the returned
        // response, even though they're all from the same schema.
        let (summaries, timeseries_by_key) = self
            .select_matching_samples(
                query_log,
                handle,
                &schema,
                &consistent_key_groups,
                pushed,
                total_rows_fetched,
            )
            .await?;
        query_summaries.extend(summaries);

        // At this point, let's construct a set of tables and run the results
        // through the transformation pipeline.
        let mut tables = vec![oxql_types::Table::from_timeseries(
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
            // Skip the alignment the database already computed, or the data
            // would be aligned twice -- the second pass would run over points
            // that are one per period already.
            if alignment.is_some()
                && matches!(tr, TableOp::Basic(BasicTableOp::Align(_)))
            {
                trace!(
                    query_log,
                    "skipping alignment computed in the database";
                    "transformation" => ?tr,
                );
                continue;
            }
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
        handle: &mut Handle,
        schema: &TimeseriesSchema,
        consistent_key_groups: &[ConsistentKeyGroup],
        pushed: PushedWork,
        total_rows_fetched: &mut u64,
    ) -> Result<
        (
            Vec<oxql_types::QuerySummary>,
            BTreeMap<TimeseriesKey, oxql_types::Timeseries>,
        ),
        Error,
    > {
        // We'll create timeseries for each key on the fly. To enable computing
        // deltas, we need to track the last measurement we've seen as well.
        let mut measurements_by_key: BTreeMap<_, Vec<_>> = BTreeMap::new();

        // If the set of consistent keys is quite large, we may run into
        // ClickHouse's SQL query size limit, which is 256KiB by default.
        // See https://clickhouse.com/docs/en/operations/settings/settings#max_query_size
        // for that limit.
        //
        // To avoid this, we have to split large groups of keys into pages, and
        // concatenate the results ourself.
        let mut n_measurements: u64 = 0;
        let mut summaries = Vec::new();
        for key_group_chunk in
            chunk_consistent_key_groups(consistent_key_groups)
        {
            let measurements_query = self.measurements_query(
                schema,
                &key_group_chunk,
                pushed,
                total_rows_fetched,
            )?;
            let result =
                self.execute_with_block(handle, &measurements_query).await?;
            let summary = result.query_summary();
            summaries.push(summary);
            let Some(block) = result.data.as_ref() else {
                return Err(Error::QueryMissingData {
                    query: measurements_query,
                });
            };
            let timeseries_keys = block
                .column_values(columns::TIMESERIES_KEY)?
                .as_u64()
                .map_err(|_| {
                    crate::native::Error::unexpected_column_type(
                        block,
                        columns::TIMESERIES_KEY,
                        "UInt64",
                    )
                })?;
            let measurements = Measurement::from_block(block, &())?;
            for (key, measurement) in
                timeseries_keys.iter().copied().zip(measurements)
            {
                measurements_by_key.entry(key).or_default().push(measurement);
                n_measurements += 1;
            }
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

        // At this point, we no longer care about the consistent_key groups. We
        // throw away the predicates that distinguished them, and merge the
        // timeseries information together.
        let info = consistent_key_groups
            .iter()
            .map(|group| group.consistent_keys.clone())
            .reduce(|mut acc, current| {
                acc.extend(current);
                acc
            })
            .expect("Should have at least one key-group for every query");

        // Remove the last measurement, returning just the keys and timeseries.
        let mut out = BTreeMap::new();
        for (key, measurements) in measurements_by_key.into_iter() {
            // Constuct a new timeseries, from the target/metric info.
            let (target, metric) = info.get(&key).unwrap();
            // An alignment computed in the database has already reduced the
            // samples to one point per period, so what came back is a gauge
            // whose data type is whatever the method produces, rather than raw
            // samples of the timeseries' own type. `avg` over an integer gauge
            // comes back a double, for instance.
            let input_data_type =
                oxql_types::point::DataType::try_from(schema.datum_type)?;
            let (data_type, metric_type) = match pushed.alignment() {
                Some(alignment) => (
                    alignment.align.method.output_data_type(input_data_type),
                    oxql_types::point::MetricType::Gauge,
                ),
                None if schema.datum_type.is_cumulative() => {
                    (input_data_type, oxql_types::point::MetricType::Delta)
                }
                None => (input_data_type, oxql_types::point::MetricType::Gauge),
            };
            let mut timeseries = oxql_types::Timeseries::new(
                target
                    .fields
                    .iter()
                    .chain(metric.fields.iter())
                    .map(|field| (field.name.clone(), field.value.clone())),
                data_type,
                metric_type,
            )?;

            // Covert its oximeter measurements into OxQL data types.
            let points = if pushed.alignment().is_none()
                && schema.datum_type.is_cumulative()
            {
                oxql_types::point::Points::delta_from_cumulative(&measurements)?
            } else {
                oxql_types::point::Points::gauge_from_gauge(&measurements)?
            };
            timeseries.points = points;

            // The database emits nothing at all for a period containing no
            // samples, where aligning in Rust emits a missing point. Restore
            // those, so that a query returns the same thing however it was
            // computed, and mark the result aligned for the table operations
            // that require it.
            if let Some(alignment) = pushed.alignment() {
                timeseries.points =
                    alignment.restore_empty_periods(&timeseries.points)?;
                timeseries.set_alignment(oxql_types::Alignment {
                    end_time: alignment.query_end,
                    period: alignment.align.period,
                });
            }
            debug!(
                query_log,
                "inserted new OxQL timeseries";
                "key" => key,
                "metric_type" => ?timeseries.points.metric_type(),
                "n_points" => timeseries.points.len(),
            );
            out.insert(key, timeseries);
        }
        Ok((summaries, out))
    }

    fn measurements_query(
        &self,
        schema: &TimeseriesSchema,
        consistent_key_groups: &[ConsistentKeyGroup],
        pushed: PushedWork,
        total_rows_fetched: &mut u64,
    ) -> Result<String, Error> {
        use std::fmt::Write;

        // Build the base query, which just selects the timeseries by name based
        // on the datum type.
        let mut query = self.measurements_query_raw(schema.datum_type);
        query.push_str(" WHERE timeseries_name = '");
        write!(query, "{}", schema.timeseries_name).unwrap();
        query.push('\'');

        // Filter down the fields to those which apply to the data itself, which
        // includes the timestamps and data values. The supported fields here
        // depend on the datum type.
        //
        // We join all the consistent key groups with OR, which mirrors how they
        // were split originally.
        let all_predicates = consistent_key_groups
            .iter()
            .map(|group| {
                // Write out the predicates on the measurements themselves,
                // which really refers to the timestamps (and possibly start
                // times).
                let maybe_predicates = group
                    .predicates
                    .as_ref()
                    .map(|preds| {
                        Self::rewrite_predicate_for_measurements(schema, preds)
                    })
                    .transpose()?
                    .flatten();

                // Push the predicate that selects the timeseries keys, which
                // are unique to this group.
                let maybe_key_set = if !group.consistent_keys.is_empty() {
                    let mut chunk = String::from("timeseries_key IN (");
                    let keys = group
                        .consistent_keys
                        .keys()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(",");
                    chunk.push_str(&keys);
                    chunk.push(')');
                    Some(chunk)
                } else {
                    None
                };

                let chunk = match (maybe_predicates, maybe_key_set) {
                    (Some(preds), None) => preds,
                    (None, Some(key_set)) => key_set,
                    (Some(preds), Some(key_set)) => {
                        format!("({preds} AND {key_set})")
                    }
                    (None, None) => String::new(),
                };
                Ok(chunk)
            })
            .collect::<Result<Vec<_>, Error>>()?
            .join(" OR ");
        if !all_predicates.is_empty() {
            query.push_str(" AND (");
            query.push_str(&all_predicates);
            query.push(')');
        }

        // If the alignment is being computed in the database, wrap what we
        // have so far in an aggregating query and return that instead.
        //
        // Everything below this point -- the sort order, the `LIMIT BY` that
        // implements `first` / `last`, the row budget -- either does not apply
        // or applies to the aggregated output rather than to the raw samples,
        // so it is handled inside the wrapper.
        if let Some(alignment) = pushed.alignment() {
            return alignment.wrap(query, total_rows_fetched);
        }

        // Always impose a strong order on these fields.
        //
        // The tables are all sorted by:
        //
        // - timeseries_name
        // - timeseries_key
        // - start_time, if present
        // - timestamp
        //
        // We care most about the timestamp ordering, since that is assumed (and
        // asserted) by downstream table operations.
        //
        // Note that although the tables are sorted by start_time, we _omit_
        // that if the query includes a limiting operation, like `first k`. This
        // is an unfortunate interaction between the `LIMIT BY` clause that
        // implements this in ClickHouse and the fact that the start times for
        // some metrics are not monotonic. In particular, those metrics
        // collected before a sled syncs with upstream NTP servers may have
        // wildly inaccurate start times. Using the `LIMIT BY` clause in
        // ClickHouse along with this sort order means we may end up taking the
        // latest samples from a block of metrics with an early start time, even
        // if there is a sample with a globally later, and accurate, timestamp,
        // but with a start_time _after_ that previous block.
        query.push_str(" ORDER BY timeseries_key");
        if schema.datum_type.is_cumulative() && pushed.limit().is_none() {
            query.push_str(", start_time");
        }
        query.push_str(", timestamp");

        // If provided, push a `LIMIT BY` clause, which implements the `first`
        // or `last` table operations directly in ClickHouse.
        //
        // This clause limits the number of rows _within each group_, which here
        // is always the `timeseries_key`. Note that the clause is completely
        // independent of the the traditional SQL `LIMIT` clause, pushed below
        // to avoid selecting too many rows at once.
        if let Some(limit) = pushed.limit() {
            // If this limit takes the _last_ samples, we need to invert the
            // sorting by timestamp to be descending.
            let is_last = matches!(limit.kind, LimitKind::Last);
            if is_last {
                query.push_str(" DESC");
            }

            // In either case, add the limit-by clause itself.
            query.push_str(" LIMIT ");
            write!(query, "{}", limit.count).unwrap();
            query.push_str(" BY timeseries_key");

            // Possibly invert the timestamp ordering again.
            //
            // To implement a `last k` operation, above we sort by descending
            // timestamps and use the `LIMIT k BY timeseries_key` clause.
            // However, this inverts the ordering by timestamp that we need for
            // all downstream operations to work correctly.
            //
            // Restore that ordering here, by putting the now-complete query
            // inside a CTE and selecting from that ordered by timestamp. Note
            if is_last {
                query = format!(
                    "WITH another_sort_bites_the_dust \
                    AS ({query}) \
                    SELECT * FROM another_sort_bites_the_dust \
                    ORDER BY timeseries_key, timestamp"
                );
            }
        }

        // Push a limit clause, which restricts the number of records we could
        // return.
        //
        // This is used to ensure that we never go above the limit in
        // `MAX_DATABASE_ROWS`. That restricts the _total_ number of rows we
        // want to retch from the database. So we set our limit to be one more
        // than the remainder on our allotment. If we get exactly as many as we
        // set in the limit, then we fail the query because there are more row
        // that _would_ be returned. We don't know how many more, but there is
        // at least 1 that pushes us over the limit. This prevents tricky
        // TOCTOU-like bugs where we need to check the limit twice, and improves
        // performance, since we don't return much more than we could possibly
        // handle.
        let remainder = MAX_DATABASE_ROWS - *total_rows_fetched;
        query.push_str(" LIMIT ");
        write!(query, "{}", remainder + 1).unwrap();
        Ok(query)
    }

    fn measurements_query_raw(
        &self,
        datum_type: oximeter::DatumType,
    ) -> String {
        let value_columns = if datum_type.is_histogram() {
            concat!(
                "timeseries_key, start_time, timestamp, bins, counts, min, max, ",
                "sum_of_samples, squared_mean, p50_marker_heights, p50_marker_positions, ",
                "p50_desired_marker_positions, p90_marker_heights, p90_marker_positions, ",
                "p90_desired_marker_positions, p99_marker_heights, p99_marker_positions, ",
                "p99_desired_marker_positions"
            )
        } else if datum_type.is_cumulative() {
            "timeseries_key, start_time, timestamp, datum"
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
        Ok(query)
    }

    // Build a reasonably efficient query to retrieve all fields for a given
    // timeseries. Joins in ClickHouse are expensive, so aggregate all relevant
    // fields from each relevant fields table in a single subquery, then join
    // the results together. This results in n - 1 joins, where n is the number
    // of relevant fields tables. Note that we may be able to improve
    // performance in future ClickHouse versions, which have better support for
    // Variant types, better support for the merge() table function, and faster
    // joins.
    fn all_fields_query_raw(
        &self,
        schema: &TimeseriesSchema,
    ) -> (bool, String) {
        match schema.field_schema.len() {
            0 => unreachable!(),
            _ => {
                // Build a vector of top-level select expressions, as well as a
                // map from fields to lists of subquery select expressions.
                let mut top_selects: Vec<String> = Vec::new();
                let mut select_map: HashMap<oximeter::FieldType, Vec<String>> =
                    HashMap::new();
                for field_schema in schema.field_schema.iter() {
                    select_map
                        .entry(field_schema.field_type)
                        .or_insert_with(|| vec![String::from("timeseries_key")])
                        .push(format!(
                            "anyIf(field_value, field_name = '{}') AS {}",
                            field_schema.name, field_schema.name
                        ));
                    top_selects.push(format!(
                        "{}_pivot.{} AS {}",
                        field_table_name(field_schema.field_type),
                        field_schema.name,
                        field_schema.name
                    ));
                }

                // Sort field tables by number of columns, descending.
                // ClickHouse recommends joining larger tables to smaller
                // tables, and doesn't currently reorder joins automatically.
                let mut field_types: Vec<oximeter::FieldType> =
                    select_map.keys().cloned().collect();
                field_types.sort_by(|a, b| {
                    select_map[b]
                        .len()
                        .cmp(&select_map[a].len())
                        .then(field_table_name(*a).cmp(&field_table_name(*b)))
                });

                // Build a map from field type to pivot subquery. We filter by
                // timeseries_name, group by timeseries_key, and use anyIf to
                // pivot fields to a wide table. We can use anyIf to take the
                // first matching value because a given timeseries key is
                // always associated with the same set of fields, so all rows
                // with a given (timeseries_key, field_name) will have the same
                // field_value.
                let mut query_map: HashMap<oximeter::FieldType, String> =
                    HashMap::new();
                for field_type in field_types.clone() {
                    let selects = &select_map[&field_type];
                    let query = format!(
                        "(
                            SELECT
                                {select}
                            FROM {db_name}.{from}
                            WHERE timeseries_name = '{timeseries_name}'
                            GROUP BY timeseries_key
                        ) AS {subquery_name}_pivot",
                        select = selects.join(", "),
                        db_name = crate::DATABASE_NAME,
                        from = field_table_name(field_type),
                        timeseries_name = schema.timeseries_name,
                        subquery_name = field_table_name(field_type),
                    );
                    query_map.insert(field_type, query);
                }

                // Assemble the final query.
                let mut from = query_map[&field_types[0]].clone();
                for field_type in field_types.iter().skip(1) {
                    from = format!(
                        "{from} JOIN {query} ON {source}_pivot.timeseries_key = {dest}_pivot.timeseries_key",
                        from = from,
                        query = query_map[field_type],
                        source = field_table_name(field_types[0]),
                        dest = field_table_name(*field_type),
                    );
                }
                top_selects.push(format!(
                    "{}_pivot.timeseries_key AS timeseries_key",
                    field_table_name(field_types[0])
                ));
                let query =
                    format!("SELECT {} FROM {}", top_selects.join(", "), from);
                (false, query)
            }
        }
    }
}

// Split the list of consistent key groups, ensuring none exceeds ClickHouse's
// query limit.
//
// The set of consistent keys for an OxQL query can be quite large. When stuffed
// into a giant list of keys and used in a SQL query like so:
//
// ```
// timeseries_key IN (list, of, many, keys)
// ```
//
// this can hit ClickHouse's SQL query size limit (defaulting to 256KiB, see
// https://clickhouse.com/docs/en/operations/settings/settings#max_query_size).
//
// This function chunks the list of consistent keys, ensuring that each group is
// small enough to fit within that query limit.
//
// Note that this unfortunately needs to chunk and reallocate the groups,
// because it may entail splitting each key group. That requires a copy of the
// internal map, to split it at a particular size.
fn chunk_consistent_key_groups(
    consistent_key_groups: &[ConsistentKeyGroup],
) -> Vec<Vec<ConsistentKeyGroup>> {
    // The max number of keys allowed in each measurement query.
    //
    // Keys are u64s, so their max is 18446744073709551615, which has 20 base-10
    // digits. We also separate the keys by a `,`, so let's call it 21 digits.
    //
    // ClickHouse's max query size is 256KiB, but we allow for 6KiB of overhead
    // for the other parts of the query (select, spaces, column names, etc).
    // That's very conservative.
    const MAX_QUERY_SIZE_FOR_KEYS: usize = 250 * 1024;
    const DIGITS_PER_KEY: usize = 21;
    const MAX_KEYS_PER_MEASUREMENT_QUERY: usize =
        MAX_QUERY_SIZE_FOR_KEYS / DIGITS_PER_KEY;
    chunk_consistent_key_groups_impl(
        consistent_key_groups,
        MAX_KEYS_PER_MEASUREMENT_QUERY,
    )
}

fn chunk_consistent_key_groups_impl(
    consistent_key_groups: &[ConsistentKeyGroup],
    chunk_size: usize,
) -> Vec<Vec<ConsistentKeyGroup>> {
    // Create the output vec-of-vec of key groups. We'll always push to the last
    // one, so grab a reference to it.
    let mut out = vec![vec![]];
    let mut current_chunk = out.last_mut().unwrap();
    let mut room = chunk_size;
    'group: for next_group in consistent_key_groups.iter().cloned() {
        // If we have room for it in this chunk, push it onto the current chunk,
        // and then continue to the next group.
        let group_size = next_group.consistent_keys.len();
        if room >= group_size {
            current_chunk.push(next_group);
            room -= group_size;
            continue;
        }

        // If we don't have enough room for this entire group, then we need to
        // split it up and push whatever we can. It's actually possible that the
        // next group needs to be split multiple times. So we'll do that until
        // it's empty, possibly adding new chunks to the output array.
        //
        // It's tricky to iterate over a map by the index / count, and since
        // we're operating on a clone anyway, convert this to a vec.
        let predicates = next_group.predicates;
        let mut group_keys: Vec<_> =
            next_group.consistent_keys.into_iter().collect();
        while !group_keys.is_empty() {
            // On a previous pass through this loop, we may have exhausted all
            // the remaining room. As we have re-entered it, we still have items
            // in this current group of keys. So "close" the last chunk and push
            // a new one, onto which we'll start adding the remaining items.
            if room == 0 {
                out.push(vec![]);
                current_chunk = out.last_mut().unwrap();
                room = chunk_size;
            }

            // Fetch up to the remaining set of keys.
            let ix = room.min(group_keys.len());
            let consistent_keys: BTreeMap<_, _> =
                group_keys.drain(..ix).collect();

            // There are no more keys in this group, we need to continue to the
            // next one.
            if consistent_keys.is_empty() {
                continue 'group;
            }

            // We need to update the amount of room we have left, to be sure we
            // don't push this whole group if the chunk boundary falls in the
            // middle of it.
            room -= consistent_keys.len();

            // Push this set of keys onto the current chunk.
            let this_group_chunk = ConsistentKeyGroup {
                predicates: predicates.clone(),
                consistent_keys,
            };
            current_chunk.push(this_group_chunk);
        }
    }
    out
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

#[cfg(test)]
mod tests {
    use super::ConsistentKeyGroup;
    use crate::OxqlResult;
    use crate::client::oxql::{
        QueryAuthzScope, chunk_consistent_key_groups_impl,
    };
    use crate::oxql::ast::grammar::query_parser;
    use crate::{Client, DATABASE_TIMESTAMP_FORMAT, DbWrite};
    use crate::{Metric, Target};
    use chrono::{DateTime, NaiveDate, Utc};
    use dropshot::test_util::LogContext;
    use omicron_test_utils::dev::clickhouse::ClickHouseDeployment;
    use omicron_test_utils::dev::test_setup_log;
    use oximeter::{
        AuthzScope, DatumType, FieldSchema, FieldSource, FieldType, Sample,
        TimeseriesSchema, Units,
    };
    use oximeter::{FieldValue, TimeseriesName, types::Cumulative};
    use oxql_types::{Table, Timeseries, point::Points};
    use std::collections::{BTreeMap, BTreeSet};
    use std::time::Duration;

    #[derive(
        Clone, Debug, Eq, PartialEq, PartialOrd, Ord, oximeter::Target,
    )]
    struct SomeTarget {
        name: String,
        index: u32,
    }

    #[derive(Clone, Debug, oximeter::Metric)]
    struct SomeMetric {
        foo: i32,
        datum: Cumulative<u64>,
    }

    // A gauge sharing the target above, for exercising the paths that only
    // apply to gauges.
    #[derive(Clone, Debug, oximeter::Metric)]
    struct SomeGauge {
        foo: i32,
        datum: i64,
    }

    #[derive(Clone, Debug)]
    #[allow(dead_code)]
    struct TestData {
        targets: Vec<SomeTarget>,
        // Note that we really want all the samples per metric _field_, not the
        // full metric. That would give us a 1-element sample array for each.
        samples_by_timeseries: BTreeMap<(SomeTarget, i32), Vec<Sample>>,
        first_timestamp: DateTime<Utc>,
    }

    struct TestContext {
        logctx: LogContext,
        clickhouse: ClickHouseDeployment,
        client: Client,
        test_data: TestData,
    }

    impl TestContext {
        async fn cleanup_successful(mut self) {
            self.clickhouse
                .cleanup()
                .await
                .expect("Failed to cleanup ClickHouse server");
            self.logctx.cleanup_successful();
        }
    }

    const N_SAMPLES_PER_TIMESERIES: usize = 16;
    const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
    const SHIFT: Duration = Duration::from_secs(1);

    fn format_timestamp(t: DateTime<Utc>) -> String {
        format!("{}", t.format("%Y-%m-%dT%H:%M:%S.%f"))
    }

    fn generate_test_samples() -> TestData {
        // We'll test with 4 different targets, each with two values for its
        // fields.
        let mut targets = Vec::with_capacity(4);
        let names = &["first-target", "second-target"];
        let indices = 1..3;
        for (name, index) in itertools::iproduct!(names, indices) {
            let target = SomeTarget { name: name.to_string(), index };
            targets.push(target);
        }

        // Create a start time for all samples.
        //
        // IMPORTANT: There is a TTL of 30 days on all data currently. I would
        // love this to be a fixed, well-known start time, to make tests easier,
        // but that's in conflict with the TTL. Instead, we'll use midnight on
        // the current day, and then store it in the test data context.
        let first_timestamp =
            Utc::now().date_naive().and_hms_opt(0, 0, 0).unwrap().and_utc();

        // For simplicity, we'll also assume all the cumulative measurements
        // start at the first timestamp as well.
        let datum = Cumulative::with_start_time(first_timestamp, 0);

        // We'll create two separate metrics, with 16 samples each.
        let foos = [-1, 1];
        let mut samples_by_timeseries = BTreeMap::new();
        let mut timeseries_index = 0;
        for target in targets.iter() {
            for foo in foos.iter() {
                // Shift this timeseries relative to the others, to ensure we
                // have some different timestamps.
                let timeseries_start =
                    first_timestamp + timeseries_index * SHIFT;

                // Create the first metric, starting from a count of 0.
                let mut metric = SomeMetric { foo: *foo, datum };

                // Create all the samples, incrementing the datum and sample
                // time.
                for i in 0..N_SAMPLES_PER_TIMESERIES {
                    let sample_time =
                        timeseries_start + SAMPLE_INTERVAL * i as u32;
                    let sample = Sample::new_with_timestamp(
                        sample_time,
                        target,
                        &metric,
                    )
                    .unwrap();
                    samples_by_timeseries
                        .entry((target.clone(), *foo))
                        .or_insert_with(|| {
                            Vec::with_capacity(N_SAMPLES_PER_TIMESERIES)
                        })
                        .push(sample);
                    metric.datum += 1;
                }
                timeseries_index += 1;
            }
        }
        TestData { targets, samples_by_timeseries, first_timestamp }
    }

    async fn setup_oxql_test(name: &str) -> TestContext {
        let logctx = test_setup_log(name);
        let db = ClickHouseDeployment::new_single_node(&logctx)
            .await
            .expect("Failed to start ClickHouse");
        let client = Client::new(db.native_address().into(), &logctx.log);
        client
            .init_single_node_db()
            .await
            .expect("Failed to init single-node oximeter database");
        let test_data = generate_test_samples();
        let samples: Vec<_> = test_data
            .samples_by_timeseries
            .values()
            .flatten()
            .cloned()
            .collect();
        client
            .insert_samples(&samples)
            .await
            .expect("Failed to insert test data");
        TestContext { logctx, clickhouse: db, client, test_data }
    }

    #[tokio::test]
    async fn test_get_fields_query() {
        let ctx = setup_oxql_test("test_get_fields_query").await;

        let schema = ctx
            .client
            .schema_for_timeseries(
                &TimeseriesName::try_from("some_target:some_metric").unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
        let query = ctx.client.all_fields_query(&schema, None).unwrap();
        let want = "SELECT
            fields_i32_pivot.foo AS foo,
            fields_u32_pivot.index AS index,
            fields_string_pivot.name AS name,
            fields_i32_pivot.timeseries_key AS timeseries_key
        FROM
        (
            SELECT
                timeseries_key,
                anyIf(field_value, field_name = 'foo') AS foo
            FROM oximeter.fields_i32
            WHERE timeseries_name = 'some_target:some_metric'
            GROUP BY timeseries_key
        ) AS fields_i32_pivot
        JOIN
        (
            SELECT
                timeseries_key,
                anyIf(field_value, field_name = 'name') AS name
            FROM oximeter.fields_string
            WHERE timeseries_name = 'some_target:some_metric'
            GROUP BY timeseries_key
        ) AS fields_string_pivot ON fields_i32_pivot.timeseries_key = fields_string_pivot.timeseries_key
        JOIN
        (
            SELECT
                timeseries_key,
                anyIf(field_value, field_name = 'index') AS index
            FROM oximeter.fields_u32
            WHERE timeseries_name = 'some_target:some_metric'
            GROUP BY timeseries_key
        ) AS fields_u32_pivot ON fields_i32_pivot.timeseries_key = fields_u32_pivot.timeseries_key";
        assert_eq!(
            want.split_whitespace().collect::<Vec<&str>>().join(" "),
            query.split_whitespace().collect::<Vec<&str>>().join(" ")
        );

        ctx.cleanup_successful().await;
    }

    #[tokio::test]
    async fn test_get_fields() {
        let ctx = setup_oxql_test("test_get_fields").await;

        #[derive(Clone, Debug, oximeter::Metric)]
        struct Metric1 {
            foo: i32,
            bar: i32,
            datum: Cumulative<u64>,
        }
        #[derive(Clone, Debug, oximeter::Metric)]
        struct Metric2 {
            foo: i32,
            baz: i32,
            datum: Cumulative<u64>,
        }

        // Insert samples for multiple metrics with partially overlapping field
        // names and types. Then we'll query for one of those metrics and
        // assert that we only get the expected fields, and not fields of the
        // same name and type from another metric.
        let samples = [
            Sample::new(
                &SomeTarget { name: String::from("ts1"), index: 1 },
                &Metric1 { foo: 1, bar: 2, datum: Cumulative::new(5) },
            )
            .unwrap(),
            Sample::new(
                &SomeTarget { name: String::from("ts1"), index: 1 },
                &Metric1 { foo: 1, bar: 2, datum: Cumulative::new(6) },
            )
            .unwrap(),
            Sample::new(
                &SomeTarget { name: String::from("ts2"), index: 1 },
                &Metric2 { foo: 3, baz: 4, datum: Cumulative::new(5) },
            )
            .unwrap(),
            Sample::new(
                &SomeTarget { name: String::from("ts2"), index: 1 },
                &Metric2 { foo: 3, baz: 4, datum: Cumulative::new(6) },
            )
            .unwrap(),
        ];
        ctx.client
            .insert_samples(&samples[..])
            .await
            .expect("failed to insert samples");

        let query = "get some_target:metric2 | filter timestamp > @2020-01-01";
        let result = ctx
            .client
            .oxql_query(query, QueryAuthzScope::Fleet)
            .await
            .expect("failed to run OxQL query");

        assert_eq!(result.tables.len(), 1, "should be exactly 1 table");
        let table = result.tables.get(0).unwrap();

        assert_eq!(table.n_timeseries(), 1, "should be exactly 1 series");
        let series: Vec<&Timeseries> = table.timeseries().collect();

        assert_eq!(series[0].fields.get("foo").unwrap(), &FieldValue::I32(3));
        assert_eq!(series[0].fields.get("baz").unwrap(), &FieldValue::I32(4));

        ctx.cleanup_successful().await;
    }

    #[tokio::test]
    async fn test_get_entire_table() {
        let ctx = setup_oxql_test("test_get_entire_table").await;
        // We need _some_ filter here to avoid a provable full-table scan.
        let query =
            "get some_target:some_metric | filter timestamp > @2020-01-01";
        let result = ctx
            .client
            .oxql_query(query, QueryAuthzScope::Fleet)
            .await
            .expect("failed to run OxQL query");
        assert_eq!(result.tables.len(), 1, "Should be exactly 1 table");
        let table = result.tables.get(0).unwrap();
        assert_eq!(
            table.n_timeseries(),
            ctx.test_data.samples_by_timeseries.len(),
            "Should have fetched every timeseries"
        );
        assert!(
            table.iter().all(|t| t.points.len() == N_SAMPLES_PER_TIMESERIES),
            "Should have fetched all points for all timeseries"
        );

        // Let's build the expected point array, from each timeseries we
        // inserted.
        let mut matched_timeseries = 0;
        for ((target, foo), samples) in
            ctx.test_data.samples_by_timeseries.iter()
        {
            let measurements: Vec<_> =
                samples.iter().map(|s| s.measurement.clone()).collect();
            let expected_points = Points::delta_from_cumulative(&measurements)
                .expect(
                "failed to create expected points from inserted measurements",
            );
            let expected_timeseries =
                find_timeseries_in_table(&table, target, foo)
                    .expect("Table did not contain an expected timeseries");
            assert_eq!(
                expected_timeseries.points, expected_points,
                "Did not reconstruct the correct points for this timeseries"
            );
            matched_timeseries += 1;
        }
        assert_eq!(matched_timeseries, table.len());
        assert_eq!(
            matched_timeseries,
            ctx.test_data.samples_by_timeseries.len()
        );

        ctx.cleanup_successful().await;
    }

    #[tokio::test]
    async fn test_get_one_timeseries() {
        let ctx = setup_oxql_test("test_get_one_timeseries").await;

        // Specify exactly one timeseries we _want_ to fetch, by picking the
        // first timeseries we inserted.
        let ((expected_target, expected_foo), expected_samples) =
            ctx.test_data.samples_by_timeseries.first_key_value().unwrap();
        let query = format!(
            "get some_target:some_metric | filter {}",
            exact_filter_for(expected_target, *expected_foo)
        );
        let result = ctx
            .client
            .oxql_query(&query, QueryAuthzScope::Fleet)
            .await
            .expect("failed to run OxQL query");
        assert_eq!(result.tables.len(), 1, "Should be exactly 1 table");
        let table = result.tables.get(0).unwrap();
        assert_eq!(
            table.n_timeseries(),
            1,
            "Should have fetched exactly the target timeseries"
        );
        assert!(
            table.iter().all(|t| t.points.len() == N_SAMPLES_PER_TIMESERIES),
            "Should have fetched all points for all timeseries"
        );

        let expected_timeseries =
            find_timeseries_in_table(&table, expected_target, expected_foo)
                .expect("Table did not contain expected timeseries");
        let measurements: Vec<_> =
            expected_samples.iter().map(|s| s.measurement.clone()).collect();
        let expected_points = Points::delta_from_cumulative(&measurements)
            .expect("failed to build expected points from measurements");
        assert_eq!(
            expected_points, expected_timeseries.points,
            "Did not reconstruct the correct points for the one \
            timeseries the query fetched"
        );

        ctx.cleanup_successful().await;
    }

    // In this test, we'll fetch the entire history of one timeseries, and only
    // the last few samples of another.
    //
    // This checks that we correctly do complex logical operations that require
    // fetching different sets of fields at different times.
    #[tokio::test]
    async fn test_get_entire_timeseries_and_part_of_another() {
        let ctx =
            setup_oxql_test("test_get_entire_timeseries_and_part_of_another")
                .await;

        let mut it = ctx.test_data.samples_by_timeseries.iter();
        let (entire, only_part) = (it.next().unwrap(), it.next().unwrap());

        let entire_filter = exact_filter_for(&entire.0.0, entire.0.1);
        let only_part_filter = exact_filter_for(&only_part.0.0, only_part.0.1);
        let start_timestamp = only_part.1[6].measurement.timestamp();
        let only_part_timestamp_filter = format_timestamp(start_timestamp);

        let query = format!(
            "get some_target:some_metric | filter ({}) || (timestamp >= @{} && {})",
            entire_filter, only_part_timestamp_filter, only_part_filter,
        );
        let result = ctx
            .client
            .oxql_query(&query, QueryAuthzScope::Fleet)
            .await
            .expect("failed to run OxQL query");
        assert_eq!(result.tables.len(), 1, "Should be exactly 1 table");
        let table = result.tables.get(0).unwrap();
        assert_eq!(
            table.n_timeseries(),
            2,
            "Should have fetched exactly the two target timeseries"
        );

        // Check that we fetched the entire timeseries for the first one.
        let expected_timeseries =
            find_timeseries_in_table(table, &entire.0.0, &entire.0.1)
                .expect("failed to fetch all of the first timeseries");
        let measurements: Vec<_> =
            entire.1.iter().map(|s| s.measurement.clone()).collect();
        let expected_points = Points::delta_from_cumulative(&measurements)
            .expect("failed to build expected points");
        assert_eq!(
            expected_timeseries.points, expected_points,
            "Did not collect the entire set of points for the first timeseries",
        );

        // And that we only get the last portion of the second timeseries.
        let expected_timeseries =
            find_timeseries_in_table(table, &only_part.0.0, &only_part.0.1)
                .expect("failed to fetch part of the second timeseries");
        let measurements: Vec<_> = only_part
            .1
            .iter()
            .filter_map(|sample| {
                let meas = &sample.measurement;
                if meas.timestamp() >= start_timestamp {
                    Some(meas.clone())
                } else {
                    None
                }
            })
            .collect();
        let expected_points = Points::delta_from_cumulative(&measurements)
            .expect("failed to build expected points");
        assert_eq!(
            expected_timeseries.points, expected_points,
            "Did not collect the last few points for the second timeseries",
        );

        ctx.cleanup_successful().await;
    }

    // Return an OxQL filter item that will exactly select the provided
    // timeseries by its target / metric.
    fn exact_filter_for(target: &SomeTarget, foo: i32) -> String {
        format!(
            "name == '{}' && index == {} && foo == {}",
            target.name, target.index, foo,
        )
    }

    // Given a table from an OxQL query, look up the timeseries for the inserted
    // target / metric, if it exists
    fn find_timeseries_in_table<'a>(
        table: &'a Table,
        target: &'a SomeTarget,
        foo: &'a i32,
    ) -> Option<&'a Timeseries> {
        for timeseries in table.iter() {
            let fields = &timeseries.fields;

            // Look up each field in turn, and compare it.
            let FieldValue::String(val) = fields.get("name")? else {
                unreachable!();
            };
            if val != &target.name {
                continue;
            }
            let FieldValue::U32(val) = fields.get("index")? else {
                unreachable!();
            };
            if val != &target.index {
                continue;
            }
            let FieldValue::I32(val) = fields.get("foo")? else {
                unreachable!();
            };
            if val != foo {
                continue;
            }

            // We done matched it.
            return Some(timeseries);
        }
        None
    }

    fn make_consistent_key_group(size: u64) -> ConsistentKeyGroup {
        let consistent_keys = (0..size)
            .map(|key| {
                let target = Target { name: "foo".to_string(), fields: vec![] };
                let metric = Metric {
                    name: "bar".to_string(),
                    fields: vec![],
                    datum_type: DatumType::U8,
                };
                (key, (target, metric))
            })
            .collect();
        ConsistentKeyGroup { predicates: None, consistent_keys }
    }

    #[test]
    fn test_chunk_consistent_key_groups_all_in_one_chunk() {
        // Create two key groups, each with 5 keys.
        //
        // With a chunk size of 12, these should all be in the same chunk, so
        // we're really just cloning the inputs. They do go into an outer vec
        // though, because we can have multiple chunks in theory.
        let keys =
            vec![make_consistent_key_group(5), make_consistent_key_group(5)];
        let chunks = chunk_consistent_key_groups_impl(&keys, 12);
        assert_eq!(
            chunks.len(),
            1,
            "All key groups should fit into one chunk when their \
            total size is less than the chunk size"
        );
        assert_eq!(
            keys, chunks[0],
            "All key groups should fit into one chunk when their \
            total size is less than the chunk size"
        );
    }

    #[test]
    fn test_chunk_consistent_key_groups_split_middle_of_key_group() {
        // Create one key group, with 10 keys.
        //
        // With a chunk size of 5, this should be split in half across two
        // chunks.
        let keys = vec![make_consistent_key_group(10)];
        let chunks = chunk_consistent_key_groups_impl(&keys, 5);
        assert_eq!(
            chunks.len(),
            2,
            "Consistent key group should be split into two chunks",
        );

        let first = keys[0]
            .consistent_keys
            .range(..5)
            .map(|(k, v)| (*k, v.clone()))
            .collect();
        assert_eq!(
            chunks[0][0].consistent_keys, first,
            "The first chunk of the consistent keys should be \
            the first half of the input keys"
        );

        let second = keys[0]
            .consistent_keys
            .range(5..)
            .map(|(k, v)| (*k, v.clone()))
            .collect();
        assert_eq!(
            chunks[1][0].consistent_keys, second,
            "The second chunk of the consistent keys should be \
            the second half of the input keys"
        );
    }

    #[test]
    fn test_chunk_consistent_key_groups_split_key_group_multiple_times() {
        // Create one key group, with 10 keys.
        //
        // With a chunk size of 4, this should be split 3 times, with the first
        // two having 4 items and the last the remaining 2.
        let keys = vec![make_consistent_key_group(10)];
        let chunks = chunk_consistent_key_groups_impl(&keys, 4);
        assert_eq!(
            chunks.len(),
            3,
            "Consistent key group should be split into three chunks",
        );

        let first = keys[0]
            .consistent_keys
            .range(..4)
            .map(|(k, v)| (*k, v.clone()))
            .collect();
        assert_eq!(
            chunks[0][0].consistent_keys, first,
            "The first chunk of the consistent keys should be \
            the first 4 input keys"
        );

        let second = keys[0]
            .consistent_keys
            .range(4..8)
            .map(|(k, v)| (*k, v.clone()))
            .collect();
        assert_eq!(
            chunks[1][0].consistent_keys, second,
            "The second chunk of the consistent keys should be \
            the next 4 input keys",
        );

        let third = keys[0]
            .consistent_keys
            .range(8..)
            .map(|(k, v)| (*k, v.clone()))
            .collect();
        assert_eq!(
            chunks[2][0].consistent_keys, third,
            "The second chunk of the consistent keys should be \
            the remaining 2 input keys",
        );
    }

    #[tokio::test]
    async fn test_limit_operations() {
        let ctx = setup_oxql_test("test_limit_operations").await;

        // Specify exactly one timeseries we _want_ to fetch, by picking the
        // first timeseries we inserted.
        let ((expected_target, expected_foo), expected_samples) =
            ctx.test_data.samples_by_timeseries.first_key_value().unwrap();
        let query = format!(
            "get some_target:some_metric | filter {} | first 1",
            exact_filter_for(expected_target, *expected_foo)
        );
        let result = ctx
            .client
            .oxql_query(&query, QueryAuthzScope::Fleet)
            .await
            .expect("failed to run OxQL query");
        assert_eq!(result.tables.len(), 1, "Should be exactly 1 table");
        let table = result.tables.get(0).unwrap();
        assert_eq!(
            table.n_timeseries(),
            1,
            "Should have fetched exactly the target timeseries"
        );
        assert!(
            table.iter().all(|t| t.points.len() == 1),
            "Should have fetched exactly 1 point for this timeseries",
        );

        let expected_timeseries =
            find_timeseries_in_table(&table, expected_target, expected_foo)
                .expect("Table did not contain expected timeseries");
        let measurements: Vec<_> = expected_samples
            .iter()
            .take(1)
            .map(|s| s.measurement.clone())
            .collect();
        let expected_points = Points::delta_from_cumulative(&measurements)
            .expect("failed to build expected points from measurements");
        assert_eq!(
            expected_points, expected_timeseries.points,
            "Did not reconstruct the correct points for the one \
            timeseries the query fetched"
        );

        ctx.cleanup_successful().await;
    }

    fn test_schema() -> TimeseriesSchema {
        TimeseriesSchema {
            timeseries_name: "foo:bar".parse().unwrap(),
            description: Default::default(),
            field_schema: BTreeSet::from([FieldSchema {
                name: String::from("f0"),
                field_type: FieldType::U32,
                source: FieldSource::Target,
                description: String::new(),
            }]),
            datum_type: DatumType::U64,
            version: 1.try_into().unwrap(),
            authz_scope: AuthzScope::Fleet,
            units: Units::None,
            created: Utc::now(),
        }
    }

    #[test]
    fn correctly_negate_field_predicate_expression() {
        let logctx =
            test_setup_log("correctly_negate_field_predicate_expression");
        let schema = test_schema();
        let filt = query_parser::filter("filter !(f0 == 0)").unwrap();
        let rewritten = Client::rewrite_predicate_for_fields(&schema, &filt)
            .unwrap()
            .expect("Should have rewritten the field predicate");
        assert_eq!(rewritten, "NOT equals(f0, 0)");
        logctx.cleanup_successful();
    }

    #[test]
    fn correctly_negate_timestamp_predicate_expression() {
        let logctx =
            test_setup_log("correctly_negate_field_predicate_expression");
        let schema = test_schema();
        let now = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc();
        let now_str = "2024-01-01";
        let filter_str = format!("filter !(timestamp > @{})", now_str);
        let filt = query_parser::filter(&filter_str).unwrap();
        let rewritten =
            Client::rewrite_predicate_for_measurements(&schema, &filt)
                .unwrap()
                .expect("Should have rewritten the timestamp predicate");
        assert_eq!(
            rewritten,
            format!(
                "NOT greater(timestamp, '{}')",
                now.format(DATABASE_TIMESTAMP_FORMAT)
            )
        );
        logctx.cleanup_successful();
    }

    // The inserted fixture is a counter that advances by exactly 1 every
    // second, so every aligned window should report a rate of 1.0/s no matter
    // what period is asked for. Running it through a real ClickHouse exercises
    // the cumulative -> delta conversion, which is where the interesting edge
    // cases live.
    #[tokio::test]
    async fn test_align_rate_over_a_real_counter() {
        let ctx = setup_oxql_test("test_align_rate_over_a_real_counter").await;
        let ((target, foo), _) =
            ctx.test_data.samples_by_timeseries.first_key_value().unwrap();

        for period in ["2s", "5s"] {
            let query = format!(
                "get some_target:some_metric | filter {} | align rate({})",
                exact_filter_for(target, *foo),
                period,
            );
            let result = ctx
                .client
                .oxql_query(&query, QueryAuthzScope::Fleet)
                .await
                .unwrap_or_else(|e| panic!("`{query}` failed: {e}"));

            let table = result.tables.first().expect("one table");
            let timeseries =
                find_timeseries_in_table(table, target, foo).expect("found");

            // Alignment walks back from the query end time, so most windows
            // fall in the gap between the fixture and now and hold no data.
            // The ones that do have data all cover the same steady counter.
            let rates: Vec<f64> = timeseries
                .points
                .values(0)
                .unwrap()
                .as_double()
                .expect("rate emits doubles")
                .iter()
                .flatten()
                .copied()
                .collect();

            assert!(
                !rates.is_empty(),
                "`{query}` produced no aligned points at all",
            );
            for rate in rates.iter() {
                assert!(
                    (rate - 1.0).abs() < 1e-9,
                    "The fixture counter advances by 1 per second, so every \
                    window with data should report 1.0/s. `{query}` gave \
                    {rates:?}",
                );
            }
        }
        ctx.cleanup_successful().await;
    }

    // A producer restart begins a new epoch: the counter resets and its
    // samples carry a new start time. The rate should be unchanged across it,
    // and in particular the dead time while the producer was down must not
    // dilute it -- the span is measured from the restart, not from the last
    // sample before it.
    #[tokio::test]
    async fn test_align_rate_across_a_producer_restart() {
        let ctx =
            setup_oxql_test("test_align_rate_across_a_producer_restart").await;

        // A counter advancing by 10 every second, interrupted by a restart
        // with a minute of downtime -- the shape of a sled reboot.
        let target = SomeTarget { name: String::from("restart"), index: 9 };
        let first = ctx.test_data.first_timestamp;
        let mut samples = Vec::new();
        for (epoch_start, offsets) in
            [(first, 1..=5u32), (first + Duration::from_secs(65), 66..=70u32)]
        {
            for (i, offset) in offsets.enumerate() {
                let datum = Cumulative::with_start_time(
                    epoch_start,
                    (i as u64 + 1) * 10,
                );
                let metric = SomeMetric { foo: 7, datum };
                samples.push(
                    Sample::new_with_timestamp(
                        first + Duration::from_secs(offset.into()),
                        &target,
                        &metric,
                    )
                    .unwrap(),
                );
            }
        }
        ctx.client.insert_samples(&samples).await.expect("inserted");

        let query = format!(
            "get some_target:some_metric | filter {} | align rate(5s)",
            exact_filter_for(&target, 7),
        );
        let result = ctx
            .client
            .oxql_query(&query, QueryAuthzScope::Fleet)
            .await
            .unwrap_or_else(|e| panic!("`{query}` failed: {e}"));

        let table = result.tables.first().expect("one table");
        let timeseries =
            find_timeseries_in_table(table, &target, &7).expect("found");
        let rates: Vec<f64> = timeseries
            .points
            .values(0)
            .unwrap()
            .as_double()
            .expect("rate emits doubles")
            .iter()
            .flatten()
            .copied()
            .collect();

        assert!(!rates.is_empty(), "`{query}` produced no aligned points");
        for rate in rates.iter() {
            assert!(
                (rate - 10.0).abs() < 1e-9,
                "Every window covering either epoch sees the same steady 10/s. \
                A window starting at the restart must measure from the restart, \
                not across the downtime, and the reset must not read as a \
                negative or enormous jump. `{query}` gave {rates:?}",
            );
        }
        ctx.cleanup_successful().await;
    }

    // The fixture counter advances by exactly 1 per second. It is cumulative,
    // so alignment is not pushed for it -- this checks the gauge path against
    // a gauge metric inserted alongside it.
    #[tokio::test]
    async fn test_pushed_alignment_matches_the_rust_path() {
        let ctx =
            setup_oxql_test("test_pushed_alignment_matches_the_rust_path")
                .await;

        // A gauge that sawtooths, so that min, max and mean all differ.
        let target = SomeTarget { name: String::from("gauge"), index: 8 };
        let first = ctx.test_data.first_timestamp;
        let mut samples = Vec::new();
        for i in 0..32u32 {
            let metric = SomeGauge { foo: 5, datum: i64::from(i % 7) };
            samples.push(
                Sample::new_with_timestamp(
                    first + Duration::from_secs(i.into()),
                    &target,
                    &metric,
                )
                .unwrap(),
            );
        }
        ctx.client.insert_samples(&samples).await.expect("inserted");

        // A counter advancing by 10 every second, restarting partway through
        // so that the epoch handling is exercised too.
        let counter = SomeTarget { name: String::from("counter"), index: 8 };
        let mut samples = Vec::new();
        for (epoch_start, offsets) in
            [(first, 0..12u32), (first + Duration::from_secs(14), 15..32u32)]
        {
            for (i, offset) in offsets.enumerate() {
                let datum = Cumulative::with_start_time(
                    epoch_start,
                    (i as u64 + 1) * 10,
                );
                let metric = SomeMetric { foo: 5, datum };
                samples.push(
                    Sample::new_with_timestamp(
                        first + Duration::from_secs(offset.into()),
                        &counter,
                        &metric,
                    )
                    .unwrap(),
                );
            }
        }
        ctx.client.insert_samples(&samples).await.expect("inserted");

        let cases = [
            ("some_gauge", &target, "max"),
            ("some_gauge", &target, "min"),
            ("some_gauge", &target, "mean_within"),
            ("some_metric", &counter, "rate"),
        ];
        for (metric, target, method) in cases {
            for period in ["3s", "5s", "10s"] {
                // Pin the end of the query. It defaults to now, and the
                // two queries below run a moment apart, which would anchor
                // their output periods a few milliseconds from each other.
                let end = format_timestamp(first + Duration::from_secs(40));
                let query = format!(
                    "get some_target:{} | filter {} \
                     && timestamp <= @{} | align {}({})",
                    metric,
                    exact_filter_for(target, 5),
                    end,
                    method,
                    period,
                );
                let pushed = ctx
                    .client
                    .oxql_query(&query, QueryAuthzScope::Fleet)
                    .await
                    .unwrap_or_else(|e| panic!("`{query}` failed: {e}"));

                // Run the same query with the alignment forced into Rust, by
                // putting a filter on the datum in front of it that cannot be
                // pushed. The filter admits everything, so the two must agree
                // point for point.
                let unpushed_query = format!(
                    "get some_target:{} | filter {} \
                     && timestamp <= @{} | filter datum >= -1 \
                     | align {}({})",
                    metric,
                    exact_filter_for(target, 5),
                    end,
                    method,
                    period,
                );
                let unpushed = ctx
                    .client
                    .oxql_query(&unpushed_query, QueryAuthzScope::Fleet)
                    .await
                    .unwrap_or_else(|e| {
                        panic!("`{unpushed_query}` failed: {e}")
                    });

                let left = only_timeseries(&pushed);
                let right = only_timeseries(&unpushed);
                assert_eq!(
                    left, right,
                    "Aligning `{method}({period})` in the database must give \
                    exactly what aligning it in Rust gives, including the \
                    data type, the timestamps and the missing points.\n\
                    pushed:   {left:?}\nunpushed: {right:?}",
                );
                assert!(
                    left.iter().any(|(_, v)| v.is_some()),
                    "`{query}` produced no data at all, so the comparison \
                    above proved nothing",
                );

                // Prove the two really did take different paths. Without
                // this the comparison would still pass if the alignment were
                // quietly never pushed, since both sides would then be the
                // same Rust code.
                // Every pushed alignment groups the samples into periods, and
                // nothing else the client issues does.
                const GROUPS_BY_PERIOD: &str = "GROUP BY timeseries_key, ";
                assert!(
                    pushed.query_summaries.iter().any(|summary| summary
                        .query
                        .contains(GROUPS_BY_PERIOD)),
                    "`{query}` should have been aligned by the database, but \
                    none of the queries it ran group samples into periods: \
                    {:#?}",
                    pushed
                        .query_summaries
                        .iter()
                        .map(|s| &s.query)
                        .collect::<Vec<_>>(),
                );
                assert!(
                    unpushed.query_summaries.iter().all(|summary| !summary
                        .query
                        .contains(GROUPS_BY_PERIOD)),
                    "A filter on the datum cannot be applied by the database, \
                    so `{unpushed_query}` must not be aligned there either -- \
                    the filter would be applied to rows that had already been \
                    aggregated away",
                );

                // Note there is deliberately no assertion here that the
                // pushed query reads fewer rows. `io_summary` counts what
                // ClickHouse scanned, and it still has to scan every sample
                // in order to aggregate it -- the two paths read the same
                // rows. What pushing down saves is the rows that come *back*:
                // one per period instead of one per sample, which is what
                // crosses the network, what is held in memory, and what
                // counts against `MAX_DATABASE_ROWS`. The protocol gives us
                // no count of returned rows to assert on, so the check that
                // this actually happened is the shape of the SQL above.
            }
        }
        ctx.cleanup_successful().await;
    }

    // Seven days of temperature sampled every second, reduced to one maximum
    // per day.
    //
    // This is the shape that motivated computing alignment in the database.
    // The answer is eight numbers, and finding them in Rust means moving all
    // 604,800 samples across the wire -- two thirds of the entire
    // `MAX_DATABASE_ROWS` budget, for a single timeseries. A month of the same
    // data would not fit in the budget at all, so the unaligned form of this
    // query would not fail slowly, it would refuse to run.
    // A day of temperatures from seven sleds, reduced to an hourly maximum per
    // sled.
    //
    // Two links on each sled, sampled every two seconds, keeps the total at
    // the same 604,800 rows as the test above while giving `group_by`
    // something to actually combine -- with one timeseries per group its
    // reducer would just hand back what it was given.
    //
    // This covers two things the single-series tests cannot. The aggregate
    // groups by `timeseries_key` as well as by period, so fourteen series have
    // to come back correctly interleaved and be split apart again. And
    // `group_by` refuses input that is not aligned, so it only accepts this at
    // all because the pushed path marks its output aligned the way the Rust
    // path does.
    #[tokio::test]
    async fn test_pushed_alignment_across_timeseries() {
        const SLEDS: u32 = 7;
        const LINKS: u32 = 2;
        const SAMPLES_PER_LINK: u32 = 43_200;
        const SAMPLE_INTERVAL_SECS: u64 = 2;

        let ctx =
            setup_oxql_test("test_pushed_alignment_across_timeseries").await;
        let first = ctx.test_data.first_timestamp;

        let insert_start = std::time::Instant::now();
        for sled in 0..SLEDS {
            let name = format!("sled-{sled}");
            for link in 0..LINKS {
                let target = SomeTarget { name: name.clone(), index: link };
                for chunk_start in (0..SAMPLES_PER_LINK).step_by(60_000) {
                    let chunk_end =
                        (chunk_start + 60_000).min(SAMPLES_PER_LINK);
                    let samples: Vec<_> = (chunk_start..chunk_end)
                        .map(|i| {
                            // The two links run ten degrees apart, so the mean
                            // across them is a value neither one reports.
                            let base = 60 + i64::from(link) * 10;
                            let metric = SomeGauge {
                                foo: 4,
                                datum: base + i64::from(i % 31),
                            };
                            Sample::new_with_timestamp(
                                first
                                    + Duration::from_secs(
                                        u64::from(i) * SAMPLE_INTERVAL_SECS,
                                    ),
                                &target,
                                &metric,
                            )
                            .unwrap()
                        })
                        .collect();
                    ctx.client
                        .insert_samples(&samples)
                        .await
                        .expect("inserted");
                }
            }
        }
        let total = SLEDS * LINKS * SAMPLES_PER_LINK;
        println!("inserted {total} samples in {:?}", insert_start.elapsed());

        let end = format_timestamp(first + Duration::from_secs(86_400));
        let tail = format!(
            "filter foo == 4 && timestamp <= @{end} \
             | align max(1h) | group_by [name], mean"
        );
        let query = format!("get some_target:some_gauge | {tail}");
        let unpushed_query = format!(
            "get some_target:some_gauge | filter foo == 4 \
             && timestamp <= @{end} | filter datum >= -1 \
             | align max(1h) | group_by [name], mean"
        );

        let start = std::time::Instant::now();
        let pushed = ctx
            .client
            .oxql_query(&query, QueryAuthzScope::Fleet)
            .await
            .unwrap_or_else(|e| panic!("`{query}` failed: {e}"));
        let pushed_elapsed = start.elapsed();

        let start = std::time::Instant::now();
        let unpushed = ctx
            .client
            .oxql_query(&unpushed_query, QueryAuthzScope::Fleet)
            .await
            .unwrap_or_else(|e| panic!("`{unpushed_query}` failed: {e}"));
        let unpushed_elapsed = start.elapsed();

        let left = timeseries_by_name(&pushed);
        let right = timeseries_by_name(&unpushed);
        println!(
            "align max(1h) | group_by [name] over {total} samples:\n  \
             pushed   {pushed_elapsed:?}\n  unpushed {unpushed_elapsed:?}\n  \
             sleds    {}\n  periods  {}",
            left.len(),
            left.values().next().map(Vec::len).unwrap_or(0),
        );

        assert_eq!(
            left, right,
            "Grouping hourly maxima across seven sleds must give the same \
            answer whether the alignment ran in the database or in Rust",
        );
        assert_eq!(
            left.len() as u32,
            SLEDS,
            "One group per sled: {:?}",
            left.keys().collect::<Vec<_>>(),
        );
        for (name, points) in left.iter() {
            // 24 hours, plus the period holding the single sample that sits on
            // the starting boundary -- see the test above for why.
            assert_eq!(points.len(), 25, "{name}: {points:?}");
            assert_eq!(
                points[0].1,
                Some(65.0),
                "{name}: the boundary period holds one sample from each link, \
                60 and 70, whose mean is 65: {points:?}",
            );
            for (_, value) in points.iter().skip(1) {
                assert_eq!(
                    *value,
                    Some(95.0),
                    "{name}: each link tops out at 90 and 100 within an hour, \
                    so the mean across them is 95: {points:?}",
                );
            }
        }
        ctx.cleanup_successful().await;
    }

    /// The timestamps and values of one aligned timeseries.
    type AlignedPoints = Vec<(chrono::DateTime<Utc>, Option<f64>)>;

    // The points of every timeseries in a result, keyed by its `name` field.
    fn timeseries_by_name(
        result: &OxqlResult,
    ) -> BTreeMap<String, AlignedPoints> {
        let table = result.tables.first().expect("one table");
        table
            .iter()
            .map(|timeseries| {
                let FieldValue::String(name) =
                    timeseries.fields.get("name").expect("a name field")
                else {
                    panic!("`name` should be a string");
                };
                let values: Vec<Option<f64>> =
                    match timeseries.points.values(0).expect("values") {
                        oxql_types::point::ValueArray::Double(values) => {
                            values.clone()
                        }
                        oxql_types::point::ValueArray::Integer(values) => {
                            values.iter().map(|v| v.map(|v| v as f64)).collect()
                        }
                        other => {
                            panic!("unexpected type: {:?}", other.data_type())
                        }
                    };
                let points = timeseries
                    .points
                    .timestamps()
                    .iter()
                    .copied()
                    .zip(values)
                    .collect();
                (name.to_string(), points)
            })
            .collect()
    }

    #[tokio::test]
    async fn test_pushed_alignment_over_a_week_of_samples() {
        const SAMPLE_COUNT: u32 = 7 * 86_400;
        let ctx =
            setup_oxql_test("test_pushed_alignment_over_a_week_of_samples")
                .await;

        let target = SomeTarget { name: String::from("thermal"), index: 3 };
        let first = ctx.test_data.first_timestamp;

        // Insert in chunks. Holding every sample at once costs far more memory
        // than the data itself does.
        let insert_start = std::time::Instant::now();
        for chunk_start in (0..SAMPLE_COUNT).step_by(60_000) {
            let chunk_end = (chunk_start + 60_000).min(SAMPLE_COUNT);
            let samples: Vec<_> = (chunk_start..chunk_end)
                .map(|i| {
                    // Cycle over a plausible range for a component
                    // temperature, so that every full day hits the same known
                    // maximum rather than whatever the last sample happened to
                    // be. Starting well above zero keeps a real reading from
                    // being mistaken for an empty period.
                    let metric =
                        SomeGauge { foo: 2, datum: 60 + i64::from(i % 31) };
                    Sample::new_with_timestamp(
                        first + Duration::from_secs(i.into()),
                        &target,
                        &metric,
                    )
                    .unwrap()
                })
                .collect();
            ctx.client.insert_samples(&samples).await.expect("inserted");
        }
        println!(
            "inserted {SAMPLE_COUNT} samples in {:?}",
            insert_start.elapsed(),
        );

        let end = format_timestamp(first + Duration::from_secs(7 * 86_400));
        let query = format!(
            "get some_target:some_gauge | filter {} && timestamp <= @{} \
             | align max(1d)",
            exact_filter_for(&target, 2),
            end,
        );
        let unpushed_query = format!(
            "get some_target:some_gauge | filter {} && timestamp <= @{} \
             | filter datum >= -1 | align max(1d)",
            exact_filter_for(&target, 2),
            end,
        );

        let start = std::time::Instant::now();
        let pushed = ctx
            .client
            .oxql_query(&query, QueryAuthzScope::Fleet)
            .await
            .unwrap_or_else(|e| panic!("`{query}` failed: {e}"));
        let pushed_elapsed = start.elapsed();

        let start = std::time::Instant::now();
        let unpushed = ctx
            .client
            .oxql_query(&unpushed_query, QueryAuthzScope::Fleet)
            .await
            .unwrap_or_else(|e| panic!("`{unpushed_query}` failed: {e}"));
        let unpushed_elapsed = start.elapsed();

        let left = only_timeseries(&pushed);
        let right = only_timeseries(&unpushed);
        println!(
            "align max(1d) over {SAMPLE_COUNT} samples:\n  \
             pushed   {pushed_elapsed:?}\n  unpushed {unpushed_elapsed:?}\n  \
             points   {}\n  values   {:?}",
            left.len(),
            left.iter().map(|(_, v)| *v).collect::<Vec<_>>(),
        );

        assert_eq!(
            left, right,
            "Aligning a week of samples in the database must give exactly \
            what aligning them in Rust gives",
        );
        // Eight periods, not seven. Periods run backwards from the end of the
        // query and are half open as `(start, end]`, and this fixture puts the
        // first sample exactly one whole number of periods before the end. So
        // that sample lands in the period *ending* at its own timestamp, and
        // the period after it, `(first, first + 1d]`, excludes it. The first
        // period therefore holds a single sample and reports its value.
        //
        // Real data lands on a period boundary only by coincidence, since the
        // end of a query is usually just "now". The reason to pin it here is
        // that it looks like an off-by-one until you work out that it isn't.
        assert_eq!(left.len(), 8, "{left:?}");
        assert_eq!(
            left[0].1,
            Some(60.0),
            "The first period holds only the sample sitting on its own \
            boundary, whose reading is the bottom of the range: {left:?}",
        );
        for (_, value) in left.iter().skip(1) {
            assert_eq!(
                *value,
                Some(90.0),
                "Every period holding a full day should reach the top of the \
                range: {left:?}",
            );
        }

        // Deliberately not asserting that the pushed query is faster. Both
        // scan the same rows in ClickHouse, the saving is in what comes back,
        // and wall-clock on a loaded test machine is far too noisy to hang a
        // failure on. The numbers are printed above to be looked at.
        ctx.cleanup_successful().await;
    }

    // The timestamps and values of the single timeseries in a result.
    fn only_timeseries(
        result: &OxqlResult,
    ) -> Vec<(chrono::DateTime<Utc>, Option<f64>)> {
        let table = result.tables.first().expect("one table");
        let timeseries = table.iter().next().expect("one timeseries");
        let values: Vec<Option<f64>> =
            match timeseries.points.values(0).expect("values") {
                oxql_types::point::ValueArray::Double(values) => values.clone(),
                oxql_types::point::ValueArray::Integer(values) => {
                    values.iter().map(|v| v.map(|v| v as f64)).collect()
                }
                other => panic!("unexpected type: {:?}", other.data_type()),
            };
        timeseries.points.timestamps().iter().copied().zip(values).collect()
    }
}
