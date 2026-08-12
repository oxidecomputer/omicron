// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Benchmark for OxQL query performance.
//!
//! Tests multiple timeseries with varying numbers of field types.

mod common;

use common::{bench_metric, bench_oxql_query, get_client, get_socket_addr};
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use oximeter_db::native::Connection;
use rand::seq::SliceRandom;
use uuid::Uuid;

/// Timeseries to benchmark, spanning a range of field table counts.
const TIMESERIES_NAMES: &[&str] = &[
    "crucible_upstairs:flush",
    "ddm_session:advertisements_received",
    "virtual_machine:vcpu_usage",
    "bgp_session:active_connections_accepted",
    "switch_data_link:bytes_sent",
];

/// Metadata about a timeseries, fetched from the database.
struct TimeseriesInfo {
    name: String,
    field_tables: u64,
    cardinality: u64,
}

/// Fetch field table count and cardinality for each timeseries.
fn get_timeseries_info(rt: &tokio::runtime::Runtime) -> Vec<TimeseriesInfo> {
    let names_list = TIMESERIES_NAMES
        .iter()
        .map(|name| format!("'{}'", name))
        .collect::<Vec<_>>()
        .join(", ");

    let query = format!(
        "SELECT
            series.timeseries_name,
            length(arrayDistinct(any(series.fields.type))) AS field_tables,
            count(DISTINCT fields.timeseries_key) AS cardinality
        FROM oximeter.timeseries_schema series
        JOIN merge('oximeter', '^fields_') fields
            ON series.timeseries_name = fields.timeseries_name
        WHERE series.timeseries_name IN ({})
        GROUP BY series.timeseries_name
        ORDER BY field_tables, cardinality",
        names_list
    );

    rt.block_on(async {
        let mut conn = Connection::new(get_socket_addr()).await.unwrap();
        let result = conn.query(Uuid::new_v4(), &query).await.unwrap();
        let block = result.data.as_ref().expect("query returned no data");

        let names = block
            .column_values("timeseries_name")
            .unwrap()
            .as_string()
            .unwrap();
        let field_tables =
            block.column_values("field_tables").unwrap().as_u64().unwrap();
        let cardinalities =
            block.column_values("cardinality").unwrap().as_u64().unwrap();

        names
            .iter()
            .zip(field_tables.iter())
            .zip(cardinalities.iter())
            .map(|((name, &field_tables), &cardinality)| TimeseriesInfo {
                name: name.clone(),
                field_tables,
                cardinality,
            })
            .collect()
    })
}

// Benchmark field lookup. As of this writing, filtering and collating fields
// can make up a significant proportion of overall query time, and its latency
// varies with both the cardinality and the number of field tables that need to
// be combined for the relevant series. Query each timeseries in TIMESERIES_NAMES,
// filtering to a future timestamp so that we only benchmark the performance of
// field lookup, and ignore measurements. Note that the user is responsible for
// populating ClickHouse with test data.
fn oxql_field_lookup(c: &mut Criterion) {
    let metric = bench_metric();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let client = get_client(&rt);
    let mut group = c.benchmark_group("oxql");

    let mut timeseries_info = get_timeseries_info(&rt);
    timeseries_info.shuffle(&mut rand::rng());

    let max_cardinality = timeseries_info
        .iter()
        .map(|ti| ti.cardinality)
        .max()
        .expect("No timeseries found");
    let cardinality_width = max_cardinality.to_string().len();

    for info in &timeseries_info {
        // Use a far-future timestamp to benchmark field lookup only, with no
        // measurements.
        let query =
            format!("get {} | filter timestamp > @2200-01-01", info.name);

        let bench_id = format!(
            "{} tables/{:0width$} keys: {}",
            info.field_tables,
            info.cardinality,
            info.name,
            width = cardinality_width
        );

        bench_oxql_query(
            &mut group,
            &rt,
            client.clone(),
            "field_lookup",
            bench_id,
            query,
            &metric,
        );
    }

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(50).noise_threshold(0.05);
    targets = oxql_field_lookup
);

criterion_main!(benches);
