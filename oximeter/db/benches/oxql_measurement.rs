// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Benchmark for OxQL measurement query performance.
//!
//! Tests queries that retrieve measurement data within specified time windows.

mod common;

use common::{bench_metric, bench_oxql_query, get_client};
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};

// Benchmark measurement queries. Unlike the field benchmark, which elides
// measurement query performance by filtering on a far-future time, this
// benchmark uses a user-provided time range that contains measurements. We
// query a range of timeseries of different types, and pass them through
// `last 1` to simulate the common workload of fetching recent metrics for
// export to Prometheus or similar.
fn oxql_measurement_query(c: &mut Criterion) {
    let metric = bench_metric();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let client = get_client(&rt);
    let mut group = c.benchmark_group("oxql_measurement");

    // Get the measurement time range from environment variables
    let start_time = std::env::var("OXQL_BENCH_START_TIME")
        .expect("OXQL_BENCH_START_TIME must be set as YYYY-MM-DDTHH:MM:SS");
    let end_time = std::env::var("OXQL_BENCH_END_TIME")
        .expect("OXQL_BENCH_END_TIME must be set as YYYY-MM-DDTHH:MM:SS");

    // Choose timeseries of different types and observed cardinality. As of
    // this writing, virtual_machine:vcpu_usage and hardware_component:temperature
    // are relatively high-cardinality, and zone:cpu_nsec and
    // hardware_component:fan_speed are lower cardinality.
    const MEASUREMENT_TIMESERIES: &[(&str, &str)] = &[
        // CumulativeU64
        ("virtual_machine:vcpu_usage", "cumulativeu64"),
        ("zone:cpu_nsec", "cumulativeu64"),
        // F32
        ("hardware_component:temperature", "f32"),
        ("hardware_component:fan_speed", "f32"),
    ];

    let start_dt =
        chrono::NaiveDateTime::parse_from_str(&start_time, "%Y-%m-%dT%H:%M:%S")
            .expect("Invalid start time format");
    let end_dt =
        chrono::NaiveDateTime::parse_from_str(&end_time, "%Y-%m-%dT%H:%M:%S")
            .expect("Invalid end time format");
    let duration = end_dt - start_dt;
    let duration_str = if duration.num_minutes() < 60 {
        format!("{}m", duration.num_minutes())
    } else {
        format!("{}h", duration.num_hours())
    };

    for (timeseries_name, measurement_type) in MEASUREMENT_TIMESERIES {
        let query = format!(
            "get {} | filter timestamp >= @{} && timestamp < @{} | last 1",
            timeseries_name, start_time, end_time
        );

        let bench_id = format!(
            "{}_last_1/{}: {}",
            duration_str, measurement_type, timeseries_name
        );

        bench_oxql_query(
            &mut group,
            &rt,
            client.clone(),
            "query",
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
    targets = oxql_measurement_query
);

criterion_main!(benches);
