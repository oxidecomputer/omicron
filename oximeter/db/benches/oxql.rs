// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Benchmark for OxQL query performance.
//!
//! Tests multiple timeseries with varying numbers of field types.

// Copyright 2024 Oxide Computer Company

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use oximeter_db::oxql::query::QueryAuthzScope;
use oximeter_db::Client;
use rand::seq::SliceRandom;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::sync::Arc;

const DEFAULT_CLICKHOUSE_PORT: u16 = 9000;

/// Timeseries for field lookup benchmarks, with their field table counts.
const FIELD_TIMESERIES: &[(&str, u8)] = &[
    ("crucible_upstairs:flush", 1),
    ("ddm_session:advertisements_received", 2),
    ("virtual_machine:vcpu_usage", 3),
    ("bgp_session:active_connections_accepted", 4),
    ("switch_data_link:bytes_sent", 6),
];

fn get_clickhouse_addr() -> IpAddr {
    std::env::var("OXQL_BENCH_ADDR")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| IpAddr::from([127, 0, 0, 1]))
}

fn get_clickhouse_port() -> u16 {
    std::env::var("OXQL_BENCH_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_CLICKHOUSE_PORT)
}

fn get_client(rt: &tokio::runtime::Runtime) -> Arc<Client> {
    let ip = get_clickhouse_addr();
    let port = get_clickhouse_port();
    let addr = SocketAddr::new(ip, port);
    let log = slog::Logger::root(slog::Discard, slog::o!());

    rt.block_on(async {
        let client = Arc::new(Client::new(addr, &log));
        client.ping().await.unwrap();
        client
    })
}

// Benchmark field lookup. As of this writing, filtering and collating fields
// can make up a significant proportion of overall query time, and its latency
// varies with both the cardinality and the number of field tables that need to
// be combined for the relevant series. Query each series in FIELD_TIMESERIES,
// filtering to a future timestamp so that we only benchmark the performance of
// field lookup, and ignore measurements. Note that the user is responsible for
// populating ClickHouse with test data.
fn oxql_field_lookup(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let client = get_client(&rt);
    let mut group = c.benchmark_group("oxql");

    let mut timeseries: Vec<_> = FIELD_TIMESERIES.iter().collect();
    timeseries.shuffle(&mut rand::rng());

    for (timeseries, field_tables) in timeseries {
        // Use a far-future timestamp to benchmark field lookup only, with no
        // measurements.
        let query =
            format!("get {} | filter timestamp > @2525-01-01", timeseries);

        rt.block_on(client.oxql_query(&query, QueryAuthzScope::Fleet))
            .unwrap();

        let bench_id = format!("{}/{}_tables", field_tables, timeseries);

        group.bench_function(BenchmarkId::new("field_lookup", &bench_id), |b| {
            let client = client.clone();
            let query = query.clone();
            b.to_async(&rt).iter(|| {
                let client = client.clone();
                let query = query.clone();
                async move {
                    client.oxql_query(&query, QueryAuthzScope::Fleet).await
                }
            })
        });
    }

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(25).noise_threshold(0.05);
    targets = oxql_field_lookup
);

criterion_main!(benches);
