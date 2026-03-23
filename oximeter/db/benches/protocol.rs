// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Benchmark comparing the native protocol to JSON-over-HTTP.

// Copyright 2024 Oxide Computer Company

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use omicron_common::address::CLICKHOUSE_TCP_PORT;
use oximeter_db::native::Connection;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

/// List of queries to run.
///
/// These are static so we're not benchmarking the creation of the query string.
const QUERIES: &[&str] = &[
    "SELECT number FROM system.numbers LIMIT 100",
    "SELECT number FROM system.numbers LIMIT 1000",
    "SELECT number FROM system.numbers LIMIT 10000",
    "SELECT number FROM system.numbers LIMIT 100000",
    "SELECT number FROM system.numbers LIMIT 1000000",
];

/// Run the provided query on the connection.
///
/// We need to be passed the runtime here to keep it alive, and avoid errors
/// complaining that the runtime is shutting down.
async fn native_impl(
    _rt: &Runtime,
    conn: &Arc<Mutex<Connection>>,
    query: &str,
) {
    conn.lock()
        .await
        .query(uuid::Uuid::new_v4(), query)
        .await
        .expect("Expected to run query");
}

/// Setup the native query benchmark.
fn native(c: &mut Criterion) {
    let mut group = c.benchmark_group("native");

    // Create a client.
    //
    // It's unfortunate that we need to wrap this in an Arc+Mutex, but it's
    // required since the query method takes `&mut self`. Otherwise we'd need to
    // create the client on each iteration, and thus be timing the handshake
    // too. This way, we're just adding the overhead of the lock, which should
    // be pretty small since it's uncontended.
    let addr = SocketAddr::new(Ipv6Addr::LOCALHOST.into(), CLICKHOUSE_TCP_PORT);
    let rt = Runtime::new().unwrap();
    let conn = Arc::new(Mutex::new(
        rt.block_on(async { Connection::new(addr).await.unwrap() }),
    ));
    for query in QUERIES {
        group.bench_with_input(
            BenchmarkId::from_parameter(*query),
            &(&rt, &conn, query),
            |b, (rt, conn, query)| {
                b.to_async(*rt).iter(|| native_impl(rt, conn, query))
            },
        );
    }
    group.finish();
}

/// Run the provided query using the HTTP interface.
async fn json_over_http_impl(client: &reqwest::Client, sql: &str) {
    let _: Vec<_> = client
        .post("http://[::1]:8123")
        .query(&[
            ("output_format_json_quote_64bit_integers", "0"),
            ("wait_end_of_query", "1"),
        ])
        .body(format!("{sql} FORMAT JSONEachRow"))
        .send()
        .await
        .expect("Expected to send query")
        .text()
        .await
        .expect("Expected query to return text")
        .lines()
        .map(|line| {
            serde_json::from_str::<serde_json::Value>(line)
                .expect("Expected JSON lines")
        })
        .collect();
}

/// Setup the JSON-over-HTTP benchmark.
fn json_over_http(c: &mut Criterion) {
    let mut group = c.benchmark_group("json-over-http");
    let client = reqwest::Client::new();
    for query in QUERIES {
        group.bench_with_input(
            BenchmarkId::from_parameter(*query),
            &(&client, query),
            |b, &(client, query)| {
                b.to_async(Runtime::new().unwrap())
                    .iter(|| json_over_http_impl(client, query))
            },
        );
    }
    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(100).noise_threshold(0.05);
    targets = json_over_http, native
);
criterion_main!(benches);
