// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Shared helpers for OxQL benchmarks.

use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, BenchmarkId};
use oximeter_db::Client;
use oximeter_db::oxql::query::QueryAuthzScope;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

pub const DEFAULT_CLICKHOUSE_PORT: u16 = 9000;

/// The metric to benchmark.
///
/// Set via BENCH_METRIC env var.
pub enum BenchMetric {
    /// Server-side query latency.
    Latency,
    /// Total cpu time (user and system).
    CpuTime,
}

pub fn bench_metric() -> BenchMetric {
    match std::env::var("BENCH_METRIC").as_deref() {
        Ok("cpu_time") => BenchMetric::CpuTime,
        Ok("latency") => BenchMetric::Latency,
        _ => panic!("BENCH_METRIC must be 'cpu_time' or 'latency'"),
    }
}

pub fn get_clickhouse_addr() -> IpAddr {
    std::env::var("CLICKHOUSE_ADDRESS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| IpAddr::from([127, 0, 0, 1]))
}

pub fn get_clickhouse_port() -> u16 {
    std::env::var("CLICKHOUSE_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_CLICKHOUSE_PORT)
}

pub fn get_socket_addr() -> SocketAddr {
    SocketAddr::new(get_clickhouse_addr(), get_clickhouse_port())
}

pub fn get_client(rt: &tokio::runtime::Runtime) -> Arc<Client> {
    let addr = get_socket_addr();
    let log = slog::Logger::root(slog::Discard, slog::o!());

    rt.block_on(async {
        let client = Arc::new(Client::new(addr, &log));
        client.ping().await.unwrap();
        client
    })
}

/// Benchmark a single OxQL query using criterion, measuring either server-side
/// latency or cpu time per [`BenchMetric`].
pub fn bench_oxql_query(
    group: &mut BenchmarkGroup<'_, WallTime>,
    rt: &tokio::runtime::Runtime,
    client: Arc<Client>,
    bench_name: &str,
    bench_id: String,
    query: String,
    metric: &BenchMetric,
) {
    // Run the query once without recording performance to warm caches.
    rt.block_on(client.oxql_query(&query, QueryAuthzScope::Fleet)).unwrap();

    group.bench_function(BenchmarkId::new(bench_name, &bench_id), |bench| {
        match metric {
            BenchMetric::CpuTime => {
                bench.to_async(rt).iter_custom(|iters| {
                    let client = client.clone();
                    let query = query.clone();
                    async move {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let result = client
                                .oxql_query(&query, QueryAuthzScope::Fleet)
                                .await
                                .unwrap();
                            let cpu_us: i64 = result
                                .query_summaries
                                .iter()
                                .map(|s| {
                                    // Profile events are occasionally and
                                    // inexplicably empty; default to 0
                                    // for rare missing events.
                                    s.profile_summary
                                        .get("UserTimeMicroseconds")
                                        .copied()
                                        .unwrap_or(0)
                                        + s.profile_summary
                                            .get("SystemTimeMicroseconds")
                                            .copied()
                                            .unwrap_or(0)
                                })
                                .sum();
                            total +=
                                Duration::from_micros(cpu_us.max(0) as u64);
                        }
                        total
                    }
                });
            }
            BenchMetric::Latency => {
                bench.to_async(rt).iter_custom(|iters| {
                    let client = client.clone();
                    let query = query.clone();
                    async move {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let result = client
                                .oxql_query(&query, QueryAuthzScope::Fleet)
                                .await
                                .unwrap();
                            let real_us: i64 = result
                                .query_summaries
                                .iter()
                                .map(|s| {
                                    s.profile_summary
                                        .get("RealTimeMicroseconds")
                                        .copied()
                                        .unwrap_or(0)
                                })
                                .sum();
                            total +=
                                Duration::from_micros(real_us.max(0) as u64);
                        }
                        total
                    }
                });
            }
        }
    });
}
