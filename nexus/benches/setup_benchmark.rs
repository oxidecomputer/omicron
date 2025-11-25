// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Benchmarks test setup/teardown.

use criterion::{Criterion, criterion_group, criterion_main};
use dropshot::test_util::LogContext;
use nexus_db_queries::db::pub_test_utils::TestDatabase;
use omicron_test_utils::dev;

// This is the default wrapper around most Nexus integration tests.
// Benchmark how long an "empty test" would take.
async fn do_full_setup() {
    let ctx = nexus_test_utils::ControlPlaneBuilder::new("full_setup")
        .start::<omicron_nexus::Server>()
        .await;

    ctx.teardown().await;
}

// Wraps exclusively the CockroachDB portion of setup/teardown.
async fn do_crdb_setup() {
    let cfg = nexus_test_utils::load_test_config();
    let logctx = LogContext::new("crdb_setup", &cfg.pkg.log);
    let db = TestDatabase::new_with_datastore(&logctx.log).await;
    db.terminate().await;
}

// Wraps exclusively the ClickhouseDB portion of setup/teardown.
async fn do_clickhouse_setup() {
    let cfg = nexus_test_utils::load_test_config();
    let logctx = LogContext::new("clickhouse_setup", &cfg.pkg.log);
    let mut clickhouse =
        dev::clickhouse::ClickHouseDeployment::new_single_node(&logctx)
            .await
            .unwrap();
    clickhouse.cleanup().await.unwrap();
}

fn setup_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("Test Setup");
    group.bench_function("do_full_setup", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| do_full_setup());
    });
    group.bench_function("do_crdb_setup", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| do_crdb_setup());
    });
    group.bench_function("do_clickhouse_setup", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| do_clickhouse_setup());
    });
    group.finish();
}

criterion_group!(
    name = benches;
    // To accomodate the fact that these benchmarks are a bit bulky,
    // we set the following:
    // - Smaller sample size, to keep running time down
    // - Higher noise threshold, to avoid avoid false positive change detection
    config = Criterion::default().sample_size(10).noise_threshold(0.10);
    targets = setup_benchmark
);
criterion_main!(benches);
