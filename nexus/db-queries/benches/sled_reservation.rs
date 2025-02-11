// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Benchmarks creating sled reservations

use criterion::black_box;
use criterion::{criterion_group, criterion_main, Criterion};
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use omicron_test_utils::dev;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::Barrier;

mod harness;

use harness::db_utils::create_reservation;
use harness::db_utils::delete_reservation;
use harness::TestHarness;

/////////////////////////////////////////////////////////////////
//
// PARAMETERS
//
// Describes varations between otherwise shared test logic

#[derive(Copy, Clone)]
struct TestParams {
    // Number of vmms to provision from the task-under-test
    vmms: usize,
    tasks: usize,
}

const VMM_PARAMS: [usize; 3] = [1, 8, 16];
const TASK_PARAMS: [usize; 3] = [1, 4, 8];

/////////////////////////////////////////////////////////////////
//
// BENCHMARKS
//
// You can run these with the following command:
//
// ```bash
// cargo bench -p nexus-db-queries
// ```

// Average a duration over a divisor.
//
// For example, if we reserve 100 vmms, you can use "100" as the divisor
// to get the "average duration to provision a vmm".
fn average_duration(duration: Duration, divisor: usize) -> Duration {
    assert_ne!(divisor, 0, "Don't divide by zero please");

    Duration::from_nanos(
        u64::try_from(duration.as_nanos() / divisor as u128)
            .expect("This benchmark is taking hundreds of years to run, maybe optimize it?")
    )
}

// Reserves "params.vmms" vmms, and later deletes their reservations.
//
// Returns the average time to provision a single vmm.
async fn reserve_vmms_and_return_average_duration(
    params: &TestParams,
    opctx: &OpContext,
    db: &DataStore,
) -> Duration {
    let mut vmm_ids = Vec::with_capacity(params.vmms);
    let start = Instant::now();

    // Clippy: We don't want to move this block outside of "black_box", even though it
    // isn't returning anything. That would defeat the whole point of using "black_box",
    // which is to avoid profiling code that is optimized based on the surrounding
    // benchmark function.
    #[allow(clippy::unit_arg)]
    black_box({
        // Create all the requested vmms.
        //
        // Note that all prior reservations will remain in the DB as we continue
        // provisioning the "next" vmm.
        for _ in 0..params.vmms {
            vmm_ids.push(
                create_reservation(opctx, db)
                    .await
                    .expect("Failed to provision vmm"),
            );
        }
    });

    // Return the "average time to provision a single vmm".
    //
    // This normalizes the results, regardless of how many vmms we are provisioning.
    //
    // Note that we expect additional contention to create more work, but it's difficult to
    // normalize "how much work is being created by contention".
    let duration = average_duration(start.elapsed(), params.vmms);

    // Clean up all our vmms.
    //
    // We don't really care how long this takes, so we omit it from the tracking time.
    for vmm_id in vmm_ids.drain(..) {
        delete_reservation(opctx, db, vmm_id)
            .await
            .expect("Failed to delete vmm");
    }

    duration
}

async fn bench_reservation(
    opctx: Arc<OpContext>,
    db: Arc<DataStore>,
    params: TestParams,
    iterations: u64,
) -> Duration {
    let duration = {
        let mut total_duration = Duration::ZERO;

        // Each iteration is an "attempt" at the test.
        for _ in 0..iterations {
            // Within each attempt, we spawn the tasks requested.
            let mut set = tokio::task::JoinSet::new();

            // This barrier exists to lessen the impact of "task spawning" on the benchmark.
            //
            // We want to have all tasks run as concurrently as possible, since we're trying to
            // measure contention explicitly.
            let barrier = Arc::new(Barrier::new(params.tasks));

            for _ in 0..params.tasks {
                set.spawn({
                    let opctx = opctx.clone();
                    let db = db.clone();
                    let barrier = barrier.clone();

                    async move {
                        // Wait until all tasks are ready...
                        barrier.wait().await;

                        // ... and then actually do the benchmark
                        reserve_vmms_and_return_average_duration(
                            &params, &opctx, &db,
                        )
                        .await
                    }
                });
            }

            // The sum of "average time to provision a single vmm" across all tasks.
            let all_tasks_duration = set
                .join_all()
                .await
                .into_iter()
                .fold(Duration::ZERO, |acc, x| acc + x);

            // The normalized "time to provision a single vmm", across both:
            // - The number of vmms reserved by each task, and
            // - The number of tasks
            //
            // As an example, if we provision 10 vmms, and have 5 tasks, and we assume
            // that VM provisioning time is exactly one second (contention has no impact, somehow):
            //
            // - Each task would take 10 seconds (10 vmms * 1 second), but would return an average
            //   duration of "1 second".
            // - Across all tasks, we'd see an "all_tasks_duration" of 5 seconds
            //   (1 second average * 5 tasks).
            // - So, we'd increment our "total_duration" by "1 second per vmm", which has been
            //   normalized cross both the tasks and quantities of vmms.
            //
            // Why bother doing this?
            //
            // When we perform this normalization, we can vary the "total vmms provisioned" as well
            // as "total tasks" significantly, but easily compare test durations with one another.
            //
            // For example: if the total number of vmms has no impact on the next provisioning
            // request, we should see similar durations for "100 vmms reserved" vs "1 vmm
            // reserved". However, if more vmms actually make reservation times slower, we'll see
            // the "100 vmm" case take longer than the "1 vmm" case. The same goes for tasks:
            total_duration +=
                average_duration(all_tasks_duration, params.tasks);
        }
        total_duration
    };

    duration
}

fn sled_reservation_benchmark(c: &mut Criterion) {
    let logctx = dev::test_setup_log("sled-reservation");

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(harness::setup_db(&logctx.log));

    let mut group = c.benchmark_group("vmm-reservation");
    for vmms in VMM_PARAMS {
        for tasks in TASK_PARAMS {
            let params = TestParams { vmms, tasks };
            let name = format!("{vmms}-vmms-{tasks}-tasks");

            // Initialize the harness before calling "bench_function" so
            // that the "warm-up" calls to "bench_function" are actually useful
            // at warming up the database.
            //
            // This mitigates any database-caching issues like "loading schema
            // on boot", or "connection pooling", as the pool stays the same
            // between calls to the benchmark function.
            let log = logctx.log.clone();
            let harness = rt.block_on(async move {
                const SLED_COUNT: usize = 4;
                TestHarness::new(&log, SLED_COUNT).await
            });

            // Actually invoke the benchmark.
            group.bench_function(&name, |b| {
                b.to_async(&rt).iter_custom(|iters| {
                    let opctx = harness.opctx();
                    let db = harness.db();
                    async move { bench_reservation(opctx, db, params, iters).await }
                })
            });

            // Clean-up the harness; we'll use a new database between
            // varations in parameters.
            rt.block_on(async move {
                harness.print_contention().await;
                harness.terminate().await;
            });
        }
    }
    group.finish();
    logctx.cleanup_successful();
}

criterion_group!(
    name = benches;
    // To accomodate the fact that these benchmarks are a bit bulky,
    // we set the following:
    // - Smaller sample size, to keep running time down
    // - Higher noise threshold, to avoid avoid false positive change detection
    config = Criterion::default()
        .sample_size(10)
        .noise_threshold(0.10);
    targets = sled_reservation_benchmark
);
criterion_main!(benches);
