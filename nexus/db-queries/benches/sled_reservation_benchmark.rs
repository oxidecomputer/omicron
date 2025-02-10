// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Benchmarks creating sled reservations

use anyhow::Context;
use anyhow::Result;
use criterion::black_box;
use criterion::{criterion_group, criterion_main, Criterion};
use nexus_db_model::ByteCount;
use nexus_db_model::Generation;
use nexus_db_model::Project;
use nexus_db_model::Resources;
use nexus_db_model::Sled;
use nexus_db_model::SledBaseboard;
use nexus_db_model::SledReservationConstraintBuilder;
use nexus_db_model::SledSystemHardware;
use nexus_db_model::SledUpdate;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::pub_test_utils::TestDatabase;
use nexus_db_queries::db::DataStore;
use nexus_test_utils::sql::process_rows;
use nexus_test_utils::sql::Row;
use nexus_types::external_api::params;
use omicron_common::api::external;
use omicron_test_utils::dev;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::PropolisUuid;
use slog::Logger;
use std::collections::HashMap;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::Barrier;
use uuid::Uuid;

/////////////////////////////////////////////////////////////////
//
// TEST HELPERS
//
// These are largely ripped out of "nexus/db-queries/src/db/datastore".
//
// Benchmarks are compiled as external binaries from library crates, so we
// can only access `pub` code.
//
// It may be worth refactoring some of these functions to a test utility
// crate to avoid the de-duplication.

async fn create_project(
    opctx: &OpContext,
    datastore: &DataStore,
) -> (authz::Project, Project) {
    let authz_silo = opctx.authn.silo_required().unwrap();

    // Create a project
    let project = Project::new(
        authz_silo.id(),
        params::ProjectCreate {
            identity: external::IdentityMetadataCreateParams {
                name: "project".parse().unwrap(),
                description: "desc".to_string(),
            },
        },
    );
    datastore.project_create(&opctx, project).await.unwrap()
}

fn rack_id() -> Uuid {
    Uuid::parse_str(nexus_test_utils::RACK_UUID).unwrap()
}

// Creates a "fake" Sled Baseboard.
fn sled_baseboard_for_test() -> SledBaseboard {
    SledBaseboard {
        serial_number: Uuid::new_v4().to_string(),
        part_number: String::from("test-part"),
        revision: 1,
    }
}

// Creates "fake" sled hardware accounting
fn sled_system_hardware_for_test() -> SledSystemHardware {
    SledSystemHardware {
        is_scrimlet: false,
        usable_hardware_threads: 32,
        usable_physical_ram: ByteCount::try_from(1 << 40).unwrap(),
        reservoir_size: ByteCount::try_from(1 << 39).unwrap(),
    }
}

fn test_new_sled_update() -> SledUpdate {
    let sled_id = Uuid::new_v4();
    let addr = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0);
    let repo_depot_port = 0;
    SledUpdate::new(
        sled_id,
        addr,
        repo_depot_port,
        sled_baseboard_for_test(),
        sled_system_hardware_for_test(),
        rack_id(),
        Generation::new(),
    )
}

async fn create_sleds(datastore: &DataStore, count: usize) -> Vec<Sled> {
    let mut sleds = vec![];
    for _ in 0..count {
        let (sled, _) =
            datastore.sled_upsert(test_new_sled_update()).await.unwrap();
        sleds.push(sled);
    }
    sleds
}

fn small_resource_request() -> Resources {
    Resources::new(
        1,
        // Just require the bare non-zero amount of RAM.
        ByteCount::try_from(1024).unwrap(),
        ByteCount::try_from(1024).unwrap(),
    )
}

async fn create_reservation(
    opctx: &OpContext,
    db: &DataStore,
) -> Result<PropolisUuid> {
    let instance_id = InstanceUuid::new_v4();
    let vmm_id = PropolisUuid::new_v4();
    db.sled_reservation_create(
        &opctx,
        instance_id,
        vmm_id,
        small_resource_request(),
        SledReservationConstraintBuilder::new().build(),
    )
    .await
    .context("Failed to create reservation")?;
    Ok(vmm_id)
}

async fn delete_reservation(
    opctx: &OpContext,
    db: &DataStore,
    vmm_id: PropolisUuid,
) -> Result<()> {
    db.sled_reservation_delete(&opctx, vmm_id)
        .await
        .context("Failed to delete reservation")?;
    Ok(())
}

/////////////////////////////////////////////////////////////////
//
// TEST HARNESS
//
// This structure shares logic between benchmarks, making it easy
// to perform shared tasks such as creating contention for reservations.

struct TestHarness {
    db: TestDatabase,
}

impl TestHarness {
    async fn new(log: &Logger, sled_count: usize) -> Self {
        let db = TestDatabase::new_with_datastore(log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let (_authz_project, _project) =
            create_project(&opctx, &datastore).await;
        create_sleds(&datastore, sled_count).await;

        Self { db }
    }

    // Emit internal CockroachDb information about contention
    async fn print_contention(&self) {
        let client = self.db.crdb().connect().await.expect("Failed to connect to db");

        let queries = [
            "SELECT table_name, index_name, num_contention_events::TEXT FROM crdb_internal.cluster_contended_indexes",
            "SELECT table_name,num_contention_events::TEXT FROM crdb_internal.cluster_contended_tables",
            "WITH c AS (SELECT DISTINCT ON (table_id, index_id) table_id, index_id, num_contention_events AS events, cumulative_contention_time AS time FROM crdb_internal.cluster_contention_events) SELECT i.descriptor_name, i.index_name, c.events::TEXT, c.time::TEXT FROM crdb_internal.table_indexes AS i JOIN c ON i.descriptor_id = c.table_id AND i.index_id = c.index_id ORDER BY c.time DESC LIMIT 10;"
        ];

        // Used for padding: get a map of "column name" -> "max value length".
        let max_lengths_by_column = |rows: &Vec<Row>| {
            let mut lengths = HashMap::new();
            for row in rows {
                for column in &row.values {
                    let value_len = column.value().unwrap().as_str().len();
                    let name_len = column.name().len();
                    let len = std::cmp::max(value_len, name_len);

                    lengths.entry(column.name().to_string())
                        .and_modify(|entry| {
                            if len > *entry {
                                *entry = len;
                            }
                        })
                        .or_insert(len);
                }
            }
            lengths
        };

        for sql in queries {
            let rows = client.query(sql, &[])
                .await.expect("Failed to query contended tables");
            let rows = process_rows(&rows);
            if rows.is_empty() {
                continue;
            }

            println!("{sql}");
            let max_lengths = max_lengths_by_column(&rows);
            let mut header = true;

            for row in rows {
                if header {
                    let mut total_len = 0;
                    for column in &row.values {
                        let width = max_lengths.get(column.name()).unwrap();
                        print!(" {:width$} ", column.name());
                        print!("|");
                        total_len += width + 3;
                    }
                    println!("");
                    println!("{:-<total_len$}", "");
                }
                header = false;

                for column in row.values {
                    let width = max_lengths.get(column.name()).unwrap();
                    let value = column.value().unwrap().as_str();
                    print!(" {value:width$} ");
                    print!("|");
                }
                println!("");
            }
            println!("");
        }

        client.cleanup().await.expect("Failed to clean up db connection");
    }

    fn opctx(&self) -> Arc<OpContext> {
        Arc::new(
            self.db
                .opctx()
                .child(std::collections::BTreeMap::new())
        )
    }

    fn db(&self) -> Arc<DataStore> {
        self.db.datastore().clone()
    }

    async fn terminate(self) {
        self.db.terminate().await;
    }
}

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

// const VMM_PARAMS: [usize; 3] = [1, 8, 16];
// const TASK_PARAMS: [usize; 3] = [1, 2, 3];
const VMM_PARAMS: [usize; 1] = [4];
const TASK_PARAMS: [usize; 1] = [4];

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
            .expect("This benchmark is taking hundreds of years to run, maybe optimize it")
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

// Typically we run our database tests using "cargo nextest run",
// which triggers the "crdb-seed" binary to create an initialized
// database when we boot up.
//
// If we're using "cargo bench", we don't get that guarantee.
// Go through the database ensuring process manually.
async fn setup_db(log: &Logger) {
    print!("setting up seed cockroachdb database... ");
    let (seed_tar, status) = dev::seed::ensure_seed_tarball_exists(
        log,
        dev::seed::should_invalidate_seed(),
    )
    .await
    .expect("Failed to create seed tarball for CRDB");
    status.log(log, &seed_tar);
    unsafe {
        std::env::set_var(dev::CRDB_SEED_TAR_ENV, seed_tar);
    }
    println!("OK");
}

fn sled_reservation_benchmark(c: &mut Criterion) {
    let logctx = dev::test_setup_log("sled-reservation");

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(setup_db(&logctx.log));

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
