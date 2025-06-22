// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Benchmarks creating sled reservations

use criterion::black_box;
use criterion::{Criterion, SamplingMode, criterion_group, criterion_main};
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use omicron_common::api::external;
use omicron_test_utils::dev;
use omicron_uuid_kinds::InstanceUuid;
use once_cell::sync::Lazy;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::Barrier;

mod harness;

use harness::TestHarness;
use harness::db_utils::create_reservation;
use harness::db_utils::delete_reservation;
use harness::db_utils::max_resource_request_count;
use nexus_db_queries::db::pub_test_utils::helpers::create_affinity_group;
use nexus_db_queries::db::pub_test_utils::helpers::create_affinity_group_member;
use nexus_db_queries::db::pub_test_utils::helpers::create_anti_affinity_group;
use nexus_db_queries::db::pub_test_utils::helpers::create_anti_affinity_group_member;
use nexus_db_queries::db::pub_test_utils::helpers::create_stopped_instance_record;
use nexus_db_queries::db::pub_test_utils::helpers::delete_affinity_group;
use nexus_db_queries::db::pub_test_utils::helpers::delete_anti_affinity_group;
use nexus_db_queries::db::pub_test_utils::helpers::delete_instance_record;

/////////////////////////////////////////////////////////////////
//
// PARAMETERS
//
// Describes varations between otherwise shared test logic

#[derive(Clone, Hash, Eq, PartialEq)]
enum GroupType {
    Affinity,
    AntiAffinity,
}

#[derive(Clone, Hash, Eq, PartialEq)]
struct GroupInfo {
    // Name of affinity/anti-affinity group
    name: &'static str,
    // Policy of the group
    policy: external::AffinityPolicy,
    // Type of group
    flavor: GroupType,
}

#[derive(Clone)]
struct InstanceGroups {
    // An instance should belong to all these groups.
    belongs_to: Vec<GroupInfo>,
}

#[derive(Clone)]
struct InstanceGroupPattern {
    description: &'static str,
    // These "instance group settings" should be applied.
    //
    // This is called "stripe" because we rotate through these groups.
    //
    // For example, with a stripe.len() == 2...
    //   - Instance 0 belongs to the groups in stripe[0]
    //   - Instance 1 belongs to the groups in stripe[1]
    //   - Instance 2 belongs to the groups in stripe[0]
    //   - etc
    stripe: Vec<InstanceGroups>,
}

#[derive(Clone)]
struct TestParams {
    // Number of vmms to provision from the task-under-test
    vmms: usize,
    // Number of tasks to concurrent provision vmms
    tasks: usize,
    // The pattern of allocations
    group_pattern: InstanceGroupPattern,
}

const VMM_PARAMS: [usize; 3] = [1, 8, 16];
const TASK_PARAMS: [usize; 3] = [1, 4, 8];
const SLED_PARAMS: [usize; 3] = [1, 4, 8];

/////////////////////////////////////////////////////////////////
//
// BENCHMARKS
//
// You can run these with the following command:
//
// ```bash
// cargo nextest bench -p nexus-db-queries
// ```
//
// You can also set the "SHOW_CONTENTION" environment variable to display
// additional data from CockroachDB tables about contention statistics.

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

// Test setup: Create all instances and group memberships that will be
// used by a particular task under test.
//
// Precondition: We expect that the test groups (shared by all tasks)
// will already exist.
async fn create_instances_with_groups(
    params: &TestParams,
    opctx: &OpContext,
    db: &DataStore,
    authz_project: &authz::Project,
    task_num: usize,
) -> Vec<InstanceUuid> {
    let mut instance_ids = Vec::with_capacity(params.vmms);
    for i in 0..params.vmms {
        let instance_id = create_stopped_instance_record(
            opctx,
            db,
            authz_project,
            &format!("task-{task_num}-instance-{i}"),
        )
        .await;

        instance_ids.push(instance_id);

        let patterns = params.group_pattern.stripe.len();
        if patterns > 0 {
            let groups = &params.group_pattern.stripe[i % patterns].belongs_to;
            for group in groups {
                match group.flavor {
                    GroupType::Affinity => {
                        create_affinity_group_member(
                            opctx,
                            db,
                            "project",
                            group.name,
                            instance_id,
                        )
                        .await
                        .expect("Failed to add instance to affinity group");
                    }
                    GroupType::AntiAffinity => {
                        create_anti_affinity_group_member(
                            opctx,
                            db,
                            "project",
                            group.name,
                            instance_id,
                        )
                        .await
                        .expect(
                            "Failed to add instance to anti-affinity group",
                        );
                    }
                }
            }
        }
    }
    instance_ids
}

// Destroys all instances.
//
// This implicitly removes group memberships.
async fn destroy_instances(
    opctx: &OpContext,
    db: &DataStore,
    instances: Vec<InstanceUuid>,
) {
    for instance_id in instances {
        delete_instance_record(opctx, db, instance_id).await;
    }
}

// Reserves "params.vmms" vmms, and later deletes their reservations.
//
// Returns the average time to provision a single vmm.
async fn reserve_vmms_and_return_average_duration(
    params: &TestParams,
    opctx: &OpContext,
    db: &DataStore,
    instance_ids: &Vec<InstanceUuid>,
    barrier: &Barrier,
) -> Duration {
    assert_eq!(
        instance_ids.len(),
        params.vmms,
        "Not enough instances to provision"
    );
    let mut vmm_ids = Vec::with_capacity(params.vmms);

    // Wait for all tasks to start at roughly the same time
    barrier.wait().await;
    let duration = black_box({
        let start = Instant::now();

        // Create all the requested vmms.
        //
        // Note that all prior reservations will remain in the DB as we continue
        // provisioning the "next" vmm.
        for _ in 0..params.vmms {
            let instance_id = InstanceUuid::new_v4();
            vmm_ids.push(
                create_reservation(opctx, db, instance_id)
                    .await
                    .expect("Failed to provision vmm"),
            );
        }
        // Return the "average time to provision a single vmm".
        //
        // This normalizes the results, regardless of how many vmms we are provisioning.
        //
        // Note that we expect additional contention to create more work, but it's difficult to
        // normalize "how much work is being created by contention".
        average_duration(start.elapsed(), params.vmms)
    });

    // Clean up all our vmms.
    //
    // We don't really care how long this takes, so we omit it from the tracking time.
    //
    // Use a barrier to ensure this does not interfere with the work of
    // concurrent reservations.
    barrier.wait().await;
    for vmm_id in vmm_ids.drain(..) {
        delete_reservation(opctx, db, vmm_id)
            .await
            .expect("Failed to delete vmm");
    }
    duration
}

async fn create_test_groups(
    opctx: &OpContext,
    db: &DataStore,
    authz_project: &authz::Project,
    params: &TestParams,
) {
    let all_groups: HashSet<_> = params
        .group_pattern
        .stripe
        .iter()
        .flat_map(|groups| groups.belongs_to.iter())
        .collect();
    for group in all_groups {
        match group.flavor {
            GroupType::Affinity => {
                create_affinity_group(
                    opctx,
                    db,
                    authz_project,
                    group.name,
                    group.policy,
                )
                .await;
            }
            GroupType::AntiAffinity => {
                create_anti_affinity_group(
                    opctx,
                    db,
                    authz_project,
                    group.name,
                    group.policy,
                )
                .await;
            }
        }
    }
}

async fn delete_test_groups(
    opctx: &OpContext,
    db: &DataStore,
    params: &TestParams,
) {
    let all_groups: HashSet<_> = params
        .group_pattern
        .stripe
        .iter()
        .flat_map(|groups| groups.belongs_to.iter())
        .collect();
    for group in all_groups {
        match group.flavor {
            GroupType::Affinity => {
                delete_affinity_group(opctx, db, "project", group.name).await;
            }
            GroupType::AntiAffinity => {
                delete_anti_affinity_group(opctx, db, "project", group.name)
                    .await;
            }
        }
    }
}

async fn bench_reservation(
    opctx: Arc<OpContext>,
    db: Arc<DataStore>,
    authz_project: authz::Project,
    params: TestParams,
    iterations: u64,
) -> Duration {
    let mut total_duration = Duration::ZERO;

    // Create all groups and instances belonging to those groups before we
    // actually do any iterations. This is a slight optimization to make the
    // benchmarks - which are focused on VMM reservation time - a little faster.
    create_test_groups(&opctx, &db, &authz_project, &params).await;
    let mut task_instances = vec![];
    for task_num in 0..params.tasks {
        task_instances.push(
            create_instances_with_groups(
                &params,
                &opctx,
                &db,
                &authz_project,
                task_num,
            )
            .await,
        );
    }

    // Each iteration is an "attempt" at the test.
    for _ in 0..iterations {
        // Within each attempt, we spawn the tasks requested.
        let mut set = tokio::task::JoinSet::new();

        // This barrier exists to lessen the impact of "task spawning" on the benchmark.
        //
        // We want to have all tasks run as concurrently as possible, since we're trying to
        // measure contention explicitly.
        let barrier = Arc::new(Barrier::new(params.tasks));

        for task_num in 0..params.tasks {
            set.spawn({
                let opctx = opctx.clone();
                let db = db.clone();
                let barrier = barrier.clone();
                let params = params.clone();
                let instances = task_instances[task_num].clone();

                async move {
                    reserve_vmms_and_return_average_duration(
                        &params, &opctx, &db, &instances, &barrier,
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
        // the "100 vmm" case take longer than the "1 vmm" case.
        //
        // The same is roughly true of tasks: as we add more tasks, unless the
        // work is blocked on access to a resource (e.g., CPU, Disk, a single
        // database table, etc), provisioning time should be constant. However,
        // once the size of the incoming task queue grows larger than the work
        // we can perform concurrently, we expect the provisioning time to
        // increase linearly, and hopefully proportional to the cost of
        // completing the VMM reservation with a single task.
        //
        // This is made explicit because it is not always true - incurring
        // fallible retry logic, for example, can cause the cost of VMM
        // reservation to increase SUPER-linearly with respect to the number of
        // tasks, if additional tasks actively interfere with each other.
        total_duration += average_duration(all_tasks_duration, params.tasks);
    }
    // Destroy all our instance records (and their affinity group memberships)
    for instance_ids in task_instances {
        destroy_instances(&opctx, &db, instance_ids).await;
    }
    delete_test_groups(&opctx, &db, &params).await;
    total_duration
}

fn sled_reservation_benchmark(c: &mut Criterion) {
    let logctx = dev::test_setup_log("sled-reservation");

    let rt = tokio::runtime::Runtime::new().unwrap();

    let group_patterns = [
        // No Affinity Groups
        InstanceGroupPattern { description: "no-groups", stripe: vec![] },
        // Alternating "Affinity Group" and "Anti-Affinity Group", both permissive
        InstanceGroupPattern {
            description: "affinity-groups-permissive",
            stripe: vec![
                InstanceGroups {
                    belongs_to: vec![GroupInfo {
                        name: "affinity-1",
                        policy: external::AffinityPolicy::Allow,
                        flavor: GroupType::Affinity,
                    }],
                },
                InstanceGroups {
                    belongs_to: vec![GroupInfo {
                        name: "anti-affinity-1",
                        policy: external::AffinityPolicy::Allow,
                        flavor: GroupType::AntiAffinity,
                    }],
                },
            ],
        },
        // TODO(https://github.com/oxidecomputer/omicron/issues/7628):
        // create a test for "policy = Fail" groups.
    ];

    for grouping in &group_patterns {
        let mut group = c.benchmark_group(format!(
            "vmm-reservation-{}",
            grouping.description
        ));

        // Flat sampling is recommended for benchmarks which run "longer than
        // milliseconds", which is the case for these benches.
        //
        // See: https://bheisler.github.io/criterion.rs/book/user_guide/advanced_configuration.html
        group.sampling_mode(SamplingMode::Flat);

        for sleds in SLED_PARAMS {
            for tasks in TASK_PARAMS {
                for vmms in VMM_PARAMS {
                    let params = TestParams {
                        vmms,
                        tasks,
                        group_pattern: grouping.clone(),
                    };
                    let name =
                        format!("{sleds}-sleds-{tasks}-tasks-{vmms}-vmms");

                    // NOTE: This can also fail depending on the group requirements,
                    // if we're using policy = fail. Maybe construct "TestParams"
                    // differently to let each test decide this.
                    if tasks * vmms > max_resource_request_count(sleds) {
                        eprintln!(
                            "{name} would request too many VMMs; skipping..."
                        );
                        continue;
                    }

                    // Initialize the harness before calling "bench_function" so
                    // that the "warm-up" calls to "bench_function" are actually useful
                    // at warming up the database.
                    //
                    // This mitigates any database-caching issues like "loading schema
                    // on boot", or "connection pooling", as the pool stays the same
                    // between calls to the benchmark function.
                    //
                    // We use a Lazy to only initialize the harness if the
                    // benchmark's in run mode, not in list mode. (We can't use
                    // std::sync::LazyLock as of Rust 1.87, because we need
                    // into_value which isn't available yet.)
                    let log = logctx.log.clone();
                    let harness = Lazy::new(|| {
                        rt.block_on(async move {
                            TestHarness::new(&log, sleds).await
                        })
                    });

                    // Actually invoke the benchmark.
                    group.bench_function(&name, |b| {
                        // Force evaluation of the harness outside to_async to
                        // avoid nested block_on.
                        let harness = &*harness;
                        b.to_async(&rt).iter_custom(|iters| {
                            let opctx = harness.opctx();
                            let db = harness.db();
                            let authz_project = harness.authz_project();
                            let params = params.clone();
                            async move {
                                bench_reservation(
                                    opctx,
                                    db,
                                    authz_project,
                                    params,
                                    iters,
                                )
                                .await
                            }
                        })
                    });
                    // Clean-up the harness; we'll use a new database between
                    // varations in parameters.
                    rt.block_on(async move {
                        if let Ok(harness) = Lazy::into_value(harness) {
                            if std::env::var("SHOW_CONTENTION").is_ok() {
                                harness.print_contention().await;
                            }
                            harness.terminate().await;
                        }
                    });
                }
            }
        }
        group.finish();
    }
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
        // Allow for 10% variance in performance without identifying performance
        // improvements/regressions
        .noise_threshold(0.10);
    targets = sled_reservation_benchmark
);
criterion_main!(benches);
