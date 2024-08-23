// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::time::Duration;

use crate::integration_tests::instances::{
    create_project_and_pool, instance_post, instance_simulate, InstanceOp,
};
use chrono::Utc;
use dropshot::test_util::ClientTestContext;
use dropshot::ResultsPage;
use http::{Method, StatusCode};
use nexus_db_queries::db::fixed_data::silo::DEFAULT_SILO_ID;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::{
    create_default_ip_pool, create_disk, create_instance, create_project,
    objects_list_page_authz, DiskTest,
};
use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::views::OxqlQueryResult;
use omicron_test_utils::dev::poll::{wait_for_condition, CondCheckError};
use omicron_uuid_kinds::{GenericUuid, InstanceUuid};
use oximeter::types::Datum;
use oximeter::types::FieldValue;
use oximeter::types::Measurement;
use oximeter::TimeseriesSchema;
use std::borrow::Borrow;
use uuid::Uuid;

pub async fn query_for_metrics(
    client: &ClientTestContext,
    path: &str,
) -> ResultsPage<Measurement> {
    let measurements: ResultsPage<Measurement> =
        objects_list_page_authz(client, path).await;
    assert!(!measurements.items.is_empty());
    measurements
}

pub async fn get_latest_system_metric(
    cptestctx: &ControlPlaneTestContext<omicron_nexus::Server>,
    metric_name: &str,
    silo_id: Option<Uuid>,
) -> i64 {
    let client = &cptestctx.external_client;
    let id_param = match silo_id {
        Some(id) => format!("&silo={}", id),
        None => "".to_string(),
    };
    let url = format!(
        "/v1/system/metrics/{metric_name}?start_time={:?}&end_time={:?}&order=descending&limit=1{}",
        cptestctx.start_time,
        Utc::now(),
        id_param,
    );
    let measurements =
        objects_list_page_authz::<Measurement>(client, &url).await;

    // prevent more confusing error on next line
    assert!(measurements.items.len() == 1, "Expected exactly one measurement");

    let item = &measurements.items[0];
    let datum = match item.datum() {
        Datum::I64(c) => c,
        _ => panic!("Unexpected datum type {:?}", item.datum()),
    };
    return *datum;
}

pub async fn get_latest_silo_metric(
    cptestctx: &ControlPlaneTestContext<omicron_nexus::Server>,
    metric_name: &str,
    project_id: Option<Uuid>,
) -> i64 {
    let client = &cptestctx.external_client;
    let id_param = match project_id {
        Some(id) => format!("&project={}", id),
        None => "".to_string(),
    };
    let url = format!(
        "/v1/metrics/{metric_name}?start_time={:?}&end_time={:?}&order=descending&limit=1{}",
        cptestctx.start_time,
        Utc::now(),
        id_param,
    );
    let measurements =
        objects_list_page_authz::<Measurement>(client, &url).await;

    // prevent more confusing error on next line
    assert_eq!(measurements.items.len(), 1, "Expected exactly one measurement");

    let item = &measurements.items[0];
    let datum = match item.datum() {
        Datum::I64(c) => c,
        _ => panic!("Unexpected datum type {:?}", item.datum()),
    };
    return *datum;
}

async fn assert_system_metrics(
    cptestctx: &ControlPlaneTestContext<omicron_nexus::Server>,
    silo_id: Option<Uuid>,
    disk: i64,
    cpus: i64,
    ram: i64,
) {
    cptestctx.oximeter.force_collect().await;
    assert_eq!(
        get_latest_system_metric(
            cptestctx,
            "virtual_disk_space_provisioned",
            silo_id,
        )
        .await,
        disk
    );
    assert_eq!(
        get_latest_system_metric(cptestctx, "cpus_provisioned", silo_id).await,
        cpus
    );
    assert_eq!(
        get_latest_system_metric(cptestctx, "ram_provisioned", silo_id).await,
        ram
    );
}

async fn assert_silo_metrics(
    cptestctx: &ControlPlaneTestContext<omicron_nexus::Server>,
    project_id: Option<Uuid>,
    disk: i64,
    cpus: i64,
    ram: i64,
) {
    cptestctx.oximeter.force_collect().await;
    assert_eq!(
        get_latest_silo_metric(
            cptestctx,
            "virtual_disk_space_provisioned",
            project_id,
        )
        .await,
        disk
    );
    assert_eq!(
        get_latest_silo_metric(cptestctx, "cpus_provisioned", project_id).await,
        cpus
    );
    assert_eq!(
        get_latest_silo_metric(cptestctx, "ram_provisioned", project_id).await,
        ram
    );
}

async fn assert_404(
    cptestctx: &ControlPlaneTestContext<omicron_nexus::Server>,
    url: &str,
) {
    NexusRequest::new(
        RequestBuilder::new(&cptestctx.external_client, Method::GET, url)
            .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected success");
}

const GIB: i64 = 1024 * 1024 * 1024;

// TODO: test segmenting by silo with multiple silos
// TODO: test metrics authz checks more thoroughly

#[nexus_test]
async fn test_metrics(
    cptestctx: &ControlPlaneTestContext<omicron_nexus::Server>,
) {
    let client = &cptestctx.external_client;
    create_default_ip_pool(&client).await; // needed for instance create to work
    DiskTest::new(cptestctx).await; // needed for disk create to work

    // Wait until Nexus registers as a producer with Oximeter.
    wait_for_producer(
        &cptestctx.oximeter,
        cptestctx.server.server_context().nexus.id(),
    )
    .await;

    // silo metrics start out zero
    assert_system_metrics(&cptestctx, None, 0, 0, 0).await;
    assert_system_metrics(&cptestctx, Some(*DEFAULT_SILO_ID), 0, 0, 0).await;
    assert_silo_metrics(&cptestctx, None, 0, 0, 0).await;

    let project1_id = create_project(&client, "p-1").await.identity.id;
    assert_silo_metrics(&cptestctx, Some(project1_id), 0, 0, 0).await;

    let project2_id = create_project(&client, "p-2").await.identity.id;
    assert_silo_metrics(&cptestctx, Some(project2_id), 0, 0, 0).await;

    // 404 if given ID of the wrong kind of thing. Normally this would be pretty
    // obvious, but all the different resources are stored the same way in
    // Clickhouse, so we need to be careful.
    let bad_silo_metrics_url = format!(
        "/v1/metrics/cpus_provisioned?start_time={:?}&end_time={:?}&order=descending&limit=1&project={}",
        cptestctx.start_time,
        Utc::now(),
        *DEFAULT_SILO_ID,
    );
    assert_404(&cptestctx, &bad_silo_metrics_url).await;
    let bad_system_metrics_url = format!(
        "/v1/system/metrics/cpus_provisioned?start_time={:?}&end_time={:?}&order=descending&limit=1&silo={}",
        cptestctx.start_time,
        Utc::now(),
        project1_id,
    );
    assert_404(&cptestctx, &bad_system_metrics_url).await;

    // create instance in project 1
    create_instance(&client, "p-1", "i-1").await;
    assert_silo_metrics(&cptestctx, Some(project1_id), 0, 4, GIB).await;
    assert_silo_metrics(&cptestctx, None, 0, 4, GIB).await;
    assert_system_metrics(&cptestctx, None, 0, 4, GIB).await;
    assert_system_metrics(&cptestctx, Some(*DEFAULT_SILO_ID), 0, 4, GIB).await;

    // create disk in project 1
    create_disk(&client, "p-1", "d-1").await;
    assert_silo_metrics(&cptestctx, Some(project1_id), GIB, 4, GIB).await;
    assert_silo_metrics(&cptestctx, None, GIB, 4, GIB).await;
    assert_system_metrics(&cptestctx, None, GIB, 4, GIB).await;
    assert_system_metrics(&cptestctx, Some(*DEFAULT_SILO_ID), GIB, 4, GIB)
        .await;

    // project 2 metrics still empty
    assert_silo_metrics(&cptestctx, Some(project2_id), 0, 0, 0).await;

    // create instance and disk in project 2
    create_instance(&client, "p-2", "i-2").await;
    create_disk(&client, "p-2", "d-2").await;
    assert_silo_metrics(&cptestctx, Some(project2_id), GIB, 4, GIB).await;

    // both instances show up in silo and fleet metrics
    assert_silo_metrics(&cptestctx, None, 2 * GIB, 8, 2 * GIB).await;
    assert_system_metrics(&cptestctx, None, 2 * GIB, 8, 2 * GIB).await;
    assert_system_metrics(
        &cptestctx,
        Some(*DEFAULT_SILO_ID),
        2 * GIB,
        8,
        2 * GIB,
    )
    .await;

    // project 1 unaffected by project 2's resources
    assert_silo_metrics(&cptestctx, Some(project1_id), GIB, 4, GIB).await;
}

/// Test that we can correctly list some timeseries schema.
#[nexus_test]
async fn test_timeseries_schema_list(
    cptestctx: &ControlPlaneTestContext<omicron_nexus::Server>,
) {
    // Nexus registers itself as a metric producer on startup, with its own UUID
    // as the producer ID. Wait for this to show up in the registered lists of
    // producers.
    let nexus_id = cptestctx.server.server_context().nexus.id();
    wait_for_producer(&cptestctx.oximeter, nexus_id).await;

    // We should be able to fetch the list of timeseries, and it should include
    // Nexus's HTTP latency distribution. This is defined in Nexus itself, and
    // should always exist after we've registered as a producer and start
    // producing data. Force a collection to ensure that happens.
    cptestctx.oximeter.force_collect().await;
    let client = &cptestctx.external_client;
    let url = "/v1/timeseries/schema";
    let schema =
        objects_list_page_authz::<TimeseriesSchema>(client, &url).await;
    schema
        .items
        .iter()
        .find(|sc| {
            sc.timeseries_name == "http_service:request_latency_histogram"
        })
        .expect("Failed to find HTTP request latency histogram schema");
}

pub async fn timeseries_query(
    cptestctx: &ControlPlaneTestContext<omicron_nexus::Server>,
    query: impl ToString,
) -> Vec<oxql_types::Table> {
    // first, make sure the latest timeseries have been collected.
    cptestctx.oximeter.force_collect().await;

    // okay, do the query
    let body = nexus_types::external_api::params::TimeseriesQuery {
        query: query.to_string(),
    };
    let query = &body.query;
    let rsp = NexusRequest::new(
        nexus_test_utils::http_testing::RequestBuilder::new(
            &cptestctx.external_client,
            http::Method::POST,
            "/v1/timeseries/query",
        )
        .body(Some(&body)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap_or_else(|e| {
        panic!("timeseries query failed: {e:?}\nquery: {query}")
    });
    rsp.parsed_body::<OxqlQueryResult>()
        .unwrap_or_else(|e| {
            panic!(
                "could not parse timeseries query response: {e:?}\n\
            query: {query}\nresponse: {rsp:#?}"
            );
        })
        .tables
}

#[nexus_test]
async fn test_instance_watcher_metrics(
    cptestctx: &ControlPlaneTestContext<omicron_nexus::Server>,
) {
    macro_rules! assert_gte {
        ($a:expr, $b:expr) => {{
            let a = $a;
            let b = $b;
            assert!(
                $a >= $b,
                concat!(
                    "assertion failed: ",
                    stringify!($a),
                    " >= ",
                    stringify!($b),
                    ", ",
                    stringify!($a),
                    " = {:?}, ",
                    stringify!($b),
                    " = {:?}",
                ),
                a,
                b
            );
        }};
    }
    const INSTANCE_ID_FIELD: &str = "instance_id";
    const STATE_FIELD: &str = "state";
    const STATE_STARTING: &str = "starting";
    const STATE_RUNNING: &str = "running";
    const STATE_STOPPING: &str = "stopping";
    const OXQL_QUERY: &str = "get virtual_machine:check";

    let client = &cptestctx.external_client;
    let internal_client = &cptestctx.internal_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let oximeter = &cptestctx.oximeter;

    // TODO(eliza): consider factoring this out to a generic
    // `activate_background_task` function in `nexus-test-utils` eventually?
    let activate_instance_watcher = || async {
        use nexus_client::types::BackgroundTask;
        use nexus_client::types::CurrentStatus;
        use nexus_client::types::CurrentStatusRunning;
        use nexus_client::types::LastResult;
        use nexus_client::types::LastResultCompleted;

        fn most_recent_start_time(
            task: &BackgroundTask,
        ) -> Option<chrono::DateTime<chrono::Utc>> {
            match task.current {
                CurrentStatus::Idle => match task.last {
                    LastResult::Completed(LastResultCompleted {
                        start_time,
                        ..
                    }) => Some(start_time),
                    LastResult::NeverCompleted => None,
                },
                CurrentStatus::Running(CurrentStatusRunning {
                    start_time,
                    ..
                }) => Some(start_time),
            }
        }

        eprintln!("\n --- activating instance watcher ---\n");
        let task = NexusRequest::object_get(
            internal_client,
            "/bgtasks/view/instance_watcher",
        )
        .execute_and_parse_unwrap::<BackgroundTask>()
        .await;
        let last_start = most_recent_start_time(&task);

        internal_client
            .make_request(
                http::Method::POST,
                "/bgtasks/activate",
                Some(serde_json::json!({
                    "bgtask_names": vec![String::from("instance_watcher")]
                })),
                http::StatusCode::NO_CONTENT,
            )
            .await
            .unwrap();
        // Wait for the instance watcher task to finish
        wait_for_condition(
            || async {
                let task = NexusRequest::object_get(
                    internal_client,
                    "/bgtasks/view/instance_watcher",
                )
                .execute_and_parse_unwrap::<BackgroundTask>()
                .await;
                if matches!(&task.current, CurrentStatus::Idle)
                    && most_recent_start_time(&task) > last_start
                {
                    Ok(())
                } else {
                    Err(CondCheckError::<()>::NotYet)
                }
            },
            &Duration::from_millis(500),
            &Duration::from_secs(60),
        )
        .await
        .unwrap();
        // Make sure that the latest metrics have been collected.
        oximeter.force_collect().await;
    };

    #[track_caller]
    fn count_state(
        table: &oxql_types::Table,
        instance_id: InstanceUuid,
        state: &'static str,
    ) -> i64 {
        use oxql_types::point::ValueArray;
        let uuid = FieldValue::Uuid(instance_id.into_untyped_uuid());
        let state = FieldValue::String(state.into());
        let mut timeserieses = table.timeseries().filter(|ts| {
            ts.fields.get(INSTANCE_ID_FIELD) == Some(&uuid)
                && ts.fields.get(STATE_FIELD) == Some(&state)
        });
        let Some(timeseries) = timeserieses.next() else {
            panic!(
                "missing timeseries for instance {instance_id}, state {state}\n\
                found: {table:#?}"
            )
        };
        if let Some(timeseries) = timeserieses.next() {
            panic!(
                "multiple timeseries for instance {instance_id}, state {state}: \
                {timeseries:?}, {timeseries:?}, ...\n\
                found: {table:#?}"
            )
        }
        match timeseries.points.values(0) {
            Some(ValueArray::Integer(ref vals)) => {
                vals.iter().filter_map(|&v| v).sum()
            }
            x => panic!(
                "expected timeseries for instance {instance_id}, \
                state {state} to be an integer, but found: {x:?}"
            ),
        }
    }

    // N.B. that we've gotta use the project name that this function hardcodes
    // if we're going to use the `instance_post` test helper later.
    let project = create_project_and_pool(&client).await;
    let project_name = project.identity.name.as_str();
    // Wait until Nexus registers as a producer with Oximeter.
    wait_for_producer(&oximeter, cptestctx.server.server_context().nexus.id())
        .await;

    eprintln!("--- creating instance 1 ---");
    let instance1 = create_instance(&client, project_name, "i-1").await;
    let instance1_uuid = InstanceUuid::from_untyped_uuid(instance1.identity.id);

    // activate the instance watcher background task.
    activate_instance_watcher().await;

    let metrics = timeseries_query(&cptestctx, OXQL_QUERY).await;
    let checks = metrics
        .iter()
        .find(|t| t.name() == "virtual_machine:check")
        .expect("missing virtual_machine:check");
    let ts = dbg!(count_state(&checks, instance1_uuid, STATE_STARTING));
    assert_gte!(ts, 1);

    // okay, make another instance
    eprintln!("--- creating instance 2 ---");
    let instance2 = create_instance(&client, project_name, "i-2").await;
    let instance2_uuid = InstanceUuid::from_untyped_uuid(instance2.identity.id);

    // activate the instance watcher background task.
    activate_instance_watcher().await;

    let metrics = timeseries_query(&cptestctx, OXQL_QUERY).await;
    let checks = metrics
        .iter()
        .find(|t| t.name() == "virtual_machine:check")
        .expect("missing virtual_machine:check");
    let ts1 = dbg!(count_state(&checks, instance1_uuid, STATE_STARTING));
    let ts2 = dbg!(count_state(&checks, instance2_uuid, STATE_STARTING));
    assert_gte!(ts1, 2);
    assert_gte!(ts2, 1);

    // poke instance 1 to get it into the running state
    eprintln!("--- starting instance 1 ---");
    instance_simulate(nexus, &instance1_uuid).await;

    // activate the instance watcher background task.
    activate_instance_watcher().await;

    let metrics = timeseries_query(&cptestctx, OXQL_QUERY).await;
    let checks = metrics
        .iter()
        .find(|t| t.name() == "virtual_machine:check")
        .expect("missing virtual_machine:check");
    let ts1_starting =
        dbg!(count_state(&checks, instance1_uuid, STATE_STARTING));
    let ts1_running = dbg!(count_state(&checks, instance1_uuid, STATE_RUNNING));
    let ts2 = dbg!(count_state(&checks, instance2_uuid, STATE_STARTING));
    assert_gte!(ts1_starting, 2);
    assert_gte!(ts1_running, 1);
    assert_gte!(ts2, 2);

    // poke instance 2 to get it into the Running state.
    eprintln!("--- starting instance 2 ---");
    instance_simulate(nexus, &instance2_uuid).await;
    // stop instance 1
    eprintln!("--- start stopping instance 1 ---");
    instance_simulate(nexus, &instance1_uuid).await;
    instance_post(&client, &instance1.identity.name.as_str(), InstanceOp::Stop)
        .await;

    // activate the instance watcher background task.
    activate_instance_watcher().await;

    let metrics = timeseries_query(&cptestctx, OXQL_QUERY).await;
    let checks = metrics
        .iter()
        .find(|t| t.name() == "virtual_machine:check")
        .expect("missing virtual_machine:check");

    let ts1_starting =
        dbg!(count_state(&checks, instance1_uuid, STATE_STARTING));
    let ts1_running = dbg!(count_state(&checks, instance1_uuid, STATE_RUNNING));
    let ts1_stopping =
        dbg!(count_state(&checks, instance1_uuid, STATE_STOPPING));
    let ts2_starting =
        dbg!(count_state(&checks, instance2_uuid, STATE_STARTING));
    let ts2_running = dbg!(count_state(&checks, instance2_uuid, STATE_RUNNING));
    assert_gte!(ts1_starting, 2);
    assert_gte!(ts1_running, 1);
    assert_gte!(ts1_stopping, 1);
    assert_gte!(ts2_starting, 2);
    assert_gte!(ts2_running, 1);

    // simulate instance 1 completing its stop, which will remove it from the
    // set of active instances in CRDB. now, it won't be checked again.

    eprintln!("--- finish stopping instance 1 ---");
    instance_simulate(nexus, &instance1_uuid).await;

    // activate the instance watcher background task.
    activate_instance_watcher().await;

    let metrics = timeseries_query(&cptestctx, OXQL_QUERY).await;
    let checks = metrics
        .iter()
        .find(|t| t.name() == "virtual_machine:check")
        .expect("missing virtual_machine:check");
    let ts1_starting =
        dbg!(count_state(&checks, instance1_uuid, STATE_STARTING));
    let ts1_running = dbg!(count_state(&checks, instance1_uuid, STATE_RUNNING));
    let ts1_stopping =
        dbg!(count_state(&checks, instance1_uuid, STATE_STOPPING));
    let ts2_starting =
        dbg!(count_state(&checks, instance2_uuid, STATE_STARTING));
    let ts2_running = dbg!(count_state(&checks, instance2_uuid, STATE_RUNNING));
    assert_gte!(ts1_starting, 2);
    assert_gte!(ts1_running, 1);
    assert_gte!(ts1_stopping, 1);
    assert_gte!(ts2_starting, 2);
    assert_gte!(ts2_running, 2);
}

#[nexus_test]
async fn test_mgs_metrics(
    cptestctx: &ControlPlaneTestContext<omicron_nexus::Server>,
) {
    // Make a MGS
    let mgs = {
        let (mut mgs_config, sp_sim_config) =
            gateway_test_utils::setup::load_test_config();
        // munge the already-parsed MGS config file to point it at the test
        // Nexus' address.
        mgs_config.metrics.get_or_insert_with(Default::default).dev =
            Some(gateway_test_utils::setup::MetricsDevConfig {
                bind_loopback: true,
                nexus_address: Some(cptestctx.internal_client.bind_address),
            });
        gateway_test_utils::setup::test_setup_with_config(
            "test_mgs_metrics",
            gateway_messages::SpPort::One,
            mgs_config,
            &sp_sim_config,
            None,
        )
        .await
    };

    // Wait until the MGS registers as a producer with Oximeter.
    wait_for_producer(&cptestctx.oximeter, &mgs.gateway_id).await;
    cptestctx.oximeter.force_collect().await;

    async fn get_timeseries(
        cptestctx: &ControlPlaneTestContext<omicron_nexus::Server>,
        name: &str,
    ) -> oxql_types::Table {
        let table = timeseries_query(&cptestctx, &format!("get {name}"))
            .await
            .into_iter()
            .find(|t| t.name() == name);
        match table {
            Some(table) => table,
            None => panic!("missing table for {name}"),
        }
    }

    #[track_caller]
    fn check_all_serials_present(table: oxql_types::Table) {
        let mut sim_gimlet_00 = 0;
        let mut sim_gimlet_01 = 0;
        for timeseries in table.timeseries() {
            let fields = &timeseries.fields;
            let n_points = timeseries.points.len();
            eprintln!("found timeseries: {fields:?} ({n_points} points)");
            assert!(n_points > 0, "timeseries {fields:?} should have points");
            let serial_str = match timeseries.fields.get("chassis_serial") {
                Some(FieldValue::String(s)) => s.borrow(),
                Some(x) => panic!(
                    "`chassis_serial` field should be a string, but got: {x:?}"
                ),
                None => {
                    panic!("timeseries should have a `chassis_serial` field")
                }
            };
            match serial_str {
                "SimGimlet00" => sim_gimlet_00 += 1,
                "SimGimlet01" => sim_gimlet_01 += 1,
                // if someone adds sensor readings to the fake sidecar later,
                // that's okay...
                _ => eprintln!("bonus simulated chassis serial {serial_str:?}"),
            }
        }

        assert!(
            sim_gimlet_00 > 0,
            "expected at least one timeseries from SimGimlet00 in {table:#?}"
        );
        assert!(
            sim_gimlet_01 > 0,
            "expected at least one timeseries from SimGimlet01 in {table:#?}"
        );
    }

    let temp_metrics =
        get_timeseries(&cptestctx, "hardware_component:temperature").await;
    check_all_serials_present(temp_metrics);

    let voltage_metrics =
        get_timeseries(&cptestctx, "hardware_component:voltage").await;
    check_all_serials_present(voltage_metrics);

    let current_metrics =
        get_timeseries(&cptestctx, "hardware_component:current").await;
    check_all_serials_present(current_metrics);

    // Because the `ControlPlaneTestContext` isn't managing the MGS we made for
    // this test, we are responsible for removing its logs.
    mgs.logctx.cleanup_successful();
}

/// Wait until a producer is registered with Oximeter.
///
/// This blocks until the producer is registered, for up to 60s. It panics if
/// the retry loop hits a permanent error.
pub async fn wait_for_producer(
    oximeter: &oximeter_collector::Oximeter,
    producer_id: &Uuid,
) {
    wait_for_condition(
        || async {
            if oximeter
                .list_producers(None, usize::MAX)
                .await
                .iter()
                .any(|p| &p.id == producer_id)
            {
                Ok(())
            } else {
                Err(CondCheckError::<()>::NotYet)
            }
        },
        &Duration::from_secs(1),
        &Duration::from_secs(60),
    )
    .await
    .expect("Failed to find producer within time limit");
}
