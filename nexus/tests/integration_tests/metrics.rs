// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::time::Duration;

use crate::integration_tests::instances::{
    create_project_and_pool, instance_post, instance_simulate, InstanceOp,
};
use chrono::Utc;
use dropshot::test_util::ClientTestContext;
use dropshot::{HttpErrorResponseBody, ResultsPage};
use http::{Method, StatusCode};
use nexus_auth::authn::USER_TEST_UNPRIVILEGED;
use nexus_db_queries::db::identity::Asset;
use nexus_test_utils::background::activate_background_task;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::{
    create_default_ip_pool, create_disk, create_instance, create_project,
    grant_iam, object_create_error, objects_list_page_authz, DiskTest,
};
use nexus_test_utils::wait_for_producer;
use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::shared::ProjectRole;
use nexus_types::external_api::views::OxqlQueryResult;
use nexus_types::silo::DEFAULT_SILO_ID;
use omicron_test_utils::dev::poll::{wait_for_condition, CondCheckError};
use omicron_uuid_kinds::{GenericUuid, InstanceUuid};
use oximeter::types::Datum;
use oximeter::types::FieldValue;
use oximeter::types::Measurement;
use oximeter::TimeseriesSchema;
use std::borrow::Borrow;
use std::collections::HashMap;
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
    cptestctx
        .oximeter
        .try_force_collect()
        .await
        .expect("Could not force oximeter collection");
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
    cptestctx
        .oximeter
        .try_force_collect()
        .await
        .expect("Could not force oximeter collection");
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
    assert_system_metrics(&cptestctx, Some(DEFAULT_SILO_ID), 0, 0, 0).await;
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
        DEFAULT_SILO_ID,
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
    assert_system_metrics(&cptestctx, Some(DEFAULT_SILO_ID), 0, 4, GIB).await;

    // create disk in project 1
    create_disk(&client, "p-1", "d-1").await;
    assert_silo_metrics(&cptestctx, Some(project1_id), GIB, 4, GIB).await;
    assert_silo_metrics(&cptestctx, None, GIB, 4, GIB).await;
    assert_system_metrics(&cptestctx, None, GIB, 4, GIB).await;
    assert_system_metrics(&cptestctx, Some(DEFAULT_SILO_ID), GIB, 4, GIB).await;

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
        Some(DEFAULT_SILO_ID),
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
async fn test_system_timeseries_schema_list(
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
    cptestctx
        .oximeter
        .try_force_collect()
        .await
        .expect("Could not force oximeter collection");
    let client = &cptestctx.external_client;
    let url = "/v1/system/timeseries/schemas";
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

/// Run an OxQL query until it succeeds or panics.
pub async fn system_timeseries_query(
    cptestctx: &ControlPlaneTestContext<omicron_nexus::Server>,
    query: impl ToString,
) -> Vec<oxql_types::Table> {
    timeseries_query_until_success(
        cptestctx,
        "/v1/system/timeseries/query",
        query,
    )
    .await
}

/// Run a project-scoped OxQL query until it succeeds or panics.
pub async fn project_timeseries_query(
    cptestctx: &ControlPlaneTestContext<omicron_nexus::Server>,
    project: &str,
    query: impl ToString,
) -> Vec<oxql_types::Table> {
    timeseries_query_until_success(
        cptestctx,
        &format!("/v1/timeseries/query?project={}", project),
        query,
    )
    .await
}

/// Run an OxQL query until it succeeds or panics.
async fn timeseries_query_until_success(
    cptestctx: &ControlPlaneTestContext<omicron_nexus::Server>,
    endpoint: &str,
    query: impl ToString,
) -> Vec<oxql_types::Table> {
    const POLL_INTERVAL: Duration = Duration::from_secs(1);
    const POLL_MAX: Duration = Duration::from_secs(30);
    let query_ = query.to_string();
    wait_for_condition(
        || async {
            match execute_timeseries_query(cptestctx, endpoint, &query_).await {
                Some(r) => Ok(r),
                None => Err(CondCheckError::<()>::NotYet),
            }
        },
        &POLL_INTERVAL,
        &POLL_MAX,
    )
    .await
    .unwrap_or_else(|_| {
        panic!(
            "Timeseries named in query are not available \
            after {:?}, query: '{}'",
            POLL_MAX,
            query.to_string(),
        )
    })
}

/// Run an OxQL query.
///
/// This returns `None` if the query resulted in client error and the body
/// indicates that a timeseries named in the query could not be found. In all
/// other cases, it either succeeds or panics.
pub async fn execute_timeseries_query(
    cptestctx: &ControlPlaneTestContext<omicron_nexus::Server>,
    endpoint: &str,
    query: impl ToString,
) -> Option<Vec<oxql_types::Table>> {
    // first, make sure the latest timeseries have been collected.
    cptestctx
        .oximeter
        .try_force_collect()
        .await
        .expect("Could not force oximeter collection");

    // okay, do the query
    let body = nexus_types::external_api::params::TimeseriesQuery {
        query: query.to_string(),
    };
    let query = &body.query;
    let rsp = NexusRequest::new(
        nexus_test_utils::http_testing::RequestBuilder::new(
            &cptestctx.external_client,
            http::Method::POST,
            endpoint,
        )
        .body(Some(&body)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap_or_else(|e| {
        panic!("timeseries query failed: {e:?}\nquery: {query}")
    });

    // Check for a timeseries-not-found error specifically.
    if rsp.status.is_client_error() {
        let err =
            rsp.parsed_body::<HttpErrorResponseBody>().unwrap_or_else(|e| {
                panic!(
                    "could not parse body as `HttpErrorResponseBody`: {e:?}\n\
                     query: {query}\nresponse: {rsp:#?}",
                )
            });

        if err.message.starts_with("Timeseries not found for: ") {
            return None;
        }
    }

    // Try to parse the query as usual, which will fail on other kinds of
    // errors.
    Some(
        rsp.parsed_body::<OxqlQueryResult>()
            .unwrap_or_else(|e| {
                panic!(
                    "could not parse timeseries query response: {e:?}\n\
                    query: {query}\nresponse: {rsp:#?}"
                );
            })
            .tables,
    )
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
    const OXQL_QUERY: &str = "get virtual_machine:check | \
                              filter timestamp > @2000-01-01";

    let client = &cptestctx.external_client;
    let internal_client = &cptestctx.internal_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let oximeter = &cptestctx.oximeter;

    let activate_instance_watcher = || async {
        use nexus_test_utils::background::activate_background_task;

        let _ = activate_background_task(&internal_client, "instance_watcher")
            .await;

        // Make sure that the latest metrics have been collected.
        cptestctx
            .oximeter
            .try_force_collect()
            .await
            .expect("Could not force oximeter collection");
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

    let metrics = system_timeseries_query(&cptestctx, OXQL_QUERY).await;
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

    let metrics = system_timeseries_query(&cptestctx, OXQL_QUERY).await;
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

    let metrics = system_timeseries_query(&cptestctx, OXQL_QUERY).await;
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

    let metrics = system_timeseries_query(&cptestctx, OXQL_QUERY).await;
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

    let metrics = system_timeseries_query(&cptestctx, OXQL_QUERY).await;
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
async fn test_project_timeseries_query(
    cptestctx: &ControlPlaneTestContext<omicron_nexus::Server>,
) {
    let client = &cptestctx.external_client;

    create_default_ip_pool(&client).await; // needed for instance create to work

    // Create two projects
    let p1 = create_project(&client, "project1").await;
    let _p2 = create_project(&client, "project2").await;

    // Create resources in each project
    let i1 = create_instance(&client, "project1", "instance1").await;
    let _i2 = create_instance(&client, "project2", "instance2").await;

    let internal_client = &cptestctx.internal_client;

    // get the instance metrics to show up
    let _ =
        activate_background_task(&internal_client, "instance_watcher").await;

    // Query with no project specified
    let q1 = "get virtual_machine:check";

    let result = project_timeseries_query(&cptestctx, "project1", q1).await;
    assert_eq!(result.len(), 1);
    assert!(result[0].timeseries().len() > 0);

    // also works with project ID
    let result =
        project_timeseries_query(&cptestctx, &p1.identity.id.to_string(), q1)
            .await;
    assert_eq!(result.len(), 1);
    assert!(result[0].timeseries().len() > 0);

    let result = project_timeseries_query(&cptestctx, "project2", q1).await;
    assert_eq!(result.len(), 1);
    assert!(result[0].timeseries().len() > 0);

    // with project specified
    let q2 = &format!("{} | filter project_id == \"{}\"", q1, p1.identity.id);

    let result = project_timeseries_query(&cptestctx, "project1", q2).await;
    assert_eq!(result.len(), 1);
    assert!(result[0].timeseries().len() > 0);

    let result = project_timeseries_query(&cptestctx, "project2", q2).await;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].timeseries().len(), 0);

    // with instance specified
    let q3 = &format!("{} | filter instance_id == \"{}\"", q1, i1.identity.id);

    // project containing instance gives me something
    let result = project_timeseries_query(&cptestctx, "project1", q3).await;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].timeseries().len(), 1);

    // should be empty or error
    let result = project_timeseries_query(&cptestctx, "project2", q3).await;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].timeseries().len(), 0);

    // expect error when querying a metric that has no project_id on it
    let q4 = "get integration_target:integration_metric";
    let url = "/v1/timeseries/query?project=project1";
    let body = nexus_types::external_api::params::TimeseriesQuery {
        query: q4.to_string(),
    };
    let result =
        object_create_error(client, url, &body, StatusCode::BAD_REQUEST).await;
    assert_eq!(result.error_code.unwrap(), "InvalidRequest");
    // Notable that the error confirms that the metric exists and says what the
    // fields are. This is helpful generally, but here it would be better if
    // we could say something more like "you can't query this timeseries from
    // this endpoint"
    const EXPECTED_ERROR_MESSAGE: &str = "\
        The filter expression refers to \
        identifiers that are not valid for its input \
        table \"integration_target:integration_metric\". \
        Invalid identifiers: [\"silo_id\", \"project_id\"], \
        valid identifiers: [\"datum\", \"metric_name\", \"target_name\", \"timestamp\"]";
    assert!(result.message.ends_with(EXPECTED_ERROR_MESSAGE));

    // nonexistent project
    let url = "/v1/timeseries/query?project=nonexistent";
    let body = nexus_types::external_api::params::TimeseriesQuery {
        query: q4.to_string(),
    };
    let result =
        object_create_error(client, url, &body, StatusCode::NOT_FOUND).await;
    assert_eq!(result.message, "not found: project with name \"nonexistent\"");

    // unprivileged user gets 404 on project that exists, but which they can't read
    let url = "/v1/timeseries/query?project=project1";
    let body = nexus_types::external_api::params::TimeseriesQuery {
        query: q1.to_string(),
    };

    let request = RequestBuilder::new(client, Method::POST, url)
        .body(Some(&body))
        .expect_status(Some(StatusCode::NOT_FOUND));
    let result = NexusRequest::new(request)
        .authn_as(AuthnMode::UnprivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body::<HttpErrorResponseBody>()
        .unwrap();
    assert_eq!(result.message, "not found: project with name \"project1\"");

    // now grant the user access to that project only
    grant_iam(
        client,
        "/v1/projects/project1",
        ProjectRole::Viewer,
        USER_TEST_UNPRIVILEGED.id(),
        AuthnMode::PrivilegedUser,
    )
    .await;

    // now they can access the timeseries. how cool is that
    let request = RequestBuilder::new(client, Method::POST, url)
        .body(Some(&body))
        .expect_status(Some(StatusCode::OK));
    let result = NexusRequest::new(request)
        .authn_as(AuthnMode::UnprivilegedUser)
        .execute_and_parse_unwrap::<OxqlQueryResult>()
        .await;
    assert_eq!(result.tables.len(), 1);
    assert_eq!(result.tables[0].timeseries().len(), 1);
}

#[nexus_test]
async fn test_mgs_metrics(
    cptestctx: &ControlPlaneTestContext<omicron_nexus::Server>,
) {
    // Make a MGS
    let (mut mgs_config, sp_sim_config) =
        gateway_test_utils::setup::load_test_config();
    let mgs = {
        // munge the already-parsed MGS config file to point it at the test
        // Nexus' address.
        mgs_config.metrics = Some(gateway_test_utils::setup::MetricsConfig {
            disabled: false,
            dev_bind_loopback: true,
            dev_nexus_address: Some(cptestctx.internal_client.bind_address),
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

    // Let's look at all the simulated SP components in the config file which
    // have sensor readings, so we can assert that there are timeseries for all
    // of them.
    let all_sp_configs = {
        let gimlet_configs =
            sp_sim_config.simulated_sps.gimlet.iter().map(|g| &g.common);
        let sidecar_configs =
            sp_sim_config.simulated_sps.sidecar.iter().map(|s| &s.common);
        gimlet_configs.chain(sidecar_configs)
    };
    // XXX(eliza): yes, this code is repetitive. We could probably make it a
    // little elss ugly with nested hash maps, but like...I already wrote it, so
    // you don't have to. :)
    //
    // TODO(eliza): presently, we just expect that the number of timeseries for
    // each serial number and sensor type lines up. If we wanted to be *really*
    // fancy, we could also assert that all the component IDs, component kinds,
    // and measurement values line up with the config. But, honestly, it's
    // pretty unlikely that a bug in MGS' sensor metrics subsystem would mess
    // that up --- the most important thing is just to make sure that the sensor
    // data is *present*, as that should catch most regressions.
    let mut temp_sensors = HashMap::new();
    let mut current_sensors = HashMap::new();
    let mut voltage_sensors = HashMap::new();
    let mut power_sensors = HashMap::new();
    let mut input_voltage_sensors = HashMap::new();
    let mut input_current_sensors = HashMap::new();
    let mut fan_speed_sensors = HashMap::new();
    let mut cpu_tctl_sensors = HashMap::new();
    for sp in all_sp_configs {
        let mut temp = 0;
        let mut current = 0;
        let mut voltage = 0;
        let mut input_voltage = 0;
        let mut input_current = 0;
        let mut power = 0;
        let mut speed = 0;
        let mut cpu_tctl = 0;
        for component in &sp.components {
            for sensor in &component.sensors {
                use gateway_messages::measurement::MeasurementKind as Kind;
                match sensor.def.kind {
                    Kind::Temperature => {
                        // Currently, Tctl measurements are reported as a
                        // "temperature" measurement, but are tracked by a
                        // different metric, as they are not actually a
                        // measurement of physical degrees Celsius.
                        if component.device == "sbtsi"
                            && sensor.def.name == "CPU"
                        {
                            cpu_tctl += 1
                        } else {
                            temp += 1;
                        }
                    }
                    Kind::Current => current += 1,
                    Kind::Voltage => voltage += 1,
                    Kind::InputVoltage => input_voltage += 1,
                    Kind::InputCurrent => input_current += 1,
                    Kind::Speed => speed += 1,
                    Kind::Power => power += 1,
                }
            }
        }
        temp_sensors.insert(sp.serial_number.clone(), temp);
        current_sensors.insert(sp.serial_number.clone(), current);
        voltage_sensors.insert(sp.serial_number.clone(), voltage);
        input_voltage_sensors.insert(sp.serial_number.clone(), input_voltage);
        input_current_sensors.insert(sp.serial_number.clone(), input_current);
        fan_speed_sensors.insert(sp.serial_number.clone(), speed);
        power_sensors.insert(sp.serial_number.clone(), power);
        cpu_tctl_sensors.insert(sp.serial_number.clone(), cpu_tctl);
    }

    async fn check_all_timeseries_present(
        cptestctx: &ControlPlaneTestContext<omicron_nexus::Server>,
        name: &str,
        expected: HashMap<String, usize>,
    ) {
        let metric_name = format!("hardware_component:{name}");
        eprintln!("\n=== checking timeseries for {metric_name} ===\n");

        if expected.values().all(|&v| v == 0) {
            eprintln!(
                "-> SP sim config contains no {name} sensors, skipping it"
            );
            return;
        }

        let query =
            format!("get {metric_name} | filter timestamp > @2000-01-01");

        // MGS polls SP sensor data once every second. It's possible that, when
        // we triggered Oximeter to collect samples from MGS, it may not have
        // run a poll yet, so retry this a few times to avoid a flaky failure if
        // no simulated SPs have been polled yet.
        //
        // We really don't need to wait that long to know that the sensor
        // metrics will never be present. This could probably be shorter
        // than 30 seconds, but I want to be fairly generous to make sure
        // there are no flaky failures even when things take way longer than
        // expected...
        const MAX_RETRY_DURATION: Duration = Duration::from_secs(30);
        let result = wait_for_condition(
            || async {
                match check_inner(cptestctx, &metric_name, &query, &expected).await {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        eprintln!("{e}; will try again to ensure all samples are collected");
                        Err(CondCheckError::<()>::NotYet)
                    }
                }
            },
            &Duration::from_secs(1),
            &MAX_RETRY_DURATION,
        )
        .await;
        if result.is_err() {
            panic!(
                "failed to find expected timeseries when running OxQL query \
                 {query:?} within {MAX_RETRY_DURATION:?}"
            )
        };

        // Note that *some* of these checks panic if they fail, but others call
        // `anyhow::ensure!`. This is because, if we don't see all the expected
        // timeseries, it's possible that this is because some sensor polls
        // haven't completed yet, so we'll retry those checks a few times. On
        // the other hand, if we see malformed timeseries, or timeseries that we
        // don't expect to exist, that means something has gone wrong, and we
        // will fail the test immediately.
        async fn check_inner(
            cptestctx: &ControlPlaneTestContext<omicron_nexus::Server>,
            name: &str,
            query: &str,
            expected: &HashMap<String, usize>,
        ) -> anyhow::Result<()> {
            cptestctx
                .oximeter
                .try_force_collect()
                .await
                .expect("Could not force oximeter collection");
            let table = system_timeseries_query(&cptestctx, &query)
                .await
                .into_iter()
                .find(|t| t.name() == name)
                .ok_or_else(|| {
                    anyhow::anyhow!("failed to find table for {query}")
                })?;

            let mut found = expected
                .keys()
                .map(|serial| (serial.clone(), 0))
                .collect::<HashMap<_, usize>>();
            for timeseries in table.timeseries() {
                let fields = &timeseries.fields;
                let n_points = timeseries.points.len();
                anyhow::ensure!(
                    n_points > 0,
                    "{name} timeseries {fields:?} should have points"
                );
                let serial_str: &str = match timeseries.fields.get("chassis_serial")
            {
                Some(FieldValue::String(s)) => s.borrow(),
                Some(x) => panic!(
                    "{name} `chassis_serial` field should be a string, but got: {x:?}"
                ),
                None => {
                    panic!("{name} timeseries should have a `chassis_serial` field")
                }
            };
                if let Some(count) = found.get_mut(serial_str) {
                    *count += 1;
                } else {
                    panic!(
                        "{name} timeseries had an unexpected chassis serial \
                        number {serial_str:?} (not in the config file)",
                    );
                }
            }

            eprintln!("-> {name}: found timeseries: {found:#?}");
            anyhow::ensure!(
                &found == expected,
                "number of {name} timeseries didn't match expected in {table:#?}",
            );
            eprintln!("-> okay, looks good!");
            Ok(())
        }
    }

    // Wait until the MGS registers as a producer with Oximeter.
    wait_for_producer(&cptestctx.oximeter, mgs.gateway_id).await;

    check_all_timeseries_present(&cptestctx, "temperature", temp_sensors).await;
    check_all_timeseries_present(&cptestctx, "voltage", voltage_sensors).await;
    check_all_timeseries_present(&cptestctx, "current", current_sensors).await;
    check_all_timeseries_present(&cptestctx, "power", power_sensors).await;
    check_all_timeseries_present(
        &cptestctx,
        "input_voltage",
        input_voltage_sensors,
    )
    .await;
    check_all_timeseries_present(
        &cptestctx,
        "input_current",
        input_current_sensors,
    )
    .await;
    check_all_timeseries_present(&cptestctx, "fan_speed", fan_speed_sensors)
        .await;
    check_all_timeseries_present(&cptestctx, "amd_cpu_tctl", cpu_tctl_sensors)
        .await;

    // Because the `ControlPlaneTestContext` isn't managing the MGS we made for
    // this test, we are responsible for removing its logs.
    mgs.logctx.cleanup_successful();
}
