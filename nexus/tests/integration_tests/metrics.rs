// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::time::Duration;

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
use omicron_test_utils::dev::poll::{wait_for_condition, CondCheckError};
use oximeter::types::Datum;
use oximeter::types::Measurement;
use oximeter::TimeseriesSchema;
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
        cptestctx.server.apictx().nexus.id(),
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
    let nexus_id = cptestctx.server.apictx().nexus.id();
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
) -> Vec<oximeter_db::oxql::Table> {
    // first, make sure the latest timeseries have been collected.
    cptestctx.oximeter.force_collect().await;

    // okay, do the query
    let body = nexus_types::external_api::params::TimeseriesQuery {
        query: query.to_string(),
    };
    let query = dbg!(&body.query);
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
    dbg!(rsp).parsed_body().unwrap_or_else(|e| {
        panic!(
            "could not parse timeseries query response: {e:?}\nquery: {query}"
        );
    })
}

#[nexus_test]
async fn test_instance_watcher_metrics(
    cptestctx: &ControlPlaneTestContext<omicron_nexus::Server>,
) {
    use oximeter::types::FieldValue;
    const INSTANCE_ID_FIELD: &str = "instance_id";
    const STATE_FIELD: &str = "state";
    const STATE_STARTING: &str = "starting";

    let client = &cptestctx.external_client;
    let internal_client = &cptestctx.internal_client;

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
    };

    #[track_caller]
    fn timeseries_for_instance(
        table: &oximeter_db::oxql::Table,
        instance_id: Uuid,
    ) -> &oximeter_db::oxql::Timeseries {
        let uuid = FieldValue::Uuid(instance_id);
        let mut timeserieses = table
            .timeseries()
            .filter(move |ts| ts.fields.get(INSTANCE_ID_FIELD) == Some(&uuid));
        let Some(timeseries) = timeserieses.next() else {
            panic!("missing timeseries for instance {instance_id}")
        };
        if let Some(timeseries) = timeserieses.next() {
            panic!(
                "multiple timeseries for instance {instance_id}: \
                {timeseries:?}, {timeseries:?}, ..."
            )
        }
        timeseries
    }

    create_default_ip_pool(&client).await; // needed for instance create to work
                                           // Wait until Nexus registers as a producer with Oximeter.
    wait_for_producer(
        &cptestctx.oximeter,
        cptestctx.server.apictx().nexus.id(),
    )
    .await;

    create_project(&client, "p-1").await;
    let instance1 = create_instance(&client, "p-1", "i-1").await;
    let instance1_uuid = instance1.identity.id;

    // activate the instance watcher background task.
    activate_instance_watcher().await;

    let metrics =
        dbg!(timeseries_query(&cptestctx, "get virtual_machine:check").await);
    let checks = metrics
        .iter()
        .find(|t| t.name() == "virtual_machine:check")
        .expect("missing virtual_machine:check");
    let ts = timeseries_for_instance(&checks, instance1_uuid);
    assert_eq!(
        ts.fields.get(STATE_FIELD).unwrap(),
        &FieldValue::from(STATE_STARTING)
    );

    // okay, make another instance
    let instance2 = create_instance(&client, "p-1", "i-2").await;
    let instance2_uuid = instance2.identity.id;

    // activate the instance watcher background task.
    activate_instance_watcher().await;

    let metrics =
        dbg!(timeseries_query(&cptestctx, "get virtual_machine:check").await);
    let checks = metrics
        .iter()
        .find(|t| t.name() == "virtual_machine:check")
        .expect("missing virtual_machine:check");
    let ts1 = timeseries_for_instance(&checks, instance1_uuid);
    let ts2 = timeseries_for_instance(&checks, instance2_uuid);
    assert_eq!(
        ts1.fields.get(STATE_FIELD).unwrap(),
        &FieldValue::from(STATE_STARTING)
    );
    assert_eq!(
        ts2.fields.get(STATE_FIELD).unwrap(),
        &FieldValue::from(STATE_STARTING)
    );
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
