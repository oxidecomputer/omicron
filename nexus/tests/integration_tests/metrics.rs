// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

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
use oximeter::types::Datum;
use oximeter::types::Measurement;
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
    assert!(measurements.items.len() == 1, "Expected exactly one measurement");

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

    cptestctx.server.register_as_producer().await; // needed for oximeter metrics to work
    create_default_ip_pool(&client).await; // needed for instance create to work
    DiskTest::new(cptestctx).await; // needed for disk create to work

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
