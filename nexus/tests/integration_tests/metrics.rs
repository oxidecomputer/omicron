// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::Utc;
use dropshot::test_util::ClientTestContext;
use dropshot::ResultsPage;
use http::{Method, StatusCode};
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::{
    create_project, objects_list_page_authz,
};
use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils_macros::nexus_test;
use omicron_nexus::db::fixed_data::silo::SILO_ID;
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

#[nexus_test]
async fn test_system_metrics(
    cptestctx: &ControlPlaneTestContext<omicron_nexus::Server>,
) {
    // Normally, Nexus is not registered as a producer for tests.
    // Turn this bit on so we can also test some metrics from Nexus itself.
    cptestctx.server.register_as_producer().await;
    assert!(true);
}

#[nexus_test]
async fn test_silo_metrics(
    cptestctx: &ControlPlaneTestContext<omicron_nexus::Server>,
) {
    // Normally, Nexus is not registered as a producer for tests.
    // Turn this bit on so we can also test some metrics from Nexus itself.
    cptestctx.server.register_as_producer().await;

    let client = &cptestctx.external_client;
    // let oximeter = &cptestctx.oximeter;
    // let apictx = &cptestctx.server.apictx();
    // let nexus = &apictx.nexus;
    // let datastore = nexus.datastore();

    let get_url = |metric_name: &str, project_id: Option<Uuid>| -> String {
        let id_param = match project_id {
            Some(id) => format!("&project={}", id),
            None => "".to_string(),
        };
        format!(
            "/v1/metrics/{metric_name}?start_time={:?}&end_time={:?}&order=descending&limit=1{}", 
            cptestctx.start_time,
            Utc::now(),
            id_param,
        )
    };

    let project_id = create_project(&client, "my-proj").await.identity.id;

    // silo metrics restricted to current silo — does not allow access to any
    // other silo. to some extent this is guaranteed by the design of the query
    // param, which only takes a project ID. so really we just test that only
    // project IDs work.

    // 404 if given project_id that is actually some other type of resource
    let error = NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::GET,
            &get_url("cpus_provisioned", Some(*SILO_ID)),
        )
        .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected success")
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .unwrap();

    dbg!(error);

    // using an actual project ID works though
    objects_list_page_authz::<Measurement>(
        client,
        &get_url("cpus_provisioned", Some(project_id)),
    )
    .await;
}
