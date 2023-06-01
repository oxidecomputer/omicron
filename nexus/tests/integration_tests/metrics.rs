// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::Utc;
use dropshot::test_util::ClientTestContext;
use dropshot::ResultsPage;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use nexus_test_utils::ControlPlaneTestContext;
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
    resource_id: Uuid,
) -> i64 {
    let client = &cptestctx.external_client;
    let url = format!(
        "/v1/system/metrics/{metric_name}?start_time={:?}&end_time={:?}&id={:?}&order=descending", 
        cptestctx.start_time,
        Utc::now(),
        resource_id,
    );
    let measurements: ResultsPage<Measurement> =
        objects_list_page_authz(client, &url).await;

    // prevent more confusing error on next line
    assert!(measurements.items.len() > 0, "Expected at least one measurement");

    let item = &measurements.items[0];
    let datum = match item.datum() {
        Datum::I64(c) => c,
        _ => panic!("Unexpected datum type {:?}", item.datum()),
    };
    return *datum;
}
