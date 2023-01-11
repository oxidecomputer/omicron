// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::test_util::ClientTestContext;
use dropshot::ResultsPage;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use oximeter::types::Datum;
use oximeter::types::Measurement;

pub async fn query_for_metrics_until_they_exist(
    client: &ClientTestContext,
    path: &str,
) -> ResultsPage<Measurement> {
    loop {
        let measurements: ResultsPage<Measurement> =
            objects_list_page_authz(client, path).await;

        if !measurements.items.is_empty() {
            return measurements;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
}

pub async fn query_for_latest_metric(
    client: &ClientTestContext,
    path: &str,
) -> i64 {
    let measurements: ResultsPage<Measurement> =
        objects_list_page_authz(client, path).await;

    let item = &measurements.items[measurements.items.len() - 1];
    let datum = match item.datum() {
        Datum::I64(c) => c,
        _ => panic!("Unexpected datum type {:?}", item.datum()),
    };
    return *datum;
}
