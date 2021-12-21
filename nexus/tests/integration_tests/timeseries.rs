// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::test_util::objects_list_page;
use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils_macros::nexus_test;
use omicron_test_utils::dev::poll::{wait_for_condition, CondCheckError};
use oximeter_db::TimeseriesSchema;
use std::convert::Infallible;
use std::time::Duration;

#[nexus_test]
async fn test_timeseries_schema(context: &ControlPlaneTestContext) {
    let client = &context.external_client;

    const POLL_INTERVAL: Duration = Duration::from_millis(500);
    const POLL_DURATION: Duration = Duration::from_secs(10);
    let page = wait_for_condition(
        || async {
            let page = objects_list_page::<TimeseriesSchema>(
                client,
                "/timeseries/schema",
            )
            .await;
            if page.items.is_empty() {
                Err(CondCheckError::<Infallible>::NotYet)
            } else {
                Ok(page)
            }
        },
        &POLL_INTERVAL,
        &POLL_DURATION,
    )
    .await
    .expect("Expected at least one timeseries schema");
    assert!(
        page.items.iter().any(|schema| schema.timeseries_name
            == "integration_target:integration_metric"),
        "Expected to find a particular timeseries schema"
    );

    let url = format!(
        "/timeseries/schema?page_token={}",
        page.next_page.as_ref().unwrap()
    );
    let page = objects_list_page::<TimeseriesSchema>(client, &url).await;
    assert!(
        page.next_page.is_none(),
        "Expected exactly one page of timeseries schema"
    );
}
