// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::test_util::ClientTestContext;
use dropshot::ResultsPage;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use omicron_common::backoff;
use oximeter::types::Datum;
use oximeter::types::Measurement;

pub async fn query_for_metrics_until_they_exist(
    client: &ClientTestContext,
    path: &str,
) -> ResultsPage<Measurement> {
    backoff::retry_notify(
        backoff::retry_policy_local(),
        || async {
            let measurements: ResultsPage<Measurement> =
                objects_list_page_authz(client, path).await;

            if measurements.items.is_empty() {
                return Err(backoff::BackoffError::transient("No metrics yet"));
            }
            Ok(measurements)
        },
        |error, _| {
            eprintln!("Failed to query {path}: {error}");
        },
    )
    .await
    .expect("Failed to query for measurements")
}

pub async fn query_for_metrics_until_it_contains(
    client: &ClientTestContext,
    path: &str,
    index: usize,
    value: i64,
) -> ResultsPage<Measurement> {
    backoff::retry_notify(
        backoff::retry_policy_local(),
        || async {
            let measurements: ResultsPage<Measurement> =
                objects_list_page_authz(client, path).await;

            if measurements.items.len() <= index {
                return Err(backoff::BackoffError::transient(format!(
                    "Not enough metrics yet (only seen: {:?})",
                    measurements.items
                )));
            }

            let item = &measurements.items[index];
            let datum = match item.datum() {
                Datum::I64(c) => c,
                _ => panic!("Unexpected datum type {:?}", item.datum()),
            };
            assert_eq!(*datum, value, "Datum exists, but has the wrong value");
            Ok(measurements)
        },
        |error, _| {
            eprintln!("Failed to query {path}: {error}");
        },
    )
    .await
    .expect("Failed to query for measurements")
}
