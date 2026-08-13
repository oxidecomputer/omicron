// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Alerts

use chrono::{DateTime, TimeDelta, Utc};
use http::{Method, StatusCode};
use nexus_db_queries::context::OpContext;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils_macros::nexus_test;
use nexus_types::alert::test_alerts;
use nexus_types::external_api::alert::{Alert, AlertListParams};
use omicron_common::api::external::http_pagination::TimeAndIdSortMode;
use omicron_uuid_kinds::{AlertUuid, GenericUuid};

const ALERTS_URL: &str = "/v1/alerts";

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

fn alert_list_query(
    params: &AlertListParams,
    sort_by: Option<TimeAndIdSortMode>,
) -> String {
    let mut query = serde_urlencoded::to_string(params)
        .expect("alert list parameters should serialize");
    if let Some(sort_by) = sort_by {
        if !query.is_empty() {
            query.push('&');
        }
        query.push_str(
            &serde_urlencoded::to_string([("sort_by", sort_by)])
                .expect("alert list sort mode should serialize"),
        );
    }
    query
}

async fn alert_list(
    client: &dropshot::test_util::ClientTestContext,
    params: &AlertListParams,
    sort_by: Option<TimeAndIdSortMode>,
    limit: Option<usize>,
) -> Vec<Alert> {
    let query = alert_list_query(params, sort_by);
    let collection = match NexusRequest::iter_collection_authn(
        client, ALERTS_URL, &query, limit,
    )
    .await
    {
        Ok(collection) => collection,
        Err(e) => panic!("listing alerts with params '{query}' failed: {e}"),
    };
    collection.all_items
}

#[nexus_test]
async fn test_alert_list_and_view(ctx: &ControlPlaneTestContext) {
    let client = &ctx.external_client;
    let nexus = &ctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(ctx.logctx.log.new(o!()), datastore.clone());

    let foo_id = AlertUuid::new_v4();
    let foo_bar_id = AlertUuid::new_v4();
    let quux_bar_id = AlertUuid::new_v4();
    nexus
        .alert_publish(
            &opctx,
            foo_id,
            &test_alerts::Foo(serde_json::json!({ "sequence": 1 })),
        )
        .await
        .expect("publishing test.foo alert");
    nexus
        .alert_publish(
            &opctx,
            foo_bar_id,
            &test_alerts::FooBar(serde_json::json!({ "sequence": 2 })),
        )
        .await
        .expect("publishing test.foo.bar alert");
    nexus
        .alert_publish(
            &opctx,
            quux_bar_id,
            &test_alerts::QuuxBar(serde_json::json!({ "sequence": 3 })),
        )
        .await
        .expect("publishing test.quux.bar alert");

    // Giving all three rows the same timestamp makes this exercise the UUID
    // component of the pagination marker, rather than merely checking that the
    // marker type happens to contain a UUID.
    let timestamp: DateTime<Utc> = "2026-08-11T12:00:00Z".parse().unwrap();
    {
        use async_bb8_diesel::AsyncRunQueryDsl;
        use diesel::prelude::*;
        use nexus_db_schema::schema::alert;

        let ids = [foo_id, foo_bar_id, quux_bar_id]
            .map(GenericUuid::into_untyped_uuid);
        diesel::update(alert::table.filter(alert::id.eq_any(ids)))
            .set((
                alert::time_created.eq(timestamp),
                alert::time_modified.eq(timestamp),
            ))
            .execute_async(
                &*datastore.pool_connection_for_tests().await.unwrap(),
            )
            .await
            .expect("setting deterministic alert timestamps");
    }

    let mut ascending_ids =
        [foo_id, foo_bar_id, quux_bar_id].map(GenericUuid::into_untyped_uuid);
    ascending_ids.sort();

    let all_test_alerts = AlertListParams {
        alert_class: Some("test.**".parse().unwrap()),
        start_time: None,
        end_time: None,
    };
    let bounded_test_alerts = AlertListParams {
        start_time: Some(timestamp),
        end_time: Some(timestamp),
        ..all_test_alerts.clone()
    };

    // The class and time selectors are recovered from the page token: only the
    // initial request carries them explicitly.
    let ascending =
        alert_list(client, &bounded_test_alerts, None, Some(1)).await;
    assert_eq!(
        ascending.iter().map(|alert| alert.identity.id).collect::<Vec<_>>(),
        ascending_ids,
    );

    let descending = alert_list(
        client,
        &bounded_test_alerts,
        Some(TimeAndIdSortMode::TimeAndIdDescending),
        Some(1),
    )
    .await;
    assert_eq!(
        descending.iter().map(|alert| alert.identity.id).collect::<Vec<_>>(),
        ascending_ids.into_iter().rev().collect::<Vec<_>>(),
    );

    let params_for_classes = |classes: &str| AlertListParams {
        alert_class: Some(classes.parse().unwrap()),
        start_time: None,
        end_time: None,
    };
    let exact =
        alert_list(client, &params_for_classes("test.foo"), None, None).await;
    assert_eq!(exact.len(), 1);
    assert_eq!(exact[0].identity.id, foo_id.into_untyped_uuid());

    let glob =
        alert_list(client, &params_for_classes("test.foo.**"), None, None)
            .await;
    assert_eq!(glob.len(), 1);
    assert_eq!(glob[0].identity.id, foo_bar_id.into_untyped_uuid());

    let unmatched =
        alert_list(client, &params_for_classes("unmatched.**"), None, None)
            .await;
    assert!(unmatched.is_empty());

    assert_eq!(
        alert_list(
            client,
            &AlertListParams {
                start_time: Some(timestamp + TimeDelta::microseconds(1)),
                ..all_test_alerts.clone()
            },
            None,
            None,
        )
        .await,
        Vec::new(),
    );
    assert_eq!(
        alert_list(
            client,
            &AlertListParams {
                end_time: Some(timestamp - TimeDelta::microseconds(1)),
                ..all_test_alerts
            },
            None,
            None,
        )
        .await,
        Vec::new(),
    );

    let view: Alert = NexusRequest::object_get(
        client,
        &format!("{ALERTS_URL}/{}", foo_bar_id.into_untyped_uuid()),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap()
    .await;
    assert_eq!(view.identity.id, foo_bar_id.into_untyped_uuid());
    assert_eq!(view.class, "test.foo.bar");
    assert_eq!(view.version, 0);
    assert_eq!(view.alert, serde_json::json!({ "sequence": 2 }));

    let invalid_range = format!(
        "{ALERTS_URL}?{}",
        alert_list_query(
            &AlertListParams {
                alert_class: None,
                start_time: Some(timestamp + TimeDelta::microseconds(1)),
                end_time: Some(timestamp),
            },
            None,
        ),
    );
    NexusRequest::new(
        RequestBuilder::new(client, Method::GET, &invalid_range)
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("invalid alert time range should be rejected");
}
