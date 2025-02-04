// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of queries for webhook deliveries

use crate::db::model::schema::webhook_delivery::{self, dsl};
use crate::db::model::WebhookDelivery;
use crate::db::raw_query_builder::{QueryBuilder, TypedSqlQuery};
use diesel::pg::Pg;
use diesel::prelude::QueryResult;
use diesel::query_builder::{AstPass, Query, QueryFragment, QueryId};
use diesel::sql_types;
use diesel::Column;
use diesel::QuerySource;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::WebhookEventUuid;
use omicron_uuid_kinds::WebhookReceiverUuid;

pub struct WebhookDeliveryDispatch {
    rx_id: WebhookReceiverUuid,
    event_id: WebhookEventUuid,
    insert: Box<dyn QueryFragment<Pg> + Send>,
}

impl WebhookDeliveryDispatch {
    pub fn new(delivery: WebhookDelivery) -> Self {
        let rx_id = delivery.rx_id.into();
        let event_id = delivery.event_id.into();
        let insert = Box::new(
            diesel::insert_into(dsl::webhook_delivery)
                .values(delivery)
                .on_conflict(dsl::id)
                .do_nothing(), // .returning(WebhookDelivery::as_returning())
        );
        Self { rx_id, event_id, insert }
    }
}

impl QueryFragment<Pg> for WebhookDeliveryDispatch {
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        self.insert.walk_ast(out.reborrow())?;
        // WHERE NOT EXISTS (
        //     SELECT 1 FROM omicron.public.webhook_delivery
        //     WHERE rx_id = $1 AND event_id = $2
        // )
        out.push_sql(" WHERE NOT EXISTS ( SELECT 1 ");
        dsl::webhook_delivery.from_clause().walk_ast(out.reborrow())?;
        out.push_sql(" WHERE ");
        out.push_identifier(dsl::rx_id::NAME);
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, _>(
            self.rx_id.as_untyped_uuid(),
        )?;
        out.push_sql(" AND ");
        out.push_identifier(dsl::event_id::NAME);
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, _>(
            self.event_id.as_untyped_uuid(),
        )?;
        out.push_sql(" )");
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::explain::ExplainableAsync;
    use crate::db::model;
    use crate::db::model::WebhookEvent;
    use crate::db::model::WebhookEventClass;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::raw_query_builder::expectorate_query_contents;

    use anyhow::Context;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use nexus_types::external_api::params;
    use nexus_types::identity::Resource;
    use omicron_common::api::external;
    use omicron_test_utils::dev;
    use uuid::Uuid;

    fn test_dispatch_query() -> WebhookDeliveryDispatch {
        let event = WebhookEvent {
            id: WebhookEventUuid::nil().into(),
            time_created: Utc::now(),
            time_dispatched: None,
            event_class: WebhookEventClass::Test,
            event: serde_json::json!({ "test": "data" }),
        };
        let delivery = WebhookDelivery::new(&event, WebhookReceiverId::nil());
        WebhookDeliveryDispatch::new(delivery)
    }

    #[tokio::test]
    async fn expectorate_delivery_dispatch_query() {
        expectorate_query_contents(
            &test_dispatch_query(),
            "tests/output/webhook_delivery_dispatch_query.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn explain_delivery_dispatch_query() {
        let logctx =
            dev::test_setup_log("explain_webhook_delivery_dispatch_query");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let query = test_dispatch_query();
        let explanation = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        eprintln!("{explanation}");

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
