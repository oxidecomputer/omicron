// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods for webhook events and event delivery dispatching.

use super::DataStore;
use crate::db::pool::DbConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use diesel::result::OptionalExtension;
use omicron_uuid_kinds::{GenericUuid, WebhookEventUuid};

use crate::db::model::WebhookEvent;
use crate::db::schema::webhook_event::dsl as event_dsl;

impl DataStore {
    /// Select the next webhook event in need of dispatching.
    ///
    /// This performs a `SELECT ... FOR UPDATE SKIP LOCKED` on the
    /// `webhook_event` table, returning the oldest webhook event which has not
    /// yet been dispatched to receivers and which is not actively being
    /// dispatched in another transaction.
    // NOTE: it would be kinda nice if this query could also select the
    // webhook receivers subscribed to this event, but I am not totally sure
    // what the CRDB semantics of joining on another table in a `SELECT ... FOR
    // UPDATE SKIP LOCKED` query are. We don't want to inadvertantly also lock
    // the webhook receivers...
    pub async fn webhook_event_select_for_dispatch(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<Option<WebhookEvent>, diesel::result::Error> {
        event_dsl::webhook_event
            .filter(event_dsl::time_dispatched.is_null())
            .order_by(event_dsl::time_created.asc())
            .limit(1)
            .for_update()
            .skip_locked()
            .select(WebhookEvent::as_select())
            .get_result_async(conn)
            .await
            .optional()
    }

    /// Mark the webhook event with the provided UUID as dispatched.
    pub async fn webhook_event_set_dispatched(
        &self,
        event_id: &WebhookEventUuid,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<(), diesel::result::Error> {
        diesel::update(event_dsl::webhook_event)
            .filter(event_dsl::id.eq(event_id.into_untyped_uuid()))
            .filter(event_dsl::time_dispatched.is_null())
            .set(event_dsl::time_dispatched.eq(diesel::dsl::now))
            .execute_async(conn)
            .await
            .map(|_| ()) // this should always be 1...
    }
}
