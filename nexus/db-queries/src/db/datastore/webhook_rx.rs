// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods for webhook receiver management.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::error::public_error_from_diesel;
use crate::db::error::retryable;
use crate::db::error::ErrorHandler;
use crate::db::model::WebhookGlob;
use crate::db::model::WebhookReceiver;
use crate::db::model::WebhookRxSubscription;
use crate::db::pool::DbConnection;
use crate::db::TransactionError;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use diesel::result::OptionalExtension;
use nexus_types::identity::Resource;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_uuid_kinds::{GenericUuid, WebhookReceiverUuid};

impl DataStore {
    pub async fn webhook_rx_create(
        &self,
        opctx: &OpContext,
        receiver: &WebhookReceiver,
        subscriptions: &[WebhookGlob],
    ) -> CreateResult<WebhookReceiver> {
        use crate::db::schema::webhook_rx::dsl;
        // TODO(eliza): someday we gotta allow creating webhooks with more
        // restrictive permissions...
        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;

        let conn = self.pool_connection_authorized(opctx).await?;
        let rx_id = WebhookReceiverUuid::from_untyped_uuid(receiver.id());
        self.transaction_retry_wrapper("webhook_rx_create")
            .transaction(&conn, |conn| {
                let receiver = receiver.clone();
                async move {
                    diesel::insert_into(dsl::webhook_rx)
                        .values(receiver)
                        // .on_conflict(dsl::id)
                        // .do_update()
                        // .set(dsl::time_modified.eq(dsl::time_modified))
                        // .returning(WebhookReceiver::as_returning())
                        .execute_async(&conn)
                        .await?;
                    // .map_err(|e| {
                    //     if retryable(&e) {
                    //         return TransactionError::Database(e);
                    //     };
                    //     TransactionError::CustomError(public_error_from_diesel(
                    //         e,
                    //         ErrorHandler::Conflict(
                    //             ResourceType::WebhookReceiver,
                    //             receiver.identity.name.as_str(),
                    //         ),
                    //     ))
                    // })?;
                    for glob in subscriptions {
                        match self
                            .webhook_add_subscription_on_conn(
                                WebhookRxSubscription::new(rx_id, glob.clone()),
                                &conn,
                            )
                            .await
                        {
                            Ok(_) => {}
                            Err(AsyncInsertError::CollectionNotFound) => {} // we just created it?
                            Err(AsyncInsertError::DatabaseError(e)) => {
                                return Err(e);
                            }
                        }
                    }
                    // TODO(eliza): secrets go here...
                    Ok(())
                }
            })
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::WebhookReceiver,
                        receiver.name().as_str(),
                    ),
                )
            })?;
        Ok(receiver.clone())
    }

    async fn webhook_add_subscription_on_conn(
        &self,
        subscription: WebhookRxSubscription,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<WebhookRxSubscription, AsyncInsertError> {
        use crate::db::schema::webhook_rx_subscription::dsl;
        let rx_id = subscription.rx_id.into_untyped_uuid();
        let subscription: WebhookRxSubscription =
            WebhookReceiver::insert_resource(
                rx_id,
                diesel::insert_into(dsl::webhook_rx_subscription)
                    .values(subscription)
                    .on_conflict((dsl::rx_id, dsl::event_class))
                    .do_update()
                    .set(dsl::time_created.eq(diesel::dsl::now)),
            )
            .insert_and_get_result_async(conn)
            .await?;
        Ok(subscription)
    }
}
