// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods for webhook receiver management.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::datastore::RunnableQuery;
use crate::db::error::public_error_from_diesel;
use crate::db::error::retryable;
use crate::db::error::ErrorHandler;
use crate::db::model::Generation;
use crate::db::model::WebhookGlob;
use crate::db::model::WebhookReceiver;
use crate::db::model::WebhookReceiverIdentity;
use crate::db::model::WebhookRxSubscription;
use crate::db::pool::DbConnection;
use crate::db::TransactionError;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use diesel::result::OptionalExtension;
use nexus_types::external_api::params;
use nexus_types::identity::Resource;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_uuid_kinds::{GenericUuid, WebhookReceiverUuid};

use crate::db::schema::webhook_rx::dsl as rx_dsl;
use crate::db::schema::webhook_rx_subscription::dsl as subscription_dsl;

use std::str::FromStr;

impl DataStore {
    pub async fn webhook_rx_create(
        &self,
        opctx: &OpContext,
        params: params::WebhookCreate,
    ) -> CreateResult<WebhookReceiver> {
        // TODO(eliza): someday we gotta allow creating webhooks with more
        // restrictive permissions...
        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;

        let conn = self.pool_connection_authorized(opctx).await?;
        let params::WebhookCreate {
            identity,
            endpoint,
            secrets,
            events,
            disable_probes,
        } = params;

        let globs = events
            .iter()
            .map(|s| WebhookGlob::from_str(s))
            .collect::<Result<Vec<_>, _>>()?;

        let rx = self
            .transaction_retry_wrapper("webhook_rx_create")
            .transaction(&conn, |conn| {
                // make a fresh UUID for each transaction, in case the
                // transaction fails because of a UUID collision.
                //
                // this probably won't happen, but, ya know...
                let id = WebhookReceiverUuid::new_v4();
                let receiver = WebhookReceiver {
                    identity: WebhookReceiverIdentity::new(
                        id.into_untyped_uuid(),
                        identity.clone(),
                    ),
                    endpoint: endpoint.to_string(),
                    probes_enabled: !disable_probes,
                    rcgen: Generation::new(),
                };
                let globs = globs.clone();
                async move {
                    let rx = diesel::insert_into(rx_dsl::webhook_rx)
                        .values(receiver)
                        .returning(WebhookReceiver::as_returning())
                        .get_result_async(&conn)
                        .await?;
                    for glob in globs {
                        match self
                            .webhook_add_subscription_on_conn(
                                WebhookRxSubscription::new(id, glob),
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
                    Ok(rx)
                }
            })
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::WebhookReceiver,
                        identity.name.as_str(),
                    ),
                )
            })?;
        Ok(rx)
    }

    async fn webhook_add_subscription_on_conn(
        &self,
        subscription: WebhookRxSubscription,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<WebhookRxSubscription, AsyncInsertError> {
        let rx_id = subscription.rx_id.into_untyped_uuid();
        let subscription: WebhookRxSubscription =
            WebhookReceiver::insert_resource(
                rx_id,
                diesel::insert_into(subscription_dsl::webhook_rx_subscription)
                    .values(subscription)
                    .on_conflict((
                        subscription_dsl::rx_id,
                        subscription_dsl::event_class,
                    ))
                    .do_update()
                    .set(subscription_dsl::time_created.eq(diesel::dsl::now)),
            )
            .insert_and_get_result_async(conn)
            .await?;
        Ok(subscription)
    }

    /// List all webhook receivers whose event class subscription globs match
    /// the provided `event_class`.
    // TODO(eliza): probably paginate this...
    pub async fn webhook_rx_list_subscribed_to_event(
        &self,
        opctx: &OpContext,
        event_class: impl ToString,
    ) -> ListResultVec<(WebhookReceiver, WebhookRxSubscription)> {
        use async_bb8_diesel::AsyncSimpleConnection;
        let conn = self.pool_connection_authorized(opctx).await?;
        let class = event_class.to_string();
        conn.transaction_async(|conn| async move {
            // TODO(eliza): this sucks, don't evaluate the globs every time we
            // dispatch an event; do them at webhook-creation time instead, OR
            // when updating to a sw version with new event classes...
            // This currently requires a full table scan over
            // `webhook_rx_subscription`, as we cannot create indices to
            // accelerate text search in the present CRDB version... :(
            conn.batch_execute_async(
                crate::db::queries::ALLOW_FULL_TABLE_SCAN_SQL,
            )
            .await
            .unwrap();
            Self::rx_list_subscribed_query(class)
                .load_async::<(WebhookReceiver, WebhookRxSubscription)>(&conn)
                .await
        })
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    fn rx_list_subscribed_query(
        event_class: String,
    ) -> impl RunnableQuery<(WebhookReceiver, WebhookRxSubscription)> {
        subscription_dsl::webhook_rx_subscription
            .filter(
                event_class
                    .to_string()
                    .into_sql::<diesel::sql_types::Text>()
                    .similar_to(subscription_dsl::similar_to),
            )
            .order_by(subscription_dsl::rx_id.asc())
            .inner_join(
                rx_dsl::webhook_rx.on(subscription_dsl::rx_id.eq(rx_dsl::id)),
            )
            .filter(rx_dsl::time_deleted.is_null())
            .select((
                WebhookReceiver::as_select(),
                WebhookRxSubscription::as_select(),
            ))
    }
    // pub async fn webhook_rx_list(
    //     &self,
    //     opctx: &OpContext,
    // ) -> ListResultVec<WebhookReceiver> {
    //     let conn = self.pool_connection_authorized(opctx).await?;
    //     rx_dsl::webhook_rx
    //         .filter(rx_dsl::time_deleted.is_null())
    //         .select(WebhookReceiver::as_select())
    //         .load_async::<WebhookReceiver>(&*conn)
    //         .await
    //         .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    // }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::db::explain::ExplainableAsync;
    use crate::db::pub_test_utils::TestDatabase;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_test_utils::dev::{self, LogContext};

    #[tokio::test]
    async fn test_event_class_globs() {
        // Test setup
        let logctx = dev::test_setup_log("test_event_class_globs");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let foo_star = datastore
            .webhook_rx_create(
                opctx,
                params::WebhookCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "foo-star".parse().unwrap(),
                        description: String::new(),
                    },
                    endpoint: "http://foo.star".parse().unwrap(),
                    secrets: Vec::new(),
                    events: vec!["foo.*".to_string()],
                    disable_probes: false,
                },
            )
            .await
            .unwrap();

        let foo_starstar = datastore
            .webhook_rx_create(
                opctx,
                params::WebhookCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "foo-starstar".parse().unwrap(),
                        description: String::new(),
                    },
                    endpoint: "http://foo.starstar".parse().unwrap(),
                    secrets: Vec::new(),
                    events: vec!["foo.**".to_string()],
                    disable_probes: false,
                },
            )
            .await
            .unwrap();

        let foo_bar = datastore
            .webhook_rx_create(
                opctx,
                params::WebhookCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "foo-bar".parse().unwrap(),
                        description: String::new(),
                    },
                    endpoint: "http://foo.bar".parse().unwrap(),
                    secrets: Vec::new(),
                    events: vec!["foo.bar".to_string()],
                    disable_probes: false,
                },
            )
            .await
            .unwrap();

        let foo_starstar_bar = datastore
            .webhook_rx_create(
                opctx,
                params::WebhookCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "foo-starstar-bar".parse().unwrap(),
                        description: String::new(),
                    },
                    endpoint: "http://foo.starstar.bar".parse().unwrap(),
                    secrets: Vec::new(),
                    events: vec!["foo.**.bar".parse().unwrap()],
                    disable_probes: false,
                },
            )
            .await
            .unwrap();

        let starstar_bar = datastore
            .webhook_rx_create(
                opctx,
                params::WebhookCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "starstar-bar".parse().unwrap(),
                        description: String::new(),
                    },
                    endpoint: "http://starstar.bar".parse().unwrap(),
                    secrets: Vec::new(),
                    events: vec!["**.bar".parse().unwrap()],
                    disable_probes: false,
                },
            )
            .await
            .unwrap();

        async fn check_event(
            datastore: &DataStore,
            opctx: &OpContext,
            // logctx: &LogContext,
            event_class: &str,
            matches: &[&WebhookReceiver],
            not_matches: &[&WebhookReceiver],
        ) {
            let subscribed = datastore
                .webhook_rx_list_subscribed_to_event(opctx, event_class)
                .await
                .unwrap()
                .into_iter()
                .map(|(rx, subscription)| {
                   eprintln!("receiver is subscribed to event {event_class:?}:\n\trx: {rx:?}\n\tsubscription: {subscription:?}");
                   rx.identity
                })
                .collect::<Vec<_>>();

            for rx in matches {
                assert!(
                    subscribed.contains(&rx.identity),
                    "expected {rx:?} to be subscribed to {event_class:?}"
                );
            }

            for rx in not_matches {
                assert!(
                    !subscribed.contains(&rx.identity),
                    "expected {rx:?} to not be subscribed to {event_class:?}"
                );
            }
        }

        check_event(
            datastore,
            opctx,
            "notfoo",
            &[],
            &[
                &foo_star,
                &foo_starstar,
                &foo_bar,
                &foo_starstar_bar,
                &starstar_bar,
            ],
        )
        .await;

        check_event(
            datastore,
            opctx,
            "foo.bar",
            &[&foo_star, &foo_starstar, &foo_bar, &starstar_bar],
            &[&foo_starstar_bar],
        )
        .await;

        check_event(
            datastore,
            opctx,
            "foo.baz",
            &[&foo_star, &foo_starstar],
            &[&foo_bar, &foo_starstar_bar, &starstar_bar],
        )
        .await;

        check_event(
            datastore,
            opctx,
            "foo.bar.baz",
            &[&foo_starstar],
            &[&foo_bar, &foo_star, &foo_starstar_bar, &starstar_bar],
        )
        .await;

        check_event(
            datastore,
            opctx,
            "foo.baz.bar",
            &[&foo_starstar, &foo_starstar_bar, &starstar_bar],
            &[&foo_bar, &foo_star],
        )
        .await;

        check_event(
            datastore,
            opctx,
            "foo.baz.quux.bar",
            &[&foo_starstar, &foo_starstar_bar, &starstar_bar],
            &[&foo_bar, &foo_star],
        )
        .await;

        check_event(
            datastore,
            opctx,
            "baz.quux.bar",
            &[&starstar_bar],
            &[&foo_bar, &foo_star, &foo_starstar, &foo_starstar_bar],
        )
        .await;
        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn explain_event_class_glob() {
        let logctx = dev::test_setup_log("explain_event_class_glob");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let query = DataStore::rx_list_subscribed_query("foo.bar".to_string());
        let explanation = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");
        println!("{explanation}");

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
