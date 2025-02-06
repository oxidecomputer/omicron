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
use crate::db::error::ErrorHandler;
use crate::db::model::Generation;
use crate::db::model::WebhookEventClass;
use crate::db::model::WebhookGlob;
use crate::db::model::WebhookReceiver;
use crate::db::model::WebhookReceiverConfig;
use crate::db::model::WebhookReceiverIdentity;
use crate::db::model::WebhookRxEventGlob;
use crate::db::model::WebhookRxSecret;
use crate::db::model::WebhookRxSubscription;
use crate::db::model::WebhookSubscriptionKind;
use crate::db::pagination::paginated;
use crate::db::pool::DbConnection;
use crate::db::schema::webhook_receiver::dsl as rx_dsl;
use crate::db::schema::webhook_rx_event_glob::dsl as glob_dsl;
use crate::db::schema::webhook_rx_secret::dsl as secret_dsl;
use crate::db::schema::webhook_rx_subscription::dsl as subscription_dsl;
use crate::db::TransactionError;
use crate::transaction_retry::OptionalError;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use nexus_types::external_api::params;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::ResourceType;
use omicron_uuid_kinds::{GenericUuid, WebhookReceiverUuid};
use uuid::Uuid;

impl DataStore {
    pub async fn webhook_rx_create(
        &self,
        opctx: &OpContext,
        params: params::WebhookCreate,
    ) -> CreateResult<WebhookReceiverConfig> {
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

        let subscriptions = events
            .into_iter()
            .map(WebhookSubscriptionKind::new)
            .collect::<Result<Vec<_>, _>>()?;
        let err = OptionalError::new();
        let (rx, secrets) = self
            .transaction_retry_wrapper("webhook_rx_create")
            .transaction(&conn, |conn| {
                // make a fresh UUID for each transaction, in case the
                // transaction fails because of a UUID collision.
                //
                // this probably won't happen, but, ya know...
                let id = WebhookReceiverUuid::new_v4();
                let receiver = WebhookReceiver {
                    identity: WebhookReceiverIdentity::new(
                        id,
                        identity.clone(),
                    ),
                    endpoint: endpoint.to_string(),
                    probes_enabled: !disable_probes,
                    rcgen: Generation::new(),
                };
                let subscriptions = subscriptions.clone();
                let secret_keys = secrets.clone();
                let err = err.clone();
                async move {
                    let rx = diesel::insert_into(rx_dsl::webhook_receiver)
                        .values(receiver)
                        .returning(WebhookReceiver::as_returning())
                        .get_result_async(&conn)
                        .await?;
                    for subscription in subscriptions {
                        self.add_subscription_on_conn(
                            opctx,
                            rx.identity.id.into(),
                            subscription,
                            &conn,
                        )
                        .await
                        .map_err(|e| match e {
                            TransactionError::CustomError(e) => err.bail(e),
                            TransactionError::Database(e) => e,
                        })?;
                    }
                    let mut secrets = Vec::with_capacity(secret_keys.len());
                    for secret in secret_keys {
                        let secret = self
                            .add_secret_on_conn(
                                WebhookRxSecret::new(id, secret),
                                &conn,
                            )
                            .await
                            .map_err(|e| match e {
                                TransactionError::CustomError(e) => err.bail(e),
                                TransactionError::Database(e) => e,
                            })?;
                        secrets.push(secret);
                    }
                    Ok((rx, secrets))
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    return err;
                }
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::WebhookReceiver,
                        identity.name.as_str(),
                    ),
                )
            })?;
        Ok(WebhookReceiverConfig { rx, secrets, events: subscriptions })
    }

    pub async fn webhook_rx_config_fetch(
        &self,
        opctx: &OpContext,
        authz_rx: &authz::WebhookReceiver,
    ) -> Result<(Vec<WebhookSubscriptionKind>, Vec<WebhookRxSecret>), Error>
    {
        opctx.authorize(authz::Action::ListChildren, authz_rx).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let subscriptions =
            self.webhook_rx_subscription_list_on_conn(authz_rx, &conn).await?;
        let secrets =
            self.webhook_rx_secret_list_on_conn(authz_rx, &conn).await?;
        Ok((subscriptions, secrets))
    }

    //
    // Subscriptions
    //

    pub async fn webhook_rx_subscription_add(
        &self,
        opctx: &OpContext,
        authz_rx: &authz::WebhookReceiver,
        subscription: WebhookSubscriptionKind,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::CreateChild, authz_rx).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let num_created = self
            .add_subscription_on_conn(opctx, authz_rx.id(), subscription, &conn)
            .await
            .map_err(|e| match e {
                TransactionError::CustomError(e) => e,
                TransactionError::Database(e) => public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_rx),
                ),
            })?;

        slog::debug!(
            &opctx.log,
            "added {num_created} webhook subscriptions";
            "webhook_id" => %authz_rx.id(),
        );

        Ok(())
    }

    async fn webhook_rx_subscription_list_on_conn(
        &self,
        authz_rx: &authz::WebhookReceiver,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> ListResultVec<WebhookSubscriptionKind> {
        let rx_id = authz_rx.id().into_untyped_uuid();

        // First, get all the exact subscriptions that aren't from globs.
        let exact = subscription_dsl::webhook_rx_subscription
            .filter(subscription_dsl::rx_id.eq(rx_id))
            .filter(subscription_dsl::glob.is_null())
            .select(subscription_dsl::event_class)
            .load_async::<WebhookEventClass>(conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_rx),
                )
            })?;
        // Then, get the globs
        let globs = glob_dsl::webhook_rx_event_glob
            .filter(glob_dsl::rx_id.eq(rx_id))
            .select(WebhookGlob::as_select())
            .load_async::<WebhookGlob>(conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_rx),
                )
            })?;
        let subscriptions = exact
            .into_iter()
            .map(WebhookSubscriptionKind::Exact)
            .chain(globs.into_iter().map(WebhookSubscriptionKind::Glob))
            .collect::<Vec<_>>();
        Ok(subscriptions)
    }

    async fn add_subscription_on_conn(
        &self,
        opctx: &OpContext,
        rx_id: WebhookReceiverUuid,
        subscription: WebhookSubscriptionKind,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<usize, TransactionError<Error>> {
        match subscription {
            WebhookSubscriptionKind::Exact(event_class) => self
                .add_exact_sub_on_conn(
                    WebhookRxSubscription::exact(rx_id, event_class),
                    &conn,
                )
                .await
                .map(|_| 1),
            WebhookSubscriptionKind::Glob(glob) => {
                let glob = WebhookRxEventGlob::new(rx_id, glob);
                let glob: WebhookRxEventGlob =
                    WebhookReceiver::insert_resource(
                        rx_id.into_untyped_uuid(),
                        diesel::insert_into(glob_dsl::webhook_rx_event_glob)
                            .values(glob)
                            .on_conflict((glob_dsl::rx_id, glob_dsl::glob))
                            .do_update()
                            .set(glob_dsl::time_created.eq(diesel::dsl::now)),
                    )
                    .insert_and_get_result_async(conn)
                    .await
                    .map_err(async_insert_error_to_txn(rx_id))?;
                self.glob_generate_exact_subs(opctx, &glob, conn).await
            }
        }
    }

    async fn add_exact_sub_on_conn(
        &self,
        subscription: WebhookRxSubscription,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<WebhookRxSubscription, TransactionError<Error>> {
        let rx_id = WebhookReceiverUuid::from(subscription.rx_id);
        let subscription: WebhookRxSubscription =
            WebhookReceiver::insert_resource(
                rx_id.into_untyped_uuid(),
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
            .await
            .map_err(async_insert_error_to_txn(rx_id))?;
        Ok(subscription)
    }

    async fn glob_generate_exact_subs(
        &self,
        opctx: &OpContext,
        glob: &WebhookRxEventGlob,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<usize, TransactionError<Error>> {
        let regex = match regex::Regex::new(&glob.glob.regex) {
            Ok(r) => r,
            Err(error) => {
                const MSG: &str =
                    "webhook glob subscription regex was not a valid regex";
                slog::error!(
                    &opctx.log,
                    "{MSG}";
                    "glob" => ?glob.glob.glob,
                    "regex" => ?glob.glob.regex,
                    "error" => %error,
                );
                return Err(TransactionError::CustomError(
                    Error::internal_error(MSG),
                ));
            }
        };
        let mut created = 0;
        for &class in WebhookEventClass::ALL_CLASSES {
            if !regex.is_match(class.as_str()) {
                slog::debug!(
                    &opctx.log,
                    "webhook glob does not matche event class";
                    "webhook_id" => ?glob.rx_id,
                    "glob" => ?glob.glob.glob,
                    "regex" => ?regex,
                    "event_class" => %class,
                );
                continue;
            }

            slog::debug!(
                &opctx.log,
                "webhook glob matches event class";
                "webhook_id" => ?glob.rx_id,
                "glob" => ?glob.glob.glob,
                "regex" => ?regex,
                "event_class" => %class,
            );
            self.add_exact_sub_on_conn(
                WebhookRxSubscription::for_glob(&glob, class),
                conn,
            )
            .await?;
            created += 1;
        }

        slog::info!(
            &opctx.log,
            "created {created} webhook subscriptions for glob";
            "webhook_id" => ?glob.rx_id,
            "glob" => ?glob.glob.glob,
            "regex" => ?regex,
        );

        Ok(created)
    }

    /// List all webhook receivers whose event class subscription globs match
    /// the provided `event_class`.
    pub async fn webhook_rx_list_subscribed_to_event(
        &self,
        opctx: &OpContext,
        event_class: WebhookEventClass,
    ) -> Result<Vec<(WebhookReceiver, WebhookRxSubscription)>, Error> {
        let conn = self.pool_connection_authorized(opctx).await?;
        Self::rx_list_subscribed_query(event_class)
            .load_async::<(WebhookReceiver, WebhookRxSubscription)>(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    fn rx_list_subscribed_query(
        event_class: WebhookEventClass,
    ) -> impl RunnableQuery<(WebhookReceiver, WebhookRxSubscription)> {
        subscription_dsl::webhook_rx_subscription
            .filter(subscription_dsl::event_class.eq(event_class))
            .order_by(subscription_dsl::rx_id.asc())
            .inner_join(
                rx_dsl::webhook_receiver
                    .on(subscription_dsl::rx_id.eq(rx_dsl::id)),
            )
            .filter(rx_dsl::time_deleted.is_null())
            .select((
                WebhookReceiver::as_select(),
                WebhookRxSubscription::as_select(),
            ))
    }

    //
    // Secrets
    //

    pub async fn webhook_rx_secret_list(
        &self,
        opctx: &OpContext,
        authz_rx: &authz::WebhookReceiver,
    ) -> ListResultVec<WebhookRxSecret> {
        opctx.authorize(authz::Action::ListChildren, authz_rx).await?;
        let conn = self.pool_connection_authorized(&opctx).await?;
        self.webhook_rx_secret_list_on_conn(authz_rx, &conn).await
    }

    async fn webhook_rx_secret_list_on_conn(
        &self,
        authz_rx: &authz::WebhookReceiver,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> ListResultVec<WebhookRxSecret> {
        secret_dsl::webhook_rx_secret
            .filter(secret_dsl::rx_id.eq(authz_rx.id().into_untyped_uuid()))
            .filter(secret_dsl::time_deleted.is_null())
            .select(WebhookRxSecret::as_select())
            .load_async(conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_rx),
                )
            })
    }

    async fn add_secret_on_conn(
        &self,
        secret: WebhookRxSecret,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<WebhookRxSecret, TransactionError<Error>> {
        let rx_id = secret.rx_id;
        let secret: WebhookRxSecret = WebhookReceiver::insert_resource(
            rx_id.into_untyped_uuid(),
            diesel::insert_into(secret_dsl::webhook_rx_secret).values(secret),
        )
        .insert_and_get_result_async(conn)
        .await
        .map_err(async_insert_error_to_txn(rx_id.into()))?;
        Ok(secret)
    }

    pub async fn webhook_rx_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<WebhookReceiver> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        paginated(rx_dsl::webhook_receiver, rx_dsl::id, pagparams)
            .filter(rx_dsl::time_deleted.is_null())
            .select(WebhookReceiver::as_select())
            .load_async::<WebhookReceiver>(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}

fn async_insert_error_to_txn(
    rx_id: WebhookReceiverUuid,
) -> impl FnOnce(AsyncInsertError) -> TransactionError<Error> {
    move |e| match e {
        AsyncInsertError::CollectionNotFound => {
            TransactionError::CustomError(Error::not_found_by_id(
                ResourceType::WebhookReceiver,
                &rx_id.into_untyped_uuid(),
            ))
        }
        AsyncInsertError::DatabaseError(e) => TransactionError::Database(e),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::db::explain::ExplainableAsync;
    use crate::db::pub_test_utils::TestDatabase;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_test_utils::dev;

    #[tokio::test]
    async fn test_event_class_globs() {
        // Test setup
        let logctx = dev::test_setup_log("test_event_class_globs");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let mut all_rxs = Vec::new();
        async fn create_rx(
            datastore: &DataStore,
            opctx: &OpContext,
            all_rxs: &mut Vec<WebhookReceiverConfig>,
            name: &str,
            subscription: &str,
        ) -> WebhookReceiverConfig {
            let rx = datastore
                .webhook_rx_create(
                    opctx,
                    params::WebhookCreate {
                        identity: IdentityMetadataCreateParams {
                            name: name.parse().unwrap(),
                            description: String::new(),
                        },
                        endpoint: format!("http://{name}").parse().unwrap(),
                        secrets: vec![name.to_string()],
                        events: vec![subscription.to_string()],
                        disable_probes: false,
                    },
                )
                .await
                .unwrap();
            all_rxs.push(rx.clone());
            rx
        }

        let test_star =
            create_rx(&datastore, &opctx, &mut all_rxs, "test-star", "test.*")
                .await;
        let test_starstar = create_rx(
            &datastore,
            &opctx,
            &mut all_rxs,
            "test-starstar",
            "test.**",
        )
        .await;
        let test_foo_star = create_rx(
            &datastore,
            &opctx,
            &mut all_rxs,
            "test-foo-star",
            "test.foo.*",
        )
        .await;
        let test_star_baz = create_rx(
            &datastore,
            &opctx,
            &mut all_rxs,
            "test-star-baz",
            "test.*.baz",
        )
        .await;
        let test_starstar_baz = create_rx(
            &datastore,
            &opctx,
            &mut all_rxs,
            "test-starstar-baz",
            "test.**.baz",
        )
        .await;
        let test_quux_star = create_rx(
            &datastore,
            &opctx,
            &mut all_rxs,
            "test-quux-star",
            "test.quux.*",
        )
        .await;
        let test_quux_starstar = create_rx(
            &datastore,
            &opctx,
            &mut all_rxs,
            "test-quux-starstar",
            "test.quux.**",
        )
        .await;

        async fn check_event(
            datastore: &DataStore,
            opctx: &OpContext,
            all_rxs: &Vec<WebhookReceiverConfig>,
            event_class: WebhookEventClass,
            matches: &[&WebhookReceiverConfig],
        ) {
            let subscribed = datastore
                .webhook_rx_list_subscribed_to_event(opctx, event_class)
                .await
                .unwrap()
                .into_iter()
                .map(|(rx, subscription)| {
                    eprintln!(
                        "receiver is subscribed to event {event_class}:\n\t\
                            rx: {} ({})\n\tsubscription: {subscription:?}",
                        rx.identity.name, rx.identity.id,
                    );
                    rx.identity
                })
                .collect::<Vec<_>>();

            for WebhookReceiverConfig { rx, events, .. } in matches {
                assert!(
                    subscribed.contains(&rx.identity),
                    "expected {rx:?} to be subscribed to {event_class}\n\
                     subscriptions: {events:?}"
                );
            }

            let not_matches = all_rxs.iter().filter(
                |WebhookReceiverConfig { rx, .. }| {
                    matches
                        .iter()
                        .all(|match_rx| rx.identity != match_rx.rx.identity)
                },
            );
            for WebhookReceiverConfig { rx, events, .. } in not_matches {
                assert!(
                    !subscribed.contains(&rx.identity),
                    "expected {rx:?} to not be subscribed to {event_class}\n\
                     subscriptions: {events:?}"
                );
            }
        }

        check_event(
            datastore,
            opctx,
            &all_rxs,
            WebhookEventClass::TestFoo,
            &[&test_star, &test_starstar],
        )
        .await;
        check_event(
            datastore,
            opctx,
            &all_rxs,
            WebhookEventClass::TestFooBar,
            &[&test_starstar, &test_foo_star],
        )
        .await;
        check_event(
            datastore,
            opctx,
            &all_rxs,
            WebhookEventClass::TestFooBaz,
            &[
                &test_starstar,
                &test_foo_star,
                &test_star_baz,
                &test_starstar_baz,
            ],
        )
        .await;
        check_event(
            datastore,
            opctx,
            &all_rxs,
            WebhookEventClass::TestQuuxBar,
            &[&test_starstar, &test_quux_star, &test_quux_starstar],
        )
        .await;
        check_event(
            datastore,
            opctx,
            &all_rxs,
            WebhookEventClass::TestQuuxBarBaz,
            &[&test_starstar, &test_quux_starstar, &test_starstar_baz],
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

        let query =
            DataStore::rx_list_subscribed_query(WebhookEventClass::TestFooBar);
        let explanation = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");
        println!("{explanation}");

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
