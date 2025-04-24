// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods for webhook receiver management.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::TransactionError;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::datastore::RunnableQuery;
use crate::db::datastore::SQL_BATCH_SIZE;
use crate::db::error::ErrorHandler;
use crate::db::error::public_error_from_diesel;
use crate::db::model::Generation;
use crate::db::model::Name;
use crate::db::model::SCHEMA_VERSION;
use crate::db::model::SemverVersion;
use crate::db::model::WebhookEventClass;
use crate::db::model::WebhookGlob;
use crate::db::model::WebhookReceiver;
use crate::db::model::WebhookReceiverConfig;
use crate::db::model::WebhookReceiverIdentity;
use crate::db::model::WebhookRxEventGlob;
use crate::db::model::WebhookRxSubscription;
use crate::db::model::WebhookSecret;
use crate::db::model::WebhookSubscriptionKind;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use crate::db::pagination::paginated_multicolumn;
use crate::db::pool::DbConnection;
use crate::db::update_and_check::UpdateAndCheck;
use crate::transaction_retry::OptionalError;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use nexus_auth::authz::ApiResource;
use nexus_db_schema::schema::webhook_delivery::dsl as delivery_dsl;
use nexus_db_schema::schema::webhook_delivery_attempt::dsl as delivery_attempt_dsl;
use nexus_db_schema::schema::webhook_event::dsl as event_dsl;
use nexus_db_schema::schema::webhook_receiver::dsl as rx_dsl;
use nexus_db_schema::schema::webhook_rx_event_glob::dsl as glob_dsl;
use nexus_db_schema::schema::webhook_rx_subscription::dsl as subscription_dsl;
use nexus_db_schema::schema::webhook_secret::dsl as secret_dsl;
use nexus_types::external_api::params;
use nexus_types::identity::Resource;
use nexus_types::internal_api::background::WebhookGlobStatus;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::WebhookReceiverUuid;
use ref_cast::RefCast;
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
        let params::WebhookCreate { identity, endpoint, secrets, events } =
            params;

        let subscriptions = events
            .into_iter()
            .map(WebhookSubscriptionKind::try_from)
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
                    secret_gen: Generation::new(),
                    subscription_gen: Generation::new(),
                };
                let subscriptions = subscriptions.clone();
                let secret_keys = secrets.clone();
                let err = err.clone();
                let name = identity.name.clone();
                async move {
                    let rx = diesel::insert_into(rx_dsl::webhook_receiver)
                        .values(receiver)
                        .returning(WebhookReceiver::as_returning())
                        .get_result_async(&conn)
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                public_error_from_diesel(
                                    e,
                                    ErrorHandler::Conflict(
                                        ResourceType::WebhookReceiver,
                                        name.as_str(),
                                    ),
                                )
                            })
                        })?;
                    let rx_id = rx.identity.id.into();

                    let mut events = Vec::new();
                    for subscription in subscriptions {
                        let sub = self
                            .rx_add_subscription_on_conn(
                                opctx,
                                rx_id,
                                subscription,
                                &conn,
                            )
                            .await
                            .map_err(|e| match e {
                                TransactionError::CustomError(e) => err.bail(e),
                                TransactionError::Database(e) => e,
                            })?;
                        events.push(sub);
                    }

                    let mut secrets = Vec::with_capacity(secret_keys.len());
                    for secret in secret_keys {
                        let secret = self
                            .add_secret_on_conn(
                                WebhookSecret::new(id, secret),
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
    ) -> Result<(Vec<WebhookSubscriptionKind>, Vec<WebhookSecret>), Error> {
        opctx.authorize(authz::Action::ListChildren, authz_rx).await?;
        self.rx_config_fetch_on_conn(
            authz_rx.id(),
            &*self.pool_connection_authorized(opctx).await?,
        )
        .await
    }

    async fn rx_config_fetch_on_conn(
        &self,
        rx_id: WebhookReceiverUuid,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<(Vec<WebhookSubscriptionKind>, Vec<WebhookSecret>), Error> {
        let subscriptions =
            self.rx_subscription_list_on_conn(rx_id, &conn).await?;
        let secrets = self.rx_secret_list_on_conn(rx_id, &conn).await?;
        Ok((subscriptions, secrets))
    }

    pub async fn webhook_rx_delete(
        &self,
        opctx: &OpContext,
        authz_rx: &authz::WebhookReceiver,
        db_rx: &WebhookReceiver,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_rx).await?;
        let rx_id = authz_rx.id().into_untyped_uuid();

        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;
        self.transaction_retry_wrapper("webhook_rx_delete").transaction(
            &conn,
            |conn| {
                let err = err.clone();
                async move {
                    let now = chrono::Utc::now();
                    // Delete the webhook's secrets.
                    let secrets_deleted =
                        diesel::delete(secret_dsl::webhook_secret)
                            .filter(secret_dsl::rx_id.eq(rx_id))
                            .filter(secret_dsl::time_deleted.is_null())
                            .execute_async(&conn)
                            .await
                            .map_err(|e| {
                                err.bail_retryable_or_else(e, |e| {
                                    public_error_from_diesel(
                                        e,
                                        ErrorHandler::Server,
                                    )
                                    .internal_context(
                                        "failed to delete secrets",
                                    )
                                })
                            })?;

                    // Delete subscriptions and globs.
                    let exact_subscriptions_deleted = diesel::delete(
                        subscription_dsl::webhook_rx_subscription,
                    )
                    .filter(subscription_dsl::rx_id.eq(rx_id))
                    .execute_async(&conn)
                    .await
                    .map_err(|e| {
                        err.bail_retryable_or_else(e, |e| {
                            public_error_from_diesel(e, ErrorHandler::Server)
                                .internal_context(
                                    "failed to delete exact subscriptions",
                                )
                        })
                    })?;

                    let globs_deleted =
                        diesel::delete(glob_dsl::webhook_rx_event_glob)
                            .filter(glob_dsl::rx_id.eq(rx_id))
                            .execute_async(&conn)
                            .await
                            .map_err(|e| {
                                err.bail_retryable_or_else(e, |e| {
                                    public_error_from_diesel(
                                        e,
                                        ErrorHandler::Server,
                                    )
                                    .internal_context("failed to delete globs")
                                })
                            })?;

                    let deliveries_deleted =
                        diesel::delete(delivery_dsl::webhook_delivery)
                            .filter(delivery_dsl::rx_id.eq(rx_id))
                            .execute_async(&conn)
                            .await
                            .map_err(|e| {
                                err.bail_retryable_or_else(e, |e| {
                                    public_error_from_diesel(
                                        e,
                                        ErrorHandler::Server,
                                    )
                                    .internal_context(
                                        "failed to delete delivery records",
                                    )
                                })
                            })?;

                    let delivery_attempts_deleted = diesel::delete(
                        delivery_attempt_dsl::webhook_delivery_attempt,
                    )
                    .filter(delivery_attempt_dsl::rx_id.eq(rx_id))
                    .execute_async(&conn)
                    .await
                    .map_err(|e| {
                        err.bail_retryable_or_else(e, |e| {
                            public_error_from_diesel(e, ErrorHandler::Server)
                                .internal_context(
                                    "failed to delete delivery attempt records",
                                )
                        })
                    })?;
                    // Finally, mark the webhook receiver record as deleted,
                    // provided that none of its children were modified in the interim.
                    let deleted = diesel::update(rx_dsl::webhook_receiver)
                        .filter(rx_dsl::id.eq(rx_id))
                        .filter(rx_dsl::time_deleted.is_null())
                        .filter(rx_dsl::subscription_gen.eq(db_rx.subscription_gen))
                        .filter(rx_dsl::secret_gen.eq(db_rx.secret_gen))
                        .set(rx_dsl::time_deleted.eq(now))
                        .execute_async(&conn)
                        .await
                        .map_err(|e| err.bail_retryable_or_else(e, |e| {
                            public_error_from_diesel(e, ErrorHandler::Server)
                                .internal_context(
                                    "failed to mark receiver as deleted",
                                )
                        }))?;
                    if deleted == 0 {
                        return Err(err.bail(Error::conflict(
                            "deletion failed due to concurrent modification",
                        )));
                    }

                    slog::info!(
                        &opctx.log,
                        "deleted webhook receiver";
                        "rx_id" => %rx_id,
                        "rx_name" => %db_rx.identity.name,
                        "secrets_deleted" => ?secrets_deleted,
                        "exact_subscriptions_deleted" => ?exact_subscriptions_deleted,
                        "globs_deleted" => ?globs_deleted,
                        "deliveries_deleted" => ?deliveries_deleted,
                        "delivery_attempts_deleted" => ?delivery_attempts_deleted,
                    );

                    Ok(())
                }
            },
        ).await
        .map_err(|e| {
            if let Some(err) = err.take() {
                return err;
            }
            public_error_from_diesel(e, ErrorHandler::Server)
        })
    }

    pub async fn webhook_rx_update(
        &self,
        opctx: &OpContext,
        authz_rx: &authz::WebhookReceiver,
        params: params::WebhookReceiverUpdate,
    ) -> UpdateResult<WebhookReceiver> {
        opctx.authorize(authz::Action::Modify, authz_rx).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        let rx_id = authz_rx.id().into_untyped_uuid();
        let update = db::model::WebhookReceiverUpdate {
            subscription_gen: None,
            name: params.identity.name.map(db::model::Name),
            description: params.identity.description,
            endpoint: params.endpoint.as_ref().map(ToString::to_string),
            time_modified: chrono::Utc::now(),
        };
        let updated = diesel::update(rx_dsl::webhook_receiver)
            .filter(rx_dsl::id.eq(rx_id))
            .filter(rx_dsl::time_deleted.is_null())
            .set(update)
            .check_if_exists(rx_id)
            .execute_and_check(&conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_rx),
                )
            })?;
        Ok(updated.found)
    }

    pub async fn webhook_rx_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<WebhookReceiverConfig> {
        let conn = self.pool_connection_authorized(opctx).await?;

        // As we would like to return a list of `WebhookReceiverConfig` structs,
        // which own `Vec`s of the receiver's secrets and event class
        // subscriptions, we'll do this by first querying the database to load
        // all the receivers, and then querying for their individual lists of
        // secrets and event class subscriptions.
        //
        // This is a bit unfortunate, and it would be nicer to do this with
        // JOINs, but it's a bit hairy as the subscriptions come from both the
        // `webhook_rx_subscription` and `webhook_rx_glob` tables...

        let receivers = match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(rx_dsl::webhook_receiver, rx_dsl::id, &pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                rx_dsl::webhook_receiver,
                rx_dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(rx_dsl::time_deleted.is_null())
        .select(WebhookReceiver::as_select())
        .load_async(&*conn)
        .await
        .map_err(|e| {
            public_error_from_diesel(e, ErrorHandler::Server)
                .internal_context("failed to list receivers")
        })?;

        // Now that we've got the current page of receivers, go and get their
        // event subscriptions and secrets.
        let mut result = Vec::with_capacity(receivers.len());
        for rx in receivers {
            let secrets = self.rx_secret_list_on_conn(rx.id(), &conn).await?;
            let events =
                self.rx_subscription_list_on_conn(rx.id(), &conn).await?;
            result.push(WebhookReceiverConfig { rx, secrets, events });
        }

        Ok(result)
    }

    //
    // Subscriptions
    //

    pub async fn webhook_rx_is_subscribed_to_event(
        &self,
        opctx: &OpContext,
        authz_rx: &authz::WebhookReceiver,
        authz_event: &authz::WebhookEvent,
    ) -> Result<bool, Error> {
        opctx.authorize(authz::Action::Read, authz_rx).await?;

        let conn = self.pool_connection_authorized(opctx).await?;
        let rx_id = authz_rx.id();

        // Before we can check whether the receiver is subscribed to the
        // provided event, ensure that its glob subscriptions are up to date.
        let mut paginator = Paginator::new(SQL_BATCH_SIZE);
        while let Some(p) = paginator.next() {
            let batch = self
                .rx_list_reprocessable_globs_on_conn(
                    Some(authz_rx.id()),
                    &p.current_pagparams(),
                    &conn,
                )
                .await?;
            paginator = p.found_batch(&batch, &|glob| {
                (glob.rx_id.into_untyped_uuid(), glob.glob.glob.clone())
            });
            for glob in batch {
                slog::debug!(
                    opctx.log,
                    "reprocessing webhook glob subscription to checking if \
                     receiver is subscribed to event";
                    "rx_id" => ?rx_id,
                    "glob" => ?glob.glob.glob,
                    "prior_version" => ?glob.schema_version,
                    "current_version" => %SCHEMA_VERSION,
                );
                self.webhook_glob_reprocess(opctx, &glob).await.map_err(
                    |e| {
                        e.internal_context(format!(
                            "failed to reprocess glob {glob:?}"
                        ))
                    },
                )?;
            }
        }

        let event_class = event_dsl::webhook_event
            .filter(event_dsl::id.eq(authz_event.id().into_untyped_uuid()))
            .select(event_dsl::event_class)
            .single_value();
        subscription_dsl::webhook_rx_subscription
            .filter(subscription_dsl::rx_id.eq(rx_id.into_untyped_uuid()))
            .filter(subscription_dsl::event_class.nullable().eq(event_class))
            .select(subscription_dsl::rx_id)
            .first_async::<Uuid>(&*conn)
            .await
            .optional()
            .map(|x| x.is_some())
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Don't forget to like and subscribe!
    pub async fn webhook_rx_subscription_add(
        &self,
        opctx: &OpContext,
        authz_rx: &authz::WebhookReceiver,
        subscription: WebhookSubscriptionKind,
    ) -> CreateResult<()> {
        opctx.authorize(authz::Action::Modify, authz_rx).await?;
        self.rx_add_subscription_on_conn(
            opctx,
            authz_rx.id(),
            subscription,
            &*self.pool_connection_authorized(opctx).await?,
        )
        .await
        .map(|_| ())
        .map_err(|e| match e {
            TransactionError::CustomError(e) => e,
            TransactionError::Database(e) => public_error_from_diesel(
                e,
                ErrorHandler::NotFoundByResource(authz_rx),
            ),
        })
    }

    pub async fn webhook_rx_subscription_delete(
        &self,
        opctx: &OpContext,
        authz_rx: &authz::WebhookReceiver,
        subscription: WebhookSubscriptionKind,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Modify, authz_rx).await?;
        let rx_id = authz_rx.id().into_untyped_uuid();
        let conn = self.pool_connection_authorized(&opctx).await?;

        let error_handler = |error| match error {
            diesel::result::Error::NotFound => {
                Error::non_resourcetype_not_found(format!(
                    "{:?} is not subscribed to \"{subscription}\"",
                    authz_rx.lookup_type()
                ))
            }
            diesel::result::Error::DatabaseError(kind, info) => {
                Error::internal_error(&crate::db::error::format_database_error(
                    kind, &*info,
                ))
            }
            error => Error::internal_error(&format!(
                "unexpected database error: {error:#}"
            )),
        };
        const LOG_MSG: &str = "unsubscribed webhook receiver";
        match subscription {
            WebhookSubscriptionKind::Glob(ref glob) => {
                // Deleting a glob subscription is performed in a transaction in
                // order to ensure that the glob is only deleted if its exact
                // subscriptions could also be deleted.
                let n_exact = self
                    .transaction_retry_wrapper("webhook_glob_delete")
                    .transaction(&conn, |conn| {
                        let glob = glob.glob.clone();
                        async move {
                            let n_exact = diesel::delete(
                                subscription_dsl::webhook_rx_subscription,
                            )
                            .filter(subscription_dsl::rx_id.eq(rx_id))
                            .filter(subscription_dsl::glob.eq(glob.clone()))
                            .execute_async(&conn)
                            .await?;
                            diesel::delete(glob_dsl::webhook_rx_event_glob)
                                .filter(glob_dsl::rx_id.eq(rx_id))
                                .filter(glob_dsl::glob.eq(glob))
                                .execute_async(&conn)
                                .await?;
                            Ok(n_exact)
                        }
                    })
                    .await
                    .map_err(error_handler)?;
                slog::debug!(
                    &opctx.log,
                    "{LOG_MSG}";
                    "rx_id" => %rx_id,
                    "subscription_glob" => &glob.glob,
                    "exact_subscriptions_deleted" => n_exact,
                );
            }
            WebhookSubscriptionKind::Exact(class) => {
                diesel::delete(subscription_dsl::webhook_rx_subscription)
                    .filter(subscription_dsl::rx_id.eq(rx_id))
                    .filter(subscription_dsl::event_class.eq(class))
                    .execute_async(&*conn)
                    .await
                    .map_err(error_handler)?;
                slog::debug!(
                    &opctx.log,
                    "{LOG_MSG}";
                    "rx_id" => %rx_id,
                    "subscription_event_class" => %class,
                );
            }
        }

        Ok(())
    }

    async fn rx_subscription_list_on_conn(
        &self,
        rx_id: WebhookReceiverUuid,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> ListResultVec<WebhookSubscriptionKind> {
        // TODO(eliza): rather than performing two separate queries, this could
        // perhaps be expressed using a SQL `union`, with an added "label"
        // column to distinguish between globs and exact subscriptions, but this
        // is a bit more complex, and would require raw SQL...

        // First, get all the exact subscriptions that aren't from globs.
        let exact = subscription_dsl::webhook_rx_subscription
            .filter(subscription_dsl::rx_id.eq(rx_id.into_untyped_uuid()))
            .filter(subscription_dsl::glob.is_null())
            .select(subscription_dsl::event_class)
            .load_async::<WebhookEventClass>(conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(e, ErrorHandler::Server)
                    .internal_context("failed to list exact subscriptions")
            })?;
        // Then, get the globs
        let globs = glob_dsl::webhook_rx_event_glob
            .filter(glob_dsl::rx_id.eq(rx_id.into_untyped_uuid()))
            .select(WebhookGlob::as_select())
            .load_async::<WebhookGlob>(conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(e, ErrorHandler::Server)
                    .internal_context("failed to list glob subscriptions")
            })?;
        let subscriptions = exact
            .into_iter()
            .map(WebhookSubscriptionKind::Exact)
            .chain(globs.into_iter().map(WebhookSubscriptionKind::Glob))
            .collect::<Vec<_>>();
        Ok(subscriptions)
    }

    async fn rx_add_subscription_on_conn(
        &self,
        opctx: &OpContext,
        rx_id: WebhookReceiverUuid,
        subscription: WebhookSubscriptionKind,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<(), TransactionError<Error>> {
        match subscription {
            WebhookSubscriptionKind::Glob(glob) => {
                let glob = WebhookRxEventGlob::new(rx_id, glob);
                let result: WebhookRxEventGlob =
                    WebhookReceiver::insert_resource(
                        rx_id.into_untyped_uuid(),
                        diesel::insert_into(glob_dsl::webhook_rx_event_glob)
                            .values(glob),
                    )
                    .insert_and_get_result_async(conn)
                    .await
                    .map_err(async_insert_error_to_txn(rx_id))?;
                slog::debug!(
                    &opctx.log,
                    "added glob subscription to webhook receiver";
                    "rx_id" => ?rx_id,
                    "subscription" => ?result,
                );
            }
            WebhookSubscriptionKind::Exact(event_class) => {
                let subscription = WebhookRxSubscription {
                    rx_id: rx_id.into(),
                    event_class,
                    glob: None,
                    time_created: chrono::Utc::now(),
                };
                let result: WebhookRxSubscription =
                    WebhookReceiver::insert_resource(
                        rx_id.into_untyped_uuid(),
                        diesel::insert_into(
                            subscription_dsl::webhook_rx_subscription,
                        )
                        .values(subscription),
                    )
                    .insert_and_get_result_async(conn)
                    .await
                    .map_err(async_insert_error_to_txn(rx_id))?;
                slog::debug!(
                    &opctx.log,
                    "added exact subscription to webhook receiver";
                    "rx_id" => ?rx_id,
                    "subscription" => ?result,
                );
            }
        }
        Ok(())
    }

    async fn add_exact_subscription_batch_on_conn(
        &self,
        rx_id: WebhookReceiverUuid,
        subscriptions: Vec<WebhookRxSubscription>,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<Vec<WebhookRxSubscription>, TransactionError<Error>> {
        <WebhookReceiver as DatastoreCollection<WebhookRxSubscription>>::insert_resource(
            rx_id.into_untyped_uuid(),
        diesel::insert_into(subscription_dsl::webhook_rx_subscription)
            .values(subscriptions)
            .on_conflict_do_nothing()
        ).insert_and_get_results_async(conn)
            .await
            .map_err(async_insert_error_to_txn(rx_id))
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
        let subscriptions = WebhookEventClass::ALL_CLASSES
            .iter()
            .filter_map(|class| {
                if regex.is_match(class.as_str()) {
                    slog::debug!(
                        &opctx.log,
                        "webhook glob matches event class";
                        "rx_id" => ?glob.rx_id,
                        "glob" => ?glob.glob.glob,
                        "regex" => ?regex,
                        "event_class" => %class,
                    );
                    Some(WebhookRxSubscription::for_glob(&glob, *class))
                } else {
                    slog::trace!(
                        &opctx.log,
                        "webhook glob does not match event class";
                        "rx_id" => ?glob.rx_id,
                        "glob" => ?glob.glob.glob,
                        "regex" => ?regex,
                        "event_class" => %class,
                    );
                    None
                }
            })
            .collect::<Vec<_>>();
        let created = self
            .add_exact_subscription_batch_on_conn(
                glob.rx_id.into(),
                subscriptions,
                conn,
            )
            .await?
            .len();
        slog::info!(
            &opctx.log,
            "created {created} webhook subscriptions for glob";
            "rx_id" => ?glob.rx_id,
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
    // Glob reprocessing
    //

    /// List webhook glob subscriptions for which new exact subscriptions have
    /// to be generated.
    ///
    /// This includes glob subscriptions that were just created and have no
    /// exact subscriptions, and globs that were last processed with a previous
    /// schema version.
    ///
    /// Such subscriptions will need to be reprocessed (by the
    /// [`DataStore::webhook_glob_reprocess`] function), as event classes
    /// matching those globs may have been added in a later schema version.
    pub async fn webhook_glob_list_reprocessable(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, (Uuid, String)>,
    ) -> ListResultVec<WebhookRxEventGlob> {
        let conn = self.pool_connection_authorized(opctx).await?;
        self.rx_list_reprocessable_globs_on_conn(None, pagparams, &conn).await
    }

    async fn rx_list_reprocessable_globs_on_conn(
        &self,
        rx_id: Option<WebhookReceiverUuid>,
        pagparams: &DataPageParams<'_, (Uuid, String)>,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> ListResultVec<WebhookRxEventGlob> {
        let (current_version, target_version) =
            self.database_schema_version().await.map_err(|e| {
                e.internal_context("couldn't load db schema version")
            })?;

        // Perform some checks to make sure we can actually attempt glob
        // reprocessing at this time.
        //
        // First, ensure we're not in the process of applying a schema
        // migration. If we are, glob reprocessing will have to wait until the
        // migration has completed.
        if let Some(target) = target_version {
            return Err(Error::InternalError {
                internal_message: format!(
                    "webhook glob reprocessing must wait until the migration \
                    from {current_version} to {target} has completed",
                ),
            });
        }

        // If this Nexus is operating with a schema version that is newer or
        // older than the current version active in CRDB, bail out now and don't
        // attempt to reprocess globs.
        //
        // Note that, at present, this defensive code guards against a scenario
        // that isn't actually possible: Nexus will fail to construct the
        // `DataStore` type at all if its schema is not up to date, and at
        // present, schema updates are only applied via mupdate, stopping all
        // Nexus processes. However, we can potentially imagine a Nexus compiled
        // against a schema version that's newer or older than the currently
        // active one running while an online update is in progress, so we check
        // for that situation just in case. Depending on how online Nexus
        // updates are actually implemented, this scenario may or may not
        // actually be possible, but let's check regardless.
        if current_version != SCHEMA_VERSION {
            return Err(Error::InternalError {
                internal_message: format!(
                    "cannot reprocess webhook globs, as our schema version \
                    ({SCHEMA_VERSION}) doess not match the current version \
                    ({current_version})",
                ),
            });
        }

        let query = paginated_multicolumn(
            glob_dsl::webhook_rx_event_glob,
            (glob_dsl::rx_id, glob_dsl::glob),
            pagparams,
        )
        // Select all globs where either:
        // - The schema version is NULL (the glob was freshly created and
        //    has not yet had exact subscriptions generated)
        // - The schema version is not equal to the current one.
        .filter(glob_dsl::schema_version.is_null().or(
            glob_dsl::schema_version.ne(SemverVersion::from(SCHEMA_VERSION)),
        ))
        .select(WebhookRxEventGlob::as_select());
        // If we were asked for globs belonging to a specific receiver, add a
        // WHERE clause to filter on the receiver's UUID. We just use a match
        // rather than boxing the query since this is the only dynamically
        // variable part of the query builder.
        match rx_id {
            Some(rx_id) => {
                query
                    .filter(glob_dsl::rx_id.eq(rx_id.into_untyped_uuid()))
                    .load_async(conn)
                    .await
            }
            None => query.load_async(conn).await,
        }
        .map_err(|e| {
            public_error_from_diesel(e, ErrorHandler::Server)
                .internal_context("failed to list outdated glob subscriptions")
        })
    }

    /// Updates the list of exact subscriptions generated for the provided glob
    /// subscription to the latest schema version.
    ///
    /// This method ensures that exact subscription records exist for all
    /// currently known event classes matching the provided glob. The webhook
    /// dispatcher must ensure that all glob subscriptions are up-to-date before
    /// dispatching events, as a receiver with outdated globs may have a glob
    /// matching a new event class but no corresponding exact subscription yet.
    pub async fn webhook_glob_reprocess(
        &self,
        opctx: &OpContext,
        glob: &WebhookRxEventGlob,
    ) -> Result<WebhookGlobStatus, Error> {
        let conn = self.pool_connection_authorized(opctx).await?;
        self.glob_reprocess_on_conn(opctx, glob, &conn).await
    }

    async fn glob_reprocess_on_conn(
        &self,
        opctx: &OpContext,
        glob: &WebhookRxEventGlob,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<WebhookGlobStatus, Error> {
        slog::trace!(
            opctx.log,
            "reprocessing outdated webhook glob";
            "rx_id" => ?glob.rx_id,
            "glob" => ?glob.glob.glob,
            "prior_version" => ?glob.schema_version,
            "current_version" => %SCHEMA_VERSION,
        );
        let err = OptionalError::new();
        let status = self
            .transaction_retry_wrapper("webhook_glob_reprocess")
            .transaction(&conn, |conn| {
                let glob = glob.clone();
                let err = err.clone();
                async move {
                    let deleted = diesel::delete(
                        subscription_dsl::webhook_rx_subscription,
                    )
                    .filter(subscription_dsl::glob.eq(glob.glob.glob.clone()))
                    .filter(subscription_dsl::rx_id.eq(glob.rx_id))
                    .execute_async(&conn)
                    .await?;
                    let created = self
                        .glob_generate_exact_subs(opctx, &glob, &conn)
                        .await
                        .map_err(|e| match e {
                            TransactionError::CustomError(e) => {
                                err.bail(Err(e))
                            }
                            TransactionError::Database(e) => e,
                        })?;
                    let update =
                        diesel::update(glob_dsl::webhook_rx_event_glob)
                            .filter(
                                glob_dsl::rx_id
                                    .eq(glob.rx_id.into_untyped_uuid()),
                            )
                            .filter(glob_dsl::glob.eq(glob.glob.glob.clone()))
                            .set(
                                glob_dsl::schema_version
                                    .eq(SemverVersion::from(SCHEMA_VERSION)),
                            );
                    let did_update = match glob.schema_version {
                        Some(ref version) => {
                            update
                                .filter(
                                    glob_dsl::schema_version
                                        .eq(version.clone()),
                                )
                                .execute_async(&conn)
                                .await
                        }
                        None => {
                            update
                                .filter(glob_dsl::schema_version.is_null())
                                .execute_async(&conn)
                                .await
                        }
                    };

                    match did_update {
                        // Either the glob has been reprocessed by someone else, or
                        // it has been deleted.
                        Err(diesel::result::Error::NotFound) | Ok(0) => {
                            return Err(err.bail(Ok(
                                WebhookGlobStatus::AlreadyReprocessed,
                            )));
                        }
                        Err(e) => return Err(e),
                        Ok(updated) => {
                            debug_assert_eq!(updated, 1);
                        }
                    }

                    Ok(WebhookGlobStatus::Reprocessed {
                        created,
                        deleted,
                        prev_version: glob
                            .schema_version
                            .clone()
                            .map(Into::into),
                    })
                }
            })
            .await
            .or_else(|e| {
                if let Some(err) = err.take() {
                    err
                } else {
                    Err(public_error_from_diesel(e, ErrorHandler::Server))
                }
            })?;

        match status {
            WebhookGlobStatus::Reprocessed {
                created,
                deleted,
                ref prev_version,
            } => {
                slog::debug!(
                    opctx.log,
                    "reprocessed outdated webhook glob";
                    "rx_id" => ?glob.rx_id,
                    "glob" => ?glob.glob.glob,
                    "prev_version" => ?prev_version,
                    "current_version" => %SCHEMA_VERSION,
                    "subscriptions_created" => ?created,
                    "subscriptions_deleted" => ?deleted,
                );
            }
            WebhookGlobStatus::AlreadyReprocessed => {
                slog::trace!(
                    opctx.log,
                    "outdated webhook glob was either already reprocessed or deleted";
                    "rx_id" => ?glob.rx_id,
                    "glob" => ?glob.glob.glob,
                    "prev_version" => ?glob.schema_version,
                    "current_version" => %SCHEMA_VERSION,
                );
            }
        }

        Ok(status)
    }

    //
    // Secrets
    //

    pub async fn webhook_rx_secret_list(
        &self,
        opctx: &OpContext,
        authz_rx: &authz::WebhookReceiver,
    ) -> ListResultVec<WebhookSecret> {
        opctx.authorize(authz::Action::ListChildren, authz_rx).await?;
        let conn = self.pool_connection_authorized(&opctx).await?;
        self.rx_secret_list_on_conn(authz_rx.id(), &conn).await
    }

    async fn rx_secret_list_on_conn(
        &self,
        rx_id: WebhookReceiverUuid,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> ListResultVec<WebhookSecret> {
        secret_dsl::webhook_secret
            .filter(secret_dsl::rx_id.eq(rx_id.into_untyped_uuid()))
            .filter(secret_dsl::time_deleted.is_null())
            .select(WebhookSecret::as_select())
            .load_async(conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(e, ErrorHandler::Server)
                    .internal_context("failed to list webhook receiver secrets")
            })
    }

    pub async fn webhook_rx_secret_create(
        &self,
        opctx: &OpContext,
        authz_rx: &authz::WebhookReceiver,
        secret: WebhookSecret,
    ) -> CreateResult<WebhookSecret> {
        opctx.authorize(authz::Action::CreateChild, authz_rx).await?;
        let conn = self.pool_connection_authorized(&opctx).await?;
        let secret = self.add_secret_on_conn(secret, &conn).await.map_err(
            |e| match e {
                TransactionError::CustomError(e) => e,
                TransactionError::Database(e) => public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_rx),
                ),
            },
        )?;
        Ok(secret)
    }

    pub async fn webhook_rx_secret_delete(
        &self,
        opctx: &OpContext,
        authz_rx: &authz::WebhookReceiver,
        authz_secret: &authz::WebhookSecret,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_secret).await?;
        diesel::delete(secret_dsl::webhook_secret)
            .filter(secret_dsl::id.eq(authz_secret.id().into_untyped_uuid()))
            .filter(secret_dsl::rx_id.eq(authz_rx.id().into_untyped_uuid()))
            .execute_async(&*self.pool_connection_authorized(&opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_secret),
                )
            })?;
        Ok(())
    }

    async fn add_secret_on_conn(
        &self,
        secret: WebhookSecret,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<WebhookSecret, TransactionError<Error>> {
        let rx_id = secret.webhook_receiver_id;
        let secret: WebhookSecret = WebhookReceiver::insert_resource(
            rx_id.into_untyped_uuid(),
            diesel::insert_into(secret_dsl::webhook_secret).values(secret),
        )
        .insert_and_get_result_async(conn)
        .await
        .map_err(async_insert_error_to_txn(rx_id.into()))?;
        Ok(secret)
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
    use crate::authz;
    use crate::db::explain::ExplainableAsync;
    use crate::db::lookup::LookupPath;
    use crate::db::pub_test_utils::TestDatabase;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::WebhookEventUuid;

    async fn create_receiver(
        datastore: &DataStore,
        opctx: &OpContext,
        name: &str,
        events: Vec<String>,
    ) -> WebhookReceiverConfig {
        datastore
            .webhook_rx_create(
                opctx,
                params::WebhookCreate {
                    identity: IdentityMetadataCreateParams {
                        name: name.parse().unwrap(),
                        description: "it'sa  webhook".to_string(),
                    },
                    endpoint: format!("http://{name}").parse().unwrap(),
                    secrets: vec![name.to_string()],
                    events: events
                        .into_iter()
                        .map(TryFrom::try_from)
                        .collect::<Result<Vec<_>, _>>()
                        .expect("test event globs shouldn't be malformed"),
                },
            )
            .await
            .expect("cant create ye webhook receiver!!!!")
    }

    async fn create_event(
        datastore: &DataStore,
        opctx: &OpContext,
        event_class: WebhookEventClass,
    ) -> (authz::WebhookEvent, crate::db::model::WebhookEvent) {
        let id = WebhookEventUuid::new_v4();
        datastore
            .webhook_event_create(opctx, id, event_class, serde_json::json!({}))
            .await
            .expect("cant create ye event");
        LookupPath::new(opctx, datastore)
            .webhook_event_id(id)
            .fetch()
            .await
            .expect(
            "cant get ye event (i just created it, so this is extra weird?)",
        )
    }

    #[tokio::test]
    async fn test_event_class_globs() {
        // Test setup
        let logctx = dev::test_setup_log("test_event_class_globs");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let mut all_rxs: Vec<WebhookReceiverConfig> = Vec::new();
        async fn create_rx(
            datastore: &DataStore,
            opctx: &OpContext,
            all_rxs: &mut Vec<WebhookReceiverConfig>,
            name: &str,
            subscription: &str,
        ) -> WebhookReceiverConfig {
            let rx = create_receiver(
                datastore,
                opctx,
                name,
                vec![subscription.to_string()],
            )
            .await;
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

        // Before we check whether the receivers are subscribed to the expected
        // event classes, we must generate exact subscriptions for their globs.
        // The webhook dispatcher background task does this prior to listing
        // subscribed receivers, so this simulates its behavior.
        let mut paginator = Paginator::new(SQL_BATCH_SIZE);
        while let Some(p) = paginator.next() {
            let batch = datastore
                .webhook_glob_list_reprocessable(opctx, &p.current_pagparams())
                .await
                .unwrap();
            paginator = p.found_batch(&batch, &|glob| {
                (glob.rx_id.into_untyped_uuid(), glob.glob.glob.clone())
            });
            for glob in batch {
                datastore
                    .webhook_glob_reprocess(opctx, dbg!(&glob))
                    .await
                    .unwrap();
            }
        }

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

    #[tokio::test]
    async fn test_rx_is_subscribed_to_event() {
        // Test setup
        let logctx = dev::test_setup_log("test_rx_is_subscribed_to_event");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let rx = create_receiver(
            datastore,
            opctx,
            "webhooked-on-phonics",
            vec!["test.*.bar".to_string()],
        )
        .await;

        let (authz_rx, _) = LookupPath::new(opctx, datastore)
            .webhook_receiver_id(rx.rx.id())
            .fetch()
            .await
            .expect("cant get ye receiver");

        let (authz_foo, _) =
            create_event(datastore, opctx, WebhookEventClass::TestFoo).await;
        let (authz_foo_bar, _) =
            create_event(datastore, opctx, WebhookEventClass::TestFooBar).await;
        let (authz_quux_bar, _) =
            create_event(datastore, opctx, WebhookEventClass::TestQuuxBar)
                .await;

        let is_subscribed_foo = datastore
            .webhook_rx_is_subscribed_to_event(opctx, &authz_rx, &authz_foo)
            .await;
        assert_eq!(is_subscribed_foo, Ok(false));

        let is_subscribed_foo_bar = datastore
            .webhook_rx_is_subscribed_to_event(opctx, &authz_rx, &authz_foo_bar)
            .await;
        assert_eq!(is_subscribed_foo_bar, Ok(true));

        let is_subscribed_quux_bar = datastore
            .webhook_rx_is_subscribed_to_event(
                opctx,
                &authz_rx,
                &authz_quux_bar,
            )
            .await;
        assert_eq!(is_subscribed_quux_bar, Ok(true));

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
