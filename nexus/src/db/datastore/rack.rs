// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Rack`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::error::TransactionError;
use crate::db::identity::Asset;
use crate::db::model::Rack;
use crate::db::model::Service;
use crate::db::model::Sled;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use async_bb8_diesel::PoolError;
use chrono::Utc;
use diesel::prelude::*;
use diesel::upsert::excluded;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use uuid::Uuid;

impl DataStore {
    pub async fn rack_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Rack> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use db::schema::rack::dsl;
        paginated(dsl::rack, dsl::id, pagparams)
            .select(Rack::as_select())
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    /// Stores a new rack in the database.
    ///
    /// This function is a no-op if the rack already exists.
    pub async fn rack_insert(
        &self,
        opctx: &OpContext,
        rack: &Rack,
    ) -> Result<Rack, Error> {
        use db::schema::rack::dsl;

        diesel::insert_into(dsl::rack)
            .values(rack.clone())
            .on_conflict(dsl::id)
            .do_update()
            // This is a no-op, since we conflicted on the ID.
            .set(dsl::id.eq(excluded(dsl::id)))
            .returning(Rack::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::Rack,
                        &rack.id().to_string(),
                    ),
                )
            })
    }

    /// Update a rack to mark that it has been initialized
    pub async fn rack_set_initialized(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
        services: Vec<Service>,
    ) -> UpdateResult<Rack> {
        use db::schema::rack::dsl as rack_dsl;
        use db::schema::service::dsl as service_dsl;

        #[derive(Debug)]
        enum RackInitError {
            ServiceInsert { err: AsyncInsertError, sled_id: Uuid, svc_id: Uuid },
            RackUpdate(PoolError),
        }
        type TxnError = TransactionError<RackInitError>;

        // NOTE: This operation could likely be optimized with a CTE, but given
        // the low-frequency of calls, this optimization has been deferred.
        self.pool_authorized(opctx)
            .await?
            .transaction_async(|conn| async move {
                // Early exit if the rack has already been initialized.
                let rack = rack_dsl::rack
                    .filter(rack_dsl::id.eq(rack_id))
                    .select(Rack::as_select())
                    .get_result_async(&conn)
                    .await
                    .map_err(|e| {
                        TxnError::CustomError(RackInitError::RackUpdate(
                            PoolError::from(e),
                        ))
                    })?;
                if rack.initialized {
                    return Ok(rack);
                }

                // Otherwise, insert services and set rack.initialized = true.
                for svc in services {
                    let sled_id = svc.sled_id;
                    <Sled as DatastoreCollection<Service>>::insert_resource(
                        sled_id,
                        diesel::insert_into(service_dsl::service)
                            .values(svc.clone())
                            .on_conflict(service_dsl::id)
                            .do_update()
                            .set((
                                service_dsl::time_modified.eq(Utc::now()),
                                service_dsl::sled_id
                                    .eq(excluded(service_dsl::sled_id)),
                                service_dsl::ip.eq(excluded(service_dsl::ip)),
                                service_dsl::port
                                    .eq(excluded(service_dsl::port)),
                                service_dsl::kind
                                    .eq(excluded(service_dsl::kind)),
                            )),
                    )
                    .insert_and_get_result_async(&conn)
                    .await
                    .map_err(|err| {
                        TxnError::CustomError(RackInitError::ServiceInsert {
                            err,
                            sled_id,
                            svc_id: svc.id(),
                        })
                    })?;
                }
                diesel::update(rack_dsl::rack)
                    .filter(rack_dsl::id.eq(rack_id))
                    .set((
                        rack_dsl::initialized.eq(true),
                        rack_dsl::time_modified.eq(Utc::now()),
                    ))
                    .returning(Rack::as_returning())
                    .get_result_async::<Rack>(&conn)
                    .await
                    .map_err(|e| {
                        TxnError::CustomError(RackInitError::RackUpdate(
                            PoolError::from(e),
                        ))
                    })
            })
            .await
            .map_err(|e| match e {
                TxnError::CustomError(RackInitError::ServiceInsert {
                    err,
                    sled_id,
                    svc_id,
                }) => match err {
                    AsyncInsertError::CollectionNotFound => {
                        Error::ObjectNotFound {
                            type_name: ResourceType::Sled,
                            lookup_type: LookupType::ById(sled_id),
                        }
                    }
                    AsyncInsertError::DatabaseError(e) => {
                        public_error_from_diesel_pool(
                            e,
                            ErrorHandler::Conflict(
                                ResourceType::Service,
                                &svc_id.to_string(),
                            ),
                        )
                    }
                },
                TxnError::CustomError(RackInitError::RackUpdate(err)) => {
                    public_error_from_diesel_pool(
                        err,
                        ErrorHandler::NotFoundByLookup(
                            ResourceType::Rack,
                            LookupType::ById(rack_id),
                        ),
                    )
                }
                TxnError::Pool(e) => {
                    Error::internal_error(&format!("Transaction error: {}", e))
                }
            })
    }
}
