// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Service`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::identity::Asset;
use crate::db::model::Service;
use crate::db::model::Sled;
use crate::db::pagination::paginated;
use crate::db::pool::DbConnection;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use async_bb8_diesel::PoolError;
use chrono::Utc;
use diesel::prelude::*;
use diesel::upsert::excluded;
use nexus_db_model::ServiceKind;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use uuid::Uuid;

impl DataStore {
    /// Stores a new service in the database.
    pub async fn service_upsert(
        &self,
        opctx: &OpContext,
        service: Service,
    ) -> CreateResult<Service> {
        let conn = self.pool_authorized(opctx).await?;
        self.service_upsert_conn(conn, service).await
    }

    /// Stores a new service in the database (using an existing db connection).
    pub(crate) async fn service_upsert_conn<ConnError>(
        &self,
        conn: &(impl AsyncConnection<DbConnection, ConnError> + Sync),
        service: Service,
    ) -> CreateResult<Service>
    where
        ConnError: From<diesel::result::Error> + Send + 'static,
        PoolError: From<ConnError>,
    {
        use db::schema::service::dsl;

        let service_id = service.id();
        let sled_id = service.sled_id;
        Sled::insert_resource(
            sled_id,
            diesel::insert_into(dsl::service)
                .values(service)
                .on_conflict(dsl::id)
                .do_update()
                .set((
                    dsl::time_modified.eq(Utc::now()),
                    dsl::sled_id.eq(excluded(dsl::sled_id)),
                    dsl::ip.eq(excluded(dsl::ip)),
                    dsl::port.eq(excluded(dsl::port)),
                    dsl::kind.eq(excluded(dsl::kind)),
                )),
        )
        .insert_and_get_result_async(conn)
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                type_name: ResourceType::Sled,
                lookup_type: LookupType::ById(sled_id),
            },
            AsyncInsertError::DatabaseError(e) => {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::Service,
                        &service_id.to_string(),
                    ),
                )
            }
        })
    }

    /// List services of a given kind
    pub async fn services_list_kind(
        &self,
        opctx: &OpContext,
        kind: ServiceKind,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Service> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use db::schema::service::dsl;
        paginated(dsl::service, dsl::id, pagparams)
            .filter(dsl::kind.eq(kind))
            .select(Service::as_select())
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }
}
