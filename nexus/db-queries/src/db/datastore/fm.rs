// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on fault management internal data.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::model::SitrepMetadata;
use crate::db::model::SitrepVersion;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use nexus_db_schema::schema::fm_sitrep::dsl as sitrep_dsl;
use nexus_db_schema::schema::fm_sitrep_version::dsl as current_sitrep_dsl;
use nexus_types::fm::Sitrep;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SitrepUuid;

impl DataStore {
    pub async fn fm_get_current_sitrep_version(
        &self,
        opctx: &OpContext,
    ) -> Result<Option<SitrepVersion>, Error> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        self.fm_get_current_sitrep_version_on_conn(&conn).await
    }

    async fn fm_get_current_sitrep_version_on_conn(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<Option<SitrepVersion>, Error> {
        current_sitrep_dsl::fm_sitrep_version
            .order_by(current_sitrep_dsl::version.desc())
            .select(SitrepVersion::as_select())
            .first_async(conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn fm_sitrep_metadata_read(
        &self,
        opctx: &OpContext,
        id: SitrepUuid,
    ) -> Result<Option<SitrepMetadata>, Error> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        self.fm_sitrep_metadata_read_on_conn(id, &conn).await
    }

    async fn fm_sitrep_metadata_read_on_conn(
        &self,
        id: SitrepUuid,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<Option<SitrepMetadata>, Error> {
        sitrep_dsl::fm_sitrep
            .filter(sitrep_dsl::id.eq(id.into_untyped_uuid()))
            .select(SitrepMetadata::as_select())
            .first_async(conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn fm_sitrep_read(
        &self,
        opctx: &OpContext,
        id: SitrepUuid,
    ) -> Result<Sitrep, Error> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        let metadata = self
            .fm_sitrep_metadata_read_on_conn(id, &conn)
            .await?
            .ok_or_else(|| {
                Error::non_resourcetype_not_found(format!("sitrep {id:?}"))
            })?
            .into();

        // TODO(eliza): this is where we would read all the other sitrep data,
        // if there was any.

        Ok(Sitrep { metadata })
    }
}
