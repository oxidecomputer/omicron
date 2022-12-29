// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Certificate`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::model::Certificate;
use crate::db::model::Name;
use crate::db::model::ServiceKind;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::ListResultVec;

impl DataStore {
    /// Stores a new certificate in the database.
    pub async fn certificate_create(
        &self,
        opctx: &OpContext,
        certificate: Certificate,
    ) -> CreateResult<Certificate> {
        use db::schema::certificate::dsl;

        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;

        diesel::insert_into(dsl::certificate)
            .values(certificate)
            .on_conflict(dsl::id)
            .do_update()
            .set(dsl::time_modified.eq(dsl::time_modified))
            .returning(Certificate::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn certificate_list_for(
        &self,
        opctx: &OpContext,
        kind: ServiceKind,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<Certificate> {
        use db::schema::certificate::dsl;

        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        paginated(dsl::certificate, dsl::name, &pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::service.eq(kind))
            .select(Certificate::as_select())
            .load_async::<Certificate>(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn certificate_delete(
        &self,
        opctx: &OpContext,
        authz_cert: &authz::Certificate,
    ) -> DeleteResult {
        use db::schema::certificate::dsl;

        opctx.authorize(authz::Action::Delete, authz_cert).await?;

        let now = Utc::now();
        diesel::update(dsl::certificate)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_cert.id()))
            .set(dsl::time_deleted.eq(now))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_cert),
                )
            })?;

        Ok(())
    }
}
