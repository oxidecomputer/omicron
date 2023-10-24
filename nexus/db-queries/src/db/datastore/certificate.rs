// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Certificate`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::model::Certificate;
use crate::db::model::Name;
use crate::db::model::ServiceKind;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_types::identity::Resource;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::ResourceType;
use ref_cast::RefCast;

impl DataStore {
    /// Stores a new certificate in the database.
    pub async fn certificate_create(
        &self,
        opctx: &OpContext,
        certificate: Certificate,
    ) -> CreateResult<Certificate> {
        use db::schema::certificate::dsl;

        let authz_silo = opctx
            .authn
            .silo_required()
            .internal_context("creating a Certificate")?;
        let authz_cert_list = authz::SiloCertificateList::new(authz_silo);
        opctx.authorize(authz::Action::CreateChild, &authz_cert_list).await?;

        let name = certificate.name().clone();
        diesel::insert_into(dsl::certificate)
            .values(certificate)
            .on_conflict(dsl::id)
            .do_update()
            .set(dsl::time_modified.eq(dsl::time_modified))
            .returning(Certificate::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::Certificate,
                        name.as_str(),
                    ),
                )
            })
    }

    pub async fn certificate_list_for(
        &self,
        opctx: &OpContext,
        kind: Option<ServiceKind>,
        pagparams: &PaginatedBy<'_>,
        silo_only: bool,
    ) -> ListResultVec<Certificate> {
        use db::schema::certificate::dsl;

        let silo = if silo_only {
            let authz_silo = opctx
                .authn
                .silo_required()
                .internal_context("listing Certificates")?;
            let silo_id = authz_silo.id();
            let authz_cert_list = authz::SiloCertificateList::new(authz_silo);
            opctx
                .authorize(authz::Action::ListChildren, &authz_cert_list)
                .await?;
            Some(silo_id)
        } else {
            opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
            None
        };

        let query;
        match pagparams {
            PaginatedBy::Id(params) => {
                query = paginated(dsl::certificate, dsl::id, &params)
                    .filter(dsl::time_deleted.is_null());
            }
            PaginatedBy::Name(params) => {
                query = paginated(
                    dsl::certificate,
                    dsl::name,
                    &params.map_name(|n| Name::ref_cast(n)),
                )
                .filter(dsl::time_deleted.is_null())
            }
        }

        let query = if let Some(kind) = kind {
            query.filter(dsl::service.eq(kind))
        } else {
            query
        };

        let query = if let Some(silo_id) = silo {
            query.filter(dsl::silo_id.eq(silo_id))
        } else {
            query
        };

        query
            .select(Certificate::as_select())
            .load_async::<Certificate>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
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
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_cert),
                )
            })?;

        Ok(())
    }
}
