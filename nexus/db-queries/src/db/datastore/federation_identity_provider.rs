// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_errors::{ErrorHandler, public_error_from_diesel};
use nexus_db_model::{
    FederationIdentityProvider, FederationIdentityProviderUpdate, Name,
};
use nexus_db_schema::schema::federation_identity_provider::dsl;
use nexus_types::external_api::federation as api;
use nexus_types::identity::Resource;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::{
    Error, InternalContext, LookupType, ResourceType,
};
use ref_cast::RefCast;
use uuid::Uuid;

async fn authorize(
    opctx: &OpContext,
    action: authz::Action,
) -> Result<Uuid, Error> {
    let silo = opctx
        .authn
        .silo_required()
        .internal_context("managing federation identity providers")?;
    let silo_id = silo.id();
    opctx
        .authorize(
            action,
            &authz::SiloFederationIdentityProviderList::new(silo),
        )
        .await?;
    Ok(silo_id)
}

fn not_found(id: Uuid) -> ErrorHandler<'static> {
    ErrorHandler::NotFoundByLookup(
        ResourceType::FederationIdentityProvider,
        LookupType::ById(id),
    )
}

impl DataStore {
    pub async fn federation_identity_provider_create(
        &self,
        opctx: &OpContext,
        params: api::FederationIdentityProviderCreate,
    ) -> Result<FederationIdentityProvider, Error> {
        let silo_id = authorize(opctx, authz::Action::CreateChild).await?;
        let provider = FederationIdentityProvider::new(silo_id, params)?;
        let name = provider.name().clone();
        diesel::insert_into(dsl::federation_identity_provider)
            .values(provider)
            .returning(FederationIdentityProvider::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::FederationIdentityProvider,
                        name.as_str(),
                    ),
                )
            })
    }

    pub async fn federation_identity_provider_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> Result<Vec<FederationIdentityProvider>, Error> {
        let silo_id = authorize(opctx, authz::Action::ListChildren).await?;
        let query = match pagparams {
            PaginatedBy::Id(params) => {
                paginated(dsl::federation_identity_provider, dsl::id, params)
            }
            PaginatedBy::Name(params) => paginated(
                dsl::federation_identity_provider,
                dsl::name,
                &params.map_name(Name::ref_cast),
            ),
        };
        query
            .filter(dsl::silo_id.eq(silo_id))
            .filter(dsl::time_deleted.is_null())
            .select(FederationIdentityProvider::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn federation_identity_provider_fetch(
        &self,
        opctx: &OpContext,
        id: Uuid,
        action: authz::Action,
    ) -> Result<FederationIdentityProvider, Error> {
        let silo_id = authorize(opctx, action).await?;
        dsl::federation_identity_provider
            .filter(dsl::id.eq(id))
            .filter(dsl::silo_id.eq(silo_id))
            .filter(dsl::time_deleted.is_null())
            .select(FederationIdentityProvider::as_select())
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, not_found(id)))
    }

    pub async fn federation_identity_provider_update(
        &self,
        opctx: &OpContext,
        provider: &FederationIdentityProvider,
        update: FederationIdentityProviderUpdate,
    ) -> Result<FederationIdentityProvider, Error> {
        let silo_id = authorize(opctx, authz::Action::Modify).await?;
        let id = provider.id();
        let name = update
            .name
            .as_ref()
            .map_or(provider.name(), |name| &name.0)
            .clone();
        diesel::update(dsl::federation_identity_provider)
            .filter(dsl::id.eq(id))
            .filter(dsl::silo_id.eq(silo_id))
            .filter(dsl::time_deleted.is_null())
            .set(update)
            .returning(FederationIdentityProvider::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| match e {
                diesel::result::Error::NotFound => {
                    public_error_from_diesel(e, not_found(id))
                }
                _ => public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::FederationIdentityProvider,
                        name.as_str(),
                    ),
                ),
            })
    }

    pub async fn federation_identity_provider_delete(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> Result<(), Error> {
        let silo_id = authorize(opctx, authz::Action::Delete).await?;
        let now = Utc::now();
        diesel::update(dsl::federation_identity_provider)
            .filter(dsl::id.eq(id))
            .filter(dsl::silo_id.eq(silo_id))
            .filter(dsl::time_deleted.is_null())
            .set((dsl::time_deleted.eq(now), dsl::time_modified.eq(now)))
            .returning(dsl::id)
            .get_result_async::<Uuid>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, not_found(id)))?;
        Ok(())
    }
}
