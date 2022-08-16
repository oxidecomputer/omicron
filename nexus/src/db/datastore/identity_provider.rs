// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods related to [`IdentityProvider`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::identity::Resource;
use crate::db::model::IdentityProvider;
use crate::db::model::Name;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::ResourceType;

impl DataStore {
    pub async fn identity_provider_list(
        &self,
        opctx: &OpContext,
        authz_idp_list: &authz::SiloIdentityProviderList,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<IdentityProvider> {
        opctx.authorize(authz::Action::ListChildren, authz_idp_list).await?;

        use db::schema::identity_provider::dsl;
        paginated(dsl::identity_provider, dsl::name, pagparams)
            .filter(dsl::silo_id.eq(authz_idp_list.silo().id()))
            .filter(dsl::time_deleted.is_null())
            .select(IdentityProvider::as_select())
            .load_async::<IdentityProvider>(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn saml_identity_provider_create(
        &self,
        opctx: &OpContext,
        authz_idp_list: &authz::SiloIdentityProviderList,
        provider: db::model::SamlIdentityProvider,
    ) -> CreateResult<db::model::SamlIdentityProvider> {
        opctx.authorize(authz::Action::CreateChild, authz_idp_list).await?;
        assert_eq!(provider.silo_id, authz_idp_list.silo().id());

        let name = provider.identity().name.to_string();
        self.pool_authorized(opctx)
            .await?
            .transaction(move |conn| {
                // insert silo identity provider record with type Saml
                use db::schema::identity_provider::dsl as idp_dsl;
                diesel::insert_into(idp_dsl::identity_provider)
                    .values(db::model::IdentityProvider {
                        identity: db::model::IdentityProviderIdentity {
                            id: provider.identity.id,
                            name: provider.identity.name.clone(),
                            description: provider.identity.description.clone(),
                            time_created: provider.identity.time_created,
                            time_modified: provider.identity.time_modified,
                            time_deleted: provider.identity.time_deleted,
                        },
                        silo_id: provider.silo_id,
                        provider_type: db::model::IdentityProviderType::Saml,
                    })
                    .execute(conn)?;

                // insert silo saml identity provider record
                use db::schema::saml_identity_provider::dsl;
                let result = diesel::insert_into(dsl::saml_identity_provider)
                    .values(provider)
                    .returning(db::model::SamlIdentityProvider::as_returning())
                    .get_result(conn)?;

                Ok(result)
            })
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::SamlIdentityProvider,
                        &name,
                    ),
                )
            })
    }
}
