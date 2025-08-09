// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods related to [`IdentityProvider`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::identity::Resource;
use crate::db::model;
use crate::db::model::IdentityProvider;
use crate::db::model::Name;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use nexus_auth::authn::silos::IdentityProviderType;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::LookupPath;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::http_pagination::PaginatedBy;
use ref_cast::RefCast;

impl DataStore {
    pub async fn identity_provider_lookup(
        &self,
        opctx: &OpContext,
        silo_name: &model::Name,
        provider_name: &model::Name,
    ) -> LookupResult<(authz::Silo, model::Silo, IdentityProviderType)> {
        let (authz_silo, db_silo) =
            LookupPath::new(opctx, self).silo_name(silo_name).fetch().await?;

        let (.., identity_provider) = LookupPath::new(opctx, self)
            .silo_name(silo_name)
            .identity_provider_name(provider_name)
            .fetch()
            .await?;

        match identity_provider.provider_type {
            model::IdentityProviderType::Saml => {
                let (.., saml_identity_provider) = LookupPath::new(opctx, self)
                    .silo_name(silo_name)
                    .saml_identity_provider_name(provider_name)
                    .fetch()
                    .await?;

                let saml_identity_provider = IdentityProviderType::Saml(
                    saml_identity_provider.try_into()
                        .map_err(|e: anyhow::Error|
                            // If an error is encountered converting from the
                            // model to the authn type here, this is a server
                            // error: it was validated before it went into the
                            // DB.
                            omicron_common::api::external::Error::internal_error(
                                &format!(
                                    "saml_identity_provider.try_into() failed! {}",
                                    &e.to_string()
                                )
                            )
                        )?
                    );

                Ok((authz_silo, db_silo, saml_identity_provider))
            }
        }
    }

    pub async fn identity_provider_list(
        &self,
        opctx: &OpContext,
        authz_idp_list: &authz::SiloIdentityProviderList,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<IdentityProvider> {
        opctx.authorize(authz::Action::ListChildren, authz_idp_list).await?;

        use nexus_db_schema::schema::identity_provider::dsl;
        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::identity_provider, dsl::id, pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::identity_provider,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::silo_id.eq(authz_idp_list.silo().id()))
        .filter(dsl::time_deleted.is_null())
        .select(IdentityProvider::as_select())
        .load_async::<IdentityProvider>(
            &*self.pool_connection_authorized(opctx).await?,
        )
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn saml_identity_provider_create(
        &self,
        opctx: &OpContext,
        authz_idp_list: &authz::SiloIdentityProviderList,
        saml_provider: db::model::SamlIdentityProvider,
    ) -> CreateResult<db::model::SamlIdentityProvider> {
        opctx.authorize(authz::Action::CreateChild, authz_idp_list).await?;
        assert_eq!(saml_provider.silo_id, authz_idp_list.silo().id());

        let name = saml_provider.identity().name.to_string();
        let conn = self.pool_connection_authorized(opctx).await?;

        // Identity providers have two records, one generic, and one
        // specialized. Create the generic one from the specialized one here.
        let provider = db::model::IdentityProvider {
            identity: db::model::IdentityProviderIdentity {
                id: saml_provider.identity.id,
                name: saml_provider.identity.name.clone(),
                description: saml_provider.identity.description.clone(),
                time_created: saml_provider.identity.time_created,
                time_modified: saml_provider.identity.time_modified,
                time_deleted: saml_provider.identity.time_deleted,
            },
            silo_id: saml_provider.silo_id,
            provider_type: db::model::IdentityProviderType::Saml,
        };

        self.transaction_retry_wrapper("saml_identity_provider_create")
            .transaction(&conn, |conn| {
                let saml_provider = saml_provider.clone();
                let provider = provider.clone();

                async move {
                    // insert silo identity provider record with type Saml
                    use nexus_db_schema::schema::identity_provider::dsl as
                        idp_dsl;

                    diesel::insert_into(idp_dsl::identity_provider)
                        .values(provider)
                        .execute_async(&conn)
                        .await?;

                    // insert silo saml identity provider record
                    use nexus_db_schema::schema::saml_identity_provider::dsl;
                    let result =
                        diesel::insert_into(dsl::saml_identity_provider)
                            .values(saml_provider)
                            .returning(
                                db::model::SamlIdentityProvider::as_returning(),
                            )
                            .get_result_async(&conn)
                            .await?;

                    Ok(result)
                }
            })
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::SamlIdentityProvider,
                        &name,
                    ),
                )
            })
    }
}
