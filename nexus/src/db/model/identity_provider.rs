// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::authn;
use crate::authz;
use crate::context::OpContext;
use crate::db::lookup::LookupPath;
use crate::db::model::impl_enum_type;
use crate::db::schema::{identity_provider, saml_identity_provider};
use crate::db::DataStore;
use async_trait::async_trait;
use db_macros::Resource;
use omicron_common::api::external::LookupResult;
use serde::{Deserialize, Serialize};
use std::io::Write;
use uuid::Uuid;

impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "provider_type"))]
    pub struct IdentityProviderTypeEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = IdentityProviderTypeEnum)]
    pub enum IdentityProviderType;

    // Enum values
    Saml => b"saml"
);

#[derive(Queryable, Insertable, Clone, Debug, Selectable, Resource)]
#[diesel(table_name = identity_provider)]
pub struct IdentityProvider {
    // Note identity here matches the specific identity provider configuration
    #[diesel(embed)]
    pub identity: IdentityProviderIdentity,

    pub silo_id: Uuid,
    pub provider_type: IdentityProviderType,
}

#[async_trait]
pub trait IdentityProviderLookup: Sized {
    /// First, look up the provider type, then look in for the specific
    /// provider details.
    async fn lookup(
        datastore: &DataStore,
        opctx: &OpContext,
        silo_name: &super::Name,
        provider_name: &super::Name,
    ) -> LookupResult<(authz::Silo, super::Silo, Self)>;
}

#[async_trait]
impl IdentityProviderLookup for authn::silos::IdentityProviderType {
    async fn lookup(
        datastore: &DataStore,
        opctx: &OpContext,
        silo_name: &super::Name,
        provider_name: &super::Name,
    ) -> LookupResult<(authz::Silo, super::Silo, Self)> {
        let (authz_silo, db_silo) = LookupPath::new(opctx, datastore)
            .silo_name(silo_name)
            .fetch()
            .await?;

        let (.., identity_provider) = LookupPath::new(opctx, datastore)
            .silo_name(silo_name)
            .identity_provider_name(provider_name)
            .fetch()
            .await?;

        match identity_provider.provider_type {
            IdentityProviderType::Saml => {
                let (.., saml_identity_provider) =
                    LookupPath::new(opctx, datastore)
                        .silo_name(silo_name)
                        .saml_identity_provider_name(provider_name)
                        .fetch()
                        .await?;

                let saml_identity_provider = authn::silos::IdentityProviderType::Saml(
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
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable, Resource)]
#[diesel(table_name = saml_identity_provider)]
pub struct SamlIdentityProvider {
    #[diesel(embed)]
    pub identity: SamlIdentityProviderIdentity,

    pub silo_id: Uuid,

    pub idp_metadata_document_string: String,

    pub idp_entity_id: String,
    pub sp_client_id: String,
    pub acs_url: String,
    pub slo_url: String,
    pub technical_contact_email: String,
    pub public_cert: Option<String>,
    pub private_key: Option<String>,
}

impl TryFrom<SamlIdentityProvider> for authn::silos::SamlIdentityProvider {
    type Error = anyhow::Error;
    fn try_from(model: SamlIdentityProvider) -> Result<Self, Self::Error> {
        let provider = authn::silos::SamlIdentityProvider {
            idp_metadata_document_string: model.idp_metadata_document_string,
            idp_entity_id: model.idp_entity_id,
            sp_client_id: model.sp_client_id,
            acs_url: model.acs_url,
            slo_url: model.slo_url,
            technical_contact_email: model.technical_contact_email,
            public_cert: model.public_cert,
            private_key: model.private_key,
        };

        // check that the idp metadata document string parses into an EntityDescriptor
        let _idp_metadata: authn::silos::EntityDescriptor =
            provider.idp_metadata_document_string.parse()?;

        // check that there is a valid sign in url
        let _sign_in_url = provider.sign_in_url(None)?;

        Ok(provider)
    }
}
