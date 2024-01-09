// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::impl_enum_type;
use crate::schema::{identity_provider, saml_identity_provider};
use db_macros::Resource;
use nexus_types::identity::Resource;

use nexus_types::external_api::views;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "provider_type", schema = "public"))]
    pub struct IdentityProviderTypeEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = IdentityProviderTypeEnum)]
    pub enum IdentityProviderType;

    // Enum values
    Saml => b"saml"
);

impl From<IdentityProviderType> for views::IdentityProviderType {
    fn from(idp_type: IdentityProviderType) -> Self {
        match idp_type {
            IdentityProviderType::Saml => views::IdentityProviderType::Saml,
        }
    }
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable, Resource)]
#[diesel(table_name = identity_provider)]
pub struct IdentityProvider {
    // Note identity here matches the specific identity provider configuration
    #[diesel(embed)]
    pub identity: IdentityProviderIdentity,

    pub silo_id: Uuid,
    pub provider_type: IdentityProviderType,
}

impl From<IdentityProvider> for views::IdentityProvider {
    fn from(idp: IdentityProvider) -> Self {
        Self {
            identity: idp.identity(),
            provider_type: idp.provider_type.into(),
        }
    }
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable, Resource)]
#[diesel(table_name = saml_identity_provider)]
pub struct SamlIdentityProvider {
    #[diesel(embed)]
    pub identity: SamlIdentityProviderIdentity,

    pub silo_id: Uuid,

    /// idp descriptor
    pub idp_metadata_document_string: String,

    /// idp's entity id
    pub idp_entity_id: String,

    /// sp's client id
    pub sp_client_id: String,

    /// service provider endpoint where the response will be sent
    pub acs_url: String,

    /// service provider endpoint where the idp should send log out requests
    pub slo_url: String,

    /// customer's technical contact for saml configuration
    pub technical_contact_email: String,

    /// base64 encoded DER corresponding to X509 pair
    pub public_cert: Option<String>,
    pub private_key: Option<String>,

    /// if set, attributes with this name will be considered to denote a user's
    /// group membership, where the values will be the group names.
    pub group_attribute_name: Option<String>,
}

impl From<SamlIdentityProvider> for views::SamlIdentityProvider {
    fn from(saml_idp: SamlIdentityProvider) -> Self {
        Self {
            identity: saml_idp.identity(),
            idp_entity_id: saml_idp.idp_entity_id,
            sp_client_id: saml_idp.sp_client_id,
            acs_url: saml_idp.acs_url,
            slo_url: saml_idp.slo_url,
            technical_contact_email: saml_idp.technical_contact_email,
            public_cert: saml_idp.public_cert,
            group_attribute_name: saml_idp.group_attribute_name,
        }
    }
}
