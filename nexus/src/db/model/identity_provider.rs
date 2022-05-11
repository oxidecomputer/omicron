// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::db::identity::Resource;
use crate::db::model::{impl_enum_type, Name};
use crate::db::schema::{identity_provider, saml_identity_provider};
use db_macros::Resource;
use omicron_common::api::external;

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

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = identity_provider)]
pub struct IdentityProvider {
    pub silo_id: Uuid,
    pub provider_type: IdentityProviderType,
    pub name: Name,
    pub provider_id: Uuid,
}

impl IdentityProvider {
    pub fn id(&self) -> Uuid {
        self.provider_id
    }
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable, Resource)]
#[diesel(table_name = saml_identity_provider)]
pub struct SamlIdentityProvider {
    #[diesel(embed)]
    pub identity: SamlIdentityProviderIdentity,

    pub silo_id: Uuid,

    pub idp_metadata_url: String,
    pub idp_metadata_document_string: String,

    pub idp_entity_id: String,
    pub sp_client_id: String,
    pub acs_url: String,
    pub slo_url: String,
    pub technical_contact_email: String,
    pub public_cert: Option<String>,
    pub private_key: Option<String>,
}

impl Into<external::SamlIdentityProvider> for SamlIdentityProvider {
    fn into(self) -> external::SamlIdentityProvider {
        external::SamlIdentityProvider {
            identity: self.identity(),
            idp_metadata_url: self.idp_metadata_url.clone(),
            idp_entity_id: self.idp_entity_id.clone(),
            sp_client_id: self.sp_client_id.clone(),
            acs_url: self.acs_url.clone(),
            slo_url: self.slo_url.clone(),
            technical_contact_email: self.technical_contact_email.clone(),
            public_cert: self.public_cert,
            private_key: self.private_key,
        }
    }
}
