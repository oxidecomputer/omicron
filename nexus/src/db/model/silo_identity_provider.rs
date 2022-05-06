// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::db::identity::Resource;
use crate::db::model::{impl_enum_type, Name};
use crate::db::schema::{silo_identity_provider, silo_saml_identity_provider};
use db_macros::Resource;
use omicron_common::api::external;

use serde::{Deserialize, Serialize};
use std::io::Write;
use uuid::Uuid;

impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "provider_type"))]
    pub struct SiloIdentityProviderTypeEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = SiloIdentityProviderTypeEnum)]
    pub enum SiloIdentityProviderType;

    // Enum values
    Saml => b"saml"
);

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = silo_identity_provider)]
pub struct SiloIdentityProvider {
    pub silo_id: Uuid,
    pub provider_type: SiloIdentityProviderType,
    pub name: Name,
    pub provider_id: Uuid,
}

impl SiloIdentityProvider {
    pub fn id(&self) -> Uuid {
        self.provider_id
    }
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable, Resource)]
#[diesel(table_name = silo_saml_identity_provider)]
pub struct SiloSamlIdentityProvider {
    #[diesel(embed)]
    pub identity: SiloSamlIdentityProviderIdentity,

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

impl Into<external::SiloSamlIdentityProvider> for SiloSamlIdentityProvider {
    fn into(self) -> external::SiloSamlIdentityProvider {
        external::SiloSamlIdentityProvider {
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
