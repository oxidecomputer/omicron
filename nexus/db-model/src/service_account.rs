// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::DateTime;
use chrono::Utc;
use db_macros::Resource;
use nexus_db_schema::schema::service_account;
use nexus_db_schema::schema::service_account_key;
use nexus_types::external_api::service_account as views;
use nexus_types::identity::Resource;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::ProjectUuid;
use omicron_uuid_kinds::SiloUuid;
use uuid::Uuid;

#[derive(Clone, Debug, Insertable, Queryable, Resource, Selectable)]
#[diesel(table_name = service_account)]
pub struct ServiceAccount {
    #[diesel(embed)]
    identity: ServiceAccountIdentity,

    pub silo_id: Uuid,
    pub project_id: Uuid,
}

impl ServiceAccount {
    pub fn new(
        silo_id: SiloUuid,
        project_id: ProjectUuid,
        params: IdentityMetadataCreateParams,
    ) -> Self {
        Self {
            identity: ServiceAccountIdentity::new(Uuid::new_v4(), params),
            silo_id: silo_id.into_untyped_uuid(),
            project_id: project_id.into_untyped_uuid(),
        }
    }

    pub fn silo_id(&self) -> SiloUuid {
        SiloUuid::from_untyped_uuid(self.silo_id)
    }

    pub fn project_id(&self) -> ProjectUuid {
        ProjectUuid::from_untyped_uuid(self.project_id)
    }
}

impl From<ServiceAccount> for views::ServiceAccount {
    fn from(sa: ServiceAccount) -> Self {
        Self { identity: sa.identity(), project_id: sa.project_id() }
    }
}

#[derive(Clone, Debug, Insertable, Queryable, Resource, Selectable)]
#[diesel(table_name = service_account_key)]
pub struct ServiceAccountKey {
    #[diesel(embed)]
    identity: ServiceAccountKeyIdentity,

    pub time_expires: Option<DateTime<Utc>>,
    pub service_account_id: Uuid,
    pub public_key: Vec<u8>,
}

impl ServiceAccountKey {
    pub fn new(
        service_account_id: Uuid,
        params: IdentityMetadataCreateParams,
        public_key_der: Vec<u8>,
        time_expires: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            identity: ServiceAccountKeyIdentity::new(Uuid::new_v4(), params),
            time_expires,
            service_account_id,
            public_key: public_key_der,
        }
    }
}

impl From<ServiceAccountKey> for views::ServiceAccountKey {
    fn from(key: ServiceAccountKey) -> Self {
        let public_key =
            oxide_http_signatures::verifying_key_from_der(&key.public_key)
                .map(|k| oxide_http_signatures::verifying_key_to_pem(&k))
                .unwrap_or_default();
        Self {
            identity: key.identity(),
            service_account_id: key.service_account_id,
            public_key,
            time_expires: key.time_expires,
        }
    }
}
