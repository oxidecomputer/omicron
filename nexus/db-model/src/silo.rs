// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{Generation, Organization};
use crate::collection::DatastoreCollectionConfig;
use crate::impl_enum_type;
use crate::schema::{organization, silo};
use db_macros::Resource;
use nexus_types::external_api::shared::SiloIdentityMode;
use nexus_types::external_api::views;
use nexus_types::external_api::{params, shared};
use nexus_types::identity::Resource;
use uuid::Uuid;

impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "user_provision_type"))]
    pub struct UserProvisionTypeEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, PartialEq)]
    #[diesel(sql_type = UserProvisionTypeEnum)]
    pub enum UserProvisionType;

    // Enum values
    ApiOnly => b"api_only"
    Jit => b"jit"
);

impl From<shared::UserProvisionType> for UserProvisionType {
    fn from(params: shared::UserProvisionType) -> Self {
        match params {
            shared::UserProvisionType::ApiOnly => UserProvisionType::ApiOnly,
            shared::UserProvisionType::Jit => UserProvisionType::Jit,
        }
    }
}

impl From<UserProvisionType> for shared::UserProvisionType {
    fn from(model: UserProvisionType) -> Self {
        match model {
            UserProvisionType::ApiOnly => Self::ApiOnly,
            UserProvisionType::Jit => Self::Jit,
        }
    }
}

/// Describes a silo within the database.
#[derive(Queryable, Insertable, Debug, Resource, Selectable)]
#[diesel(table_name = silo)]
pub struct Silo {
    #[diesel(embed)]
    identity: SiloIdentity,

    pub discoverable: bool,

    pub user_provision_type: UserProvisionType,

    /// child resource generation number, per RFD 192
    pub rcgen: Generation,
}

impl Silo {
    /// Creates a new database Silo object.
    pub fn new(params: params::SiloCreate) -> Self {
        Self::new_with_id(Uuid::new_v4(), params)
    }

    pub fn new_with_id(id: Uuid, params: params::SiloCreate) -> Self {
        Self {
            identity: SiloIdentity::new(id, params.identity),
            discoverable: params.discoverable,
            user_provision_type: params
                .identity_mode
                .user_provision_type()
                .into(),
            rcgen: Generation::new(),
        }
    }
}

impl From<Silo> for views::Silo {
    fn from(silo: Silo) -> Self {
        // In the future, we'll want to store the authentication mode and look
        // at that here, too.
        let identity_mode = match silo.user_provision_type {
            UserProvisionType::Jit => SiloIdentityMode::SamlJit,
            UserProvisionType::ApiOnly => SiloIdentityMode::LocalOnly,
        };
        Self {
            identity: silo.identity(),
            discoverable: silo.discoverable,
            identity_mode,
        }
    }
}

impl DatastoreCollectionConfig<Organization> for Silo {
    type CollectionId = Uuid;
    type GenerationNumberColumn = silo::dsl::rcgen;
    type CollectionTimeDeletedColumn = silo::dsl::time_deleted;
    type CollectionIdColumn = organization::dsl::silo_id;
}
