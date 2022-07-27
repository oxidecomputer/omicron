// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{Generation, Organization};
use crate::collection::DatastoreCollectionConfig;
use crate::impl_enum_type;
use crate::schema::{organization, silo};
use db_macros::Resource;
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
    Fixed => b"fixed"
    Jit => b"jit"
);

impl From<shared::UserProvisionType> for UserProvisionType {
    fn from(params: shared::UserProvisionType) -> Self {
        match params {
            shared::UserProvisionType::Fixed => UserProvisionType::Fixed,
            shared::UserProvisionType::Jit => UserProvisionType::Jit,
        }
    }
}

impl From<UserProvisionType> for shared::UserProvisionType {
    fn from(model: UserProvisionType) -> Self {
        match model {
            UserProvisionType::Fixed => Self::Fixed,
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
            user_provision_type: params.user_provision_type.into(),
            rcgen: Generation::new(),
        }
    }
}

impl From<Silo> for views::Silo {
    fn from(silo: Silo) -> Self {
        Self {
            identity: silo.identity(),
            discoverable: silo.discoverable,
            user_provision_type: silo.user_provision_type.into(),
        }
    }
}

impl DatastoreCollectionConfig<Organization> for Silo {
    type CollectionId = Uuid;
    type GenerationNumberColumn = silo::dsl::rcgen;
    type CollectionTimeDeletedColumn = silo::dsl::time_deleted;
    type CollectionIdColumn = organization::dsl::silo_id;
}
