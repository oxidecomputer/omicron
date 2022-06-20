// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use crate::db::schema::role_assignment;
use crate::external_api::shared;
use serde::{Deserialize, Serialize};
use std::io::Write;
use uuid::Uuid;

impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "identity_type"))]
    pub struct IdentityTypeEnum;

    #[derive(
        Clone,
        Debug,
        AsExpression,
        FromSqlRow,
        Serialize,
        Deserialize,
        PartialEq
    )]
    #[diesel(sql_type = IdentityTypeEnum)]
    pub enum IdentityType;

    // Enum values
    UserBuiltin => b"user_builtin"
    SiloUser => b"silo_user"
);

impl From<shared::IdentityType> for IdentityType {
    fn from(other: shared::IdentityType) -> Self {
        match other {
            shared::IdentityType::SiloUser => IdentityType::SiloUser,
        }
    }
}

/// Describes an assignment of a built-in role for a user
#[derive(Clone, Queryable, Insertable, Debug, Selectable)]
#[diesel(table_name = role_assignment)]
pub struct RoleAssignment {
    pub identity_type: IdentityType,
    pub identity_id: Uuid,
    pub resource_type: String,
    pub resource_id: Uuid,
    pub role_name: String,
}

impl RoleAssignment {
    /// Creates a new database RoleAssignment object.
    pub fn new(
        identity_type: IdentityType,
        identity_id: Uuid,
        resource_type: omicron_common::api::external::ResourceType,
        resource_id: Uuid,
        role_name: &str,
    ) -> Self {
        Self {
            identity_type,
            identity_id,
            resource_type: resource_type.to_string(),
            resource_id,
            role_name: String::from(role_name),
        }
    }
}
