// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{DatabaseString, impl_enum_type};
use anyhow::anyhow;
use nexus_db_schema::schema::role_assignment;
use nexus_types::external_api::shared;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::BuiltInUserUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SiloUserUuid;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

impl_enum_type!(
    IdentityTypeEnum:

    #[derive(
        Clone,
        Debug,
        AsExpression,
        FromSqlRow,
        Serialize,
        Deserialize,
        PartialEq
    )]
    pub enum IdentityType;

    // Enum values
    UserBuiltin => b"user_builtin"
    SiloUser => b"silo_user"
    SiloGroup => b"silo_group"
);

impl From<shared::IdentityType> for IdentityType {
    fn from(other: shared::IdentityType) -> Self {
        match other {
            shared::IdentityType::SiloUser => IdentityType::SiloUser,
            shared::IdentityType::SiloGroup => IdentityType::SiloGroup,
        }
    }
}

impl TryFrom<IdentityType> for shared::IdentityType {
    type Error = anyhow::Error;

    fn try_from(other: IdentityType) -> Result<Self, Self::Error> {
        match other {
            IdentityType::UserBuiltin => {
                Err(anyhow!("unsupported db identity type: {:?}", other))
            }
            IdentityType::SiloUser => Ok(shared::IdentityType::SiloUser),
            IdentityType::SiloGroup => Ok(shared::IdentityType::SiloGroup),
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

    /// Creates a new database RoleAssignment object for a silo user
    pub fn new_for_silo_user(
        user_id: SiloUserUuid,
        resource_type: omicron_common::api::external::ResourceType,
        resource_id: Uuid,
        role_name: &str,
    ) -> Self {
        Self::new(
            IdentityType::SiloUser,
            user_id.into_untyped_uuid(),
            resource_type,
            resource_id,
            role_name,
        )
    }

    /// Creates a new database RoleAssignment object for a built-in user
    pub fn new_for_builtin_user(
        user_id: BuiltInUserUuid,
        resource_type: omicron_common::api::external::ResourceType,
        resource_id: Uuid,
        role_name: &str,
    ) -> Self {
        Self::new(
            IdentityType::UserBuiltin,
            user_id.into_untyped_uuid(),
            resource_type,
            resource_id,
            role_name,
        )
    }
}

impl<AllowedRoles> TryFrom<RoleAssignment>
    for shared::RoleAssignment<AllowedRoles>
where
    AllowedRoles: DatabaseString,
{
    type Error = Error;

    fn try_from(role_asgn: RoleAssignment) -> Result<Self, Self::Error> {
        Ok(Self {
            identity_type: shared::IdentityType::try_from(
                role_asgn.identity_type,
            )
            .map_err(|error| {
                Error::internal_error(&format!(
                    "parsing database role assignment: {:#}",
                    error
                ))
            })?,
            identity_id: role_asgn.identity_id,
            role_name: AllowedRoles::from_database_string(&role_asgn.role_name)
                .map_err(|error| {
                    Error::internal_error(&format!(
                        "parsing database role assignment: \
                        unrecognized role name {:?}: {:#}",
                        &role_asgn.role_name, error,
                    ))
                })?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use omicron_common::api::external::ResourceType;
    use std::borrow::Cow;

    #[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
    #[serde(rename_all = "kebab-case")]
    pub enum DummyRoles {
        Bogus,
    }

    impl crate::DatabaseString for DummyRoles {
        type Error = anyhow::Error;

        fn to_database_string(&self) -> Cow<'_, str> {
            unimplemented!()
        }

        fn from_database_string(s: &str) -> Result<Self, Self::Error> {
            if s == "bogus" {
                Ok(DummyRoles::Bogus)
            } else {
                Err(anyhow!("unsupported DummyRoles: {:?}", s))
            }
        }
    }

    #[test]
    fn test_role_assignment_from_database() {
        let identity_id =
            "75ec4a39-67cf-4549-9e74-44b92947c37c".parse().unwrap();
        let resource_id =
            "9e3e3be8-4051-4ddb-92fa-32cc5294f066".parse().unwrap();

        let ok_input = crate::RoleAssignment {
            identity_type: crate::IdentityType::SiloUser,
            identity_id,
            resource_type: ResourceType::Project.to_string(),
            resource_id,
            role_name: String::from("bogus"),
        };

        let bad_input_role = crate::RoleAssignment {
            role_name: String::from("bogosity"),
            ..ok_input.clone()
        };

        let bad_input_idtype = crate::RoleAssignment {
            identity_type: crate::IdentityType::UserBuiltin,
            ..ok_input.clone()
        };

        let error =
            <shared::RoleAssignment<DummyRoles>>::try_from(bad_input_role)
                .expect_err("unexpectedly succeeding parsing database role");
        println!("error: {:#}", error);
        if let Error::InternalError { internal_message } = error {
            assert_eq!(
                internal_message,
                "parsing database role assignment: unrecognized role name \
                \"bogosity\": unsupported DummyRoles: \"bogosity\""
            );
        } else {
            panic!(
                "expected internal error for database parse failure, \
                found {:?}",
                error
            );
        }

        let error =
            <shared::RoleAssignment<DummyRoles>>::try_from(bad_input_idtype)
                .expect_err("unexpectedly succeeding parsing database role");
        println!("error: {:#}", error);
        if let Error::InternalError { internal_message } = error {
            assert_eq!(
                internal_message,
                "parsing database role assignment: \
                unsupported db identity type: UserBuiltin"
            );
        } else {
            panic!(
                "expected internal error for database parse failure, \
                found {:?}",
                error
            );
        }

        let success = <shared::RoleAssignment<DummyRoles>>::try_from(ok_input)
            .expect("parsing valid role assignment from database");
        println!("success: {:?}", success);
        assert_eq!(success.identity_type, shared::IdentityType::SiloUser);
        assert_eq!(success.identity_id, identity_id);
        assert_eq!(success.role_name, DummyRoles::Bogus);
    }
}
