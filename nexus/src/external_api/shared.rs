// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types that are used as both views and params

use crate::db;
use omicron_common::api::external::Error;
use schemars::JsonSchema;
use serde::de::Error as _;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use std::str::FromStr;
use uuid::Uuid;

/// Maximum number of role assignments allowed on any one resource
// Today's implementation assumes a relatively small number of role assignments
// per resource.  Things should work if we bump this up, but we'll want to look
// into scalability improvements (e.g., use pagination for fetching and updating
// the role assignments, and consider the impact on authz checks as well).
//
// Most importantly: by keeping this low to start with, it's impossible for
// customers to develop a dependency on a huge number of role assignments.  That
// maximizes our flexibility in the future.
//
// TODO This should be runtime-configurable.  But it doesn't belong in the Nexus
// configuration file, since it's a constraint on database objects more than it
// is Nexus.  We should have some kinds of config that lives in the database.
pub const MAX_ROLE_ASSIGNMENTS_PER_RESOURCE: usize = 64;

/// Client view of a [`Policy`], which describes how this resource may be
/// accessed
///
/// Note that the Policy only describes access granted explicitly for this
/// resource.  The policies of parent resources can also cause a user to have
/// access to this resource.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
#[schemars(rename = "{AllowedRoles}Policy")]
pub struct Policy<AllowedRoles: serde::de::DeserializeOwned> {
    /// Roles directly assigned on this resource
    #[serde(deserialize_with = "role_assignments_deserialize")]
    pub role_assignments: Vec<RoleAssignment<AllowedRoles>>,
}

fn role_assignments_deserialize<'de, D, R>(
    d: D,
) -> Result<Vec<RoleAssignment<R>>, D::Error>
where
    D: Deserializer<'de>,
    R: serde::de::DeserializeOwned,
{
    let v = Vec::<_>::deserialize(d)?;
    if v.len() > MAX_ROLE_ASSIGNMENTS_PER_RESOURCE {
        return Err(D::Error::invalid_length(
            v.len(),
            &format!(
                "a list of at most {} role assignments",
                MAX_ROLE_ASSIGNMENTS_PER_RESOURCE
            )
            .as_str(),
        ));
    }
    Ok(v)
}

/// Describes the assignment of a particular role on a particular resource to a
/// particular identity (user, group, etc.)
///
/// The resource is not part of this structure.  Rather, [`RoleAssignment`]s are
/// put into a [`Policy`] and that Policy is applied to a particular resource.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
#[schemars(rename = "{AllowedRoles}RoleAssignment")]
pub struct RoleAssignment<AllowedRoles> {
    pub identity_type: IdentityType,
    pub identity_id: Uuid,
    pub role_name: AllowedRoles,
}

impl<AllowedRoles, E> TryFrom<db::model::RoleAssignment>
    for RoleAssignment<AllowedRoles>
where
    AllowedRoles: FromStr<Err = E>,
    E: std::fmt::Display,
{
    type Error = Error;

    fn try_from(
        role_asgn: db::model::RoleAssignment,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            identity_type: IdentityType::from(role_asgn.identity_type),
            identity_id: role_asgn.identity_id,
            role_name: AllowedRoles::from_str(&role_asgn.role_name).map_err(
                |error| {
                    Error::internal_error(&format!(
                        "parsing database role assignment: \
                        unrecognized role name {:?}: {:#}",
                        &role_asgn.role_name, error,
                    ))
                },
            )?,
        })
    }
}

/// Describes what kind of identity is described by an id
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum IdentityType {
    UserBuiltin,
    SiloUser,
}

impl From<db::model::IdentityType> for IdentityType {
    fn from(other: db::model::IdentityType) -> Self {
        match other {
            db::model::IdentityType::UserBuiltin => IdentityType::UserBuiltin,
            db::model::IdentityType::SiloUser => IdentityType::SiloUser,
        }
    }
}

#[cfg(test)]
mod test {
    use super::IdentityType;
    use super::Policy;
    use super::MAX_ROLE_ASSIGNMENTS_PER_RESOURCE;
    use crate::db;
    use crate::external_api::shared;
    use omicron_common::api::external::Error;
    use omicron_common::api::external::ResourceType;
    use parse_display::FromStr;
    use serde_with::DeserializeFromStr;

    #[derive(
        Clone, Copy, Debug, DeserializeFromStr, Eq, FromStr, PartialEq,
    )]
    #[display(style = "kebab-case")]
    pub enum DummyRoles {
        Bogus,
    }

    #[test]
    fn test_policy_parsing() {
        // Success case (edge case: max number of role assignments)
        let role_assignment = serde_json::json!({
            "identity_type": "user_builtin",
            "identity_id": "75ec4a39-67cf-4549-9e74-44b92947c37c",
            "role_name": "bogus"
        });
        const MAX: usize = MAX_ROLE_ASSIGNMENTS_PER_RESOURCE;
        let okay_input =
            serde_json::Value::Array(vec![role_assignment.clone(); MAX]);
        let policy: Policy<DummyRoles> =
            serde_json::from_value(serde_json::json!({
                "role_assignments": okay_input
            }))
            .expect("unexpectedly failed with okay input");
        assert_eq!(policy.role_assignments[0].role_name, DummyRoles::Bogus);

        // Failure case: too many role assignments
        let bad_input =
            serde_json::Value::Array(vec![role_assignment; MAX + 1]);
        let error =
            serde_json::from_value::<Policy<DummyRoles>>(serde_json::json!({
                "role_assignments": bad_input
            }))
            .expect_err("unexpectedly succeeded with too many items");
        assert_eq!(
            error.to_string(),
            "invalid length 65, expected a list of at most 64 role assignments"
        );
    }

    #[test]
    fn test_role_assignment_from_database() {
        let identity_id =
            "75ec4a39-67cf-4549-9e74-44b92947c37c".parse().unwrap();
        let resource_id =
            "9e3e3be8-4051-4ddb-92fa-32cc5294f066".parse().unwrap();
        let bad_input = db::model::RoleAssignment {
            identity_type: db::model::IdentityType::UserBuiltin,
            identity_id,
            resource_type: ResourceType::Organization.to_string(),
            resource_id,
            role_name: String::from("bogosity"),
        };

        let ok_input = db::model::RoleAssignment {
            role_name: String::from("bogus"),
            ..bad_input.clone()
        };

        let error = <shared::RoleAssignment<DummyRoles>>::try_from(bad_input)
            .expect_err("unexpectedly succeeding parsing database role");
        println!("error: {:#}", error);
        if let Error::InternalError { internal_message } = error {
            assert_eq!(
                internal_message,
                "parsing database role assignment: unrecognized role name \
                \"bogosity\": parse failed."
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
        assert_eq!(success.identity_type, IdentityType::UserBuiltin);
        assert_eq!(success.identity_id, identity_id);
        assert_eq!(success.role_name, DummyRoles::Bogus);
    }
}
