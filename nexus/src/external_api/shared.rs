// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types that are used as both views and params

use crate::db;
use schemars::JsonSchema;
use serde::de::Error;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
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
pub struct Policy {
    /// Roles directly assigned on this resource
    #[serde(deserialize_with = "role_assignments_deserialize")]
    pub role_assignments: Vec<RoleAssignment>,
}

fn role_assignments_deserialize<'de, D>(
    d: D,
) -> Result<Vec<RoleAssignment>, D::Error>
where
    D: Deserializer<'de>,
{
    let v = <Vec<RoleAssignment> as Deserialize>::deserialize(d)?;
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
pub struct RoleAssignment {
    pub identity_type: IdentityType,
    pub identity_id: Uuid,
    pub role_name: String,
}

impl From<db::model::RoleAssignment> for RoleAssignment {
    fn from(role_asgn: db::model::RoleAssignment) -> Self {
        Self {
            identity_type: IdentityType::from(role_asgn.identity_type),
            identity_id: role_asgn.identity_id,
            role_name: role_asgn.role_name,
        }
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
    use super::Policy;
    use super::MAX_ROLE_ASSIGNMENTS_PER_RESOURCE;

    #[test]
    fn test_policy_length_limit() {
        let role_assignment = serde_json::json!({
            "identity_type": "user_builtin",
            "identity_id": "75ec4a39-67cf-4549-9e74-44b92947c37c",
            "role_name": "bogus"
        });
        const MAX: usize = MAX_ROLE_ASSIGNMENTS_PER_RESOURCE;
        let okay_input =
            serde_json::Value::Array(vec![role_assignment.clone(); MAX]);
        let policy: Policy = serde_json::from_value(serde_json::json!({
            "role_assignments": okay_input
        }))
        .expect("unexpectedly failed with okay input");
        assert_eq!(policy.role_assignments[0].role_name, "bogus");

        let bad_input =
            serde_json::Value::Array(vec![role_assignment; MAX + 1]);
        let error = serde_json::from_value::<Policy>(serde_json::json!({
            "role_assignments": bad_input
        }))
        .expect_err("unexpectedly succeeded with too many items");
        assert_eq!(
            error.to_string(),
            "invalid length 65, expected a list of at most 64 role assignments"
        );
    }
}
