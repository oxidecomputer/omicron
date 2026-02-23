// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Policy and role types for the Nexus external API.

use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SiloGroupUuid;
use omicron_uuid_kinds::SiloUserUuid;
use parse_display::FromStr;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::de::Error as _;
use strum::EnumIter;
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

/// Policy for a particular resource
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
/// The resource is not part of this structure.  Rather, `RoleAssignment`s are
/// put into a `Policy` and that Policy is applied to a particular resource.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
#[schemars(rename = "{AllowedRoles}RoleAssignment")]
pub struct RoleAssignment<AllowedRoles> {
    pub identity_type: IdentityType,
    pub identity_id: Uuid,
    pub role_name: AllowedRoles,
}

impl<AllowedRoles> RoleAssignment<AllowedRoles> {
    pub fn for_silo_user(
        silo_user_id: SiloUserUuid,
        role_name: AllowedRoles,
    ) -> Self {
        Self {
            identity_type: IdentityType::SiloUser,
            identity_id: silo_user_id.into_untyped_uuid(),
            role_name,
        }
    }

    pub fn for_silo_group(
        silo_group_id: SiloGroupUuid,
        role_name: AllowedRoles,
    ) -> Self {
        Self {
            identity_type: IdentityType::SiloGroup,
            identity_id: silo_group_id.into_untyped_uuid(),
            role_name,
        }
    }
}

#[derive(
    Clone,
    Copy,
    Debug,
    Deserialize,
    EnumIter,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
    JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetRole {
    Admin,
    Collaborator,
    Viewer,
    // There are other Fleet roles, but they are not externally-visible and so
    // they do not show up in this enum.
}

#[derive(
    Clone,
    Copy,
    Debug,
    Deserialize,
    EnumIter,
    Eq,
    FromStr,
    Ord,
    PartialOrd,
    PartialEq,
    Serialize,
    JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum SiloRole {
    Admin,
    Collaborator,
    LimitedCollaborator,
    Viewer,
}

#[derive(
    Clone,
    Copy,
    Debug,
    Deserialize,
    EnumIter,
    Eq,
    FromStr,
    PartialEq,
    Serialize,
    JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ProjectRole {
    Admin,
    Collaborator,
    LimitedCollaborator,
    Viewer,
}

/// Describes what kind of identity is described by an id
// This is a subset of the identity types that might be found in the database
// because we do not expose some (e.g., built-in users) externally.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum IdentityType {
    SiloUser,
    SiloGroup,
}
