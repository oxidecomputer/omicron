// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types that are used as both views and params

use crate::db;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

/// Client view of a [`Policy`]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct Policy {
    // XXX-dap can we limit the length of this in schemars/serde?
    pub role_assignments: Vec<RoleAssignment>,
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
    pub role_name: String, // XXX-dap should this be RoleName?
}

impl From<db::model::RoleAssignment> for RoleAssignment {
    fn from(role_asgn: db::model::RoleAssignment) -> Self {
        Self {
            identity_type: IdentityType::from(role_asgn.actor_type),
            identity_id: role_asgn.actor_id,
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

impl From<db::model::ActorType> for IdentityType {
    fn from(other: db::model::ActorType) -> Self {
        match other {
            db::model::ActorType::UserBuiltin => IdentityType::UserBuiltin,
            db::model::ActorType::SiloUser => IdentityType::SiloUser,
        }
    }
}
