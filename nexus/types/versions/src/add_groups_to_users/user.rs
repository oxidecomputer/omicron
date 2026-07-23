// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! User types for version ADD_GROUPS_TO_USERS.
//!
//! Adds a `groups` field to User and CurrentUser.

use chrono::{DateTime, Utc};
use omicron_common::api::external::Name;
use omicron_uuid_kinds::{SiloGroupUuid, SiloUserUuid};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A group of which a user is a member
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct UserGroup {
    #[schemars(with = "Uuid")]
    pub id: SiloGroupUuid,

    /// Human-readable name that can identify the group
    pub display_name: String,
}

/// View of a User
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct User {
    #[schemars(with = "Uuid")]
    pub id: SiloUserUuid,

    /// Human-readable name that can identify the user
    pub display_name: String,

    /// Uuid of the silo to which this user belongs
    pub silo_id: Uuid,

    /// Timestamp when this user was created
    pub time_created: DateTime<Utc>,

    /// Timestamp when this user was last modified
    pub time_modified: DateTime<Utc>,

    /// Groups to which this user belongs, sorted by display name
    pub groups: Vec<UserGroup>,
}

impl From<User> for crate::v2026_03_02_00::user::User {
    fn from(new: User) -> crate::v2026_03_02_00::user::User {
        crate::v2026_03_02_00::user::User {
            id: new.id,
            display_name: new.display_name,
            silo_id: new.silo_id,
            time_created: new.time_created,
            time_modified: new.time_modified,
        }
    }
}

/// Info about the current user
// Add silo name to User because the console needs to display it
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct CurrentUser {
    #[serde(flatten)]
    pub user: User,
    /// Name of the silo to which this user belongs.
    pub silo_name: Name,
    /// Whether this user has the viewer role on the fleet. Used by the web
    /// console to determine whether to show system-level UI.
    pub fleet_viewer: bool,
    /// Whether this user has the admin role on their silo. Used by the web
    /// console to determine whether to show admin-only UI elements.
    pub silo_admin: bool,
}

impl From<CurrentUser> for crate::v2026_03_02_00::user::CurrentUser {
    fn from(new: CurrentUser) -> crate::v2026_03_02_00::user::CurrentUser {
        crate::v2026_03_02_00::user::CurrentUser {
            user: new.user.into(),
            silo_name: new.silo_name,
            fleet_viewer: new.fleet_viewer,
            silo_admin: new.silo_admin,
        }
    }
}
