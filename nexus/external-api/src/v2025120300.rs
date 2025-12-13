// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus external types that changed from 2025120300 to 2025121200

use nexus_types::external_api::views;
use omicron_common::api::external::Name;
use omicron_uuid_kinds::{SiloGroupUuid, SiloUserUuid};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// View of a User
///
/// This is the version from 2025120300, without time_created and time_modified fields.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct User {
    #[schemars(with = "Uuid")]
    pub id: SiloUserUuid,

    /// Human-readable name that can identify the user
    pub display_name: String,

    /// Uuid of the silo to which this user belongs
    pub silo_id: Uuid,
}

impl From<views::User> for User {
    fn from(new: views::User) -> User {
        User {
            id: new.id,
            display_name: new.display_name,
            silo_id: new.silo_id,
        }
    }
}

/// View of a Group
///
/// This is the version from 2025120300, without time_created and time_modified fields.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct Group {
    #[schemars(with = "Uuid")]
    pub id: SiloGroupUuid,

    /// Human-readable name that can identify the group
    pub display_name: String,

    /// Uuid of the silo to which this group belongs
    pub silo_id: Uuid,
}

impl From<views::Group> for Group {
    fn from(new: views::Group) -> Group {
        Group {
            id: new.id,
            display_name: new.display_name,
            silo_id: new.silo_id,
        }
    }
}

/// Info about the current user
///
/// This is the version from 2025120300, without time_created and time_modified fields.
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

impl From<views::CurrentUser> for CurrentUser {
    fn from(new: views::CurrentUser) -> CurrentUser {
        CurrentUser {
            user: new.user.into(),
            silo_name: new.silo_name,
            fleet_viewer: new.fleet_viewer,
            silo_admin: new.silo_admin,
        }
    }
}
