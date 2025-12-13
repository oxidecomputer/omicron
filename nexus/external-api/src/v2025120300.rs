// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus external types that changed from 2025120300 to 2025120900

use nexus_types::external_api::views;
use omicron_uuid_kinds::SiloGroupUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// View of a Group (without member_count field)
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct Group {
    #[schemars(with = "Uuid")]
    pub id: SiloGroupUuid,

    /// Human-readable name that can identify the group
    pub display_name: String,

    /// Uuid of the silo to which this group belongs
    pub silo_id: Uuid,
}

impl From<Group> for views::Group {
    fn from(old: Group) -> views::Group {
        views::Group {
            id: old.id,
            display_name: old.display_name,
            silo_id: old.silo_id,
            // Default member_count to 0 for old clients that didn't have this field
            member_count: 0,
        }
    }
}

impl From<views::Group> for Group {
    fn from(new: views::Group) -> Group {
        Group {
            id: new.id,
            display_name: new.display_name,
            silo_id: new.silo_id,
            // Drop member_count when converting to old version
        }
    }
}
