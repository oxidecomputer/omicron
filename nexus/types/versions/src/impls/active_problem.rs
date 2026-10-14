// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::latest::active_problem::ImpactedResource;
use omicron_uuid_kinds::GenericUuid;
use uuid::Uuid;

impl ImpactedResource {
    /// Returns the ID of the impacted resource.
    ///
    /// API resource IDs are unique across resource types, so this alone
    /// identifies the resource. This can be used to determine the page marker for the subsequent page.q
    pub fn id(&self) -> Uuid {
        match self {
            Self::Sled { id } => id.into_untyped_uuid(),
            Self::PhysicalDisk { id } => id.into_untyped_uuid(),
            Self::Instance { id } => id.into_untyped_uuid(),
            Self::Disk { id } => id.into_untyped_uuid(),
            Self::Switch { id }
            | Self::SwitchPort { id }
            | Self::Image { id }
            | Self::Snapshot { id } => *id,
        }
    }
}
