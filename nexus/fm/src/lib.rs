// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Fault management

use nexus_types::fm;
use nexus_types::inventory;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SitrepUuid;

#[derive(Debug)]
pub struct SitrepBuilder<'a> {
    inventory: &'a inventory::Collection,
    parent_sitrep: Option<&'a fm::Sitrep>,
    comment: String,
}

impl<'a> SitrepBuilder<'a> {
    pub fn new(
        inventory: &'a inventory::Collection,
        parent_sitrep: Option<&'a fm::Sitrep>,
    ) -> Self {
        SitrepBuilder { inventory, parent_sitrep, comment: String::new() }
    }
    
    

    pub fn build(self, creator_id: OmicronZoneUuid) -> fm::Sitrep {
        fm::Sitrep {
            metadata: fm::SitrepMetadata {
                id: SitrepUuid::new_v4(),
                parent_sitrep_id: self.parent_sitrep.map(|s| s.metadata.id),
                inv_collection_id: self.inventory.id,
                creator_id,
                comment: self.comment,
                time_created: chrono::Utc::now(),
            },
            // TODO(eliza): draw the rest of the owl...
        }
    }
}
