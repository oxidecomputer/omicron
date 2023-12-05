// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::schema::sled_underlay_subnet_allocation;
use uuid::Uuid;

/// Underlay allocation for a sled added to an initialized rack
#[derive(Queryable, Insertable, Debug, Clone, Selectable)]
#[diesel(table_name = sled_underlay_subnet_allocation)]
pub struct SledUnderlaySubnetAllocation {
    pub rack_id: Uuid,
    pub sled_id: Uuid,
    pub subnet_octet: i16,
    pub hw_baseboard_id: Uuid,
}
