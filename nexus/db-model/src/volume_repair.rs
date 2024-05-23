// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::schema::volume_repair;
use uuid::Uuid;

/// When modifying a Volume by replacing its parts, Nexus should take care to
/// only replace one region or snapshot for a volume at a time. Technically, the
/// Upstairs can support two at a time, but codifying "only one at a time" is
/// safer, and does not allow the possiblity for a Nexus bug to replace all
/// three regions of a region set at a time (aka total data loss!). This "one at
/// a time" constraint is enforced by each repair also creating a VolumeRepair
/// record, a table for which there is a UNIQUE CONSTRAINT on the volume ID.
#[derive(Queryable, Insertable, Debug, Selectable, Clone)]
#[diesel(table_name = volume_repair)]
pub struct VolumeRepair {
    pub volume_id: Uuid,
    pub repair_id: Uuid,
}
