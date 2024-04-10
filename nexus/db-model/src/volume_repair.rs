// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::schema::volume_repair;
use uuid::Uuid;

#[derive(Queryable, Insertable, Debug, Selectable, Clone)]
#[diesel(table_name = volume_repair)]
pub struct VolumeRepair {
    pub volume_id: Uuid,
    pub repair_id: Uuid,
}
