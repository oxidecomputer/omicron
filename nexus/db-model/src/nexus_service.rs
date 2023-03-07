// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::schema::nexus_service;
use uuid::Uuid;

/// Nexus-specific extended service information.
#[derive(Queryable, Insertable, Debug, Clone, Selectable)]
#[diesel(table_name = nexus_service)]
pub struct NexusService {
    pub id: Uuid,
    pub external_ip_id: Uuid,
}

impl NexusService {
    pub fn new(id: Uuid, external_ip_id: Uuid) -> Self {
        Self { id, external_ip_id }
    }
}
