// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::schema::resource_lock;
use uuid::Uuid;

/// Database representation of a resource lock
#[derive(Queryable, Insertable, Debug, Clone, Selectable)]
#[diesel(table_name = resource_lock)]
pub struct ResourceLock {
    pub resource_id: Uuid,
    pub lock_id: Uuid,
}
