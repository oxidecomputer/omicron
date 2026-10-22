// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Model types for `unadopted_rack` and `unadopted_sled` tables, which are used
//! as part of the multirack join process to track racks and sleds that have not
//! yet joined the cluster.

use crate::{DbTypedUuid, Ipv6Addr, Ipv6Net};
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::{unadopted_rack, unadopted_sled};
use omicron_uuid_kinds::{RackKind, SledKind};

#[derive(Queryable, Insertable, Selectable, Clone, Debug)]
#[diesel(table_name = unadopted_rack)]
pub struct UnadoptedRack {
    pub id: DbTypedUuid<RackKind>,

    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,

    pub rack_subnet: Ipv6Net,

    pub join_service_ip: Ipv6Addr,
}

#[derive(Queryable, Insertable, Selectable, Clone, Debug)]
#[diesel(table_name = unadopted_sled)]
pub struct UnadoptedSled {
    pub id: DbTypedUuid<SledKind>,
    pub rack_id: DbTypedUuid<RackKind>,

    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,

    pub serial_number: String,
    pub part_number: String,

    pub sled_agent_ip: Ipv6Addr,

    pub sled_agent_started: bool,
    pub last_start_error_seen: Option<String>,
}
