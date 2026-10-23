// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Model types for `unadopted_rack` and `unadopted_sled` tables, which are used
//! as part of the multirack join process to track racks and sleds that have not
//! yet joined the cluster.

use crate::{DbTypedUuid, Ipv6Net};
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::{unadopted_rack, unadopted_sled};
use omicron_uuid_kinds::{RackKind, RackUuid, SledKind, SledUuid};

#[derive(Queryable, Insertable, Selectable, Clone, Debug)]
#[diesel(table_name = unadopted_rack)]
pub struct UnadoptedRack {
    pub id: DbTypedUuid<RackKind>,

    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,

    pub rack_subnet: Ipv6Net,

    pub join_service_ip: crate::Ipv6Addr,
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

    pub sled_agent_ip: crate::Ipv6Addr,

    pub sled_agent_started: bool,
    pub last_start_error_seen: Option<String>,
}

impl UnadoptedRack {
    pub fn new(
        id: RackUuid,
        rack_subnet: oxnet::Ipv6Net,
        join_service_ip: std::net::Ipv6Addr,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: id.into(),
            time_created: now,
            time_modified: now,
            time_deleted: None,
            rack_subnet: rack_subnet.into(),
            join_service_ip: join_service_ip.into(),
        }
    }

    pub fn id(&self) -> RackUuid {
        self.id.into()
    }
}

impl UnadoptedSled {
    /// Create a new row for a sled whose sled-agent has not yet been started.
    pub fn new(
        id: SledUuid,
        rack_id: RackUuid,
        serial_number: String,
        part_number: String,
        sled_agent_ip: std::net::Ipv6Addr,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: id.into(),
            rack_id: rack_id.into(),
            time_created: now,
            time_modified: now,
            time_deleted: None,
            serial_number,
            part_number,
            sled_agent_ip: sled_agent_ip.into(),
            sled_agent_started: false,
            last_start_error_seen: None,
        }
    }

    pub fn id(&self) -> SledUuid {
        self.id.into()
    }

    pub fn rack_id(&self) -> RackUuid {
        self.rack_id.into()
    }
}
