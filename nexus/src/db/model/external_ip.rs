// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Model types for external IPs, both for instances and externally-facing
//! services.

use crate::db::model::SqlU16;
use crate::db::schema::instance_external_ip;
use chrono::DateTime;
use chrono::Utc;
use diesel::Queryable;
use diesel::Selectable;
use ipnetwork::IpNetwork;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Selectable, Queryable, Insertable)]
#[diesel(table_name = instance_external_ip)]
pub struct InstanceExternalIp {
    pub id: Uuid,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,
    pub ip_pool_id: Uuid,
    pub ip_pool_range_id: Uuid,
    pub instance_id: Uuid,
    pub ip: IpNetwork,
    pub first_port: SqlU16,
    pub last_port: SqlU16,
}

impl From<InstanceExternalIp> for sled_agent_client::types::ExternalIp {
    fn from(eip: InstanceExternalIp) -> Self {
        Self {
            ip: eip.ip.ip().to_string(),
            first_port: eip.first_port.0,
            last_port: eip.last_port.0,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct IncompleteInstanceExternalIp {
    pub id: Uuid,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,
    pub instance_id: Uuid,
}

impl IncompleteInstanceExternalIp {
    pub fn new(instance_id: Uuid) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            time_created: now,
            time_modified: now,
            time_deleted: None,
            instance_id,
        }
    }
}
