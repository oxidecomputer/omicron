// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::db::schema::oximeter;
use crate::internal_api;
use chrono::{DateTime, Utc};
use uuid::Uuid;

/// Message used to notify Nexus that this oximeter instance is up and running.
#[derive(Queryable, Insertable, Debug, Clone, Copy)]
#[diesel(table_name = oximeter)]
pub struct OximeterInfo {
    /// The ID for this oximeter instance.
    pub id: Uuid,
    /// When this resource was created.
    pub time_created: DateTime<Utc>,
    /// When this resource was last modified.
    pub time_modified: DateTime<Utc>,
    /// The address on which this oximeter instance listens for requests
    pub ip: ipnetwork::IpNetwork,
    // TODO: Make use of SqlU16
    pub port: i32,
}

impl OximeterInfo {
    pub fn new(info: &internal_api::params::OximeterInfo) -> Self {
        let now = Utc::now();
        Self {
            id: info.collector_id,
            time_created: now,
            time_modified: now,
            ip: info.address.ip().into(),
            port: info.address.port().into(),
        }
    }
}
