// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::SqlU16;
use crate::schema::oximeter;
use chrono::{DateTime, Utc};
use nexus_types::internal_api;
use uuid::Uuid;

/// A record representing a registered `oximeter` collector.
#[derive(Queryable, Insertable, Debug, Clone, Copy, PartialEq, Eq)]
#[diesel(table_name = oximeter)]
pub struct OximeterInfo {
    /// The ID for this oximeter instance.
    pub id: Uuid,
    /// When this resource was created.
    pub time_created: DateTime<Utc>,
    /// When this resource was last modified.
    pub time_modified: DateTime<Utc>,
    /// When this resource was deleted.
    pub time_deleted: Option<DateTime<Utc>>,
    /// The address on which this `oximeter` instance listens for requests.
    pub ip: ipnetwork::IpNetwork,
    /// The port on which this `oximeter` instance listens for requests.
    pub port: SqlU16,
}

impl OximeterInfo {
    pub fn new(info: &internal_api::params::OximeterInfo) -> Self {
        let now = Utc::now();
        Self {
            id: info.collector_id,
            time_created: now,
            time_modified: now,
            time_deleted: None,
            ip: info.address.ip().into(),
            port: info.address.port().into(),
        }
    }
}
