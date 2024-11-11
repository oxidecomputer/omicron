// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::SqlU16;
use crate::schema::oximeter;
use chrono::{DateTime, Utc};
use nexus_types::internal_api;
use uuid::Uuid;

/// A record representing a registered `oximeter` collector.
#[derive(
    Queryable, Insertable, Selectable, Debug, Clone, Copy, PartialEq, Eq,
)]
#[diesel(table_name = oximeter)]
pub struct OximeterInfo {
    /// The ID for this oximeter instance.
    pub id: Uuid,
    /// When this resource was created.
    pub time_created: DateTime<Utc>,
    /// When this resource was last modified.
    pub time_modified: DateTime<Utc>,
    /// When this resource was expunged.
    //
    // We typically refer to _zones_ as expunged; this isn't quite the same
    // thing since this is the record of a running Oximeter instance. Some time
    // after an Oximeter zone has been expunged (usually not very long!), the
    // blueprint_executor RPW will mark the Oximeter instance that was running
    // in that zone as expunged, setting this field to a non-None value, which
    // will cause it to no longer be chosen as a potential collector for
    // producers (and will result in any producers it had been assigned being
    // reassigned to some other collector).
    pub time_expunged: Option<DateTime<Utc>>,
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
            time_expunged: None,
            ip: info.address.ip().into(),
            port: info.address.port().into(),
        }
    }
}
