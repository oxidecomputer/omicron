// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! NAT entries mapping external IP addresses to their hosting sled.
//!
//! The data here represents the "Dendrite side" of NAT for external
//! addresses. It maps the external IP to information about the host sled, OPTE
//! instance, and VNI for the external address, but notably does not contain the
//! actual internal or private address that the external address is translated
//! to.
//!
//! That address is provided to OPTE instead, which implements the "other side"
//! of the address translation. It decapsulates the packet and possibly rewrites
//! the IP address on the way to its guest or public Oxide service. The data
//! here is what we need to tell Dendrite how to encapsulate the packet so that
//! it traverses the underlay and OPTE knows how to decap it.

use super::MacAddr;
use crate::{IpNet, Ipv6Net, SqlU16, Vni};
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::{nat_changes, nat_entry};
use nexus_types::internal_api::views::NatEntryView;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

/// Values used to create a `NatEntry`
#[derive(Insertable, Debug, Clone, Eq, PartialEq)]
#[diesel(table_name = nat_entry)]
pub struct NatEntryValues {
    pub external_address: IpNet,
    pub first_port: SqlU16,
    pub last_port: SqlU16,
    pub sled_address: Ipv6Net,
    pub vni: Vni,
    pub mac: MacAddr,
}

/// Database representation of a NAT Entry.
#[derive(Queryable, Debug, Clone, Selectable, Serialize, Deserialize)]
#[diesel(table_name = nat_entry)]
pub struct NatEntry {
    pub id: Uuid,
    pub external_address: IpNet,
    pub first_port: SqlU16,
    pub last_port: SqlU16,
    pub sled_address: Ipv6Net,
    pub vni: Vni,
    pub mac: MacAddr,
    pub version_added: i64,
    pub version_removed: Option<i64>,
    pub time_created: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,
}

impl NatEntry {
    pub fn first_port(&self) -> u16 {
        self.first_port.into()
    }

    pub fn last_port(&self) -> u16 {
        self.last_port.into()
    }
}

/// Summary of changes to NAT entries.
#[derive(Queryable, Debug, Clone, Selectable, Serialize, Deserialize)]
#[diesel(table_name = nat_changes)]
pub struct NatChange {
    pub external_address: IpNet,
    pub first_port: SqlU16,
    pub last_port: SqlU16,
    pub sled_address: Ipv6Net,
    pub vni: Vni,
    pub mac: MacAddr,
    pub version: i64,
    pub deleted: bool,
}

impl From<NatChange> for NatEntryView {
    fn from(value: NatChange) -> Self {
        Self {
            external_address: ::std::net::IpAddr::from(value.external_address),
            first_port: value.first_port.into(),
            last_port: value.last_port.into(),
            sled_address: value.sled_address.addr(),
            vni: value.vni.0,
            mac: *value.mac,
            gen: value.version,
            deleted: value.deleted,
        }
    }
}
