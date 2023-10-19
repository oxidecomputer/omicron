use std::net::{Ipv4Addr, Ipv6Addr};

use super::MacAddr;
use crate::{
    schema::{ipv4_nat_entry, ipv4_nat_version},
    SqlU16, SqlU32, Vni,
};
use chrono::{DateTime, Utc};
use omicron_common::api::external;
use schemars::JsonSchema;
use serde::Serialize;
use uuid::Uuid;

// TODO correctness
// If we're not going to store ipv4 and ipv6
// NAT entries in the same table, and we don't
// need any of the special properties of the IpNetwork
// column type, does it make sense to use a different
// column type?
/// Database representation of an Ipv4 NAT Entry.
#[derive(Insertable, Debug, Clone)]
#[diesel(table_name = ipv4_nat_entry)]
pub struct Ipv4NatValues {
    pub external_address: ipnetwork::IpNetwork,
    pub first_port: SqlU16,
    pub last_port: SqlU16,
    pub sled_address: ipnetwork::IpNetwork,
    pub vni: Vni,
    pub mac: MacAddr,
}

// TODO correctness
// If we're not going to store ipv4 and ipv6
// NAT entries in the same table, we should probably
// make the types more restrictive to prevent an
// accidental ipv6 entry from being created.
#[derive(Queryable, Debug, Clone, Selectable)]
#[diesel(table_name = ipv4_nat_entry)]
pub struct Ipv4NatEntry {
    pub id: Uuid,
    pub external_address: ipnetwork::IpNetwork,
    pub first_port: SqlU16,
    pub last_port: SqlU16,
    pub sled_address: ipnetwork::IpNetwork,
    pub vni: Vni,
    pub mac: MacAddr,
    pub version_added: SqlU32,
    pub version_removed: Option<SqlU32>,
    pub time_created: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,
}

impl Ipv4NatEntry {
    pub fn first_port(&self) -> u16 {
        self.first_port.into()
    }

    pub fn last_port(&self) -> u16 {
        self.last_port.into()
    }

    pub fn version_added(&self) -> u32 {
        self.version_added.into()
    }

    pub fn version_removed(&self) -> Option<u32> {
        self.version_removed.map(|i| i.into())
    }
}

#[derive(Queryable, Debug, Clone, Selectable)]
#[diesel(table_name = ipv4_nat_version)]
pub struct Ipv4NatGen {
    pub last_value: SqlU32,
    pub log_cnt: SqlU32,
    pub is_called: bool,
}

/// NAT Record
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct Ipv4NatEntryView {
    pub external_address: Ipv4Addr,
    pub first_port: u16,
    pub last_port: u16,
    pub sled_address: Ipv6Addr,
    pub vni: external::Vni,
    pub mac: external::MacAddr,
    pub gen: u32,
    pub deleted: bool,
}

impl From<Ipv4NatEntry> for Ipv4NatEntryView {
    fn from(value: Ipv4NatEntry) -> Self {
        let external_address = match value.external_address.ip() {
            std::net::IpAddr::V4(a) => a,
            std::net::IpAddr::V6(_) => unreachable!(),
        };

        let sled_address = match value.sled_address.ip() {
            std::net::IpAddr::V4(_) => unreachable!(),
            std::net::IpAddr::V6(a) => a,
        };

        let (gen, deleted) = match value.version_removed {
            Some(gen) => (*gen, true),
            None => (*value.version_added, false),
        };

        Self {
            external_address,
            first_port: value.first_port(),
            last_port: value.last_port(),
            sled_address,
            vni: value.vni.0,
            mac: *value.mac,
            gen,
            deleted,
        }
    }
}

/// NAT Generation
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct Ipv4NatGenView {
    pub gen: u32,
}
