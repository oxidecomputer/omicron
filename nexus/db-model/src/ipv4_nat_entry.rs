use std::net::{Ipv4Addr, Ipv6Addr};

use super::MacAddr;
use crate::{
    schema::{ipv4_nat_entry, ipv4_nat_version},
    Ipv4Net, Ipv6Net, SqlU16, Vni,
};
use chrono::{DateTime, Utc};
use omicron_common::api::external;
use schemars::JsonSchema;
use serde::Serialize;
use uuid::Uuid;

/// Values used to create an Ipv4NatEntry
#[derive(Insertable, Debug, Clone)]
#[diesel(table_name = ipv4_nat_entry)]
pub struct Ipv4NatValues {
    pub external_address: Ipv4Net,
    pub first_port: SqlU16,
    pub last_port: SqlU16,
    pub sled_address: Ipv6Net,
    pub vni: Vni,
    pub mac: MacAddr,
}

/// Database representation of an Ipv4 NAT Entry.
#[derive(Queryable, Debug, Clone, Selectable)]
#[diesel(table_name = ipv4_nat_entry)]
pub struct Ipv4NatEntry {
    pub id: Uuid,
    pub external_address: Ipv4Net,
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

impl Ipv4NatEntry {
    pub fn first_port(&self) -> u16 {
        self.first_port.into()
    }

    pub fn last_port(&self) -> u16 {
        self.last_port.into()
    }
}

/// Database representation of an Ipv4 NAT Generation.
#[derive(Queryable, Debug, Clone, Selectable)]
#[diesel(table_name = ipv4_nat_version)]
pub struct Ipv4NatGen {
    pub last_value: i64,
    pub log_cnt: i64,
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
    pub gen: i64,
    pub deleted: bool,
}

impl From<Ipv4NatEntry> for Ipv4NatEntryView {
    fn from(value: Ipv4NatEntry) -> Self {
        let (gen, deleted) = match value.version_removed {
            Some(gen) => (gen, true),
            None => (value.version_added, false),
        };

        Self {
            external_address: value.external_address.ip(),
            first_port: value.first_port(),
            last_port: value.last_port(),
            sled_address: value.sled_address.ip(),
            vni: value.vni.0,
            mac: *value.mac,
            gen,
            deleted,
        }
    }
}
