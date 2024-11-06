// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/5.0/.

// Copyright 2024 Oxide Computer Company

//! Database types for tracking Tofino ASIC table utilization

use super::impl_enum_wrapper;
use crate::schema::asic_table_utilization;
use crate::SqlU32;
use chrono::DateTime;
use chrono::Utc;
use nexus_defaults::IPV4_ADDRESS_TABLE;
use nexus_defaults::IPV4_ARP_TABLE;
use nexus_defaults::IPV4_NAT_TABLE;
use nexus_defaults::IPV4_ROUTING_TABLE;
use nexus_defaults::IPV6_ADDRESS_TABLE;
use nexus_defaults::IPV6_NAT_TABLE;
use nexus_defaults::IPV6_NEIGHBOR_TABLE;
use nexus_defaults::IPV6_ROUTING_TABLE;
use omicron_common::api::external::Error;
use omicron_common::api::internal::nexus;
use serde::Deserialize;
use serde::Serialize;
use std::io::Write;
use uuid::Uuid;

impl_enum_wrapper!(
    #[derive(Clone, Debug, QueryId, SqlType)]
    #[diesel(postgres_type(name = "asic_table", schema = "public"))]
    pub struct AsicTableEnum;

    #[derive(
        AsExpression,
        Clone,
        Copy,
        Debug,
        Deserialize,
        Eq,
        FromSqlRow,
        PartialEq,
        Serialize
    )]
    #[diesel(sql_type = AsicTableEnum)]
    pub struct AsicTable(pub nexus::AsicTable);

    // Enum values
    Ipv4Routing => b"ipv4_routing"
    Ipv6Routing => b"ipv6_routing"
    Ipv4Addresses => b"ipv4_addresses"
    Ipv6Addresses => b"ipv6_addresses"
    Ipv4Nat => b"ipv4_nat"
    Ipv6Nat => b"ipv6_nat"
    Ipv4Arp => b"ipv4_arp"
    Ipv6Neighbor => b"ipv6_neighbor"
);

impl From<nexus::AsicTable> for AsicTable {
    fn from(value: nexus::AsicTable) -> Self {
        Self(value)
    }
}

impl std::fmt::Display for AsicTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Utilization of an ASIC table contributed by one API resource.
#[derive(
    Clone,
    Copy,
    Debug,
    Deserialize,
    Eq,
    Queryable,
    Insertable,
    Selectable,
    Serialize,
    PartialEq,
)]
#[diesel(table_name = asic_table_utilization)]
pub struct AsicTableUtilization {
    /// The ID of the API resource.
    pub id: Uuid,

    /// The ASIC table this entry belongs to.
    pub asic_table: AsicTable,

    /// The number of entries in the table consumed by this resource.
    pub n_entries: SqlU32,

    /// The time this entry was created.
    pub time_created: DateTime<Utc>,

    /// The time this entry was deleted.
    pub time_deleted: Option<DateTime<Utc>>,
}

impl AsicTableUtilization {
    /// Create a new utilization entry consuming one entry on the ASIC table.
    pub fn one(id: Uuid, table: AsicTable) -> Self {
        Self::new(id, table, 1).unwrap()
    }

    /// Create a new utilization entry consuming `n_entries` entries on the ASIC
    /// table.
    ///
    /// This returns an error if `count` is zero, or is greater than the
    /// allocatable capacity of the named table.
    pub fn new(
        id: Uuid,
        table: AsicTable,
        n_entries: u32,
    ) -> Result<Self, Error> {
        if n_entries == 0 {
            return Err(Error::internal_error(
                "Number of entries must be nonzero",
            ));
        }
        let cap = match table.0 {
            nexus::AsicTable::Ipv4Routing => &IPV4_ROUTING_TABLE,
            nexus::AsicTable::Ipv6Routing => &IPV6_ROUTING_TABLE,
            nexus::AsicTable::Ipv4Addresses => &IPV4_ADDRESS_TABLE,
            nexus::AsicTable::Ipv6Addresses => &IPV6_ADDRESS_TABLE,
            nexus::AsicTable::Ipv4Nat => &IPV4_NAT_TABLE,
            nexus::AsicTable::Ipv6Nat => &IPV6_NAT_TABLE,
            nexus::AsicTable::Ipv4Arp => &IPV4_ARP_TABLE,
            nexus::AsicTable::Ipv6Neighbor => &IPV6_NEIGHBOR_TABLE,
        };
        if n_entries > cap.n_allocatable() {
            return Err(Error::internal_error(&format!(
                "Cannot consume {} entries in ASIC table \
                {}, the maximum allocatable is {}",
                n_entries,
                table.0,
                cap.n_allocatable(),
            )));
        }
        Ok(Self {
            id,
            asic_table: table,
            n_entries: n_entries.into(),
            time_created: Utc::now(),
            time_deleted: None,
        })
    }
}
