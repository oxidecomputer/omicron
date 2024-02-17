// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::impl_enum_type;
use crate::schema::{loopback_address, switch_vlan_interface_config};
use crate::SqlU16;
use db_macros::Asset;
use ipnetwork::IpNetwork;
use nexus_types::external_api::params;
use nexus_types::identity::Asset;
use omicron_common::api::external;
use omicron_uuid_kinds::LoopbackAddressKind;
use omicron_uuid_kinds::TypedUuid;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

impl_enum_type!(
    #[derive(SqlType, Debug, Clone, Copy)]
    #[diesel(postgres_type(name = "switch_interface_kind", schema = "public"))]
    pub struct DbSwitchInterfaceKindEnum;

    #[derive(
        Clone,
        Copy,
        Debug,
        AsExpression,
        FromSqlRow,
        PartialEq,
        Serialize,
        Deserialize
    )]
    #[diesel(sql_type = DbSwitchInterfaceKindEnum)]
    pub enum DbSwitchInterfaceKind;

    Primary => b"primary"
    Vlan => b"vlan"
    Loopback => b"loopback"
);

impl From<params::SwitchInterfaceKind> for DbSwitchInterfaceKind {
    fn from(k: params::SwitchInterfaceKind) -> Self {
        match k {
            params::SwitchInterfaceKind::Primary => {
                DbSwitchInterfaceKind::Primary
            }
            params::SwitchInterfaceKind::Vlan(_) => DbSwitchInterfaceKind::Vlan,
            params::SwitchInterfaceKind::Loopback => {
                DbSwitchInterfaceKind::Loopback
            }
        }
    }
}

impl Into<external::SwitchInterfaceKind> for DbSwitchInterfaceKind {
    fn into(self) -> external::SwitchInterfaceKind {
        match self {
            DbSwitchInterfaceKind::Primary => {
                external::SwitchInterfaceKind::Primary
            }
            DbSwitchInterfaceKind::Vlan => external::SwitchInterfaceKind::Vlan,
            DbSwitchInterfaceKind::Loopback => {
                external::SwitchInterfaceKind::Loopback
            }
        }
    }
}

#[derive(
    Queryable,
    Insertable,
    Selectable,
    Clone,
    Debug,
    Serialize,
    Deserialize,
    AsChangeset,
)]
#[diesel(table_name = switch_vlan_interface_config)]
pub struct SwitchVlanInterfaceConfig {
    pub interface_config_id: Uuid,
    pub vid: SqlU16,
}

impl SwitchVlanInterfaceConfig {
    pub fn new(interface_config_id: Uuid, vid: u16) -> Self {
        Self { interface_config_id, vid: vid.into() }
    }
}

impl Into<external::SwitchVlanInterfaceConfig> for SwitchVlanInterfaceConfig {
    fn into(self) -> external::SwitchVlanInterfaceConfig {
        external::SwitchVlanInterfaceConfig {
            interface_config_id: self.interface_config_id,
            vlan_id: self.vid.into(),
        }
    }
}

#[derive(
    Queryable,
    Insertable,
    Selectable,
    Clone,
    Debug,
    Asset,
    Serialize,
    Deserialize,
)]
#[diesel(table_name = loopback_address)]
#[asset(uuid_kind = LoopbackAddressKind)]
pub struct LoopbackAddress {
    #[diesel(embed)]
    pub identity: LoopbackAddressIdentity,
    pub address_lot_block_id: Uuid,
    pub rsvd_address_lot_block_id: Uuid,
    pub rack_id: Uuid,
    pub switch_location: String,
    pub address: IpNetwork,
    pub anycast: bool,
}

impl LoopbackAddress {
    pub fn new(
        id: Option<TypedUuid<LoopbackAddressKind>>,
        address_lot_block_id: Uuid,
        rsvd_address_lot_block_id: Uuid,
        rack_id: Uuid,
        switch_location: String,
        address: IpNetwork,
        anycast: bool,
    ) -> Self {
        Self {
            identity: LoopbackAddressIdentity::new(
                id.unwrap_or(TypedUuid::new_v4()),
            ),
            address_lot_block_id,
            rsvd_address_lot_block_id,
            rack_id,
            switch_location,
            address,
            anycast,
        }
    }
}

impl Into<external::LoopbackAddress> for LoopbackAddress {
    fn into(self) -> external::LoopbackAddress {
        external::LoopbackAddress {
            id: self.identity().id,
            address_lot_block_id: self.address_lot_block_id,
            rack_id: self.rack_id,
            switch_location: self.switch_location.clone(),
            address: self.address.into(),
        }
    }
}
