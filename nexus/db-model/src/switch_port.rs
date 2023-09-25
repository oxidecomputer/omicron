// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::impl_enum_type;
use crate::schema::{
    lldp_config, lldp_service_config, switch_port, switch_port_settings,
    switch_port_settings_address_config, switch_port_settings_bgp_peer_config,
    switch_port_settings_group, switch_port_settings_groups,
    switch_port_settings_interface_config, switch_port_settings_link_config,
    switch_port_settings_port_config, switch_port_settings_route_config,
};
use crate::SqlU16;
use db_macros::Resource;
use ipnetwork::IpNetwork;
use nexus_types::identity::Resource;
use omicron_common::api::external;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

impl_enum_type!(
    #[derive(SqlType, Debug, Clone, Copy)]
    #[diesel(postgres_type(name = "switch_port_geometry"))]
    pub struct SwitchPortGeometryEnum;

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
    #[diesel(sql_type = SwitchPortGeometryEnum)]
    pub enum SwitchPortGeometry;

    Qsfp28x1 => b"Qsfp28x1"
    Qsfp28x2 => b"Qsfp28x2"
    Sfp28x4 => b"Sfp28x4"
);

impl
    From<nexus_types::external_api::networking::switch_port::SwitchPortGeometry>
    for SwitchPortGeometry
{
    fn from(
        g: nexus_types::external_api::networking::switch_port::SwitchPortGeometry,
    ) -> Self {
        match g {
            nexus_types::external_api::networking::switch_port::SwitchPortGeometry::Qsfp28x1 => {
                SwitchPortGeometry::Qsfp28x1
            }
            nexus_types::external_api::networking::switch_port::SwitchPortGeometry::Qsfp28x2 => {
                SwitchPortGeometry::Qsfp28x2
            }
            nexus_types::external_api::networking::switch_port::SwitchPortGeometry::Sfp28x4 => SwitchPortGeometry::Sfp28x4,
        }
    }
}

impl Into<nexus_types::external_api::networking::SwitchPortGeometry>
    for SwitchPortGeometry
{
    fn into(self) -> nexus_types::external_api::networking::SwitchPortGeometry {
        match self {
            SwitchPortGeometry::Qsfp28x1 => {
                nexus_types::external_api::networking::SwitchPortGeometry::Qsfp28x1
            }
            SwitchPortGeometry::Qsfp28x2 => {
                nexus_types::external_api::networking::SwitchPortGeometry::Qsfp28x2
            }
            SwitchPortGeometry::Sfp28x4 => {
                nexus_types::external_api::networking::SwitchPortGeometry::Sfp28x4
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
    Hash,
    Eq,
    PartialEq,
)]
#[diesel(table_name = switch_port)]
pub struct SwitchPort {
    pub id: Uuid,
    pub rack_id: Uuid,
    // TODO: #3594 Correctness
    // Change this field to a `SwitchLocation` type.
    pub switch_location: String,
    pub port_name: String,
    pub port_settings_id: Option<Uuid>,
}

impl SwitchPort {
    pub fn new(
        rack_id: Uuid,
        switch_location: String,
        port_name: String,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            rack_id,
            switch_location,
            port_name,
            port_settings_id: None,
        }
    }
}

impl Into<nexus_types::external_api::networking::SwitchPort> for SwitchPort {
    fn into(self) -> nexus_types::external_api::networking::SwitchPort {
        nexus_types::external_api::networking::SwitchPort {
            id: self.id,
            rack_id: self.rack_id,
            switch_location: self.switch_location,
            port_name: self.port_name,
            port_settings_id: self.port_settings_id,
        }
    }
}

#[derive(
    Queryable,
    Insertable,
    Selectable,
    Clone,
    Debug,
    Resource,
    Serialize,
    Deserialize,
)]
#[diesel(table_name = switch_port_settings)]
pub struct SwitchPortSettings {
    #[diesel(embed)]
    pub identity: SwitchPortSettingsIdentity,
}

impl SwitchPortSettings {
    pub fn new(id: &external::IdentityMetadataCreateParams) -> Self {
        Self {
            identity: SwitchPortSettingsIdentity::new(
                Uuid::new_v4(),
                id.clone(),
            ),
        }
    }
}

impl Into<nexus_types::external_api::networking::SwitchPortSettings>
    for SwitchPortSettings
{
    fn into(self) -> nexus_types::external_api::networking::SwitchPortSettings {
        nexus_types::external_api::networking::SwitchPortSettings {
            identity: self.identity(),
        }
    }
}

#[derive(
    Queryable, Insertable, Selectable, Clone, Debug, Serialize, Deserialize,
)]
#[diesel(table_name = switch_port_settings_groups)]
pub struct SwitchPortSettingsGroups {
    pub port_settings_id: Uuid,
    pub port_settings_group_id: Uuid,
}

impl Into<nexus_types::external_api::networking::SwitchPortSettingsGroups>
    for SwitchPortSettingsGroups
{
    fn into(
        self,
    ) -> nexus_types::external_api::networking::SwitchPortSettingsGroups {
        nexus_types::external_api::networking::SwitchPortSettingsGroups {
            port_settings_id: self.port_settings_id,
            port_settings_group_id: self.port_settings_group_id,
        }
    }
}

#[derive(
    Queryable,
    Insertable,
    Selectable,
    Clone,
    Debug,
    Resource,
    Serialize,
    Deserialize,
)]
#[diesel(table_name = switch_port_settings_group)]
pub struct SwitchPortSettingsGroup {
    #[diesel(embed)]
    pub identity: SwitchPortSettingsGroupIdentity,
    pub port_settings_id: Uuid,
}

impl Into<nexus_types::external_api::networking::SwitchPortSettingsGroup>
    for SwitchPortSettingsGroup
{
    fn into(
        self,
    ) -> nexus_types::external_api::networking::SwitchPortSettingsGroup {
        nexus_types::external_api::networking::SwitchPortSettingsGroup {
            identity: self.identity(),
            port_settings_id: self.port_settings_id,
        }
    }
}

#[derive(
    Queryable, Insertable, Selectable, Clone, Debug, Serialize, Deserialize,
)]
#[diesel(table_name = switch_port_settings_port_config)]
pub struct SwitchPortConfig {
    pub port_settings_id: Uuid,
    pub geometry: SwitchPortGeometry,
}

impl SwitchPortConfig {
    pub fn new(port_settings_id: Uuid, geometry: SwitchPortGeometry) -> Self {
        Self { port_settings_id, geometry }
    }
}

impl Into<nexus_types::external_api::networking::SwitchPortConfig>
    for SwitchPortConfig
{
    fn into(self) -> nexus_types::external_api::networking::SwitchPortConfig {
        nexus_types::external_api::networking::SwitchPortConfig {
            port_settings_id: self.port_settings_id,
            geometry: self.geometry.into(),
        }
    }
}

#[derive(
    Queryable, Insertable, Selectable, Clone, Debug, Serialize, Deserialize,
)]
#[diesel(table_name = switch_port_settings_link_config)]
pub struct SwitchPortLinkConfig {
    pub port_settings_id: Uuid,
    pub lldp_service_config_id: Uuid,
    pub link_name: String,
    pub mtu: SqlU16,
}

impl SwitchPortLinkConfig {
    pub fn new(
        port_settings_id: Uuid,
        lldp_service_config_id: Uuid,
        link_name: String,
        mtu: u16,
    ) -> Self {
        Self {
            port_settings_id,
            lldp_service_config_id,
            link_name,
            mtu: mtu.into(),
        }
    }
}

impl Into<nexus_types::external_api::networking::SwitchPortLinkConfig>
    for SwitchPortLinkConfig
{
    fn into(
        self,
    ) -> nexus_types::external_api::networking::SwitchPortLinkConfig {
        nexus_types::external_api::networking::SwitchPortLinkConfig {
            port_settings_id: self.port_settings_id,
            lldp_service_config_id: self.lldp_service_config_id,
            link_name: self.link_name.clone(),
            mtu: self.mtu.into(),
        }
    }
}

#[derive(
    Queryable, Insertable, Selectable, Clone, Debug, Serialize, Deserialize,
)]
#[diesel(table_name = lldp_service_config)]
pub struct LldpServiceConfig {
    pub id: Uuid,
    pub enabled: bool,
    pub lldp_config_id: Option<Uuid>,
}

impl LldpServiceConfig {
    pub fn new(enabled: bool, lldp_config_id: Option<Uuid>) -> Self {
        Self { id: Uuid::new_v4(), enabled, lldp_config_id }
    }
}

impl Into<nexus_types::external_api::networking::LldpServiceConfig>
    for LldpServiceConfig
{
    fn into(self) -> nexus_types::external_api::networking::LldpServiceConfig {
        nexus_types::external_api::networking::LldpServiceConfig {
            id: self.id,
            lldp_config_id: self.lldp_config_id,
            enabled: self.enabled,
        }
    }
}

#[derive(
    Queryable,
    Insertable,
    Selectable,
    Clone,
    Debug,
    Resource,
    Serialize,
    Deserialize,
)]
#[diesel(table_name = lldp_config)]
pub struct LldpConfig {
    #[diesel(embed)]
    pub identity: LldpConfigIdentity,
    pub chassis_id: String,
    pub system_name: String,
    pub system_description: String,
    pub management_ip: IpNetwork,
}

impl Into<nexus_types::external_api::networking::LldpConfig> for LldpConfig {
    fn into(self) -> nexus_types::external_api::networking::LldpConfig {
        nexus_types::external_api::networking::LldpConfig {
            identity: self.identity(),
            chassis_id: self.chassis_id.clone(),
            system_name: self.system_name.clone(),
            system_description: self.system_description.clone(),
            management_ip: self.management_ip.into(),
        }
    }
}

#[derive(
    Queryable, Insertable, Selectable, Clone, Debug, Serialize, Deserialize,
)]
#[diesel(table_name = switch_port_settings_interface_config)]
pub struct SwitchInterfaceConfig {
    pub port_settings_id: Uuid,
    pub id: Uuid,
    pub interface_name: String,
    pub v6_enabled: bool,
    pub kind: crate::DbSwitchInterfaceKind,
}

impl SwitchInterfaceConfig {
    pub fn new(
        port_settings_id: Uuid,
        interface_name: String,
        v6_enabled: bool,
        kind: crate::DbSwitchInterfaceKind,
    ) -> Self {
        Self {
            port_settings_id,
            id: Uuid::new_v4(),
            interface_name,
            v6_enabled,
            kind,
        }
    }
}

impl Into<nexus_types::external_api::networking::SwitchInterfaceConfig>
    for SwitchInterfaceConfig
{
    fn into(
        self,
    ) -> nexus_types::external_api::networking::SwitchInterfaceConfig {
        nexus_types::external_api::networking::SwitchInterfaceConfig {
            port_settings_id: self.port_settings_id,
            id: self.id,
            interface_name: self.interface_name,
            v6_enabled: self.v6_enabled,
            kind: self.kind.into(),
        }
    }
}

#[derive(
    Queryable, Insertable, Selectable, Clone, Debug, Serialize, Deserialize,
)]
#[diesel(table_name = switch_port_settings_route_config)]
pub struct SwitchPortRouteConfig {
    pub port_settings_id: Uuid,
    pub interface_name: String,
    pub dst: IpNetwork,
    pub gw: IpNetwork,
    pub vid: Option<SqlU16>,
}

impl SwitchPortRouteConfig {
    pub fn new(
        port_settings_id: Uuid,
        interface_name: String,
        dst: IpNetwork,
        gw: IpNetwork,
        vid: Option<SqlU16>,
    ) -> Self {
        Self { port_settings_id, interface_name, dst, gw, vid }
    }
}

impl Into<nexus_types::external_api::networking::SwitchPortRouteConfig>
    for SwitchPortRouteConfig
{
    fn into(
        self,
    ) -> nexus_types::external_api::networking::SwitchPortRouteConfig {
        nexus_types::external_api::networking::SwitchPortRouteConfig {
            port_settings_id: self.port_settings_id,
            interface_name: self.interface_name.clone(),
            dst: self.dst.into(),
            gw: self.gw.into(),
            vlan_id: self.vid.map(Into::into),
        }
    }
}

#[derive(
    Queryable, Insertable, Selectable, Clone, Debug, Serialize, Deserialize,
)]
#[diesel(table_name = switch_port_settings_bgp_peer_config)]
pub struct SwitchPortBgpPeerConfig {
    pub port_settings_id: Uuid,
    pub bgp_announce_set_id: Uuid,
    pub bgp_config_id: Uuid,
    pub interface_name: String,
    pub addr: IpNetwork,
}

impl SwitchPortBgpPeerConfig {
    pub fn new(
        port_settings_id: Uuid,
        bgp_announce_set_id: Uuid,
        bgp_config_id: Uuid,
        interface_name: String,
        addr: IpNetwork,
    ) -> Self {
        Self {
            port_settings_id,
            bgp_announce_set_id,
            bgp_config_id,
            interface_name,
            addr,
        }
    }
}

impl Into<nexus_types::external_api::networking::SwitchPortBgpPeerConfig>
    for SwitchPortBgpPeerConfig
{
    fn into(
        self,
    ) -> nexus_types::external_api::networking::SwitchPortBgpPeerConfig {
        nexus_types::external_api::networking::SwitchPortBgpPeerConfig {
            port_settings_id: self.port_settings_id,
            bgp_announce_set_id: self.bgp_announce_set_id,
            bgp_config_id: self.bgp_config_id,
            interface_name: self.interface_name.clone(),
            addr: self.addr.ip(),
        }
    }
}

#[derive(
    Queryable, Insertable, Selectable, Clone, Debug, Serialize, Deserialize,
)]
#[diesel(table_name = switch_port_settings_address_config)]
pub struct SwitchPortAddressConfig {
    pub port_settings_id: Uuid,
    pub address_lot_block_id: Uuid,
    pub rsvd_address_lot_block_id: Uuid,
    pub address: IpNetwork,
    pub interface_name: String,
}

impl SwitchPortAddressConfig {
    pub fn new(
        port_settings_id: Uuid,
        address_lot_block_id: Uuid,
        rsvd_address_lot_block_id: Uuid,
        address: IpNetwork,
        interface_name: String,
    ) -> Self {
        Self {
            port_settings_id,
            address_lot_block_id,
            rsvd_address_lot_block_id,
            address,
            interface_name,
        }
    }
}

impl Into<nexus_types::external_api::networking::SwitchPortAddressConfig>
    for SwitchPortAddressConfig
{
    fn into(
        self,
    ) -> nexus_types::external_api::networking::SwitchPortAddressConfig {
        nexus_types::external_api::networking::SwitchPortAddressConfig {
            port_settings_id: self.port_settings_id,
            address_lot_block_id: self.address_lot_block_id,
            address: self.address.into(),
            interface_name: self.interface_name,
        }
    }
}
