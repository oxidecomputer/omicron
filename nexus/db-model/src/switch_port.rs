// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::schema::{
    lldp_config, lldp_service_config, switch_port, switch_port_settings,
    switch_port_settings_address_config, switch_port_settings_bgp_peer_config,
    switch_port_settings_group, switch_port_settings_groups,
    switch_port_settings_interface_config, switch_port_settings_link_config,
    switch_port_settings_port_config, switch_port_settings_route_config,
};
use crate::SqlU16;
use crate::{impl_enum_type, SqlU32};
use db_macros::Resource;
use diesel::AsChangeset;
use ipnetwork::IpNetwork;
use nexus_types::external_api::params;
use nexus_types::identity::Resource;
use omicron_common::api::external;
use omicron_common::api::internal::shared::{PortFec, PortSpeed};
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

impl_enum_type!(
    #[derive(SqlType, Debug, Clone, Copy)]
    #[diesel(postgres_type(name = "switch_link_fec"))]
    pub struct SwitchLinkFecEnum;

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
    #[diesel(sql_type = SwitchLinkFecEnum)]
    pub enum SwitchLinkFec;

    Firecode => b"Firecode"
    None => b"None"
    Rs => b"Rs"
);

impl_enum_type!(
    #[derive(SqlType, Debug, Clone, Copy)]
    #[diesel(postgres_type(name = "switch_link_speed"))]
    pub struct SwitchLinkSpeedEnum;

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
    #[diesel(sql_type = SwitchLinkSpeedEnum)]
    pub enum SwitchLinkSpeed;

    Speed0G => b"0G"
    Speed1G => b"1G"
    Speed10G => b"10G"
    Speed25G => b"25G"
    Speed40G => b"40G"
    Speed50G => b"50G"
    Speed100G => b"100G"
    Speed200G => b"200G"
    Speed400G => b"400G"
);

impl From<SwitchLinkFec> for PortFec {
    fn from(value: SwitchLinkFec) -> Self {
        match value {
            SwitchLinkFec::Firecode => PortFec::Firecode,
            SwitchLinkFec::None => PortFec::None,
            SwitchLinkFec::Rs => PortFec::Rs,
        }
    }
}

impl From<params::LinkFec> for SwitchLinkFec {
    fn from(value: params::LinkFec) -> Self {
        match value {
            params::LinkFec::Firecode => SwitchLinkFec::Firecode,
            params::LinkFec::None => SwitchLinkFec::None,
            params::LinkFec::Rs => SwitchLinkFec::Rs,
        }
    }
}

impl From<SwitchLinkSpeed> for PortSpeed {
    fn from(value: SwitchLinkSpeed) -> Self {
        match value {
            SwitchLinkSpeed::Speed0G => PortSpeed::Speed0G,
            SwitchLinkSpeed::Speed1G => PortSpeed::Speed1G,
            SwitchLinkSpeed::Speed10G => PortSpeed::Speed10G,
            SwitchLinkSpeed::Speed25G => PortSpeed::Speed25G,
            SwitchLinkSpeed::Speed40G => PortSpeed::Speed40G,
            SwitchLinkSpeed::Speed50G => PortSpeed::Speed50G,
            SwitchLinkSpeed::Speed100G => PortSpeed::Speed100G,
            SwitchLinkSpeed::Speed200G => PortSpeed::Speed200G,
            SwitchLinkSpeed::Speed400G => PortSpeed::Speed400G,
        }
    }
}

impl From<params::LinkSpeed> for SwitchLinkSpeed {
    fn from(value: params::LinkSpeed) -> Self {
        match value {
            params::LinkSpeed::Speed0G => SwitchLinkSpeed::Speed0G,
            params::LinkSpeed::Speed1G => SwitchLinkSpeed::Speed1G,
            params::LinkSpeed::Speed10G => SwitchLinkSpeed::Speed10G,
            params::LinkSpeed::Speed25G => SwitchLinkSpeed::Speed25G,
            params::LinkSpeed::Speed40G => SwitchLinkSpeed::Speed40G,
            params::LinkSpeed::Speed50G => SwitchLinkSpeed::Speed50G,
            params::LinkSpeed::Speed100G => SwitchLinkSpeed::Speed100G,
            params::LinkSpeed::Speed200G => SwitchLinkSpeed::Speed200G,
            params::LinkSpeed::Speed400G => SwitchLinkSpeed::Speed400G,
        }
    }
}

impl From<params::SwitchPortGeometry> for SwitchPortGeometry {
    fn from(g: params::SwitchPortGeometry) -> Self {
        match g {
            params::SwitchPortGeometry::Qsfp28x1 => {
                SwitchPortGeometry::Qsfp28x1
            }
            params::SwitchPortGeometry::Qsfp28x2 => {
                SwitchPortGeometry::Qsfp28x2
            }
            params::SwitchPortGeometry::Sfp28x4 => SwitchPortGeometry::Sfp28x4,
        }
    }
}

impl Into<external::SwitchPortGeometry> for SwitchPortGeometry {
    fn into(self) -> external::SwitchPortGeometry {
        match self {
            SwitchPortGeometry::Qsfp28x1 => {
                external::SwitchPortGeometry::Qsfp28x1
            }
            SwitchPortGeometry::Qsfp28x2 => {
                external::SwitchPortGeometry::Qsfp28x2
            }
            SwitchPortGeometry::Sfp28x4 => {
                external::SwitchPortGeometry::Sfp28x4
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

impl Into<external::SwitchPort> for SwitchPort {
    fn into(self) -> external::SwitchPort {
        external::SwitchPort {
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
    pub fn new(meta: &external::IdentityMetadataCreateParams) -> Self {
        Self {
            identity: SwitchPortSettingsIdentity::new(
                Uuid::new_v4(),
                meta.clone(),
            ),
        }
    }

    pub fn with_id(
        id: Uuid,
        meta: &external::IdentityMetadataCreateParams,
    ) -> Self {
        Self { identity: SwitchPortSettingsIdentity::new(id, meta.clone()) }
    }
}

impl Into<external::SwitchPortSettings> for SwitchPortSettings {
    fn into(self) -> external::SwitchPortSettings {
        external::SwitchPortSettings { identity: self.identity() }
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

impl Into<external::SwitchPortSettingsGroups> for SwitchPortSettingsGroups {
    fn into(self) -> external::SwitchPortSettingsGroups {
        external::SwitchPortSettingsGroups {
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

impl Into<external::SwitchPortSettingsGroup> for SwitchPortSettingsGroup {
    fn into(self) -> external::SwitchPortSettingsGroup {
        external::SwitchPortSettingsGroup {
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

impl Into<external::SwitchPortConfig> for SwitchPortConfig {
    fn into(self) -> external::SwitchPortConfig {
        external::SwitchPortConfig {
            port_settings_id: self.port_settings_id,
            geometry: self.geometry.into(),
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
#[diesel(table_name = switch_port_settings_link_config)]
pub struct SwitchPortLinkConfig {
    pub port_settings_id: Uuid,
    pub lldp_service_config_id: Uuid,
    pub link_name: String,
    pub mtu: SqlU16,
    pub fec: SwitchLinkFec,
    pub speed: SwitchLinkSpeed,
    pub autoneg: bool,
}

impl SwitchPortLinkConfig {
    pub fn new(
        port_settings_id: Uuid,
        lldp_service_config_id: Uuid,
        link_name: String,
        mtu: u16,
        fec: SwitchLinkFec,
        speed: SwitchLinkSpeed,
        autoneg: bool,
    ) -> Self {
        Self {
            port_settings_id,
            lldp_service_config_id,
            link_name,
            fec,
            speed,
            autoneg,
            mtu: mtu.into(),
        }
    }
}

impl Into<external::SwitchPortLinkConfig> for SwitchPortLinkConfig {
    fn into(self) -> external::SwitchPortLinkConfig {
        external::SwitchPortLinkConfig {
            port_settings_id: self.port_settings_id,
            lldp_service_config_id: self.lldp_service_config_id,
            link_name: self.link_name.clone(),
            mtu: self.mtu.into(),
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

impl Into<external::LldpServiceConfig> for LldpServiceConfig {
    fn into(self) -> external::LldpServiceConfig {
        external::LldpServiceConfig {
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

impl Into<external::LldpConfig> for LldpConfig {
    fn into(self) -> external::LldpConfig {
        external::LldpConfig {
            identity: self.identity(),
            chassis_id: self.chassis_id.clone(),
            system_name: self.system_name.clone(),
            system_description: self.system_description.clone(),
            management_ip: self.management_ip.into(),
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

impl Into<external::SwitchInterfaceConfig> for SwitchInterfaceConfig {
    fn into(self) -> external::SwitchInterfaceConfig {
        external::SwitchInterfaceConfig {
            port_settings_id: self.port_settings_id,
            id: self.id,
            interface_name: self.interface_name,
            v6_enabled: self.v6_enabled,
            kind: self.kind.into(),
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

impl Into<external::SwitchPortRouteConfig> for SwitchPortRouteConfig {
    fn into(self) -> external::SwitchPortRouteConfig {
        external::SwitchPortRouteConfig {
            port_settings_id: self.port_settings_id,
            interface_name: self.interface_name.clone(),
            dst: self.dst.into(),
            gw: self.gw.into(),
            vlan_id: self.vid.map(Into::into),
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
#[diesel(table_name = switch_port_settings_bgp_peer_config)]
pub struct SwitchPortBgpPeerConfig {
    pub port_settings_id: Uuid,
    pub bgp_config_id: Uuid,
    pub interface_name: String,
    pub addr: IpNetwork,
    pub hold_time: SqlU32,
    pub idle_hold_time: SqlU32,
    pub delay_open: SqlU32,
    pub connect_retry: SqlU32,
    pub keepalive: SqlU32,
}

impl SwitchPortBgpPeerConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        port_settings_id: Uuid,
        bgp_config_id: Uuid,
        interface_name: String,
        addr: IpNetwork,
        hold_time: SqlU32,
        idle_hold_time: SqlU32,
        delay_open: SqlU32,
        connect_retry: SqlU32,
        keepalive: SqlU32,
    ) -> Self {
        Self {
            port_settings_id,
            bgp_config_id,
            interface_name,
            addr,
            hold_time,
            idle_hold_time,
            delay_open,
            connect_retry,
            keepalive,
        }
    }
}

impl Into<external::SwitchPortBgpPeerConfig> for SwitchPortBgpPeerConfig {
    fn into(self) -> external::SwitchPortBgpPeerConfig {
        external::SwitchPortBgpPeerConfig {
            port_settings_id: self.port_settings_id,
            bgp_config_id: self.bgp_config_id,
            interface_name: self.interface_name.clone(),
            addr: self.addr.ip(),
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

impl Into<external::SwitchPortAddressConfig> for SwitchPortAddressConfig {
    fn into(self) -> external::SwitchPortAddressConfig {
        external::SwitchPortAddressConfig {
            port_settings_id: self.port_settings_id,
            address_lot_block_id: self.address_lot_block_id,
            address: self.address.into(),
            interface_name: self.interface_name,
        }
    }
}
