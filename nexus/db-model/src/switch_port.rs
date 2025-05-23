// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{SqlU8, SqlU16};
use crate::{SqlU32, impl_enum_type};
use chrono::{DateTime, Utc};
use db_macros::Resource;
use diesel::AsChangeset;
use ipnetwork::IpNetwork;
use nexus_db_schema::schema::{
    lldp_link_config, switch_port, switch_port_settings,
    switch_port_settings_address_config, switch_port_settings_bgp_peer_config,
    switch_port_settings_bgp_peer_config_allow_export,
    switch_port_settings_bgp_peer_config_allow_import,
    switch_port_settings_bgp_peer_config_communities,
    switch_port_settings_group, switch_port_settings_groups,
    switch_port_settings_interface_config, switch_port_settings_link_config,
    switch_port_settings_port_config, switch_port_settings_route_config,
    tx_eq_config,
};
use nexus_types::external_api::params;
use nexus_types::identity::Resource;
use omicron_common::api::external;
use omicron_common::api::external::{BgpPeer, ImportExportPolicy};
use omicron_common::api::internal::shared::{PortFec, PortSpeed};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

impl_enum_type!(
    SwitchPortGeometryEnum:

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
    pub enum SwitchPortGeometry;

    Qsfp28x1 => b"Qsfp28x1"
    Qsfp28x2 => b"Qsfp28x2"
    Sfp28x4 => b"Sfp28x4"
);

impl_enum_type!(
    SwitchLinkFecEnum:

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
    pub enum SwitchLinkFec;

    Firecode => b"Firecode"
    None => b"None"
    Rs => b"Rs"
);

impl_enum_type!(
    SwitchLinkSpeedEnum:

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

impl From<external::LinkFec> for SwitchLinkFec {
    fn from(value: external::LinkFec) -> Self {
        match value {
            external::LinkFec::Firecode => SwitchLinkFec::Firecode,
            external::LinkFec::None => SwitchLinkFec::None,
            external::LinkFec::Rs => SwitchLinkFec::Rs,
        }
    }
}

impl From<SwitchLinkFec> for external::LinkFec {
    fn from(value: SwitchLinkFec) -> Self {
        match value {
            SwitchLinkFec::Firecode => external::LinkFec::Firecode,
            SwitchLinkFec::None => external::LinkFec::None,
            SwitchLinkFec::Rs => external::LinkFec::Rs,
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

impl From<external::LinkSpeed> for SwitchLinkSpeed {
    fn from(value: external::LinkSpeed) -> Self {
        match value {
            external::LinkSpeed::Speed0G => SwitchLinkSpeed::Speed0G,
            external::LinkSpeed::Speed1G => SwitchLinkSpeed::Speed1G,
            external::LinkSpeed::Speed10G => SwitchLinkSpeed::Speed10G,
            external::LinkSpeed::Speed25G => SwitchLinkSpeed::Speed25G,
            external::LinkSpeed::Speed40G => SwitchLinkSpeed::Speed40G,
            external::LinkSpeed::Speed50G => SwitchLinkSpeed::Speed50G,
            external::LinkSpeed::Speed100G => SwitchLinkSpeed::Speed100G,
            external::LinkSpeed::Speed200G => SwitchLinkSpeed::Speed200G,
            external::LinkSpeed::Speed400G => SwitchLinkSpeed::Speed400G,
        }
    }
}

impl From<SwitchLinkSpeed> for external::LinkSpeed {
    fn from(value: SwitchLinkSpeed) -> Self {
        match value {
            SwitchLinkSpeed::Speed0G => external::LinkSpeed::Speed0G,
            SwitchLinkSpeed::Speed1G => external::LinkSpeed::Speed1G,
            SwitchLinkSpeed::Speed10G => external::LinkSpeed::Speed10G,
            SwitchLinkSpeed::Speed25G => external::LinkSpeed::Speed25G,
            SwitchLinkSpeed::Speed40G => external::LinkSpeed::Speed40G,
            SwitchLinkSpeed::Speed50G => external::LinkSpeed::Speed50G,
            SwitchLinkSpeed::Speed100G => external::LinkSpeed::Speed100G,
            SwitchLinkSpeed::Speed200G => external::LinkSpeed::Speed200G,
            SwitchLinkSpeed::Speed400G => external::LinkSpeed::Speed400G,
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

impl Into<external::SwitchPortSettingsIdentity> for SwitchPortSettings {
    fn into(self) -> external::SwitchPortSettingsIdentity {
        external::SwitchPortSettingsIdentity { identity: self.identity() }
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
    pub lldp_link_config_id: Option<Uuid>,
    pub link_name: String,
    pub mtu: SqlU16,
    pub fec: Option<SwitchLinkFec>,
    pub speed: SwitchLinkSpeed,
    pub autoneg: bool,
    pub tx_eq_config_id: Option<Uuid>,
}

impl SwitchPortLinkConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        port_settings_id: Uuid,
        lldp_link_config_id: Uuid,
        link_name: String,
        mtu: u16,
        fec: Option<SwitchLinkFec>,
        speed: SwitchLinkSpeed,
        autoneg: bool,
        tx_eq_config_id: Option<Uuid>,
    ) -> Self {
        Self {
            port_settings_id,
            lldp_link_config_id: Some(lldp_link_config_id),
            link_name,
            fec,
            speed,
            autoneg,
            mtu: mtu.into(),
            tx_eq_config_id,
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
#[diesel(table_name = lldp_link_config)]
pub struct LldpLinkConfig {
    pub id: Uuid,
    pub enabled: bool,
    pub link_name: Option<String>,
    pub link_description: Option<String>,
    pub chassis_id: Option<String>,
    pub system_name: Option<String>,
    pub system_description: Option<String>,
    pub management_ip: Option<IpNetwork>,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,
}

impl LldpLinkConfig {
    pub fn new(
        enabled: bool,
        link_name: Option<String>,
        link_description: Option<String>,
        chassis_id: Option<String>,
        system_name: Option<String>,
        system_description: Option<String>,
        management_ip: Option<IpNetwork>,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            enabled,
            link_name,
            link_description,
            chassis_id,
            system_name,
            system_description,
            management_ip,
            time_created: now,
            time_modified: now,
            time_deleted: None,
        }
    }
}

// This converts the internal database version of the config into the
// user-facing version.
impl Into<external::LldpLinkConfig> for LldpLinkConfig {
    fn into(self) -> external::LldpLinkConfig {
        external::LldpLinkConfig {
            id: self.id,
            enabled: self.enabled,
            link_name: self.link_name.clone(),
            link_description: self.link_description.clone(),
            chassis_id: self.chassis_id.clone(),
            system_name: self.system_name.clone(),
            system_description: self.system_description.clone(),
            management_ip: self.management_ip.map(|a| a.ip()),
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
#[diesel(table_name = tx_eq_config)]
pub struct TxEqConfig {
    pub id: Uuid,
    pub pre1: Option<i32>,
    pub pre2: Option<i32>,
    pub main: Option<i32>,
    pub post2: Option<i32>,
    pub post1: Option<i32>,
}

impl TxEqConfig {
    pub fn new(
        pre1: Option<i32>,
        pre2: Option<i32>,
        main: Option<i32>,
        post2: Option<i32>,
        post1: Option<i32>,
    ) -> Self {
        Self { id: Uuid::new_v4(), pre1, pre2, main, post2, post1 }
    }
}

// This converts the internal database version of the config into the
// user-facing version.
impl Into<external::TxEqConfig> for TxEqConfig {
    fn into(self) -> external::TxEqConfig {
        external::TxEqConfig {
            pre1: self.pre1,
            pre2: self.pre2,
            main: self.main,
            post2: self.post2,
            post1: self.post1,
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
    #[diesel(column_name = local_pref)]
    pub rib_priority: Option<SqlU8>,
}

impl SwitchPortRouteConfig {
    pub fn new(
        port_settings_id: Uuid,
        interface_name: String,
        dst: IpNetwork,
        gw: IpNetwork,
        vid: Option<SqlU16>,
        rib_priority: Option<SqlU8>,
    ) -> Self {
        Self { port_settings_id, interface_name, dst, gw, vid, rib_priority }
    }
}

impl Into<external::SwitchPortRouteConfig> for SwitchPortRouteConfig {
    fn into(self) -> external::SwitchPortRouteConfig {
        external::SwitchPortRouteConfig {
            port_settings_id: self.port_settings_id,
            interface_name: self.interface_name.clone(),
            dst: self.dst.into(),
            gw: self.gw.ip(),
            vlan_id: self.vid.map(Into::into),
            rib_priority: self.rib_priority.map(Into::into),
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
    pub remote_asn: Option<SqlU32>,
    pub min_ttl: Option<SqlU8>,
    pub md5_auth_key: Option<String>,
    pub multi_exit_discriminator: Option<SqlU32>,
    pub local_pref: Option<SqlU32>,
    pub enforce_first_as: bool,
    pub allow_import_list_active: bool,
    pub allow_export_list_active: bool,
    pub vlan_id: Option<SqlU16>,
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
#[diesel(table_name = switch_port_settings_bgp_peer_config_communities)]
pub struct SwitchPortBgpPeerConfigCommunity {
    pub port_settings_id: Uuid,
    pub interface_name: String,
    pub addr: IpNetwork,
    pub community: SqlU32,
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
#[diesel(table_name = switch_port_settings_bgp_peer_config_allow_export)]
pub struct SwitchPortBgpPeerConfigAllowExport {
    /// Parent switch port configuration
    pub port_settings_id: Uuid,
    /// Interface peer is reachable on
    pub interface_name: String,
    /// Peer Address
    pub addr: IpNetwork,
    /// Allowed Prefix
    pub prefix: IpNetwork,
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
#[diesel(table_name = switch_port_settings_bgp_peer_config_allow_import)]
pub struct SwitchPortBgpPeerConfigAllowImport {
    /// Parent switch port configuration
    pub port_settings_id: Uuid,
    /// Interface peer is reachable on
    pub interface_name: String,
    /// Peer Address
    pub addr: IpNetwork,
    /// Allowed Prefix
    pub prefix: IpNetwork,
}

impl SwitchPortBgpPeerConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        port_settings_id: Uuid,
        bgp_config_id: Uuid,
        interface_name: String,
        p: &BgpPeer,
    ) -> Self {
        Self {
            port_settings_id,
            bgp_config_id,
            interface_name,
            addr: p.addr.into(),
            hold_time: p.hold_time.into(),
            idle_hold_time: p.idle_hold_time.into(),
            delay_open: p.delay_open.into(),
            connect_retry: p.connect_retry.into(),
            keepalive: p.keepalive.into(),
            remote_asn: p.remote_asn.map(|x| x.into()),
            min_ttl: p.min_ttl.map(|x| x.into()),
            md5_auth_key: p.md5_auth_key.clone(),
            multi_exit_discriminator: p
                .multi_exit_discriminator
                .map(|x| x.into()),
            local_pref: p.local_pref.map(|x| x.into()),
            enforce_first_as: p.enforce_first_as,
            allow_import_list_active: match &p.allowed_import {
                ImportExportPolicy::NoFiltering => false,
                _ => true,
            },
            allow_export_list_active: match &p.allowed_export {
                ImportExportPolicy::NoFiltering => false,
                _ => true,
            },
            vlan_id: p.vlan_id.map(|x| x.into()),
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
    pub vlan_id: Option<SqlU16>,
}

impl SwitchPortAddressConfig {
    pub fn new(
        port_settings_id: Uuid,
        address_lot_block_id: Uuid,
        rsvd_address_lot_block_id: Uuid,
        address: IpNetwork,
        interface_name: String,
        vlan_id: Option<u16>,
    ) -> Self {
        Self {
            port_settings_id,
            address_lot_block_id,
            rsvd_address_lot_block_id,
            address,
            interface_name,
            vlan_id: vlan_id.map(|x| x.into()),
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
            vlan_id: self.vlan_id.map(|x| x.into()),
        }
    }
}
