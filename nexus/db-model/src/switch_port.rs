// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::typed_uuid::DbTypedUuid;
use crate::{Name, SqlU8, SqlU16};
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
use nexus_types::external_api::networking as networking_types;
use nexus_types::identity::Resource;
use omicron_common::api::external;
use omicron_uuid_kinds::BgpPeerConfigAllowExportKind;
use omicron_uuid_kinds::BgpPeerConfigAllowImportKind;
use omicron_uuid_kinds::BgpPeerConfigCommunityKind;
use omicron_uuid_kinds::TypedUuid;
use oxnet::IpNet;
use serde::{Deserialize, Serialize};
use sled_agent_types::early_networking::ImportExportPolicy;
use sled_agent_types::early_networking::InvalidIpAddrError;
use sled_agent_types::early_networking::PortFec;
use sled_agent_types::early_networking::PortSpeed;
use sled_agent_types::early_networking::RouterLifetimeConfig;
use sled_agent_types::early_networking::RouterLifetimeConfigError;
use sled_agent_types::early_networking::RouterPeerIpAddr;
use sled_agent_types::early_networking::RouterPeerIpAddrError;
use sled_agent_types::early_networking::RouterPeerType;
use sled_agent_types::early_networking::SwitchSlot;
use sled_agent_types::early_networking::UplinkAddress;
use uuid::Uuid;

/// Extension trait on [`RouterPeerType`] for converting it to and from the way
/// we represent peer addresses in the database.
///
/// This trait should only be used by database model and query functions.
pub trait RouterPeerTypeDbRepresentation: Sized {
    /// Get the database representation of the address of this peer.
    ///
    /// For numbered peers, returns `Some(ip)` (corresponding to a non-NULL
    /// `INET`); for unnumbered peers, returns `None` (corresponding to NULL).
    fn ip_db_repr(&self) -> Option<IpNetwork>;

    /// Convert the database representation of a peer back into a
    /// [`RouterPeerType`].
    ///
    /// Unconditionally requires the caller to supply a `router_lifetime`, but
    /// this argument is only used if `ip` is `None` (indicating an unnumbered
    /// peer). This matches the database table's storage of a NULLable address
    /// (the peer IP, where NULL means unnumbered) alongside a non-NULL
    /// router_lifetime (left at 0 for numbered peers).
    fn from_db_repr(
        ip: Option<IpNetwork>,
        router_lifetime: RouterLifetimeConfig,
    ) -> Result<Self, RouterPeerIpAddrError>;
}

impl RouterPeerTypeDbRepresentation for RouterPeerType {
    fn ip_db_repr(&self) -> Option<IpNetwork> {
        match self {
            Self::Unnumbered { .. } => None,
            Self::Numbered { ip } => Some((*ip).into()),
        }
    }

    fn from_db_repr(
        ip: Option<IpNetwork>,
        router_lifetime: RouterLifetimeConfig,
    ) -> Result<Self, RouterPeerIpAddrError> {
        match ip.map(|ip| ip.ip()) {
            Some(ip) => {
                let ip = RouterPeerIpAddr::try_from(ip)?;
                Ok(Self::Numbered { ip })
            }
            None => Ok(Self::Unnumbered { router_lifetime }),
        }
    }
}

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

impl PartialEq<networking_types::SwitchPortGeometry> for SwitchPortGeometry {
    fn eq(&self, other: &networking_types::SwitchPortGeometry) -> bool {
        match self {
            Self::Qsfp28x1 => {
                matches!(other, networking_types::SwitchPortGeometry::Qsfp28x1)
            }
            Self::Qsfp28x2 => {
                matches!(other, networking_types::SwitchPortGeometry::Qsfp28x2)
            }
            Self::Sfp28x4 => {
                matches!(other, networking_types::SwitchPortGeometry::Sfp28x4)
            }
        }
    }
}

impl PartialEq<SwitchPortGeometry> for networking_types::SwitchPortGeometry {
    fn eq(&self, other: &SwitchPortGeometry) -> bool {
        other.eq(self)
    }
}

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

impl From<networking_types::SwitchPortGeometry> for SwitchPortGeometry {
    fn from(g: networking_types::SwitchPortGeometry) -> Self {
        match g {
            networking_types::SwitchPortGeometry::Qsfp28x1 => {
                SwitchPortGeometry::Qsfp28x1
            }
            networking_types::SwitchPortGeometry::Qsfp28x2 => {
                SwitchPortGeometry::Qsfp28x2
            }
            networking_types::SwitchPortGeometry::Sfp28x4 => {
                SwitchPortGeometry::Sfp28x4
            }
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

impl_enum_type!(
    SwitchSlotEnum:

    #[derive(
        Clone,
        Copy,
        Debug,
        AsExpression,
        FromSqlRow,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        Serialize,
        Deserialize,
    )]
    pub enum DbSwitchSlot;

    Switch0 => b"switch0"
    Switch1 => b"switch1"
);

impl From<DbSwitchSlot> for SwitchSlot {
    fn from(value: DbSwitchSlot) -> Self {
        match value {
            DbSwitchSlot::Switch0 => Self::Switch0,
            DbSwitchSlot::Switch1 => Self::Switch1,
        }
    }
}

impl From<SwitchSlot> for DbSwitchSlot {
    fn from(value: SwitchSlot) -> Self {
        match value {
            SwitchSlot::Switch0 => Self::Switch0,
            SwitchSlot::Switch1 => Self::Switch1,
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
    pub port_name: Name,
    pub port_settings_id: Option<Uuid>,
    pub switch_slot: DbSwitchSlot,
}

impl SwitchPort {
    pub fn new(
        rack_id: Uuid,
        switch_slot: SwitchSlot,
        port_name: Name,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            rack_id,
            switch_slot: switch_slot.into(),
            port_name,
            port_settings_id: None,
        }
    }
}

impl Into<networking_types::SwitchPort> for SwitchPort {
    fn into(self) -> networking_types::SwitchPort {
        networking_types::SwitchPort {
            id: self.id,
            rack_id: self.rack_id,
            switch_slot: self.switch_slot.into(),
            port_name: self.port_name.into(),
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
    pub link_name: Name,
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
        link_name: Name,
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
    pub interface_name: Name,
    pub v6_enabled: bool,
    pub kind: crate::DbSwitchInterfaceKind,
}

impl SwitchInterfaceConfig {
    pub fn new(
        port_settings_id: Uuid,
        interface_name: Name,
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
            interface_name: self.interface_name.into(),
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
    pub interface_name: Name,
    pub dst: IpNetwork,
    pub gw: IpNetwork,
    pub vid: Option<SqlU16>,
    pub rib_priority: Option<SqlU8>,
}

impl SwitchPortRouteConfig {
    pub fn new(
        port_settings_id: Uuid,
        interface_name: Name,
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
            interface_name: self.interface_name.into(),
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
    pub interface_name: Name,
    addr: Option<IpNetwork>,
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
    pub id: Uuid,
    router_lifetime: SqlU16,
}

#[derive(Debug, thiserror::Error)]
pub enum SwitchPortBgpPeerConfigInvalidData {
    #[error(
        "database inconsistency: \
        invalid peer address in BGP peer config {port_settings_id}"
    )]
    PeerAddress {
        port_settings_id: Uuid,
        #[source]
        err: RouterPeerIpAddrError,
    },
    #[error(
        "database inconsistency: \
        invalid router lifetime in BGP peer config {port_settings_id}"
    )]
    RouterLifetime {
        port_settings_id: Uuid,
        #[source]
        err: RouterLifetimeConfigError,
    },
}

impl SwitchPortBgpPeerConfig {
    /// Return the [`RouterPeerType`] (numbered or unnumbered, with additional
    /// details specific to each type) of this peer.
    ///
    /// Only fails if invalid data has been stored in the database.
    pub fn peer_type(
        &self,
    ) -> Result<RouterPeerType, SwitchPortBgpPeerConfigInvalidData> {
        // We only expect NULL (corresponding to unnumbered, in which case we
        // expect a valid `router_lifetime` too) or `Some(ip)` where `ip` is a
        // valid router peer IP.
        match self.addr {
            Some(db_ip) => {
                let ip =
                    RouterPeerIpAddr::try_from(db_ip.ip()).map_err(|err| {
                        SwitchPortBgpPeerConfigInvalidData::PeerAddress {
                            port_settings_id: self.port_settings_id,
                            err,
                        }
                    })?;
                Ok(RouterPeerType::Numbered { ip })
            }
            None => {
                let router_lifetime = RouterLifetimeConfig::new(
                    *self.router_lifetime,
                )
                .map_err(|err| {
                    SwitchPortBgpPeerConfigInvalidData::RouterLifetime {
                        port_settings_id: self.port_settings_id,
                        err,
                    }
                })?;
                Ok(RouterPeerType::Unnumbered { router_lifetime })
            }
        }
    }

    /// Get the raw, database representation of this peer's IP address.
    ///
    /// This should only be used in database queries that need the database
    /// representation. Other code (Nexus, etc.) should work with the
    /// [`RouterPeerType`] returned by [`SwitchPortBgpPeerConfig::peer_type()`].
    pub fn raw_ip_in_db_repr(&self) -> Option<IpNetwork> {
        self.addr
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
#[diesel(table_name = switch_port_settings_bgp_peer_config_communities)]
pub struct SwitchPortBgpPeerConfigCommunity {
    pub port_settings_id: Uuid,
    pub interface_name: Name,
    addr: Option<IpNetwork>,
    pub community: SqlU32,
    pub id: DbTypedUuid<BgpPeerConfigCommunityKind>,
}

impl SwitchPortBgpPeerConfigCommunity {
    pub fn new(
        port_settings_id: Uuid,
        interface_name: Name,
        addr: RouterPeerType,
        community: u32,
    ) -> Self {
        Self {
            port_settings_id,
            interface_name,
            addr: addr.ip_db_repr(),
            community: community.into(),
            id: TypedUuid::new_v4().into(),
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
#[diesel(table_name = switch_port_settings_bgp_peer_config_allow_export)]
pub struct SwitchPortBgpPeerConfigAllowExport {
    /// Parent switch port configuration
    pub port_settings_id: Uuid,
    /// Interface peer is reachable on
    pub interface_name: Name,
    /// Peer Address
    addr: Option<IpNetwork>,
    /// Allowed Prefix
    pub prefix: IpNetwork,
    pub id: DbTypedUuid<BgpPeerConfigAllowExportKind>,
}

impl SwitchPortBgpPeerConfigAllowExport {
    pub fn new(
        port_settings_id: Uuid,
        interface_name: Name,
        addr: RouterPeerType,
        prefix: IpNet,
    ) -> Self {
        Self {
            port_settings_id,
            interface_name,
            addr: addr.ip_db_repr(),
            prefix: prefix.into(),
            id: TypedUuid::new_v4().into(),
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
#[diesel(table_name = switch_port_settings_bgp_peer_config_allow_import)]
pub struct SwitchPortBgpPeerConfigAllowImport {
    /// Parent switch port configuration
    pub port_settings_id: Uuid,
    /// Interface peer is reachable on
    pub interface_name: Name,
    /// Peer Address
    addr: Option<IpNetwork>,
    /// Allowed Prefix
    pub prefix: IpNetwork,
    pub id: DbTypedUuid<BgpPeerConfigAllowImportKind>,
}

impl SwitchPortBgpPeerConfigAllowImport {
    pub fn new(
        port_settings_id: Uuid,
        interface_name: Name,
        addr: RouterPeerType,
        prefix: IpNet,
    ) -> Self {
        Self {
            port_settings_id,
            interface_name,
            addr: addr.ip_db_repr(),
            prefix: prefix.into(),
            id: TypedUuid::new_v4().into(),
        }
    }
}

impl SwitchPortBgpPeerConfig {
    pub fn new(
        port_settings_id: Uuid,
        bgp_config_id: Uuid,
        interface_name: Name,
        p: &networking_types::BgpPeer,
    ) -> Self {
        // Unnumbered peers have a `router_lifetime`; for numbered peers, we
        // must use the default (0). This is enforced by a CHECK constraint.
        let router_lifetime = match p.addr {
            RouterPeerType::Numbered { .. } => RouterLifetimeConfig::default(),
            RouterPeerType::Unnumbered { router_lifetime } => router_lifetime,
        };
        Self {
            id: Uuid::new_v4(),
            port_settings_id,
            bgp_config_id,
            interface_name,
            addr: p.addr.ip_db_repr(),
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
            router_lifetime: router_lifetime.as_u16().into(),
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
    address: IpNetwork,
    pub interface_name: Name,
    pub vlan_id: Option<SqlU16>,
}

impl SwitchPortAddressConfig {
    pub fn new(
        port_settings_id: Uuid,
        address_lot_block_id: Uuid,
        rsvd_address_lot_block_id: Uuid,
        address: UplinkAddress,
        interface_name: Name,
        vlan_id: Option<u16>,
    ) -> Self {
        // TODO-cleanup `switch_port_settings_address_config.address` is not
        // nullable; we store addrconf addresses as the sentinel value `::/128`.
        // We should consider reworking this to be consistent with BGP peers
        // (e.g., store addrconf as `NULL`):
        // https://github.com/oxidecomputer/omicron/issues/9832#issuecomment-4092974372
        let address = address.ip_net_squashing_addrconf_to_unspecified();
        Self {
            port_settings_id,
            address_lot_block_id,
            rsvd_address_lot_block_id,
            address: address.into(),
            interface_name,
            vlan_id: vlan_id.map(|x| x.into()),
        }
    }

    /// Return the address of this address config.
    ///
    /// Only fails if we've stored invalid data in the DB (i.e., an address that
    /// contains an IP that we don't allow for uplink addresses).
    pub fn address(&self) -> Result<UplinkAddress, InvalidIpAddrError> {
        UplinkAddress::try_from_ip_net_treating_unspecified_as_addrconf(
            self.address.into(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sled_agent_types::early_networking::RouterLifetimeConfig;
    use sled_agent_types::early_networking::RouterPeerIpAddr;
    use sled_agent_types::early_networking::RouterPeerType;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    #[test]
    fn router_peer_repr_round_trip_numbered_v4() {
        let ip_addr = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let ip = RouterPeerIpAddr::try_from(ip_addr).unwrap();
        let original = RouterPeerType::Numbered { ip };

        let db_repr = original.ip_db_repr();
        assert_eq!(db_repr, Some(IpNetwork::from(ip_addr)));

        let reconstructed = RouterPeerType::from_db_repr(
            db_repr,
            RouterLifetimeConfig::default(),
        )
        .unwrap();
        assert_eq!(original, reconstructed);
    }

    #[test]
    fn router_peer_repr_round_trip_numbered_v6() {
        let ip_addr = IpAddr::V6(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1));
        let ip = RouterPeerIpAddr::try_from(ip_addr).unwrap();
        let original = RouterPeerType::Numbered { ip };

        let db_repr = original.ip_db_repr();
        assert_eq!(db_repr, Some(IpNetwork::from(ip_addr)));

        let reconstructed = RouterPeerType::from_db_repr(
            db_repr,
            RouterLifetimeConfig::default(),
        )
        .unwrap();
        assert_eq!(original, reconstructed);
    }

    #[test]
    fn router_peer_repr_round_trip_unnumbered() {
        let lifetime = RouterLifetimeConfig::new(1800).unwrap();
        let original = RouterPeerType::Unnumbered { router_lifetime: lifetime };

        assert_eq!(original.ip_db_repr(), None);

        let reconstructed =
            RouterPeerType::from_db_repr(None, lifetime).unwrap();
        assert_eq!(original, reconstructed);
    }

    #[test]
    fn router_peer_from_db_repr_rejects_invalid_ips() {
        let cases: &[(IpAddr, InvalidIpAddrError)] = &[
            (
                Ipv4Addr::UNSPECIFIED.into(),
                InvalidIpAddrError::UnspecifiedAddress,
            ),
            (Ipv4Addr::LOCALHOST.into(), InvalidIpAddrError::LoopbackAddress),
            (Ipv4Addr::BROADCAST.into(), InvalidIpAddrError::Ipv4Broadcast),
            (
                Ipv6Addr::UNSPECIFIED.into(),
                InvalidIpAddrError::UnspecifiedAddress,
            ),
            (Ipv6Addr::LOCALHOST.into(), InvalidIpAddrError::LoopbackAddress),
        ];
        let lifetime = RouterLifetimeConfig::default();
        for (ip, expected_err) in cases {
            let err = RouterPeerType::from_db_repr(
                Some(IpNetwork::from(*ip)),
                lifetime,
            )
            .unwrap_err();
            assert_eq!(err.ip, *ip);
            assert_eq!(err.err, *expected_err, "wrong error for {ip}");
        }
    }

    fn make_bgp_peer(addr: RouterPeerType) -> networking_types::BgpPeer {
        networking_types::BgpPeer {
            bgp_config: external::NameOrId::Name("test-bgp".parse().unwrap()),
            addr,
            hold_time: 6,
            idle_hold_time: 6,
            delay_open: 0,
            connect_retry: 3,
            keepalive: 2,
            remote_asn: None,
            min_ttl: None,
            md5_auth_key: None,
            multi_exit_discriminator: None,
            communities: vec![],
            local_pref: None,
            enforce_first_as: false,
            allowed_import: ImportExportPolicy::NoFiltering,
            allowed_export: ImportExportPolicy::NoFiltering,
            vlan_id: None,
        }
    }

    #[test]
    fn peer_type_round_trip_numbered() {
        let ip =
            RouterPeerIpAddr::try_from(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)))
                .unwrap();
        let original = RouterPeerType::Numbered { ip };
        let db_peer = SwitchPortBgpPeerConfig::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "phy0".parse::<external::Name>().unwrap().into(),
            &make_bgp_peer(original),
        );
        // Numbered peers store Some(ip) in the DB and router_lifetime = 0.
        assert_eq!(
            db_peer.raw_ip_in_db_repr(),
            Some(IpNetwork::from(IpAddr::from(ip)))
        );
        assert_eq!(db_peer.router_lifetime, SqlU16(0));
        assert_eq!(db_peer.peer_type().unwrap(), original);
    }

    #[test]
    fn peer_type_round_trip_unnumbered() {
        let lifetime = RouterLifetimeConfig::new(300).unwrap();
        let original = RouterPeerType::Unnumbered { router_lifetime: lifetime };
        let db_peer = SwitchPortBgpPeerConfig::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "phy0".parse::<external::Name>().unwrap().into(),
            &make_bgp_peer(original),
        );
        // Unnumbered peers store NULL addr in the DB.
        assert_eq!(db_peer.raw_ip_in_db_repr(), None);
        assert_eq!(db_peer.router_lifetime, SqlU16(lifetime.as_u16()));
        assert_eq!(db_peer.peer_type().unwrap(), original);
    }
}
