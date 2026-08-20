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
use omicron_uuid_kinds::BgpConfigKind;
use omicron_uuid_kinds::BgpConfigUuid;
use omicron_uuid_kinds::BgpPeerConfigAllowExportKind;
use omicron_uuid_kinds::BgpPeerConfigAllowImportKind;
use omicron_uuid_kinds::BgpPeerConfigCommunityKind;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::RackKind;
use omicron_uuid_kinds::RackUuid;
use omicron_uuid_kinds::SwitchPortSettingsKind;
use omicron_uuid_kinds::SwitchPortSettingsUuid;
use omicron_uuid_kinds::TypedUuid;
use oxnet::IpNet;
use serde::{Deserialize, Serialize};
use sled_agent_types::early_networking::LinkFec;
use sled_agent_types::early_networking::LinkSpeed;
use sled_agent_types::early_networking::RouterLifetimeConfig;
use sled_agent_types::early_networking::RouterLifetimeConfigError;
use sled_agent_types::early_networking::RouterPeerIpAddr;
use sled_agent_types::early_networking::RouterPeerIpAddrError;
use sled_agent_types::early_networking::RouterPeerType;
use sled_agent_types::early_networking::SwitchSlot;
use sled_agent_types::early_networking::{
    AddressFamilyMismatchError, UnnumberedRouter,
};
use sled_agent_types::early_networking::{ImportExportPolicy, NumberedRouter};
use std::net::IpAddr;
use uuid::Uuid;

/// Extension trait on [`RouterPeerType`] for converting it to the way
/// we represent peer addresses in the database.
///
/// This trait should only be used by database model and query functions.
pub trait RouterPeerTypeDbRepresentation: Sized {
    /// Get the database representation of the address of this peer.
    ///
    /// For numbered peers, returns `Some(ip)` (corresponding to a non-NULL
    /// `INET`); for unnumbered peers, returns `None` (corresponding to NULL).
    fn ip_db_repr(&self) -> Option<IpNetwork>;
}

impl RouterPeerTypeDbRepresentation for RouterPeerType {
    fn ip_db_repr(&self) -> Option<IpNetwork> {
        match self {
            Self::Unnumbered(_) => None,
            Self::Numbered(numbered_router) => {
                Some((numbered_router.target_addr()).into())
            }
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

impl From<SwitchLinkFec> for LinkFec {
    fn from(value: SwitchLinkFec) -> Self {
        match value {
            SwitchLinkFec::Firecode => Self::Firecode,
            SwitchLinkFec::None => Self::None,
            SwitchLinkFec::Rs => Self::Rs,
        }
    }
}

impl From<LinkFec> for SwitchLinkFec {
    fn from(value: LinkFec) -> Self {
        match value {
            LinkFec::Firecode => Self::Firecode,
            LinkFec::None => Self::None,
            LinkFec::Rs => Self::Rs,
        }
    }
}

impl From<SwitchLinkSpeed> for LinkSpeed {
    fn from(value: SwitchLinkSpeed) -> Self {
        match value {
            SwitchLinkSpeed::Speed0G => Self::Speed0G,
            SwitchLinkSpeed::Speed1G => Self::Speed1G,
            SwitchLinkSpeed::Speed10G => Self::Speed10G,
            SwitchLinkSpeed::Speed25G => Self::Speed25G,
            SwitchLinkSpeed::Speed40G => Self::Speed40G,
            SwitchLinkSpeed::Speed50G => Self::Speed50G,
            SwitchLinkSpeed::Speed100G => Self::Speed100G,
            SwitchLinkSpeed::Speed200G => Self::Speed200G,
            SwitchLinkSpeed::Speed400G => Self::Speed400G,
        }
    }
}

impl From<LinkSpeed> for SwitchLinkSpeed {
    fn from(value: LinkSpeed) -> Self {
        match value {
            LinkSpeed::Speed0G => Self::Speed0G,
            LinkSpeed::Speed1G => Self::Speed1G,
            LinkSpeed::Speed10G => Self::Speed10G,
            LinkSpeed::Speed25G => Self::Speed25G,
            LinkSpeed::Speed40G => Self::Speed40G,
            LinkSpeed::Speed50G => Self::Speed50G,
            LinkSpeed::Speed100G => Self::Speed100G,
            LinkSpeed::Speed200G => Self::Speed200G,
            LinkSpeed::Speed400G => Self::Speed400G,
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

impl Into<networking_types::SwitchPortGeometry> for SwitchPortGeometry {
    fn into(self) -> networking_types::SwitchPortGeometry {
        match self {
            SwitchPortGeometry::Qsfp28x1 => {
                networking_types::SwitchPortGeometry::Qsfp28x1
            }
            SwitchPortGeometry::Qsfp28x2 => {
                networking_types::SwitchPortGeometry::Qsfp28x2
            }
            SwitchPortGeometry::Sfp28x4 => {
                networking_types::SwitchPortGeometry::Sfp28x4
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
    pub rack_id: DbTypedUuid<RackKind>,
    pub port_name: Name,
    pub port_settings_id: Option<DbTypedUuid<SwitchPortSettingsKind>>,
    pub switch_slot: DbSwitchSlot,
}

impl SwitchPort {
    pub fn new(
        rack_id: RackUuid,
        switch_slot: SwitchSlot,
        port_name: Name,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            rack_id: rack_id.into(),
            switch_slot: switch_slot.into(),
            port_name,
            port_settings_id: None,
        }
    }

    pub fn rack_id(&self) -> RackUuid {
        self.rack_id.into()
    }

    pub fn port_settings_id(&self) -> Option<SwitchPortSettingsUuid> {
        self.port_settings_id.map(Into::into)
    }
}

impl Into<networking_types::SwitchPort> for SwitchPort {
    fn into(self) -> networking_types::SwitchPort {
        networking_types::SwitchPort {
            id: self.id,
            rack_id: self.rack_id.into_untyped_uuid(),
            switch_slot: self.switch_slot.into(),
            port_name: self.port_name.into(),
            port_settings_id: self
                .port_settings_id
                .map(|id| id.into_untyped_uuid()),
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
#[resource(uuid_kind = SwitchPortSettingsKind)]
pub struct SwitchPortSettings {
    #[diesel(embed)]
    pub identity: SwitchPortSettingsIdentity,
}

impl SwitchPortSettings {
    pub fn new(meta: &external::IdentityMetadataCreateParams) -> Self {
        Self {
            identity: SwitchPortSettingsIdentity::new(
                SwitchPortSettingsUuid::new_v4(),
                meta.clone(),
            ),
        }
    }

    pub fn with_id(
        id: SwitchPortSettingsUuid,
        meta: &external::IdentityMetadataCreateParams,
    ) -> Self {
        Self { identity: SwitchPortSettingsIdentity::new(id, meta.clone()) }
    }

    pub fn id(&self) -> SwitchPortSettingsUuid {
        self.identity.id.into()
    }
}

impl Into<networking_types::SwitchPortSettingsIdentity> for SwitchPortSettings {
    fn into(self) -> networking_types::SwitchPortSettingsIdentity {
        networking_types::SwitchPortSettingsIdentity {
            identity: self.identity(),
        }
    }
}

#[derive(
    Queryable, Insertable, Selectable, Clone, Debug, Serialize, Deserialize,
)]
#[diesel(table_name = switch_port_settings_groups)]
pub struct SwitchPortSettingsGroups {
    pub port_settings_id: DbTypedUuid<SwitchPortSettingsKind>,
    pub port_settings_group_id: Uuid,
}

impl Into<networking_types::SwitchPortSettingsGroups>
    for SwitchPortSettingsGroups
{
    fn into(self) -> networking_types::SwitchPortSettingsGroups {
        networking_types::SwitchPortSettingsGroups {
            port_settings_id: self.port_settings_id.into_untyped_uuid(),
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
    pub port_settings_id: DbTypedUuid<SwitchPortSettingsKind>,
}

impl Into<networking_types::SwitchPortSettingsGroup>
    for SwitchPortSettingsGroup
{
    fn into(self) -> networking_types::SwitchPortSettingsGroup {
        networking_types::SwitchPortSettingsGroup {
            identity: self.identity(),
            port_settings_id: self.port_settings_id.into_untyped_uuid(),
        }
    }
}

#[derive(
    Queryable, Insertable, Selectable, Clone, Debug, Serialize, Deserialize,
)]
#[diesel(table_name = switch_port_settings_port_config)]
pub struct SwitchPortConfig {
    pub port_settings_id: DbTypedUuid<SwitchPortSettingsKind>,
    pub geometry: SwitchPortGeometry,
    /// Whether DDM traffic is permitted on this port.
    pub allow_ddm_traffic: bool,
}

impl SwitchPortConfig {
    pub fn new(
        port_settings_id: SwitchPortSettingsUuid,
        geometry: SwitchPortGeometry,
        allow_ddm_traffic: bool,
    ) -> Self {
        Self {
            port_settings_id: port_settings_id.into(),
            geometry,
            allow_ddm_traffic,
        }
    }
}

impl Into<networking_types::SwitchPortConfig> for SwitchPortConfig {
    fn into(self) -> networking_types::SwitchPortConfig {
        networking_types::SwitchPortConfig {
            port_settings_id: self.port_settings_id.into_untyped_uuid(),
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
    pub port_settings_id: DbTypedUuid<SwitchPortSettingsKind>,
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
        port_settings_id: SwitchPortSettingsUuid,
        lldp_link_config_id: Uuid,
        link_name: Name,
        mtu: u16,
        fec: Option<SwitchLinkFec>,
        speed: SwitchLinkSpeed,
        autoneg: bool,
        tx_eq_config_id: Option<Uuid>,
    ) -> Self {
        Self {
            port_settings_id: port_settings_id.into(),
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
impl Into<networking_types::LldpLinkConfig> for LldpLinkConfig {
    fn into(self) -> networking_types::LldpLinkConfig {
        networking_types::LldpLinkConfig {
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
impl Into<sled_agent_types::early_networking::TxEqConfig> for TxEqConfig {
    fn into(self) -> sled_agent_types::early_networking::TxEqConfig {
        sled_agent_types::early_networking::TxEqConfig {
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
    pub port_settings_id: DbTypedUuid<SwitchPortSettingsKind>,
    pub id: Uuid,
    pub interface_name: Name,
    pub v6_enabled: bool,
    pub kind: crate::DbSwitchInterfaceKind,
}

impl SwitchInterfaceConfig {
    pub fn new(
        port_settings_id: SwitchPortSettingsUuid,
        interface_name: Name,
        v6_enabled: bool,
        kind: crate::DbSwitchInterfaceKind,
    ) -> Self {
        Self {
            port_settings_id: port_settings_id.into(),
            id: Uuid::new_v4(),
            interface_name,
            v6_enabled,
            kind,
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
    pub port_settings_id: DbTypedUuid<SwitchPortSettingsKind>,
    pub interface_name: Name,
    pub dst: IpNetwork,
    pub gw: IpNetwork,
    pub vid: Option<SqlU16>,
    pub rib_priority: Option<SqlU8>,
}

impl SwitchPortRouteConfig {
    pub fn new(
        port_settings_id: SwitchPortSettingsUuid,
        interface_name: Name,
        dst: IpNetwork,
        gw: IpNetwork,
        vid: Option<SqlU16>,
        rib_priority: Option<SqlU8>,
    ) -> Self {
        Self {
            port_settings_id: port_settings_id.into(),
            interface_name,
            dst,
            gw,
            vid,
            rib_priority,
        }
    }
}

impl Into<networking_types::SwitchPortRouteConfig> for SwitchPortRouteConfig {
    fn into(self) -> networking_types::SwitchPortRouteConfig {
        networking_types::SwitchPortRouteConfig {
            port_settings_id: self.port_settings_id.into_untyped_uuid(),
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
    pub port_settings_id: DbTypedUuid<SwitchPortSettingsKind>,
    pub bgp_config_id: DbTypedUuid<BgpConfigKind>,
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
    src_addr: Option<IpNetwork>,
}

#[derive(Debug, thiserror::Error)]
pub enum SwitchPortBgpPeerConfigInvalidData {
    #[error(
        "database inconsistency: \
        invalid peer address in BGP peer config {port_settings_id}"
    )]
    PeerAddress {
        port_settings_id: SwitchPortSettingsUuid,
        #[source]
        err: RouterPeerIpAddrError,
    },
    #[error(
        "database inconsistency: \
        invalid router lifetime in BGP peer config {port_settings_id}"
    )]
    RouterLifetime {
        port_settings_id: SwitchPortSettingsUuid,
        #[source]
        err: RouterLifetimeConfigError,
    },
    #[error(
        "database inconsistency: \
        invalid source address in BGP peer config {port_settings_id}"
    )]
    SrcAddress {
        port_settings_id: SwitchPortSettingsUuid,
        #[source]
        err: RouterPeerIpAddrError,
    },
    #[error(
        "database inconsistency: \
        src_addr is set for an unnumbered peer {port_settings_id}"
    )]
    SrcAddressUse { port_settings_id: SwitchPortSettingsUuid },
    #[error(
        "database inconsistency: \
        mismatched address families in BGP peer config {port_settings_id}"
    )]
    AddressFamily {
        port_settings_id: SwitchPortSettingsUuid,
        #[source]
        err: AddressFamilyMismatchError,
    },
}

impl SwitchPortBgpPeerConfig {
    /// Return the ID of the BGP config this peer references.
    pub fn bgp_config_id(&self) -> BgpConfigUuid {
        self.bgp_config_id.into()
    }

    /// Return the [`RouterPeerType`] (numbered or unnumbered, with additional
    /// details specific to each type) of this peer.
    ///
    /// Only fails if invalid data has been stored in the database.
    pub fn peer_type(
        &self,
    ) -> Result<RouterPeerType, SwitchPortBgpPeerConfigInvalidData> {
        // We only expect NULL (corresponding to unnumbered, in which case we
        // expect a valid `router_lifetime` too) or `Some(ip)` where `ip` is a
        // valid router peer IP. `src_addr` should only be set for numbered
        // peers, and should have a matching address family.
        match self.addr {
            Some(db_ip) => {
                let ip =
                    RouterPeerIpAddr::try_from(db_ip.ip()).map_err(|err| {
                        SwitchPortBgpPeerConfigInvalidData::PeerAddress {
                            port_settings_id: self.port_settings_id.into(),
                            err,
                        }
                    })?;

                let src_addr = self
                    .src_addr
                    .map(|network| RouterPeerIpAddr::try_from(network.ip()))
                    .transpose()
                    .map_err(|err| {
                        SwitchPortBgpPeerConfigInvalidData::SrcAddress {
                            port_settings_id: self.port_settings_id.into(),
                            err,
                        }
                    })?;

                let router =
                    NumberedRouter::new(ip, src_addr).map_err(|err| {
                        SwitchPortBgpPeerConfigInvalidData::AddressFamily {
                            port_settings_id: self.port_settings_id.into(),
                            err,
                        }
                    })?;

                Ok(router.into())
            }
            None => {
                let router_lifetime = RouterLifetimeConfig::new(
                    *self.router_lifetime,
                )
                .map_err(|err| {
                    SwitchPortBgpPeerConfigInvalidData::RouterLifetime {
                        port_settings_id: self.port_settings_id.into(),
                        err,
                    }
                })?;

                if self.src_addr.is_some() {
                    return Err(
                        SwitchPortBgpPeerConfigInvalidData::SrcAddressUse {
                            port_settings_id: self.port_settings_id.into(),
                        },
                    );
                }

                Ok(UnnumberedRouter { router_lifetime }.into())
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
    pub port_settings_id: DbTypedUuid<SwitchPortSettingsKind>,
    pub interface_name: Name,
    addr: Option<IpNetwork>,
    pub community: SqlU32,
    pub id: DbTypedUuid<BgpPeerConfigCommunityKind>,
}

impl SwitchPortBgpPeerConfigCommunity {
    pub fn new(
        port_settings_id: SwitchPortSettingsUuid,
        interface_name: Name,
        addr: RouterPeerType,
        community: u32,
    ) -> Self {
        Self {
            port_settings_id: port_settings_id.into(),
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
    pub port_settings_id: DbTypedUuid<SwitchPortSettingsKind>,
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
        port_settings_id: SwitchPortSettingsUuid,
        interface_name: Name,
        addr: RouterPeerType,
        prefix: IpNet,
    ) -> Self {
        Self {
            port_settings_id: port_settings_id.into(),
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
    pub port_settings_id: DbTypedUuid<SwitchPortSettingsKind>,
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
        port_settings_id: SwitchPortSettingsUuid,
        interface_name: Name,
        addr: RouterPeerType,
        prefix: IpNet,
    ) -> Self {
        Self {
            port_settings_id: port_settings_id.into(),
            interface_name,
            addr: addr.ip_db_repr(),
            prefix: prefix.into(),
            id: TypedUuid::new_v4().into(),
        }
    }
}

impl SwitchPortBgpPeerConfig {
    pub fn new(
        port_settings_id: SwitchPortSettingsUuid,
        bgp_config_id: BgpConfigUuid,
        interface_name: Name,
        p: &networking_types::BgpPeer,
    ) -> Self {
        // Unnumbered peers have a `router_lifetime`; for numbered peers, we
        // must use the default (0). This is enforced by a CHECK constraint.
        let router_lifetime = match p.addr {
            RouterPeerType::Numbered { .. } => RouterLifetimeConfig::default(),
            RouterPeerType::Unnumbered(u) => u.router_lifetime,
        };
        Self {
            id: Uuid::new_v4(),
            port_settings_id: port_settings_id.into(),
            bgp_config_id: bgp_config_id.into(),
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
            src_addr: match p.addr {
                RouterPeerType::Numbered(numbered_router) => {
                    numbered_router.src_addr().map(|a| IpAddr::from(a).into())
                }
                RouterPeerType::Unnumbered(_) => None,
            },
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
    pub port_settings_id: DbTypedUuid<SwitchPortSettingsKind>,
    pub address_lot_block_id: Uuid,
    pub rsvd_address_lot_block_id: Uuid,
    pub address: IpNetwork,
    pub interface_name: Name,
    pub vlan_id: Option<SqlU16>,
}

impl SwitchPortAddressConfig {
    pub fn new(
        port_settings_id: SwitchPortSettingsUuid,
        address_lot_block_id: Uuid,
        rsvd_address_lot_block_id: Uuid,
        address: IpNetwork,
        interface_name: Name,
        vlan_id: Option<u16>,
    ) -> Self {
        Self {
            port_settings_id: port_settings_id.into(),
            address_lot_block_id,
            rsvd_address_lot_block_id,
            address,
            interface_name,
            vlan_id: vlan_id.map(|x| x.into()),
        }
    }
}

impl Into<networking_types::SwitchPortAddressConfig>
    for SwitchPortAddressConfig
{
    fn into(self) -> networking_types::SwitchPortAddressConfig {
        networking_types::SwitchPortAddressConfig {
            port_settings_id: self.port_settings_id.into_untyped_uuid(),
            address_lot_block_id: self.address_lot_block_id,
            address: self.address.into(),
            interface_name: self.interface_name.into(),
            vlan_id: self.vlan_id.map(|x| x.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sled_agent_types::early_networking::NumberedRouter;
    use sled_agent_types::early_networking::RouterLifetimeConfig;
    use sled_agent_types::early_networking::RouterPeerIpAddr;
    use sled_agent_types::early_networking::RouterPeerType;
    use sled_agent_types::early_networking::UnnumberedRouter;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    #[test]
    fn router_peer_type_test_ip_db_repr() {
        let ip_addr = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let ip = RouterPeerIpAddr::try_from(ip_addr).unwrap();
        let src_addr = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
        let src = RouterPeerIpAddr::try_from(src_addr).unwrap();
        let original: RouterPeerType =
            NumberedRouter::new(ip, Some(src)).unwrap().into();

        let db_repr = original.ip_db_repr();
        assert_eq!(db_repr, Some(IpNetwork::from(ip_addr)));

        let ip_addr = IpAddr::V6(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1));
        let ip = RouterPeerIpAddr::try_from(ip_addr).unwrap();
        let src_addr = IpAddr::V6(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 2));
        let src = RouterPeerIpAddr::try_from(src_addr).unwrap();
        let original: RouterPeerType =
            NumberedRouter::new(ip, Some(src)).unwrap().into();

        let db_repr = original.ip_db_repr();
        assert_eq!(db_repr, Some(IpNetwork::from(ip_addr)));

        let router_lifetime = RouterLifetimeConfig::new(1800).unwrap();
        let original: RouterPeerType =
            UnnumberedRouter { router_lifetime }.into();

        assert_eq!(original.ip_db_repr(), None);
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
        let src_addr = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
        let src = RouterPeerIpAddr::try_from(src_addr).unwrap();
        let original = NumberedRouter::new(ip, Some(src)).unwrap().into();
        let db_peer = SwitchPortBgpPeerConfig::new(
            SwitchPortSettingsUuid::new_v4(),
            BgpConfigUuid::new_v4(),
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
        let router_lifetime = RouterLifetimeConfig::new(300).unwrap();
        let original = UnnumberedRouter { router_lifetime }.into();
        let db_peer = SwitchPortBgpPeerConfig::new(
            SwitchPortSettingsUuid::new_v4(),
            BgpConfigUuid::new_v4(),
            "phy0".parse::<external::Name>().unwrap().into(),
            &make_bgp_peer(original),
        );
        // Unnumbered peers store NULL addr in the DB.
        assert_eq!(db_peer.raw_ip_in_db_repr(), None);
        assert_eq!(db_peer.router_lifetime, SqlU16(router_lifetime.as_u16()));
        assert_eq!(db_peer.peer_type().unwrap(), original);
    }
}
