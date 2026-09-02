// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::Name;
use crate::typed_uuid::DbTypedUuid;
use crate::{BfdMode, DbSwitchSlot, SqlU8, SqlU16, SqlU32};
use chrono::{DateTime, Utc};
use db_macros::Resource;
use diesel::NullableExpressionMethods;
use ipnetwork::IpNetwork;
use nexus_db_schema::schema::{
    control_plane_router_configuration, router_configuration,
    router_configuration_bfd_peer, router_configuration_bgp_peer,
    router_configuration_static_route, silo_router_configuration,
};
use nexus_types::external_api::networking;
use nexus_types::identity::Resource;
use omicron_common::api::external::Error;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_uuid_kinds::{
    BgpAnnounceSetKind, BgpAnnounceSetUuid, GenericUuid,
    RouterConfigurationKind, RouterConfigurationUuid,
};
use serde::{Deserialize, Serialize};
use sled_agent_types::early_networking::ImportExportPolicy;
use sled_agent_types::early_networking::MaxPathConfig;
use sled_agent_types::early_networking::RouterLifetimeConfig;
use sled_agent_types::early_networking::RouterPeerIpAddr;
use slog_error_chain::InlineErrorChain;
use std::net::IpAddr;
use uuid::Uuid;

/// The BGP configuration stored inline on a `router_configuration` row.
///
/// All of its columns are nullable in the table (a router configuration may
/// have no BGP configuration), but a CHECK constraint ensures they are either
/// all set or all null, so the parent embeds this group as
/// `Option<RouterConfigurationBgpConfig>` with non-optional fields.
#[derive(
    Queryable, Insertable, Selectable, Clone, Debug, Serialize, Deserialize,
)]
#[diesel(table_name = router_configuration)]
pub struct RouterConfigurationBgpConfig {
    #[diesel(select_expression = router_configuration::bgp_asn.assume_not_null())]
    pub bgp_asn: SqlU32,
    #[diesel(select_expression = router_configuration::bgp_max_paths.assume_not_null())]
    pub bgp_max_paths: SqlU8,
    #[diesel(select_expression = router_configuration::bgp_announce_set_id.assume_not_null())]
    pub bgp_announce_set_id: DbTypedUuid<BgpAnnounceSetKind>,
}

impl TryFrom<RouterConfigurationBgpConfig>
    for networking::RouterConfigurationBgpConfig
{
    type Error = Error;

    fn try_from(value: RouterConfigurationBgpConfig) -> Result<Self, Error> {
        let max_paths =
            MaxPathConfig::new(*value.bgp_max_paths).map_err(|err| {
                Error::internal_error(&format!(
                    "invalid database contents: \
                     could not convert MaxPathConfig: {}",
                    InlineErrorChain::new(&err)
                ))
            })?;
        Ok(Self {
            asn: value.bgp_asn.into(),
            max_paths,
            bgp_announce_set: BgpAnnounceSetUuid::from(
                value.bgp_announce_set_id,
            )
            .into_untyped_uuid()
            .into(),
        })
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
#[resource(uuid_kind = RouterConfigurationKind)]
#[diesel(table_name = router_configuration)]
pub struct RouterConfiguration {
    #[diesel(embed)]
    pub identity: RouterConfigurationIdentity,
    pub switch: DbSwitchSlot,
    #[diesel(embed)]
    pub bgp_config: Option<RouterConfigurationBgpConfig>,
}

impl RouterConfiguration {
    pub fn new(c: &networking::RouterConfigurationCreate) -> Self {
        Self {
            identity: RouterConfigurationIdentity::new(
                RouterConfigurationUuid::new_v4(),
                IdentityMetadataCreateParams {
                    name: c.identity.name.clone(),
                    description: c.identity.description.clone(),
                },
            ),
            switch: c.switch.into(),
            bgp_config: None,
        }
    }

    /// Returns the BGP configuration stored inline in this row, if set.
    pub fn bgp_config(
        &self,
    ) -> Result<Option<networking::RouterConfigurationBgpConfig>, Error> {
        self.bgp_config.clone().map(TryInto::try_into).transpose()
    }
}

impl TryFrom<RouterConfiguration> for networking::RouterConfiguration {
    type Error = Error;

    fn try_from(value: RouterConfiguration) -> Result<Self, Self::Error> {
        let bgp_config = value.bgp_config()?;
        Ok(Self {
            identity: value.identity(),
            switch: value.switch.into(),
            bgp_config,
            bgp_peers: Vec::new(),
            routes: Vec::new(),
            bfd_peers: Vec::new(),
        })
    }
}

#[derive(AsChangeset, Clone, Debug)]
#[diesel(table_name = router_configuration)]
pub struct RouterConfigurationUpdate {
    pub name: Option<Name>,
    pub description: Option<String>,
    pub switch: Option<DbSwitchSlot>,
    pub time_modified: DateTime<Utc>,
}

impl From<networking::RouterConfigurationUpdate> for RouterConfigurationUpdate {
    fn from(update: networking::RouterConfigurationUpdate) -> Self {
        Self {
            name: update.identity.name.map(Into::into),
            description: update.identity.description,
            switch: update.switch.map(Into::into),
            time_modified: Utc::now(),
        }
    }
}

fn import_export_policy_to_db(
    policy: &ImportExportPolicy,
) -> Option<Vec<IpNetwork>> {
    match policy {
        ImportExportPolicy::NoFiltering => None,
        ImportExportPolicy::Allow(list) => {
            Some(list.iter().map(|net| (*net).into()).collect())
        }
    }
}

fn import_export_policy_from_db(
    list: Option<Vec<IpNetwork>>,
) -> ImportExportPolicy {
    match list {
        None => ImportExportPolicy::NoFiltering,
        Some(list) => ImportExportPolicy::Allow(
            list.into_iter().map(Into::into).collect(),
        ),
    }
}

/// A BGP peer for a [`RouterConfiguration`].
///
/// A numbered peer has only `addr` set, while an unnumbered peer has only
/// `port_name` and `router_lifetime` set; a CHECK constraint enforces this,
/// so [`RouterConfigurationBgpPeer::peer()`] can rebuild the peer as a
/// [`networking::BgpPeerKind`].
#[derive(
    Queryable, Insertable, Selectable, Clone, Debug, Serialize, Deserialize,
)]
#[diesel(table_name = router_configuration_bgp_peer)]
pub struct RouterConfigurationBgpPeer {
    pub router_configuration_id: DbTypedUuid<RouterConfigurationKind>,
    pub name: Name,
    pub addr: Option<IpNetwork>,
    pub port_name: Option<Name>,
    pub remote_asn: Option<SqlU32>,
    pub allowed_import: Option<Vec<IpNetwork>>,
    pub allowed_export: Option<Vec<IpNetwork>>,
    pub hold_time: SqlU32,
    pub keepalive: SqlU32,
    pub connect_retry: SqlU32,
    pub delay_open: SqlU32,
    pub idle_hold_time: SqlU32,
    pub local_pref: Option<SqlU32>,
    pub communities: Vec<SqlU32>,
    pub multi_exit_discriminator: Option<SqlU32>,
    pub enforce_first_as: bool,
    pub md5_auth_key: Option<String>,
    pub min_ttl: Option<SqlU8>,
    pub vlan_id: Option<SqlU16>,
    pub router_lifetime: Option<SqlU16>,
}

impl RouterConfigurationBgpPeer {
    pub fn new(
        router_configuration_id: RouterConfigurationUuid,
        peer: networking::RouterConfigurationBgpPeer,
    ) -> Self {
        let (addr, port_name, router_lifetime) = match peer.peer {
            networking::BgpPeerKind::Numbered { addr } => {
                (Some(IpAddr::from(addr).into()), None, None)
            }
            networking::BgpPeerKind::Unnumbered { port, router_lifetime } => {
                (None, Some(port), Some(router_lifetime.as_u16().into()))
            }
        };
        Self {
            router_configuration_id: router_configuration_id.into(),
            name: peer.name.into(),
            addr,
            port_name: port_name.map(Into::into),
            remote_asn: peer.remote_asn.map(Into::into),
            allowed_import: import_export_policy_to_db(&peer.allowed_import),
            allowed_export: import_export_policy_to_db(&peer.allowed_export),
            hold_time: peer.hold_time.into(),
            keepalive: peer.keepalive.into(),
            connect_retry: peer.connect_retry.into(),
            delay_open: peer.delay_open.into(),
            idle_hold_time: peer.idle_hold_time.into(),
            local_pref: peer.local_pref.map(Into::into),
            communities: peer.communities.into_iter().map(Into::into).collect(),
            multi_exit_discriminator: peer
                .multi_exit_discriminator
                .map(Into::into),
            enforce_first_as: peer.enforce_first_as,
            md5_auth_key: peer.md5_auth_key,
            min_ttl: peer.min_ttl.map(Into::into),
            vlan_id: peer.vlan_id.map(Into::into),
            router_lifetime,
        }
    }

    /// Returns the peer (numbered or unnumbered) described by this row.
    ///
    /// Only fails if invalid data has been stored in the database.
    pub fn peer(&self) -> Result<networking::BgpPeerKind, Error> {
        match (self.addr, &self.port_name, self.router_lifetime) {
            (Some(addr), None, None) => {
                let addr =
                    RouterPeerIpAddr::try_from(addr.ip()).map_err(|err| {
                        Error::internal_error(&format!(
                            "invalid database contents: \
                             could not convert RouterPeerIpAddr: {}",
                            InlineErrorChain::new(&err)
                        ))
                    })?;
                Ok(networking::BgpPeerKind::Numbered { addr })
            }
            (None, Some(port), Some(lifetime)) => {
                let router_lifetime = RouterLifetimeConfig::new(*lifetime)
                    .map_err(|err| {
                        Error::internal_error(&format!(
                            "invalid database contents: \
                             could not convert RouterLifetimeConfig: {}",
                            InlineErrorChain::new(&err)
                        ))
                    })?;
                Ok(networking::BgpPeerKind::Unnumbered {
                    port: port.clone().into(),
                    router_lifetime,
                })
            }
            _ => Err(Error::internal_error(
                "invalid database contents: a BGP peer must have either \
                 addr set (numbered) or port_name and router_lifetime set \
                 (unnumbered)",
            )),
        }
    }
}

impl TryFrom<RouterConfigurationBgpPeer>
    for networking::RouterConfigurationBgpPeer
{
    type Error = Error;

    fn try_from(value: RouterConfigurationBgpPeer) -> Result<Self, Error> {
        let peer = value.peer()?;
        Ok(Self {
            name: value.name.into(),
            peer,
            remote_asn: value.remote_asn.map(Into::into),
            allowed_import: import_export_policy_from_db(value.allowed_import),
            allowed_export: import_export_policy_from_db(value.allowed_export),
            hold_time: value.hold_time.into(),
            keepalive: value.keepalive.into(),
            connect_retry: value.connect_retry.into(),
            delay_open: value.delay_open.into(),
            idle_hold_time: value.idle_hold_time.into(),
            local_pref: value.local_pref.map(Into::into),
            communities: value
                .communities
                .into_iter()
                .map(Into::into)
                .collect(),
            multi_exit_discriminator: value
                .multi_exit_discriminator
                .map(Into::into),
            enforce_first_as: value.enforce_first_as,
            md5_auth_key: value.md5_auth_key,
            min_ttl: value.min_ttl.map(|v| v.0),
            vlan_id: value.vlan_id.map(|v| v.0),
        })
    }
}

#[derive(
    Queryable, Insertable, Selectable, Clone, Debug, Serialize, Deserialize,
)]
#[diesel(table_name = router_configuration_static_route)]
pub struct RouterConfigurationStaticRoute {
    pub router_configuration_id: DbTypedUuid<RouterConfigurationKind>,
    pub name: Name,
    pub dst: IpNetwork,
    pub gw: IpNetwork,
    pub rib_priority: Option<SqlU8>,
    pub vlan_id: Option<SqlU16>,
}

impl RouterConfigurationStaticRoute {
    pub fn new(
        router_configuration_id: RouterConfigurationUuid,
        route: networking::StaticRoute,
    ) -> Self {
        Self {
            router_configuration_id: router_configuration_id.into(),
            name: route.name.into(),
            dst: route.dst.into(),
            gw: route.gw.into(),
            rib_priority: route.rib_priority.map(Into::into),
            vlan_id: route.vlan_id.map(Into::into),
        }
    }
}

impl From<RouterConfigurationStaticRoute> for networking::StaticRoute {
    fn from(value: RouterConfigurationStaticRoute) -> Self {
        Self {
            name: value.name.into(),
            dst: value.dst.into(),
            gw: value.gw.ip(),
            rib_priority: value.rib_priority.map(|v| v.0),
            vlan_id: value.vlan_id.map(|v| v.0),
        }
    }
}

#[derive(
    Queryable, Insertable, Selectable, Clone, Debug, Serialize, Deserialize,
)]
#[diesel(table_name = router_configuration_bfd_peer)]
pub struct RouterConfigurationBfdPeer {
    pub router_configuration_id: DbTypedUuid<RouterConfigurationKind>,
    pub name: Name,
    pub remote: IpNetwork,
    pub local: Option<IpNetwork>,
    pub mode: BfdMode,
    pub detection_threshold: SqlU8,
    pub required_rx: SqlU32,
}

impl RouterConfigurationBfdPeer {
    pub fn new(
        router_configuration_id: RouterConfigurationUuid,
        peer: networking::BfdPeer,
    ) -> Self {
        Self {
            router_configuration_id: router_configuration_id.into(),
            name: peer.name.into(),
            remote: peer.remote.into(),
            local: peer.local.map(Into::into),
            mode: peer.mode.into(),
            detection_threshold: peer.detection_threshold.into(),
            required_rx: SqlU32::new(
                peer.required_rx.try_into().unwrap_or(u32::MAX),
            ),
        }
    }
}

impl From<RouterConfigurationBfdPeer> for networking::BfdPeer {
    fn from(value: RouterConfigurationBfdPeer) -> Self {
        Self {
            name: value.name.into(),
            remote: value.remote.ip(),
            local: value.local.map(|v| v.ip()),
            mode: value.mode.into(),
            detection_threshold: value.detection_threshold.0,
            required_rx: (*value.required_rx).into(),
        }
    }
}

/// Links a silo to a router configuration it uses with a unique priority
/// within the silo.
#[derive(Queryable, Insertable, Selectable, Clone, Debug)]
#[diesel(table_name = silo_router_configuration)]
pub struct SiloRouterConfiguration {
    pub silo_id: Uuid,
    pub router_configuration_id: DbTypedUuid<RouterConfigurationKind>,
    pub priority: SqlU16,
}

impl SiloRouterConfiguration {
    pub fn new(
        silo_id: Uuid,
        router_configuration_id: RouterConfigurationUuid,
        priority: u16,
    ) -> Self {
        Self {
            silo_id,
            router_configuration_id: router_configuration_id.into(),
            priority: priority.into(),
        }
    }
}

/// One entry of the fleet-wide router-configuration list used by
/// control-plane (service) OPTE ports.
///
/// A `None` `router_configuration_id` is only valid in the single marker row
/// `(0, NULL)` that records "explicitly configured empty" — without it, an
/// empty table would be indistinguishable from "never configured" (which
/// falls back to the built-in default list).
#[derive(Queryable, Insertable, Selectable, Clone, Debug)]
#[diesel(table_name = control_plane_router_configuration)]
pub struct ControlPlaneRouterConfiguration {
    pub priority: SqlU16,
    pub router_configuration_id: Option<DbTypedUuid<RouterConfigurationKind>>,
}

impl ControlPlaneRouterConfiguration {
    pub fn new(
        router_configuration_id: RouterConfigurationUuid,
        priority: u16,
    ) -> Self {
        Self {
            router_configuration_id: Some(router_configuration_id.into()),
            priority: priority.into(),
        }
    }

    /// The marker row meaning "explicitly configured empty".
    pub fn empty_marker() -> Self {
        Self { router_configuration_id: None, priority: 0.into() }
    }
}
