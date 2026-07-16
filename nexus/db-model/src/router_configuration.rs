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
    router_configuration, router_configuration_bfd_peer,
    router_configuration_bgp_peer, router_configuration_static_route,
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
use slog_error_chain::InlineErrorChain;

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
    pub time_modified: DateTime<Utc>,
}

impl From<networking::RouterConfigurationUpdate> for RouterConfigurationUpdate {
    fn from(update: networking::RouterConfigurationUpdate) -> Self {
        Self {
            name: update.identity.name.map(Into::into),
            description: update.identity.description,
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

#[derive(
    Queryable, Insertable, Selectable, Clone, Debug, Serialize, Deserialize,
)]
#[diesel(table_name = router_configuration_bgp_peer)]
pub struct RouterConfigurationBgpPeer {
    pub router_configuration_id: DbTypedUuid<RouterConfigurationKind>,
    pub name: Name,
    pub addr: Option<IpNetwork>,
    pub port_name: Name,
    pub remote_asn: SqlU32,
    pub allowed_import: Option<Vec<IpNetwork>>,
    pub allowed_export: Option<Vec<IpNetwork>>,
    pub hold_time: SqlU32,
    pub keepalive: SqlU32,
    pub connect_retry: SqlU32,
    pub delay_open: SqlU32,
    pub idle_hold_time: SqlU32,
    pub local_pref: Option<SqlU32>,
    pub communities: Vec<i64>,
    pub multi_exit_discriminator: Option<SqlU32>,
    pub enforce_first_as: bool,
    pub md5_auth_key: Option<String>,
    pub min_ttl: Option<SqlU8>,
    pub vlan_id: Option<SqlU16>,
    pub router_lifetime: SqlU16,
}

impl RouterConfigurationBgpPeer {
    pub fn new(
        router_configuration_id: RouterConfigurationUuid,
        peer: networking::RouterConfigurationBgpPeer,
    ) -> Self {
        let (addr, port_name) = match peer.peer {
            networking::BgpPeerKind::Numbered { addr, port } => {
                (Some(addr.into()), port)
            }
            networking::BgpPeerKind::Unnumbered { port } => (None, port),
        };
        Self {
            router_configuration_id: router_configuration_id.into(),
            name: peer.name.into(),
            addr,
            port_name: port_name.into(),
            remote_asn: peer.remote_asn.into(),
            allowed_import: import_export_policy_to_db(&peer.allowed_import),
            allowed_export: import_export_policy_to_db(&peer.allowed_export),
            hold_time: peer.hold_time.into(),
            keepalive: peer.keepalive.into(),
            connect_retry: peer.connect_retry.into(),
            delay_open: peer.delay_open.into(),
            idle_hold_time: peer.idle_hold_time.into(),
            local_pref: peer.local_pref.map(Into::into),
            communities: peer
                .communities
                .into_iter()
                .map(|c| i64::from(c))
                .collect(),
            multi_exit_discriminator: peer
                .multi_exit_discriminator
                .map(Into::into),
            enforce_first_as: peer.enforce_first_as,
            md5_auth_key: peer.md5_auth_key,
            min_ttl: peer.min_ttl.map(Into::into),
            vlan_id: peer.vlan_id.map(Into::into),
            router_lifetime: peer.router_lifetime.as_u16().into(),
        }
    }
}

impl TryFrom<RouterConfigurationBgpPeer>
    for networking::RouterConfigurationBgpPeer
{
    type Error = Error;

    fn try_from(value: RouterConfigurationBgpPeer) -> Result<Self, Error> {
        let port = value.port_name.into();
        let peer = match value.addr {
            Some(addr) => {
                networking::BgpPeerKind::Numbered { addr: addr.ip(), port }
            }
            None => networking::BgpPeerKind::Unnumbered { port },
        };
        let router_lifetime = RouterLifetimeConfig::new(*value.router_lifetime)
            .map_err(|err| {
                Error::internal_error(&format!(
                    "invalid database contents: \
                     could not convert RouterLifetimeConfig: {}",
                    InlineErrorChain::new(&err)
                ))
            })?;
        let communities = value
            .communities
            .into_iter()
            .map(|c| {
                u32::try_from(c).map_err(|_| {
                    Error::internal_error(
                        "invalid database contents: \
                         BGP community out of range",
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            name: value.name.into(),
            peer,
            remote_asn: value.remote_asn.into(),
            allowed_import: import_export_policy_from_db(value.allowed_import),
            allowed_export: import_export_policy_from_db(value.allowed_export),
            hold_time: value.hold_time.into(),
            keepalive: value.keepalive.into(),
            connect_retry: value.connect_retry.into(),
            delay_open: value.delay_open.into(),
            idle_hold_time: value.idle_hold_time.into(),
            local_pref: value.local_pref.map(Into::into),
            communities,
            multi_exit_discriminator: value
                .multi_exit_discriminator
                .map(Into::into),
            enforce_first_as: value.enforce_first_as,
            md5_auth_key: value.md5_auth_key,
            min_ttl: value.min_ttl.map(|v| v.0),
            vlan_id: value.vlan_id.map(|v| v.0),
            router_lifetime,
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
    pub switch: DbSwitchSlot,
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
            switch: peer.switch.into(),
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
            switch: value.switch.into(),
        }
    }
}
