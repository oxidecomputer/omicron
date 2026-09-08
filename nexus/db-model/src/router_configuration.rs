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
use sled_agent_types::early_networking::NumberedRouter;
use sled_agent_types::early_networking::RouterLifetimeConfig;
use sled_agent_types::early_networking::RouterPeerIpAddr;
use slog_error_chain::InlineErrorChain;
use std::net::IpAddr;
use std::num::NonZeroU8;
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
/// A numbered peer has `addr` and an optional same-family `src_addr`, while
/// an unnumbered peer has only `port_name` and `router_lifetime` set. CHECK
/// constraints enforce this,
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
    pub src_addr: Option<IpNetwork>,
}

impl RouterConfigurationBgpPeer {
    pub fn new(
        router_configuration_id: RouterConfigurationUuid,
        peer: networking::RouterConfigurationBgpPeer,
    ) -> Result<Self, Error> {
        let (addr, src_addr, port_name, router_lifetime) = match peer.peer {
            networking::BgpPeerKind::Numbered { addr, src_addr } => {
                NumberedRouter::new(addr, src_addr).map_err(|err| {
                    Error::invalid_request(&format!(
                        "BGP peer {}: {err}",
                        peer.name,
                    ))
                })?;
                (
                    Some(IpAddr::from(addr).into()),
                    src_addr.map(|addr| IpAddr::from(addr).into()),
                    None,
                    None,
                )
            }
            networking::BgpPeerKind::Unnumbered { port, router_lifetime } => {
                (None, None, Some(port), Some(router_lifetime.as_u16().into()))
            }
        };
        Ok(Self {
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
            src_addr,
        })
    }

    /// Returns the peer (numbered or unnumbered) described by this row.
    ///
    /// Only fails if invalid data has been stored in the database.
    pub fn peer(&self) -> Result<networking::BgpPeerKind, Error> {
        match (self.addr, self.src_addr, &self.port_name, self.router_lifetime)
        {
            (Some(addr), src_addr, None, None) => {
                let addr =
                    RouterPeerIpAddr::try_from(addr.ip()).map_err(|err| {
                        Error::internal_error(&format!(
                            "invalid database contents: \
                             could not convert RouterPeerIpAddr: {}",
                            InlineErrorChain::new(&err)
                        ))
                    })?;
                let src_addr = src_addr
                    .map(|src| RouterPeerIpAddr::try_from(src.ip()))
                    .transpose()
                    .map_err(|err| Error::internal_error(&format!(
                        "invalid database contents: BGP source address: {err}",
                    )))?;
                NumberedRouter::new(addr, src_addr).map_err(|err| {
                    Error::internal_error(&format!(
                        "invalid database contents: BGP source address: {err}",
                    ))
                })?;
                Ok(networking::BgpPeerKind::Numbered { addr, src_addr })
            }
            (None, None, Some(port), Some(lifetime)) => {
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
    /// Convert an API static route for storage. This is the single
    /// admission point for create and update: the destination and gateway
    /// must be of the same address family, otherwise the route could only
    /// fail later, at render or apply time, after it had been persisted.
    pub fn new(
        router_configuration_id: RouterConfigurationUuid,
        route: networking::StaticRoute,
    ) -> Result<Self, Error> {
        let dst_v4 = matches!(route.dst, oxnet::IpNet::V4(_));
        let gw_v4 = route.gw.is_ipv4();
        if dst_v4 != gw_v4 {
            return Err(Error::invalid_request(&format!(
                "static route {}: destination {} and gateway {} must be of \
                 the same address family",
                route.name, route.dst, route.gw,
            )));
        }
        Ok(Self {
            router_configuration_id: router_configuration_id.into(),
            name: route.name.into(),
            dst: route.dst.into(),
            gw: route.gw.into(),
            rib_priority: route.rib_priority.map(Into::into),
            vlan_id: route.vlan_id.map(Into::into),
        })
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
    /// Convert an API BFD peer for storage. This is the single admission
    /// point for create and update. The API type already bounds
    /// `required_rx` to 32 bits and `detection_threshold` to a nonzero
    /// value, so both are stored exactly as given; a local address, when
    /// present, must be of the remote address's family.
    pub fn new(
        router_configuration_id: RouterConfigurationUuid,
        peer: networking::BfdPeer,
    ) -> Result<Self, Error> {
        if let Some(local) = peer.local {
            if local.is_ipv4() != peer.remote.is_ipv4() {
                return Err(Error::invalid_request(&format!(
                    "bfd peer {}: local address {local} and remote address \
                     {} must be of the same address family",
                    peer.name, peer.remote,
                )));
            }
        }
        Ok(Self {
            router_configuration_id: router_configuration_id.into(),
            name: peer.name.into(),
            remote: peer.remote.into(),
            local: peer.local.map(Into::into),
            mode: peer.mode.into(),
            detection_threshold: SqlU8::new(peer.detection_threshold.get()),
            required_rx: SqlU32::new(peer.required_rx),
        })
    }
}

impl TryFrom<RouterConfigurationBfdPeer> for networking::BfdPeer {
    type Error = Error;

    fn try_from(value: RouterConfigurationBfdPeer) -> Result<Self, Error> {
        let detection_threshold = NonZeroU8::new(value.detection_threshold.0)
            .ok_or_else(|| {
                Error::internal_error(&format!(
                    "invalid database contents: bfd peer {} has a zero \
                     detection threshold",
                    value.name,
                ))
            })?;
        Ok(Self {
            name: value.name.into(),
            remote: value.remote.ip(),
            local: value.local.map(|v| v.ip()),
            mode: value.mode.into(),
            detection_threshold,
            required_rx: *value.required_rx,
        })
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

#[cfg(test)]
mod tests {
    use super::*;
    use omicron_common::api::external;
    use sled_agent_types::early_networking::BfdMode as ApiBfdMode;

    fn name(s: &str) -> external::Name {
        s.parse().unwrap()
    }

    fn route(dst: &str, gw: &str) -> networking::StaticRoute {
        networking::StaticRoute {
            name: name("r"),
            dst: dst.parse().unwrap(),
            gw: gw.parse().unwrap(),
            rib_priority: None,
            vlan_id: None,
        }
    }

    fn bfd(remote: &str, local: Option<&str>) -> networking::BfdPeer {
        networking::BfdPeer {
            name: name("b"),
            remote: remote.parse().unwrap(),
            local: local.map(|l| l.parse().unwrap()),
            mode: ApiBfdMode::MultiHop,
            detection_threshold: NonZeroU8::new(3).unwrap(),
            required_rx: u32::MAX,
        }
    }

    fn is_invalid_request(e: &Error) -> bool {
        matches!(e, Error::InvalidRequest { .. })
    }

    #[test]
    fn bgp_source_address_validates_and_round_trips() {
        let id = RouterConfigurationUuid::new_v4();
        for (target, source) in [
            ("10.99.0.3", Some("10.99.0.4")),
            ("2001:db8::3", Some("2001:db8::4")),
            ("10.99.0.3", None),
        ] {
            let api: networking::RouterConfigurationBgpPeer =
                serde_json::from_value(serde_json::json!({
                    "name": "source-test",
                    "peer": {"type": "numbered", "addr": target, "src_addr": source},
                    "hold_time": 6, "keepalive": 2, "connect_retry": 3,
                    "delay_open": 0, "idle_hold_time": 3, "enforce_first_as": false,
                })).unwrap();
            let mut db =
                RouterConfigurationBgpPeer::new(id, api.clone()).unwrap();
            let back =
                networking::RouterConfigurationBgpPeer::try_from(db.clone())
                    .unwrap();
            assert_eq!(back, api);
            // Defend reads even if stored data bypassed the constraints.
            db.src_addr = Some(if target.contains(':') {
                "10.99.0.4".parse::<IpAddr>().unwrap().into()
            } else {
                "2001:db8::4".parse::<IpAddr>().unwrap().into()
            });
            assert!(matches!(db.peer(), Err(Error::InternalError { .. })));
            let mut invalid = api;
            if let networking::BgpPeerKind::Numbered { src_addr, .. } =
                &mut invalid.peer
            {
                *src_addr = Some(
                    RouterPeerIpAddr::try_from(db.src_addr.unwrap().ip())
                        .unwrap(),
                );
            }
            assert!(matches!(
                RouterConfigurationBgpPeer::new(id, invalid),
                Err(Error::InvalidRequest { .. })
            ));
        }
    }

    #[test]
    fn static_route_requires_matching_families() {
        let id = RouterConfigurationUuid::new_v4();
        assert!(RouterConfigurationStaticRoute::new(id, route("10.0.0.0/8", "10.1.1.1")).is_ok());
        assert!(RouterConfigurationStaticRoute::new(id, route("fd00::/8", "fd00::1")).is_ok());
        let e = RouterConfigurationStaticRoute::new(id, route("10.0.0.0/8", "fd00::1"))
            .unwrap_err();
        assert!(is_invalid_request(&e), "{e}");
        let e = RouterConfigurationStaticRoute::new(id, route("fd00::/8", "10.1.1.1"))
            .unwrap_err();
        assert!(is_invalid_request(&e), "{e}");
    }

    #[test]
    fn bfd_peer_requires_matching_families_and_keeps_required_rx() {
        let id = RouterConfigurationUuid::new_v4();
        let db = RouterConfigurationBfdPeer::new(id, bfd("203.0.113.10", None)).unwrap();
        assert_eq!(*db.required_rx, u32::MAX);
        let back: networking::BfdPeer = db.try_into().unwrap();
        assert_eq!(back.required_rx, u32::MAX);
        assert_eq!(back.detection_threshold.get(), 3);

        assert!(RouterConfigurationBfdPeer::new(id, bfd("203.0.113.10", Some("203.0.113.1"))).is_ok());
        assert!(RouterConfigurationBfdPeer::new(id, bfd("fd00::10", Some("fd00::1"))).is_ok());
        let e = RouterConfigurationBfdPeer::new(id, bfd("203.0.113.10", Some("fd00::1")))
            .unwrap_err();
        assert!(is_invalid_request(&e), "{e}");
        let e = RouterConfigurationBfdPeer::new(id, bfd("fd00::10", Some("203.0.113.1")))
            .unwrap_err();
        assert!(is_invalid_request(&e), "{e}");
    }

    #[test]
    fn bfd_peer_zero_threshold_in_database_is_an_internal_error() {
        let id = RouterConfigurationUuid::new_v4();
        let mut db = RouterConfigurationBfdPeer::new(id, bfd("203.0.113.10", None)).unwrap();
        db.detection_threshold = SqlU8::new(0);
        let e = networking::BfdPeer::try_from(db).unwrap_err();
        assert!(matches!(e, Error::InternalError { .. }), "{e}");
    }
}
