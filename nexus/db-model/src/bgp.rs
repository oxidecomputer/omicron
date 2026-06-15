// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{DbSwitchSlot, RouterPeerTypeDbRepresentation};
use crate::{SqlU8, SqlU16, SqlU32};
use db_macros::Resource;
use ipnetwork::IpNetwork;
use nexus_db_schema::schema::{
    bgp_announce_set, bgp_announcement, bgp_config, bgp_peer_view,
};
use nexus_types::external_api::networking;
use nexus_types::identity::Resource;
use omicron_common::api::external::Error;
use omicron_common::api::external::IdentityMetadataCreateParams;
use serde::{Deserialize, Serialize};
use sled_agent_types::early_networking::BgpPeerConfig;
use sled_agent_types::early_networking::ImportExportPolicy;
use sled_agent_types::early_networking::MaxPathConfig;
use sled_agent_types::early_networking::RouterLifetimeConfig;
use sled_agent_types::early_networking::RouterLifetimeConfigError;
use sled_agent_types::early_networking::RouterPeerIpAddr;
use sled_agent_types::early_networking::RouterPeerIpAddrError;
use sled_agent_types::early_networking::RouterPeerType;
use slog_error_chain::InlineErrorChain;
use uuid::Uuid;

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
#[diesel(table_name = bgp_config)]
pub struct BgpConfig {
    #[diesel(embed)]
    pub identity: BgpConfigIdentity,
    pub asn: SqlU32,
    pub bgp_announce_set_id: Uuid,
    pub vrf: Option<String>,
    pub shaper: Option<String>,
    pub checker: Option<String>,
    pub max_paths: SqlU8,
}

impl TryFrom<BgpConfig> for networking::BgpConfig {
    type Error = Error;

    fn try_from(value: BgpConfig) -> Result<Self, Self::Error> {
        let max_paths =
            MaxPathConfig::new(*value.max_paths).map_err(|err| {
                Error::internal_error(&format!(
                    "invalid database contents: \
                     could not convert MaxPathConfig: {}",
                    InlineErrorChain::new(&err)
                ))
            })?;
        Ok(Self {
            identity: value.identity(),
            asn: value.asn.into(),
            vrf: value.vrf,
            max_paths,
        })
    }
}

impl BgpConfig {
    pub fn from_config_create(
        c: &networking::BgpConfigCreate,
        bgp_announce_set_id: Uuid,
    ) -> BgpConfig {
        BgpConfig {
            identity: BgpConfigIdentity::new(
                Uuid::new_v4(),
                IdentityMetadataCreateParams {
                    name: c.identity.name.clone(),
                    description: c.identity.description.clone(),
                },
            ),
            asn: c.asn.into(),
            bgp_announce_set_id,
            vrf: c.vrf.as_ref().map(|x| x.to_string()),
            shaper: c.shaper.as_ref().map(|x| x.to_string()),
            checker: c.checker.as_ref().map(|x| x.to_string()),
            max_paths: c.max_paths.as_u8().into(),
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
#[diesel(table_name = bgp_announce_set)]
pub struct BgpAnnounceSet {
    #[diesel(embed)]
    pub identity: BgpAnnounceSetIdentity,
}

impl From<networking::BgpAnnounceSetCreate> for BgpAnnounceSet {
    fn from(x: networking::BgpAnnounceSetCreate) -> BgpAnnounceSet {
        BgpAnnounceSet {
            identity: BgpAnnounceSetIdentity::new(
                Uuid::new_v4(),
                IdentityMetadataCreateParams {
                    name: x.identity.name.clone(),
                    description: x.identity.description.clone(),
                },
            ),
        }
    }
}

impl Into<networking::BgpAnnounceSet> for BgpAnnounceSet {
    fn into(self) -> networking::BgpAnnounceSet {
        networking::BgpAnnounceSet { identity: self.identity() }
    }
}

#[derive(
    Queryable, Insertable, Selectable, Clone, Debug, Serialize, Deserialize,
)]
#[diesel(table_name = bgp_announcement)]
pub struct BgpAnnouncement {
    pub announce_set_id: Uuid,
    pub address_lot_block_id: Uuid,
    pub network: IpNetwork,
}

impl Into<networking::BgpAnnouncement> for BgpAnnouncement {
    fn into(self) -> networking::BgpAnnouncement {
        networking::BgpAnnouncement {
            announce_set_id: self.announce_set_id,
            address_lot_block_id: self.address_lot_block_id,
            network: self.network.into(),
        }
    }
}

#[derive(Queryable, Selectable, Clone, Debug, Serialize, Deserialize)]
#[diesel(table_name = bgp_peer_view)]
pub struct BgpPeerView {
    pub switch_slot: DbSwitchSlot,
    pub port_name: String,
    pub addr: Option<IpNetwork>,
    pub src_addr: Option<IpNetwork>,
    pub asn: SqlU32,
    pub connect_retry: SqlU32,
    pub delay_open: SqlU32,
    pub hold_time: SqlU32,
    pub idle_hold_time: SqlU32,
    pub keepalive: SqlU32,
    pub remote_asn: Option<SqlU32>,
    pub min_ttl: Option<SqlU8>,
    pub md5_auth_key: Option<String>,
    pub multi_exit_discriminator: Option<SqlU32>,
    pub local_pref: Option<SqlU32>,
    pub enforce_first_as: bool,
    pub vlan_id: Option<SqlU16>,
    pub router_lifetime: SqlU16,
}

#[derive(Debug, thiserror::Error)]
pub enum BgpPeerConfigDataError {
    #[error("database contains illegal router lifetime value")]
    RouterLifetime(#[from] RouterLifetimeConfigError),
    #[error("database contains illegal router peer address")]
    Address(#[from] RouterPeerIpAddrError),
}

impl TryFrom<BgpPeerView> for BgpPeerConfig {
    type Error = BgpPeerConfigDataError;

    fn try_from(value: BgpPeerView) -> Result<Self, Self::Error> {
        // Exhaustive destructuring ensures the compiler rejects any new field
        // added to BgpPeerView without an explicit decision here.
        let BgpPeerView {
            switch_slot: _, // DB join key; not propagated to BgpPeerConfig
            port_name,
            addr,
            src_addr,
            asn,
            connect_retry,
            delay_open,
            hold_time,
            idle_hold_time,
            keepalive,
            remote_asn,
            min_ttl,
            md5_auth_key,
            multi_exit_discriminator,
            local_pref,
            enforce_first_as,
            vlan_id,
            router_lifetime,
        } = value;

        // We have a CHECK constraint that ensures this should never fail.
        let router_lifetime_cfg = RouterLifetimeConfig::new(router_lifetime.0)?;

        // Convert weaker database representation IP address back to a
        // strongly-typed `RouterPeerType`, then populate src_addr for numbered
        // peers. We never expect these conversions to fail because we have
        // CHECK constraints / validation at the API boundary.
        let base_addr =
            RouterPeerType::from_db_repr(addr, router_lifetime_cfg)?;
        let peer_addr = match base_addr {
            RouterPeerType::Numbered { ip, src_addr: _ } => {
                let src_addr = src_addr
                    .map(|n| RouterPeerIpAddr::try_from(n.ip()))
                    .transpose()?;
                RouterPeerType::Numbered { ip, src_addr }
            }
            RouterPeerType::Unnumbered { router_lifetime } => {
                RouterPeerType::Unnumbered { router_lifetime }
            }
        };

        Ok(Self {
            asn: *asn,
            port: port_name,
            addr: peer_addr,
            hold_time: Some(hold_time.0.into()),
            idle_hold_time: Some(idle_hold_time.0.into()),
            delay_open: Some(delay_open.0.into()),
            connect_retry: Some(connect_retry.0.into()),
            keepalive: Some(keepalive.0.into()),
            enforce_first_as,
            local_pref: local_pref.map(|x| x.into()),
            md5_auth_key,
            min_ttl: min_ttl.map(|val| val.0),
            multi_exit_discriminator: multi_exit_discriminator
                .map(|x| x.into()),
            remote_asn: remote_asn.map(|x| x.into()),
            communities: Vec::new(),
            allowed_export: ImportExportPolicy::NoFiltering,
            allowed_import: ImportExportPolicy::NoFiltering,
            vlan_id: vlan_id.map(|x| x.0),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DbSwitchSlot;
    use std::net::{IpAddr, Ipv4Addr};

    /// Construct a [`BgpPeerView`] with a numbered peer (all optional fields
    /// set) and assert that [`BgpPeerConfig`] preserves each field, including
    /// the newly-added `src_addr`.
    ///
    /// Using explicit field construction (no `..`) means this test won't
    /// compile if `BgpPeerView` gains a new field without a corresponding
    /// update here.
    #[test]
    fn bgp_peer_view_conversion_preserves_all_fields() {
        let peer_ip: IpAddr = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));
        let src_ip: IpAddr = IpAddr::V4(Ipv4Addr::new(198, 51, 100, 1));

        let view = BgpPeerView {
            switch_slot: DbSwitchSlot::Switch0,
            port_name: "phy0".to_string(),
            addr: Some(IpNetwork::from(peer_ip)),
            src_addr: Some(IpNetwork::from(src_ip)),
            asn: SqlU32(65001),
            connect_retry: SqlU32(3),
            delay_open: SqlU32(1),
            hold_time: SqlU32(90),
            idle_hold_time: SqlU32(15),
            keepalive: SqlU32(30),
            remote_asn: Some(SqlU32(65002)),
            min_ttl: Some(SqlU8(1)),
            md5_auth_key: Some("secret".to_string()),
            multi_exit_discriminator: Some(SqlU32(100)),
            local_pref: Some(SqlU32(200)),
            enforce_first_as: true,
            vlan_id: Some(SqlU16(42)),
            // router_lifetime is ignored for numbered peers but must be valid.
            router_lifetime: SqlU16(0),
        };

        let config = BgpPeerConfig::try_from(view).expect("conversion failed");

        assert_eq!(config.asn, 65001);
        assert_eq!(config.port, "phy0");
        assert_eq!(
            config.addr,
            RouterPeerType::Numbered {
                ip: RouterPeerIpAddr::try_from(peer_ip).unwrap(),
                src_addr: Some(RouterPeerIpAddr::try_from(src_ip).unwrap()),
            }
        );
        assert_eq!(config.hold_time, Some(90));
        assert_eq!(config.idle_hold_time, Some(15));
        assert_eq!(config.delay_open, Some(1));
        assert_eq!(config.connect_retry, Some(3));
        assert_eq!(config.keepalive, Some(30));
        assert_eq!(config.remote_asn, Some(65002));
        assert_eq!(config.min_ttl, Some(1));
        assert_eq!(config.md5_auth_key.as_deref(), Some("secret"));
        assert_eq!(config.multi_exit_discriminator, Some(100));
        assert_eq!(config.local_pref, Some(200));
        assert!(config.enforce_first_as);
        assert_eq!(config.vlan_id, Some(42));
    }

    /// Verify that an unnumbered peer round-trips through the conversion,
    /// propagating `router_lifetime` correctly.
    #[test]
    fn bgp_peer_view_conversion_unnumbered() {
        let view = BgpPeerView {
            switch_slot: DbSwitchSlot::Switch1,
            port_name: "phy1".to_string(),
            addr: None,     // None → unnumbered
            src_addr: None, // src_addr is only for numbered peers
            asn: SqlU32(65000),
            connect_retry: SqlU32(3),
            delay_open: SqlU32(0),
            hold_time: SqlU32(6),
            idle_hold_time: SqlU32(6),
            keepalive: SqlU32(2),
            remote_asn: None,
            min_ttl: None,
            md5_auth_key: None,
            multi_exit_discriminator: None,
            local_pref: None,
            enforce_first_as: false,
            vlan_id: None,
            router_lifetime: SqlU16(1800),
        };

        let config = BgpPeerConfig::try_from(view).expect("conversion failed");

        assert_eq!(config.asn, 65000);
        assert_eq!(config.port, "phy1");
        assert!(
            matches!(
                config.addr,
                RouterPeerType::Unnumbered { router_lifetime }
                    if router_lifetime.as_u16() == 1800
            ),
            "expected Unnumbered with router_lifetime=1800, got {:?}",
            config.addr
        );
    }
}
