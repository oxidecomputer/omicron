// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

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
use slog_error_chain::InlineErrorChain;
use std::net::IpAddr;
use std::net::Ipv6Addr;
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
    pub switch_location: String,
    pub port_name: String,
    pub addr: Option<IpNetwork>,
    pub asn: SqlU32,
    pub connect_retry: SqlU32,
    pub delay_open: SqlU32,
    pub hold_time: SqlU32,
    pub idle_hold_time: SqlU32,
    pub keepalive: SqlU32,
    pub remote_asn: Option<SqlU32>,
    pub min_ttl: Option<SqlU32>,
    pub md5_auth_key: Option<String>,
    pub multi_exit_discriminator: Option<SqlU32>,
    pub local_pref: Option<SqlU32>,
    pub enforce_first_as: bool,
    pub vlan_id: Option<SqlU16>,
    pub router_lifetime: SqlU16,
}

impl TryFrom<BgpPeerView> for BgpPeerConfig {
    type Error = RouterLifetimeConfigError;

    fn try_from(value: BgpPeerView) -> Result<Self, Self::Error> {
        // For unnumbered peers (addr is None), use UNSPECIFIED
        let addr = match value.addr {
            None => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            Some(addr) => addr.ip(),
        };

        // TODO-correctness We should have db constraints to ensure this can't
        // fail.
        let router_lifetime =
            RouterLifetimeConfig::new(value.router_lifetime.0)?;

        Ok(Self {
            asn: *value.asn,
            port: value.port_name,
            addr,
            hold_time: Some(value.hold_time.0.into()),
            idle_hold_time: Some(value.idle_hold_time.0.into()),
            delay_open: Some(value.delay_open.0.into()),
            connect_retry: Some(value.connect_retry.0.into()),
            keepalive: Some(value.keepalive.0.into()),
            enforce_first_as: value.enforce_first_as,
            local_pref: value.local_pref.map(|x| x.into()),
            md5_auth_key: value.md5_auth_key,
            min_ttl: value.min_ttl.map(|x| x.0 as u8), //TODO avoid cast return error
            multi_exit_discriminator: value
                .multi_exit_discriminator
                .map(|x| x.into()),
            remote_asn: value.remote_asn.map(|x| x.into()),
            communities: Vec::new(),
            allowed_export: ImportExportPolicy::NoFiltering,
            allowed_import: ImportExportPolicy::NoFiltering,
            vlan_id: value.vlan_id.map(|x| x.0),
            router_lifetime,
        })
    }
}
