// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::schema::{
    bgp_announce_set, bgp_announcement, bgp_config, bgp_peer_view,
};
use crate::SqlU32;
use db_macros::Resource;
use ipnetwork::IpNetwork;
use nexus_types::external_api::params;
use nexus_types::identity::Resource;
use omicron_common::api::external::{self, IdentityMetadataCreateParams};
use serde::{Deserialize, Serialize};
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
}

impl Into<external::BgpConfig> for BgpConfig {
    fn into(self) -> external::BgpConfig {
        external::BgpConfig {
            identity: self.identity(),
            asn: self.asn.into(),
            vrf: self.vrf,
        }
    }
}

impl BgpConfig {
    pub fn from_config_create(
        c: &params::BgpConfigCreate,
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

impl From<params::BgpAnnounceSetCreate> for BgpAnnounceSet {
    fn from(x: params::BgpAnnounceSetCreate) -> BgpAnnounceSet {
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

impl Into<external::BgpAnnounceSet> for BgpAnnounceSet {
    fn into(self) -> external::BgpAnnounceSet {
        external::BgpAnnounceSet { identity: self.identity() }
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

impl Into<external::BgpAnnouncement> for BgpAnnouncement {
    fn into(self) -> external::BgpAnnouncement {
        external::BgpAnnouncement {
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
    pub addr: IpNetwork,
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
    pub vlan_id: Option<SqlU32>,
}
