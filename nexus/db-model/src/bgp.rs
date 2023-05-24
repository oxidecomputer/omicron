// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::schema::{bgp_announce_set, bgp_announcement, bgp_config};
use crate::SqlU32;
use db_macros::Resource;
use ipnetwork::IpNetwork;
use nexus_types::identity::Resource;
use omicron_common::api::external;
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
    pub vrf: Option<String>,
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
