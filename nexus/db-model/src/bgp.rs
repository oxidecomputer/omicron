// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::schema::{bgp_announce_set, bgp_announcement, bgp_config};
use crate::SqlU32;
use db_macros::Resource;
use ipnetwork::IpNetwork;
use nexus_types::identity::Resource;
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

impl Into<nexus_types::external_api::networking::BgpConfig> for BgpConfig {
    fn into(self) -> nexus_types::external_api::networking::BgpConfig {
        nexus_types::external_api::networking::BgpConfig {
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

impl Into<nexus_types::external_api::networking::BgpAnnounceSet>
    for BgpAnnounceSet
{
    fn into(self) -> nexus_types::external_api::networking::BgpAnnounceSet {
        nexus_types::external_api::networking::BgpAnnounceSet {
            identity: self.identity(),
        }
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

impl Into<nexus_types::external_api::networking::BgpAnnouncement>
    for BgpAnnouncement
{
    fn into(self) -> nexus_types::external_api::networking::BgpAnnouncement {
        nexus_types::external_api::networking::BgpAnnouncement {
            announce_set_id: self.announce_set_id,
            address_lot_block_id: self.address_lot_block_id,
            network: self.network.into(),
        }
    }
}
