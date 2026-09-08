// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::typed_uuid::DbTypedUuid;
use crate::{Name, SqlU8, SqlU32};
use chrono::{DateTime, Utc};
use db_macros::Resource;
use ipnetwork::IpNetwork;
use nexus_db_schema::schema::{bgp_announce_set, bgp_announcement, bgp_config};
use nexus_types::external_api::networking;
use nexus_types::identity::Resource;
use omicron_common::api::external::Error;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_uuid_kinds::{
    BgpAnnounceSetKind, BgpAnnounceSetUuid, BgpConfigUuid, GenericUuid,
};
use serde::{Deserialize, Serialize};
use sled_agent_types::early_networking::MaxPathConfig;
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
#[resource(uuid_kind = BgpConfigKind)]
#[diesel(table_name = bgp_config)]
pub struct BgpConfig {
    #[diesel(embed)]
    pub identity: BgpConfigIdentity,
    pub asn: SqlU32,
    pub bgp_announce_set_id: DbTypedUuid<BgpAnnounceSetKind>,
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
        bgp_announce_set_id: BgpAnnounceSetUuid,
    ) -> BgpConfig {
        BgpConfig {
            identity: BgpConfigIdentity::new(
                BgpConfigUuid::new_v4(),
                IdentityMetadataCreateParams {
                    name: c.identity.name.clone(),
                    description: c.identity.description.clone(),
                },
            ),
            asn: c.asn.into(),
            bgp_announce_set_id: bgp_announce_set_id.into(),
            vrf: c.vrf.as_ref().map(|x| x.to_string()),
            shaper: c.shaper.as_ref().map(|x| x.to_string()),
            checker: c.checker.as_ref().map(|x| x.to_string()),
            max_paths: c.max_paths.as_u8().into(),
        }
    }

    pub fn bgp_announce_set_id(&self) -> BgpAnnounceSetUuid {
        self.bgp_announce_set_id.into()
    }
}

#[derive(AsChangeset, Clone, Debug)]
#[diesel(table_name = bgp_config)]
pub struct BgpConfigUpdate {
    pub name: Option<Name>,
    pub description: Option<String>,
    pub time_modified: DateTime<Utc>,
    pub bgp_announce_set_id: Uuid,
    pub max_paths: Option<SqlU8>,
}

impl BgpConfigUpdate {
    pub fn new(
        bgp_update: networking::BgpConfigUpdate,
        bgp_announce_set_id: Uuid,
    ) -> Self {
        Self {
            name: bgp_update.identity.name.map(Into::into),
            description: bgp_update.identity.description,
            time_modified: Utc::now(),
            bgp_announce_set_id,
            max_paths: bgp_update.max_paths.map(|x| x.as_u8().into()),
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
#[resource(uuid_kind = BgpAnnounceSetKind)]
#[diesel(table_name = bgp_announce_set)]
pub struct BgpAnnounceSet {
    #[diesel(embed)]
    pub identity: BgpAnnounceSetIdentity,
}

impl From<networking::BgpAnnounceSetCreate> for BgpAnnounceSet {
    fn from(x: networking::BgpAnnounceSetCreate) -> BgpAnnounceSet {
        BgpAnnounceSet {
            identity: BgpAnnounceSetIdentity::new(
                BgpAnnounceSetUuid::new_v4(),
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
    pub announce_set_id: DbTypedUuid<BgpAnnounceSetKind>,
    pub address_lot_block_id: Uuid,
    pub network: IpNetwork,
}

impl BgpAnnouncement {
    pub fn announce_set_id(&self) -> BgpAnnounceSetUuid {
        self.announce_set_id.into()
    }
}

impl Into<networking::BgpAnnouncement> for BgpAnnouncement {
    fn into(self) -> networking::BgpAnnouncement {
        networking::BgpAnnouncement {
            announce_set_id: self.announce_set_id.into_untyped_uuid(),
            address_lot_block_id: self.address_lot_block_id,
            network: self.network.into(),
        }
    }
}
