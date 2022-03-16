// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{Generation, Ipv6Net, Name, VpcFirewallRule};
use crate::db::collection_insert::DatastoreCollection;
use crate::db::model::Vni;
use crate::db::schema::{vpc, vpc_firewall_rule};
use crate::defaults;
use crate::external_api::params;
use chrono::{DateTime, Utc};
use db_macros::Resource;
use omicron_common::api::external;
use uuid::Uuid;

#[derive(Queryable, Insertable, Clone, Debug, Selectable, Resource)]
#[diesel(table_name = vpc)]
pub struct Vpc {
    #[diesel(embed)]
    identity: VpcIdentity,

    pub project_id: Uuid,
    pub system_router_id: Uuid,
    pub vni: Vni,
    pub ipv6_prefix: Ipv6Net,
    pub dns_name: Name,

    /// firewall generation number, used as a child resource generation number
    /// per RFD 192
    pub firewall_gen: Generation,
}

impl Vpc {
    pub fn new(
        vpc_id: Uuid,
        project_id: Uuid,
        system_router_id: Uuid,
        params: params::VpcCreate,
    ) -> Result<Self, external::Error> {
        let identity = VpcIdentity::new(vpc_id, params.identity);
        let ipv6_prefix = match params.ipv6_prefix {
            None => defaults::random_vpc_ipv6_prefix(),
            Some(prefix) => {
                if prefix.is_vpc_prefix() {
                    Ok(prefix)
                } else {
                    Err(external::Error::invalid_request(
                        "VPC IPv6 address prefixes must be in the 
                            Unique Local Address range `fd00::/48` (RFD 4193)",
                    ))
                }
            }
        }?
        .into();
        Ok(Self {
            identity,
            project_id,
            system_router_id,
            vni: Vni(external::Vni::random()),
            ipv6_prefix,
            dns_name: params.dns_name.into(),
            firewall_gen: Generation::new(),
        })
    }
}

impl DatastoreCollection<VpcFirewallRule> for Vpc {
    type CollectionId = Uuid;
    type GenerationNumberColumn = vpc::dsl::firewall_gen;
    type CollectionTimeDeletedColumn = vpc::dsl::time_deleted;
    type CollectionIdColumn = vpc_firewall_rule::dsl::vpc_id;
}

#[derive(AsChangeset)]
#[diesel(table_name = vpc)]
pub struct VpcUpdate {
    pub name: Option<Name>,
    pub description: Option<String>,
    pub time_modified: DateTime<Utc>,
    pub dns_name: Option<Name>,
}

impl From<params::VpcUpdate> for VpcUpdate {
    fn from(params: params::VpcUpdate) -> Self {
        Self {
            name: params.identity.name.map(Name),
            description: params.identity.description,
            time_modified: Utc::now(),
            dns_name: params.dns_name.map(Name),
        }
    }
}
