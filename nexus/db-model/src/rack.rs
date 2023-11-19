// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::schema::rack;
use db_macros::Asset;
use ipnetwork::{IpNetwork, Ipv6Network};
use nexus_types::{external_api::views, identity::Asset};
use omicron_common::api;
use uuid::Uuid;

/// Information about a local rack.
#[derive(Queryable, Insertable, Debug, Clone, Selectable, Asset)]
#[diesel(table_name = rack)]
pub struct Rack {
    #[diesel(embed)]
    pub identity: RackIdentity,
    pub initialized: bool,
    pub tuf_base_url: Option<String>,
    pub rack_subnet: Option<IpNetwork>,
    pub reconfiguration_epoch: i64,
}

impl Rack {
    pub fn new(id: Uuid) -> Self {
        Self {
            identity: RackIdentity::new(id),
            initialized: false,
            tuf_base_url: None,
            rack_subnet: None,
            reconfiguration_epoch: 0,
        }
    }

    pub fn subnet(&self) -> Result<Ipv6Network, api::external::Error> {
        match self.rack_subnet {
            Some(IpNetwork::V6(subnet)) => Ok(subnet),
            Some(IpNetwork::V4(_)) => {
                return Err(api::external::Error::InternalError {
                    internal_message: "rack subnet not IPv6".into(),
                })
            }
            None => {
                return Err(api::external::Error::InternalError {
                    internal_message: "rack subnet not set".into(),
                })
            }
        }
    }
}

impl From<Rack> for views::Rack {
    fn from(rack: Rack) -> Self {
        Self { identity: rack.identity() }
    }
}
