// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::schema::rack;
use db_macros::Asset;
use ipnetwork::IpNetwork;
use nexus_types::{external_api::views, identity::Asset};
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
}

impl Rack {
    pub fn new(id: Uuid) -> Self {
        Self {
            identity: RackIdentity::new(id),
            initialized: false,
            tuf_base_url: None,
            rack_subnet: None,
        }
    }
}

impl From<Rack> for views::Rack {
    fn from(rack: Rack) -> Self {
        Self { identity: rack.identity() }
    }
}
