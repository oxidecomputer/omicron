// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use db_macros::Asset;
use ipnetwork::IpNetwork;
use nexus_db_schema::schema::rack;
use nexus_types::external_api::rack as rack_types;
use nexus_types::identity::Asset;
use omicron_uuid_kinds::RackUuid;

/// Information about a local rack.
#[derive(Queryable, Insertable, Debug, Clone, Selectable, Asset)]
#[diesel(table_name = rack)]
#[asset(uuid_kind = RackKind)]
pub struct Rack {
    #[diesel(embed)]
    pub identity: RackIdentity,
    pub initialized: bool,
    pub rack_subnet: Option<IpNetwork>,
}

impl Rack {
    pub fn new(id: RackUuid) -> Self {
        Self {
            identity: RackIdentity::new(id),
            initialized: false,
            rack_subnet: None,
        }
    }
}

impl From<Rack> for rack_types::Rack {
    fn from(rack: Rack) -> Self {
        Self { identity: rack.identity() }
    }
}
