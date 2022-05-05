// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::db::schema::rack;
use db_macros::Asset;

// NOTE: This object is not currently stored in the database.
//
// However, it likely will be in the future - for the single-rack
// case, however, it is synthesized.
#[derive(Queryable, Insertable, Debug, Clone, Selectable, Asset)]
#[diesel(table_name = rack)]
pub struct Rack {
    #[diesel(embed)]
    pub identity: RackIdentity,

    pub tuf_base_url: Option<String>,
}
