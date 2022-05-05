// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{Generation, Organization};
use crate::db::collection_insert::DatastoreCollection;
use crate::db::schema::{organization, silo};
use crate::external_api::params;
use db_macros::Resource;
use uuid::Uuid;

/// Describes a silo within the database.
#[derive(Queryable, Insertable, Debug, Resource, Selectable)]
#[diesel(table_name = silo)]
pub struct Silo {
    #[diesel(embed)]
    identity: SiloIdentity,

    pub discoverable: bool,

    /// child resource generation number, per RFD 192
    pub rcgen: Generation,
}

impl Silo {
    /// Creates a new database Silo object.
    pub fn new(params: params::SiloCreate) -> Self {
        Self::new_with_id(Uuid::new_v4(), params)
    }

    pub fn new_with_id(id: Uuid, params: params::SiloCreate) -> Self {
        Self {
            identity: SiloIdentity::new(id, params.identity),
            discoverable: params.discoverable,
            rcgen: Generation::new(),
        }
    }
}

impl DatastoreCollection<Organization> for Silo {
    type CollectionId = Uuid;
    type GenerationNumberColumn = silo::dsl::rcgen;
    type CollectionTimeDeletedColumn = silo::dsl::time_deleted;
    type CollectionIdColumn = organization::dsl::silo_id;
}
