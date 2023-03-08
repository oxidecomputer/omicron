// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{Generation, Name, Project};
use crate::collection::DatastoreCollectionConfig;
use crate::schema::{organization, project};
use chrono::{DateTime, Utc};
use db_macros::Resource;
use nexus_types::external_api::params;
use nexus_types::external_api::views;
use nexus_types::identity::Resource;
use uuid::Uuid;

/// Describes an organization within the database.
#[derive(Queryable, Insertable, Debug, Resource, Selectable)]
#[diesel(table_name = organization)]
pub struct Organization {
    #[diesel(embed)]
    identity: OrganizationIdentity,

    pub silo_id: Uuid,

    /// child resource generation number, per RFD 192
    pub rcgen: Generation,
}

impl Organization {
    /// Creates a new database Organization object.
    pub fn new(params: params::OrganizationCreate, silo_id: Uuid) -> Self {
        Self::with_id(Uuid::new_v4(), params, silo_id)
    }

    pub fn with_id(
        id: Uuid,
        params: params::OrganizationCreate,
        silo_id: Uuid,
    ) -> Self {
        Self {
            identity: OrganizationIdentity::new(id, params.identity),
            silo_id,
            rcgen: Generation::new(),
        }
    }
}

impl From<Organization> for views::Organization {
    fn from(org: Organization) -> Self {
        Self { identity: org.identity() }
    }
}

impl DatastoreCollectionConfig<Project> for Organization {
    type CollectionId = Uuid;
    type GenerationNumberColumn = organization::dsl::rcgen;
    type CollectionTimeDeletedColumn = organization::dsl::time_deleted;
    type CollectionIdColumn = project::dsl::organization_id;
}

/// Describes a set of updates for the [`Organization`] model.
#[derive(AsChangeset)]
#[diesel(table_name = organization)]
pub struct OrganizationUpdate {
    pub name: Option<Name>,
    pub description: Option<String>,
    pub time_modified: DateTime<Utc>,
}

impl From<params::OrganizationUpdate> for OrganizationUpdate {
    fn from(params: params::OrganizationUpdate) -> Self {
        Self {
            name: params.identity.name.map(|n| n.into()),
            description: params.identity.description,
            time_modified: Utc::now(),
        }
    }
}
