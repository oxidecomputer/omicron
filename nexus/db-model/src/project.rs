// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::Name;
use crate::schema::project;
use chrono::{DateTime, Utc};
use db_macros::Resource;
use nexus_types::external_api::params;
use nexus_types::external_api::views;
use nexus_types::identity::Resource;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

/// Describes a project within the database.
#[derive(
    Selectable, Queryable, Insertable, Debug, Resource, Serialize, Deserialize,
)]
#[diesel(table_name = project)]
pub struct Project {
    #[diesel(embed)]
    identity: ProjectIdentity,

    pub organization_id: Uuid,
}

impl Project {
    /// Creates a new database Project object.
    pub fn new(organization_id: Uuid, params: params::ProjectCreate) -> Self {
        Self {
            identity: ProjectIdentity::new(Uuid::new_v4(), params.identity),
            organization_id,
        }
    }
}

impl From<Project> for views::Project {
    fn from(project: Project) -> Self {
        Self {
            identity: project.identity(),
            organization_id: project.organization_id,
        }
    }
}

/// Describes a set of updates for the [`Project`] model.
#[derive(AsChangeset)]
#[diesel(table_name = project)]
pub struct ProjectUpdate {
    pub name: Option<Name>,
    pub description: Option<String>,
    pub time_modified: DateTime<Utc>,
}

impl From<params::ProjectUpdate> for ProjectUpdate {
    fn from(params: params::ProjectUpdate) -> Self {
        Self {
            name: params.identity.name.map(Name),
            description: params.identity.description,
            time_modified: Utc::now(),
        }
    }
}
