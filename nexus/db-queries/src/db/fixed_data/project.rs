// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::db;
use crate::db::fixed_data::silo;
use lazy_static::lazy_static;
use nexus_types::external_api::params;
use omicron_common::api::external::IdentityMetadataCreateParams;

/// The name of the built-in project for Oxide services.
pub const SERVICE_PROJECT_NAME: &str = "oxide-services";

lazy_static! {
    /// The ID of the built-in project for Oxide services.
    pub static ref SERVICE_PROJECT_ID: uuid::Uuid = "001de000-4301-4000-8000-000000000000"
        .parse()
        .expect("invalid uuid for builtin service project id");

    /// The built-in project for Oxide services.
    pub static ref SERVICE_PROJECT: db::model::Project = db::model::Project::new_with_id(
        *SERVICE_PROJECT_ID,
        *silo::SILO_ID,
        params::ProjectCreate {
            identity: IdentityMetadataCreateParams {
                name: SERVICE_PROJECT_NAME.parse().unwrap(),
                description: "Project for Oxide Services".to_string(),
            },
        },
    );
}
