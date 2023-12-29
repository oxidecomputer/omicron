// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::db;
use crate::db::datastore::SERVICES_DB_NAME;
use nexus_types::external_api::params;
use omicron_common::api::external::IdentityMetadataCreateParams;
use once_cell::sync::Lazy;

/// UUID of built-in project for internal services on the rack.
pub static SERVICES_PROJECT_ID: Lazy<uuid::Uuid> = Lazy::new(|| {
    "001de000-4401-4000-8000-000000000000"
        .parse()
        .expect("invalid uuid for builtin services project id")
});

/// Built-in Project for internal services on the rack.
pub static SERVICES_PROJECT: Lazy<db::model::Project> = Lazy::new(|| {
    db::model::Project::new_with_id(
        *SERVICES_PROJECT_ID,
        *super::silo::INTERNAL_SILO_ID,
        params::ProjectCreate {
            identity: IdentityMetadataCreateParams {
                name: SERVICES_DB_NAME.parse().unwrap(),
                description: "Built-in project for Oxide Services".to_string(),
            },
        },
    )
});
