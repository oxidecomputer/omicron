// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::db;
use lazy_static::lazy_static;
use nexus_types::external_api::{params, shared};
use omicron_common::api::external::IdentityMetadataCreateParams;

lazy_static! {
    pub static ref SILO_ID: uuid::Uuid = "001de000-5110-4000-8000-000000000000"
        .parse()
        .expect("invalid uuid for builtin silo id");

    /// "Default" Silo
    ///
    /// This was historically used for demos and the unit tests.  The plan is to
    /// remove it per omicron#2305.
    pub static ref DEFAULT_SILO: db::model::Silo =
        db::model::Silo::new_with_id(
            *SILO_ID,
            params::SiloCreate {
                identity: IdentityMetadataCreateParams {
                    name: "default-silo".parse().unwrap(),
                    description: "default silo".to_string(),
                },
                // TODO: Should the default silo have a quota? If so, what should the defaults be?
                quotas: params::SiloQuotasCreate::empty(),
                discoverable: false,
                identity_mode: shared::SiloIdentityMode::LocalOnly,
                admin_group_name: None,
                tls_certificates: vec![],
                mapped_fleet_roles: Default::default(),
            },
        )
        .unwrap();

    /// UUID of built-in internal silo.
    pub static ref INTERNAL_SILO_ID: uuid::Uuid =
        "001de000-5110-4000-8000-000000000001"
            .parse()
            .expect("invalid uuid for builtin silo id");

    /// Built-in Silo to house internal resources. It contains no users and
    /// can't be logged into.
    pub static ref INTERNAL_SILO: db::model::Silo =
        db::model::Silo::new_with_id(
            *INTERNAL_SILO_ID,
            params::SiloCreate {
                identity: IdentityMetadataCreateParams {
                    name: "oxide-internal".parse().unwrap(),
                    description: "Built-in internal Silo.".to_string(),
                },
                // The internal silo contains no virtual resources, so it has no allotted capacity.
                quotas: params::SiloQuotasCreate::empty(),
                discoverable: false,
                identity_mode: shared::SiloIdentityMode::LocalOnly,
                admin_group_name: None,
                tls_certificates: vec![],
                mapped_fleet_roles: Default::default(),
            },
        ).unwrap();
}
