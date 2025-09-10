// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_db_model as model;
use nexus_types::{
    external_api::{params, shared},
    silo::{
        DEFAULT_SILO_ID, INTERNAL_SILO_ID, default_silo_name,
        internal_silo_name,
    },
};
use omicron_common::api::external::IdentityMetadataCreateParams;
use std::sync::LazyLock;

/// "Default" Silo
///
/// This was historically used for demos and the unit tests.  The plan is to
/// remove it per omicron#2305.
pub static DEFAULT_SILO: LazyLock<model::Silo> = LazyLock::new(|| {
    model::Silo::new_with_id(
        DEFAULT_SILO_ID,
        params::SiloCreate {
            identity: IdentityMetadataCreateParams {
                name: default_silo_name().clone(),
                description: "default silo".to_string(),
            },
            // This quota is actually _unused_ because the default silo
            // isn't constructed in the same way a normal silo would be.
            quotas: params::SiloQuotasCreate::empty(),
            discoverable: false,
            identity_mode: shared::SiloIdentityMode::LocalOnly,
            admin_group_name: None,
            tls_certificates: vec![],
            mapped_fleet_roles: Default::default(),
            network_admin_required: None, // Default silo uses standard permissions
        },
    )
    .unwrap()
});

/// Built-in Silo to house internal resources. It contains no users and
/// can't be logged into.
pub static INTERNAL_SILO: LazyLock<model::Silo> = LazyLock::new(|| {
    model::Silo::new_with_id(
        INTERNAL_SILO_ID,
        params::SiloCreate {
            identity: IdentityMetadataCreateParams {
                name: internal_silo_name().clone(),
                description: "Built-in internal Silo.".to_string(),
            },
            // The internal silo contains no virtual resources, so it has no allotted capacity.
            quotas: params::SiloQuotasCreate::empty(),
            discoverable: false,
            identity_mode: shared::SiloIdentityMode::LocalOnly,
            admin_group_name: None,
            tls_certificates: vec![],
            mapped_fleet_roles: Default::default(),
            network_admin_required: None, // Internal silo uses standard permissions
        },
    )
    .unwrap()
});
