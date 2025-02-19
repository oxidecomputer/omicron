// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// XXX-dap-last-step delete me

use crate::apis::{ApiBoundary, Versions};
use anyhow::Result;
use dropshot::{ApiDescription, ApiDescriptionBuildErrors, StubContext};
use openapi_manager_types::ValidationContext;
use openapiv3::OpenAPI;

/// All APIs managed by openapi-manager.
// TODO The metadata here overlaps with metadata in api-manifest.toml.
pub fn all_apis() -> Vec<ApiSpec> {
    vec![
        ApiSpec {
            title: "Bootstrap Agent API",
            versions: Versions::Lockstep { version: semver::Version::new(0,0,1) },
            description: "Per-sled API for setup and teardown",
            boundary: ApiBoundary::Internal,
            api_description:
                bootstrap_agent_api::bootstrap_agent_api_mod::stub_api_description,
            file_stem: "bootstrap-agent",
            extra_validation: None,
        },
        ApiSpec {
            title: "ClickHouse Cluster Admin Keeper API",
            versions: Versions::Lockstep { version: semver::Version::new(0,0,1) },
            description: "API for interacting with the Oxide \
                control plane's ClickHouse cluster keepers",
            boundary: ApiBoundary::Internal,
            api_description:
                clickhouse_admin_api::clickhouse_admin_keeper_api_mod::stub_api_description,
            file_stem: "clickhouse-admin-keeper",
            extra_validation: None,
        },
        ApiSpec {
            title: "ClickHouse Cluster Admin Server API",
            versions: Versions::Lockstep { version: semver::Version::new(0,0,1) },
            description: "API for interacting with the Oxide \
                control plane's ClickHouse cluster replica servers",
            boundary: ApiBoundary::Internal,
            api_description:
                clickhouse_admin_api::clickhouse_admin_server_api_mod::stub_api_description,
            file_stem: "clickhouse-admin-server",
            extra_validation: None,
        },
        ApiSpec {
            title: "ClickHouse Single-Node Admin Server API",
            versions: Versions::Lockstep { version: semver::Version::new(0,0,1) },
            description: "API for interacting with the Oxide \
                control plane's single-node ClickHouse database",
            boundary: ApiBoundary::Internal,
            api_description:
                clickhouse_admin_api::clickhouse_admin_single_api_mod::stub_api_description,
            file_stem: "clickhouse-admin-single",
            extra_validation: None,
        },
        ApiSpec {
            title: "CockroachDB Cluster Admin API",
            versions: Versions::Lockstep { version: semver::Version::new(0,0,1) },
            description: "API for interacting with the Oxide \
                control plane's CockroachDB cluster",
            boundary: ApiBoundary::Internal,
            api_description:
                cockroach_admin_api::cockroach_admin_api_mod::stub_api_description,
            file_stem: "cockroach-admin",
            extra_validation: None,
        },
        ApiSpec {
            title: "Oxide Management Gateway Service API",
            versions: Versions::Lockstep { version: semver::Version::new(0,0,1) },
            description: "API for interacting with the Oxide \
                control plane's gateway service",
            boundary: ApiBoundary::Internal,
            api_description:
                gateway_api::gateway_api_mod::stub_api_description,
            file_stem: "gateway",
            extra_validation: None,
        },
        ApiSpec {
            title: "Internal DNS",
            versions: Versions::new_versioned(
                dns_server_api::supported_versions()
            ),
            description: "API for the internal DNS server",
            boundary: ApiBoundary::Internal,
            api_description:
                dns_server_api::dns_server_api_mod::stub_api_description,
            file_stem: "dns-server",
            extra_validation: None,
        },
        ApiSpec {
            title: "Installinator API",
            versions: Versions::Lockstep { version: semver::Version::new(0,0,1) },
            description: "API for installinator to fetch artifacts \
                and report progress",
            boundary: ApiBoundary::Internal,
            api_description:
                installinator_api::installinator_api_mod::stub_api_description,
            file_stem: "installinator",
            extra_validation: None,
        },
        ApiSpec {
            title: "Oxide Region API",
            versions: Versions::Lockstep { version: semver::Version::new(20241204,0,0) },
            description: "API for interacting with the Oxide control plane",
            boundary: ApiBoundary::External,
            api_description:
                nexus_external_api::nexus_external_api_mod::stub_api_description,
            file_stem: "nexus",
            extra_validation: Some(nexus_external_api::validate_api),
        },
        ApiSpec {
            title: "Nexus internal API",
            versions: Versions::Lockstep { version: semver::Version::new(0,0,1) },
            description: "Nexus internal API",
            boundary: ApiBoundary::Internal,
            api_description:
                nexus_internal_api::nexus_internal_api_mod::stub_api_description,
            file_stem: "nexus-internal",
            extra_validation: None,
        },
        ApiSpec {
            title: "Oxide Oximeter API",
            versions: Versions::Lockstep { version: semver::Version::new(0,0,1) },
            description: "API for interacting with oximeter",
            boundary: ApiBoundary::Internal,
            api_description:
                oximeter_api::oximeter_api_mod::stub_api_description,
            file_stem: "oximeter",
            extra_validation: None,
        },
        ApiSpec {
            title: "Oxide TUF Repo Depot API",
            versions: Versions::Lockstep { version: semver::Version::new(0,0,1) },
            description: "API for fetching update artifacts",
            boundary: ApiBoundary::Internal,
            api_description: repo_depot_api::repo_depot_api_mod::stub_api_description,
            file_stem: "repo-depot",
            extra_validation: None,
        },
        ApiSpec {
            title: "Oxide Sled Agent API",
            versions: Versions::Lockstep { version: semver::Version::new(0,0,1) },
            description: "API for interacting with individual sleds",
            boundary: ApiBoundary::Internal,
            api_description:
                sled_agent_api::sled_agent_api_mod::stub_api_description,
            file_stem: "sled-agent",
            extra_validation: None,
        },
        ApiSpec {
            title: "Oxide Technician Port Control Service",
            versions: Versions::Lockstep { version: semver::Version::new(0,0,1) },
            description: "API for use by the technician port TUI: wicket",
            boundary: ApiBoundary::Internal,
            api_description: wicketd_api::wicketd_api_mod::stub_api_description,
            file_stem: "wicketd",
            extra_validation: None,
        },
        // Add your APIs here! Please keep this list sorted by filename.
    ]
}

pub struct ApiSpec {
    /// The title.
    pub title: &'static str,

    /// Supported version(s) of this API
    pub(crate) versions: Versions,

    /// The description string.
    pub description: &'static str,

    /// Whether this API is internal or external.
    pub boundary: ApiBoundary,

    /// The API description function, typically a reference to
    /// `stub_api_description`.
    pub api_description:
        fn() -> Result<ApiDescription<StubContext>, ApiDescriptionBuildErrors>,

    /// The spec-specific part of the filename for API descriptions
    pub(crate) file_stem: &'static str,

    /// Extra validation to perform on the OpenAPI spec, if any.
    pub extra_validation: Option<fn(&OpenAPI, ValidationContext<'_>)>,
}
