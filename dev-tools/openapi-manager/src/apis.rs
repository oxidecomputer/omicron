// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::bail;
use dropshot::{ApiDescription, ApiDescriptionBuildErrors, StubContext};
use itertools::Either;
use openapi_manager_types::ValidationContext;
use openapiv3::OpenAPI;
use std::{collections::BTreeMap, fmt};

/// Describes an API managed by the openapi-manager crate and CLI tool
///
/// This struct exactly matches how we want developers to configure the list of
/// APIs managed by this tool.
pub struct ManagedApiConfig {
    /// The API-specific part of the filename that's used for API descriptions
    ///
    /// This string is sometimes used as an identifier for developers.
    pub ident: &'static str,

    /// how this API is versioned
    pub versions: Versions,

    /// title of the API (goes into OpenAPI spec)
    pub title: &'static str,

    /// human-readable description of the API (goes into OpenAPI spec)
    pub description: &'static str,

    /// whether this API is internal or external
    ///
    /// This affects some of the validation applied to the OpenAPI spec.
    pub boundary: ApiBoundary,

    /// The API description function, typically a reference to
    /// `stub_api_description`
    ///
    /// This is used to generate the OpenAPI spec that matches the current
    /// server implementation.
    pub api_description:
        fn() -> Result<ApiDescription<StubContext>, ApiDescriptionBuildErrors>,

    /// Extra validation to perform on the OpenAPI spec, if any.
    pub extra_validation: Option<fn(&OpenAPI, ValidationContext<'_>)>,
}

// XXX-dap rip this out and convert callers
impl From<crate::spec::ApiSpec> for ManagedApiConfig {
    fn from(value: crate::spec::ApiSpec) -> Self {
        ManagedApiConfig {
            ident: value.file_stem,
            versions: value.versions,
            title: value.title,
            description: value.description,
            boundary: value.boundary,
            api_description: value.api_description,
            extra_validation: value.extra_validation,
        }
    }
}

/// Used internally to describe an API managed by this tool
#[derive(Debug)]
pub struct ManagedApi {
    /// The API-specific part of the filename that's used for API descriptions
    ///
    /// This string is sometimes used as an identifier for developers.
    ident: ApiIdent,

    /// how this API is versioned
    versions: Versions,

    /// title of the API (goes into OpenAPI spec)
    title: &'static str,

    /// human-readable description of the API (goes into OpenAPI spec)
    description: &'static str,

    /// whether this API is internal or external
    ///
    /// This affects some of the validation applied to the OpenAPI spec.
    boundary: ApiBoundary,

    /// The API description function, typically a reference to
    /// `stub_api_description`
    ///
    /// This is used to generate the OpenAPI spec that matches the current
    /// server implementation.
    api_description:
        fn() -> Result<ApiDescription<StubContext>, ApiDescriptionBuildErrors>,

    /// Extra validation to perform on the OpenAPI spec, if any.
    extra_validation: Option<fn(&OpenAPI, ValidationContext<'_>)>,
}

impl From<ManagedApiConfig> for ManagedApi {
    fn from(value: ManagedApiConfig) -> Self {
        ManagedApi {
            ident: ApiIdent::from(value.ident.to_owned()),
            versions: value.versions,
            title: value.title,
            description: value.description,
            boundary: value.boundary,
            api_description: value.api_description,
            extra_validation: value.extra_validation,
        }
    }
}

impl ManagedApi {
    pub fn is_lockstep(&self) -> bool {
        self.versions.is_lockstep()
    }

    pub fn is_versioned(&self) -> bool {
        self.versions.is_versioned()
    }

    pub fn iter_versions(&self) -> impl Iterator<Item = &semver::Version> + '_ {
        self.versions.iter_versions()
    }

    pub fn has_version(&self, version: &semver::Version) -> bool {
        self.iter_versions().any(|v| v == version)
    }
}

/// Describes the Rust-defined configuration for all of the APIs managed by this
/// tool
#[derive(Debug)]
pub struct ManagedApis {
    // XXX-dap use IdMap?
    apis: BTreeMap<ApiIdent, ManagedApi>,
}

impl ManagedApis {
    pub fn all() -> anyhow::Result<ManagedApis> {
        ManagedApis::new(
            crate::spec::all_apis()
                .into_iter()
                .map(ManagedApiConfig::from)
                .map(ManagedApi::from)
                .collect(),
        )
    }

    pub fn new(api_list: Vec<ManagedApi>) -> anyhow::Result<ManagedApis> {
        let mut apis = BTreeMap::new();
        for api in api_list {
            if let Some(old) = apis.insert(api.ident.clone(), api) {
                bail!("API is defined twice: {:?}", &old.ident);
            }
        }

        Ok(ManagedApis { apis })
    }

    pub fn api(&self, ident: &ApiIdent) -> Option<&ManagedApi> {
        self.apis.get(ident)
    }

    pub fn into_map(self) -> BTreeMap<ApiIdent, ManagedApi> {
        self.apis
    }
}

/// Newtype for API identifiers

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct ApiIdent(String);
NewtypeDebug! { () pub struct ApiIdent(String); }
NewtypeDeref! { () pub struct ApiIdent(String); }
NewtypeDerefMut! { () pub struct ApiIdent(String); }
NewtypeDisplay! { () pub struct ApiIdent(String); }
NewtypeFrom! { () pub struct ApiIdent(String); }
// XXX-dap do I need this
// impl std::borrow::Borrow<str> for ApiIdent {
//     fn borrow(&self) -> &str {
//         self.0.as_str()
//     }
// }

/// Whether an API is exposed externally from the Oxide system
///
/// This affects the kind of validation that's done.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApiBoundary {
    Internal,
    External,
}

impl fmt::Display for ApiBoundary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApiBoundary::Internal => write!(f, "internal"),
            ApiBoundary::External => write!(f, "external"),
        }
    }
}

/// Describes how an API is versioned
#[derive(Debug)]
pub enum Versions {
    /// There is only ever one version of this API
    ///
    /// Clients and servers are updated at runtime in lockstep.
    Lockstep { version: semver::Version },

    /// There are multiple supported versions of this API
    ///
    /// Clients and servers may be updated independently of each other.  Other
    /// parts of the system may constrain things so that either clients or
    /// servers are always updated first, but this tool does not assume that.
    Versioned { supported_versions: Vec<semver::Version> },
}

impl Versions {
    /// Constructor for a lockstep API
    pub fn new_lockstep(version: semver::Version) -> Versions {
        Versions::Lockstep { version }
    }

    /// Constructor for a versioned API
    ///
    /// # Panics
    ///
    /// Panics if the list of supported versions is not already sorted.
    // We could choose to sort the supported versions here.  However, this value
    // is generally coming in through a definition in the source code and we
    // want people to write these definitions in sorted order in order to ensure
    // that semantic conflicts result in git conflicts.
    pub fn new_versioned(supported_versions: &[&semver::Version]) -> Versions {
        assert!(!supported_versions.is_empty());
        // XXX-dap uncomment this when
        // https://github.com/oxidecomputer/omicron/pull/7334 lands (update to
        // Rust 1.84).  This stabilized in 1.82.
        // assert!(
        //     supported_versions.is_sorted(),
        //     "array of supported API versions is not sorted"
        // );
        Versions::Versioned {
            supported_versions: supported_versions
                .into_iter()
                .cloned()
                .cloned()
                .collect(),
        }
    }

    /// Returns whether this API is versioned (as opposed to lockstep)
    pub fn is_versioned(&self) -> bool {
        match self {
            Versions::Lockstep { .. } => false,
            Versions::Versioned { .. } => true,
        }
    }

    /// Returns whether this API is lockstep (as opposed to versioned)
    pub fn is_lockstep(&self) -> bool {
        match self {
            Versions::Lockstep { .. } => true,
            Versions::Versioned { .. } => false,
        }
    }

    /// Iterate over the semver versions of an API that are supported
    pub fn iter_versions(&self) -> impl Iterator<Item = &semver::Version> + '_ {
        match self {
            Versions::Lockstep { version } => {
                Either::Left(std::iter::once(version))
            }
            Versions::Versioned { supported_versions } => {
                Either::Right(supported_versions.iter())
            }
        }
    }
}
