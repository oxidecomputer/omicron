// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Collecting load-time errors so we can report many at once.
//!
//! Loading API metadata runs a sequence of independent validation passes (parse
//! the manifest, cross-check it against the Cargo dependency tree, walk
//! deployment units, and so on).  Each pass records its errors into a shared
//! [`ErrorAccumulator`] and keeps going wherever it sensibly can, so a single
//! run can surface many problems at once.  The caller gates between passes that
//! depend on each other -- there's no point validating the dependency graph if
//! the manifest itself didn't parse -- and reports everything collected so far
//! via [`LoadErrors`].
//!
//! Errors are modeled as a structured [`LoadError`] enum, so callers can match
//! on the specific problem and each variant's data is captured explicitly.

use crate::ClientPackageName;
use crate::ServerComponentName;
use crate::ServerPackageName;
use crate::plural;
use camino::Utf8PathBuf;
use indent_write::indentable::Indentable;
use itertools::Itertools;
use omicron_deployment_graph::DeploymentUnitName;
use std::error::Error;
use std::fmt;
use swrite::{SWrite as _, swrite};

/// A single problem encountered while loading API metadata.
///
/// Variants are grouped by the validation pass that produces them.  Passes
/// record these into an `ErrorAccumulator`, so a single run can surface every
/// problem at once.
#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    //
    // Fatal load failures.
    //
    /// The API manifest file could not be read or parsed.
    #[error("reading API manifest {path}")]
    ReadManifest {
        path: Utf8PathBuf,
        #[source]
        source: anyhow::Error,
    },

    /// Cargo workspace metadata could not be loaded.
    #[error("loading Cargo workspace metadata")]
    LoadWorkspaces {
        #[source]
        source: anyhow::Error,
    },

    //
    // Manifest validation (`AllApiMetadata::from_raw`).
    //
    /// The same client package name was listed for more than one API.
    #[error("duplicate client package name in API metadata: {name}")]
    DuplicateClientPackage { name: ClientPackageName },

    /// An embedded component's `inside` does not name a package in the same
    /// unit.
    #[error(
        "embedded component {embedded_component:?} has `inside = {inside:?}`, \
         but no such package exists in deployment unit \
         {deployment_unit:?}'s `packages` list"
    )]
    EmbeddedComponentInsideMissing {
        embedded_component: ServerComponentName,
        inside: ServerComponentName,
        deployment_unit: DeploymentUnitName,
    },

    /// Two deployment units share a name.
    #[error("duplicate deployment unit name in API metadata: {name}")]
    DuplicateDeploymentUnit { name: DeploymentUnitName },

    /// A server component is listed twice within one deployment unit.
    #[error(
        "server component {component:?} appears more than once in deployment \
         unit {deployment_unit}; each component must appear exactly once across \
         `packages` and `embedded_components`"
    )]
    ServerComponentRepeatedInUnit {
        component: ServerComponentName,
        deployment_unit: DeploymentUnitName,
    },

    /// A server component is declared in two different deployment units'
    /// entries.
    #[error(
        "server component {component:?} appears in multiple deployment unit \
         entries ({first} and {second}); each component must appear exactly \
         once across `packages` and `embedded_components`"
    )]
    ServerComponentDeclaredInMultipleUnits {
        component: ServerComponentName,
        first: DeploymentUnitName,
        second: DeploymentUnitName,
    },

    /// A dependency filter rule names a client that isn't a known API.
    #[error("dependency rule references unknown client: {client:?}")]
    UnknownDependencyRuleClient { client: ClientPackageName },

    /// The same entry appears twice in `ignored_non_clients`.
    #[error("entry in ignored_non_clients appeared twice: {client:?}")]
    DuplicateIgnoredNonClient { client: ClientPackageName },

    /// An IDU-only edge names a server component that doesn't exist.
    #[error(
        "intra_deployment_unit_only_edges: unknown server component {server:?}"
    )]
    IduUnknownServerComponent { server: ServerComponentName },

    /// An IDU-only edge names a client that isn't a known API.
    #[error("intra_deployment_unit_only_edges: unknown client {client:?}")]
    IduUnknownClient { client: ClientPackageName },

    //
    // Workspace consistency (`Workspaces::load`).
    //
    /// A workspace exposes a Progenitor client that the manifest doesn't list.
    #[error(
        "workspace {workspace}: found Progenitor-based client package missing \
         from API manifest: {client}"
    )]
    ClientPackageMissingFromManifest {
        workspace: String,
        client: ClientPackageName,
    },

    /// The manifest lists a client package that no workspace exposes.
    #[error("API manifest refers to unknown client package: {client}")]
    UnknownClientPackageInManifest { client: ClientPackageName },

    //
    // Deployment-unit walk (`SystemApis::load`).
    //
    /// Failed to resolve the workspace for an embedded component's `inside`
    /// package.
    #[error(
        "resolving workspace for embedded component {embedded_component:?} \
         via its `inside` package {inside:?}"
    )]
    ResolveEmbeddedComponentWorkspace {
        embedded_component: ServerComponentName,
        inside: ServerComponentName,
        #[source]
        source: anyhow::Error,
    },

    /// An embedded component's package wasn't found in its expected workspace.
    #[error(
        "embedded component {embedded_component:?} not found in its expected \
         workspace\n  \
         expected workspace: {workspace:?} (the workspace of its `inside` \
         package {inside:?})\n  \
         deployment unit: {deployment_unit:?}\n  \
         hint: check `name` for typos, or that the package exists in that workspace"
    )]
    EmbeddedComponentNotInWorkspace {
        embedded_component: ServerComponentName,
        workspace: String,
        inside: ServerComponentName,
        deployment_unit: DeploymentUnitName,
    },

    /// Failed to resolve the workspace for a server component.
    #[error("resolving workspace for server component {component:?}")]
    ResolveServerComponentWorkspace {
        component: ServerComponentName,
        #[source]
        source: anyhow::Error,
    },

    /// Failed to walk the Cargo dependencies of a server component.
    #[error("walking Cargo dependencies of server component {component:?}")]
    WalkDependencies {
        component: ServerComponentName,
        #[source]
        source: anyhow::Error,
    },

    /// An embedded component is declared `inside` a package that doesn't
    /// depend on it, so omitting it from that package's dependency walk did
    /// nothing.
    #[error(
        "embedded component {embedded_component:?} is declared `inside` a \
         package that doesn't depend on it\n  \
         deployment unit: {deployment_unit:?}\n  \
         `inside` package: {inside:?} (no normal or build Cargo dependency on \
         this embedded component)\n  \
         hint: remove the stale `embedded_components` entry, fix its `inside` \
         field, or restore the dependency if it was removed or made dev-only"
    )]
    StaleEmbeddedComponent {
        embedded_component: ServerComponentName,
        deployment_unit: DeploymentUnitName,
        inside: ServerComponentName,
    },

    //
    // API producer/consumer validation (`SystemApis::load`).
    //
    /// An API restricts its consumers to one that isn't a known server
    /// component.
    #[error(
        "api {client} specifies unknown consumer: {consumer} (with expected \
         reason: {reason})"
    )]
    ApiUnknownRestrictedConsumer {
        client: ClientPackageName,
        consumer: ServerComponentName,
        reason: String,
    },

    /// A deployed API has no producer in any deployment unit.
    #[error(
        "found no producer for API with client package name {client:?} in any \
         deployment unit (should have been one that contains server package \
         {server:?})"
    )]
    NoProducerForApi { client: ClientPackageName, server: ServerPackageName },

    /// An API marked as having no deployed producer nonetheless has one.
    #[error(
        "metadata says there should be no deployed producer for API with \
         client package name {client:?}, but found one(s): {}",
        .producers.iter().format(", ")
    )]
    UnexpectedProducerForApi {
        client: ClientPackageName,
        producers: Vec<ServerComponentName>,
    },

    //
    // Intra-deployment-unit-only edge validation (`SystemApis::load`).
    //
    /// An IDU-only edge's server is absent from the tracked server components.
    /// (Validated earlier, so this indicates an internal inconsistency.)
    #[error(
        "internal error: intra_deployment_unit_only specifies server \
         {server:?} that does not exist in server components"
    )]
    IduServerNotTracked { server: ServerComponentName },

    /// An IDU-only edge's client doesn't correspond to a known API.
    /// (Validated earlier, so this indicates an internal inconsistency.)
    #[error(
        "internal error: intra_deployment_unit_only specifies client \
         {client:?} that does not correspond to a known API"
    )]
    IduClientNotTracked { client: ClientPackageName },

    /// An IDU-only edge names a non-deployed client API that has no producer in
    /// any deployment unit, so the edge can never be satisfied.  (A *deployed*
    /// API in this state is reported as [`LoadError::NoProducerForApi`]
    /// instead, so the same misconfiguration is never reported twice.)
    #[error(
        "intra_deployment_unit_only specifies server {server:?} with client \
         {client:?}, but {client:?} has no producer in any deployment unit"
    )]
    IduClientWithoutProducer {
        server: ServerComponentName,
        client: ClientPackageName,
    },

    /// None of an IDU-only edge's client's producers live in the same
    /// deployment unit as its server.
    #[error(
        "intra_deployment_unit_only specifies server {server:?} in deployment \
         unit {deployment_unit:?}, but none of the producers of client \
         {client:?} are in that deployment unit: {}",
        .producers.iter().format(", ")
    )]
    IduProducersNotInServerUnit {
        server: ServerComponentName,
        deployment_unit: DeploymentUnitName,
        client: ClientPackageName,
        producers: Vec<ServerComponentName>,
    },
}

/// Collects [`LoadError`]s encountered while loading system API metadata.
#[derive(Debug, Default)]
pub(crate) struct ErrorAccumulator {
    errors: Vec<LoadError>,
}

impl ErrorAccumulator {
    pub(crate) fn new() -> ErrorAccumulator {
        ErrorAccumulator::default()
    }

    /// Records an error and keep going.
    pub(crate) fn push(&mut self, error: LoadError) {
        self.errors.push(error);
    }

    /// Returns whether any error has been recorded so far.
    pub(crate) fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// If any errors have been recorded, drains them into a [`LoadErrors`].
    /// Otherwise, returns `None`.
    pub(crate) fn take_load_errors(&mut self) -> Option<LoadErrors> {
        if self.errors.is_empty() {
            None
        } else {
            Some(LoadErrors { errors: std::mem::take(&mut self.errors) })
        }
    }
}

/// The aggregate of every [`LoadError`] collected while loading API metadata.
///
/// This is the error type returned by [`crate::SystemApis::load`].  Its
/// `Display` lists each underlying problem on its own line (including the source
/// chain of any wrapped error), rather than collapsing them to just the first.
#[derive(Debug)]
pub struct LoadErrors {
    errors: Vec<LoadError>,
}

impl LoadErrors {
    /// The individual errors that were collected.
    ///
    /// This is always non-empty: a `LoadErrors` is only ever built from at
    /// least one recorded error.
    pub fn errors(&self) -> &[LoadError] {
        &self.errors
    }
}

impl fmt::Display for LoadErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.errors.len();
        writeln!(
            f,
            "found {} {} while loading API metadata:",
            count,
            plural::errors_str(count),
        )?;
        for error in &self.errors {
            // Render the error and its source chain, one cause per line, then
            // indent any continuation lines (chain causes, or a multi-line
            // message) so they align under the bullet's text.
            let mut rendered = error.to_string();
            let mut source = error.source();
            while let Some(cause) = source {
                swrite!(rendered, "\ncaused by: {cause}");
                source = cause.source();
            }
            writeln!(f, "  - {}", rendered.indented_skip_initial("    "))?;
        }
        Ok(())
    }
}

// `LoadErrors` is a list of errors, so it deliberately does not expose a single
// `source`.  Use the Display impl to print out all errors, and `errors()` to
// iterate over them.
impl Error for LoadErrors {}
