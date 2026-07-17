// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Update (mupdate) types for the commissioning API.
//!
//! The progress type here is a flat, server-computed projection of wicketd's
//! internal update-engine event buffers: callers get a single per-SP state plus
//! a human-readable step summary, rather than the full event-report graph.

use std::collections::BTreeSet;

use iddqd::{IdOrdItem, id_upcast};
use schemars::JsonSchema;
use semver::Version;
use serde::{Deserialize, Serialize};

use crate::v1::inventory::SpIdentifier;

/// A description of the TUF repository currently held by wicketd.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RepositoryDescription {
    /// The system version of the uploaded TUF repository, if one is present.
    pub system_version: Option<Version>,
}

/// Options controlling how an update is performed.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct StartUpdateOptions {
    /// If true, update every targeted component even if it already appears to
    /// be at the desired version, skipping the usual version checks.
    #[serde(default)]
    pub force: bool,
}

/// Parameters for starting an update.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StartUpdateParams {
    /// The service processors to update. Must be non-empty.
    pub targets: BTreeSet<SpIdentifier>,
    /// Options controlling the update.
    pub options: StartUpdateOptions,
}

/// Parameters for clearing update state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ClearUpdateStateParams {
    /// The service processors to clear update state for. Must be non-empty.
    pub targets: BTreeSet<SpIdentifier>,
}

/// The state of an update for a single service processor.
///
/// A service processor appears in the update-progress response only once an
/// update has been started for it; a service processor with no started update
/// is absent from the response entirely.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum SpUpdateProgress {
    /// An update has been started, but no progress has been reported yet.
    Waiting,
    /// The update is in progress.
    InProgress {
        /// The 1-based index of the currently-running top-level step.
        step: u32,
        /// The total number of top-level steps in the update.
        total_steps: u32,
        /// A human-readable description of the innermost running step.
        description: String,
    },
    /// The update ran to completion and at least one step did real work.
    Completed {
        /// Messages from any steps that were skipped or completed with a
        /// warning while the update as a whole succeeded.
        ///
        /// A common example is skipping the RoT bootloader update because the
        /// service processor must be upgraded first. An empty list means the
        /// update completed with no skips or warnings.
        warnings: Vec<String>,
    },
    /// Every step of the update was skipped because there was nothing to do.
    Skipped,
    /// The update failed.
    Failed {
        /// A human-readable description of the failure, combining the failed
        /// step and its error message.
        message: String,
    },
    /// The update was aborted.
    Aborted {
        /// A human-readable description of the abort, combining the aborted
        /// step and its message.
        message: String,
    },
}

/// A single service processor's update progress.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SpUpdateProgressEntry {
    /// The service processor this entry describes.
    pub sp: SpIdentifier,
    /// The update progress for that service processor.
    pub progress: SpUpdateProgress,
}

impl IdOrdItem for SpUpdateProgressEntry {
    type Key<'a> = SpIdentifier;

    fn key(&self) -> Self::Key<'_> {
        self.sp
    }

    id_upcast!();
}
