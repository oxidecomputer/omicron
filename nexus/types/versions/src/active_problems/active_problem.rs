// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Active Problem types for version `ACTIVE_PROBLEMS`.

use chrono::{DateTime, Utc};
use omicron_uuid_kinds::ActiveProblemUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::v2025_11_20_00::asset::AssetIdentityMetadata;

/// An Active Problem.
///
/// Active Problems describe conditions detected by the fault management
/// subsystem in which some aspect of the system's operation is degraded. This
/// includes both ongoing problems and historical data describing resolved
/// problems.
///
/// The `time_created` field records when the problem was detected, and the
/// `time_modified` field records the time of its most recent revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ActiveProblem {
    #[serde(flatten)]
    pub identity: AssetIdentityMetadata,
    /// The current state of this Active Problem.
    pub state: ActiveProblemState,
    /// The current revision number of this Active Problem.
    ///
    /// The revision number is incremented each time the Active Problem is
    /// updated. Changes to an Active Problem's impact list, description, and
    /// relationships with other Active Problems all increment the revision
    /// number.
    pub revision: u32,
    /// The problem code describing this Active Problem.
    ///
    /// Problem codes are opaque numeric identifiers that indicate the nature of
    /// the issue an Active Problem represents. They may be used to reference
    /// documentation and service instructions for that particular kind of
    /// problem, and to communicate with Oxide support. The problem code may
    /// change if the diagnosis of the problem is refined, which creates a new
    /// revision.
    pub code: u32,
    /// The class of component affected by this problem.
    ///
    /// Problem classes are dot-separated strings, such as
    /// `hardware.power_shelf`, in which each segment is more specific than the
    /// last. The class is determined by the problem code.
    pub class: String,
    /// A brief, human-readable summary of the problem.
    ///
    /// This is short enough to be used as a title. It is intended for display
    /// to humans, and is not a stable identifier. For a machine-consumable
    /// identifier, use the problem code field.
    pub summary: String,
    /// A longer human-readable description of the problem.
    ///
    /// This may change as the problem progresses.
    pub description: String,
    /// Other Active Problems which cause this problem.
    pub causes: Vec<RelatedActiveProblem>,
    /// Other Active Problems caused by this problem.
    pub causing: Vec<RelatedActiveProblem>,
}

/// The state of an Active Problem.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ActiveProblemState {
    /// The condition represented by the Active Problem still exists.
    Active,
    /// The system has determined that the problem no longer exists.
    ///
    /// A resolved Active Problem never becomes active again. If the same
    /// condition is detected again, a new Active Problem with a new ID is
    /// created.
    Resolved {
        /// The time at which the problem was resolved.
        time_resolved: DateTime<Utc>,
    },
    /// The Active Problem was manually dismissed, because it was believed to
    /// have been detected in error.
    Dismissed {
        /// The time at which the problem was dismissed.
        time_dismissed: DateTime<Utc>,
    },
}

/// A reference to an Active Problem related to another Active Problem
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RelatedActiveProblem {
    /// ID of the related Active Problem
    #[schemars(with = "Uuid")]
    pub id: ActiveProblemUuid,
    /// A human-readable summary of why the problems are related.
    pub reason: String,
    /// The revision of the referencing Active Problem in which this
    /// relationship was added.
    pub revision_added: u32,
}

/// Path parameters to look up an Active Problem by its ID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ActiveProblemSelector {
    /// ID of the Active Problem
    #[schemars(with = "Uuid")]
    pub problem_id: ActiveProblemUuid,
}
