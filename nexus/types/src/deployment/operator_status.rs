// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Summary of what the system is doing, intended for operators
//!
//! These types carry more detail than the external API exposes.  The external
//! API includes only the top-level kind of status (see
//! [`crate::external_api::update::OperatorStatus`]) plus the message produced
//! by this module's `Display` impls.

use crate::deployment::MgsUpdateComponent;
use crate::deployment::planning_report::WaitingOnSledEvacuationDetails;
use crate::external_api::update as external_api_update;
use itertools::Itertools;
use omicron_uuid_kinds::SledUuid;
use sled_agent_types::inventory::ZoneKind;
use sled_hardware_types::BaseboardId;
use std::collections::BTreeSet;
use std::fmt;

/// What the system is doing, as summarized for operators
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OperatorStatus {
    /// The reconfiguration system is at rest because no changes are required
    AtRest,
    /// The system is busy with reconfigurations that don't have a more specific
    /// description
    Busy,
    /// The system is waiting for resolution after having detected MUPdates
    WaitingForMupdateResolution,
    /// The system is working on an automated software update
    WaitingForUpdate(UpdateWait),
    /// Unable to determine the current status
    Unknown(UnknownReason),
}

impl fmt::Display for OperatorStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OperatorStatus::AtRest => write!(f, "No changes in progress"),
            OperatorStatus::Busy => write!(f, "System is busy"),
            OperatorStatus::WaitingForMupdateResolution => {
                write!(f, "Waiting for operator to resolve MUPdate")
            }
            OperatorStatus::WaitingForUpdate(UpdateWait::ServiceRecovery {
                service,
            }) => {
                write!(
                    f,
                    "Waiting for internal services to recover after recent \
                     changes ({})",
                    service.report_str(),
                )
            }
            OperatorStatus::WaitingForUpdate(UpdateWait::SledEvacuation {
                sled_id,
                baseboard_id,
                progress,
            }) => {
                write!(
                    f,
                    "Waiting for sled to be evacuated in preparation for \
                     update (sled {sled_id}, baseboard {baseboard_id}): \
                     {progress}",
                )
            }
            OperatorStatus::WaitingForUpdate(UpdateWait::SledAvailable {
                sled_id,
                baseboard_id,
            }) => {
                write!(
                    f,
                    "Waiting for sled to be returned to service after \
                     update (sled {sled_id}, baseboard {baseboard_id})",
                )
            }
            OperatorStatus::WaitingForUpdate(UpdateWait::Mgs {
                baseboard_id,
                component,
            }) => {
                write!(f, "Waiting for update of {component} on {baseboard_id}")
            }
            OperatorStatus::WaitingForUpdate(UpdateWait::Zones { kinds }) => {
                if kinds.is_empty() {
                    write!(
                        f,
                        "Waiting for a zone update to finish \
                         (details unavailable)"
                    )
                } else {
                    let kinds = kinds.iter().map(|k| k.report_str()).join(", ");
                    write!(f, "Waiting for a zone update to finish ({kinds})")
                }
            }
            OperatorStatus::WaitingForUpdate(UpdateWait::HandoffSagas) => {
                write!(f, "Waiting for sagas to finish running")
            }
            OperatorStatus::WaitingForUpdate(UpdateWait::HandoffFinal) => {
                write!(f, "Waiting for final update handoff to complete")
            }
            OperatorStatus::Unknown(UnknownReason::PlannerNotYetRun) => {
                write!(f, "Unknown: the system is determining what to do next")
            }
            OperatorStatus::Unknown(UnknownReason::PlannerFailed { error }) => {
                write!(f, "Unknown: planner produced error: {error}")
            }
        }
    }
}

impl From<&OperatorStatus> for external_api_update::OperatorStatus {
    fn from(status: &OperatorStatus) -> Self {
        match status {
            OperatorStatus::AtRest => Self::AtRest,
            OperatorStatus::Busy => Self::Busy,
            OperatorStatus::WaitingForMupdateResolution => {
                Self::WaitingForMupdateResolution
            }
            OperatorStatus::WaitingForUpdate(_) => Self::WaitingForUpdate,
            OperatorStatus::Unknown(_) => Self::Unknown,
        }
    }
}

/// Why the current status could not be determined
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UnknownReason {
    /// The planner has not yet run
    PlannerNotYetRun,
    /// The planner has run, but it has never succeeded
    PlannerFailed {
        /// Error from the most recent planning run
        error: String,
    },
}

/// What an automated software update is waiting for
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UpdateWait {
    /// The system is waiting for internal services to recover before proceeding
    /// with next steps
    ServiceRecovery {
        /// kind of zone providing the service
        service: ZoneKind,
    },
    /// The system is waiting for a sled to be evacuated so it can be updated
    SledEvacuation {
        sled_id: SledUuid,
        baseboard_id: BaseboardId,
        progress: SledEvacuationProgress,
    },
    /// The system is waiting for a sled to be put back into service
    SledAvailable { sled_id: SledUuid, baseboard_id: BaseboardId },
    /// The system is waiting for a board update to finish
    Mgs { baseboard_id: BaseboardId, component: MgsUpdateComponent },
    /// The system is waiting for zone updates to finish
    Zones {
        /// Kinds of the zones being updated
        kinds: BTreeSet<ZoneKind>,
    },
    /// The system is waiting for sagas to finish running
    HandoffSagas,
    /// The system is waiting for the final handoff to the new Nexus instances
    HandoffFinal,
}

impl UpdateWait {
    /// Returns an `UpdateWait::Zones` for zones of the given kinds
    pub fn for_zones(kinds: impl IntoIterator<Item = ZoneKind>) -> Self {
        UpdateWait::Zones { kinds: kinds.into_iter().collect() }
    }
}

/// How far along a sled's evacuation is, as far as the planner knows
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SledEvacuationProgress {
    /// The planner is marking the sled for evacuation in this blueprint.
    ///
    /// The planner has not yet checked on the evacuation's progress, so it
    /// doesn't know how many VMMs are on the sled.
    Starting,
    /// The planner is waiting for the sled's evacuation to finish.
    Waiting(WaitingOnSledEvacuationDetails),
}

impl fmt::Display for SledEvacuationProgress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SledEvacuationProgress::Starting => {
                write!(f, "marking sled for evacuation")
            }
            SledEvacuationProgress::Waiting(
                WaitingOnSledEvacuationDetails::InstanceManagerRegisteredVmms {
                    num_registered_vmms,
                },
            ) => {
                let s = if num_registered_vmms.get() == 1 { "" } else { "s" };
                write!(f, "{num_registered_vmms} VMM{s} remaining")
            }
            SledEvacuationProgress::Waiting(
                details @ (WaitingOnSledEvacuationDetails::MissingFromInventory
                | WaitingOnSledEvacuationDetails::MissingLedgeredSledConfig
                | WaitingOnSledEvacuationDetails::WaitingForSledConfigGeneration {
                    ..
                }
                | WaitingOnSledEvacuationDetails::InstanceManagerNoConfig
                | WaitingOnSledEvacuationDetails::InstanceManagerAvailable),
            ) => write!(f, "{details}"),
        }
    }
}
// <review>
// I wonder if it's worth creating an expectorate test that constructs every possible variant here and prints out the status so that we can see what each looks like?  It could print the debug version and then the to_string() version.  What do you think?
// </review>
