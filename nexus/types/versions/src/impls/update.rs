// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functional code for update types.

use crate::latest::update::OperatorStatus;
use crate::latest::update::SledEvacuationProgress;
use crate::latest::update::UnknownReason;
use crate::latest::update::UpdateWait;
use sled_agent_types_versions::latest::inventory::ZoneKind;
use std::fmt;

impl UpdateWait {
    /// Returns an `UpdateWait::Zones` for zones of the given kinds
    pub fn for_zones(kinds: impl IntoIterator<Item = ZoneKind>) -> Self {
        UpdateWait::Zones {
            kinds: kinds.into_iter().map(|k| format!("{k:?}")).collect(),
        }
    }
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
                     changes ({service})",
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
                    let kinds =
                        kinds.iter().cloned().collect::<Vec<_>>().join(", ");
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

impl fmt::Display for SledEvacuationProgress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SledEvacuationProgress::Starting => {
                write!(f, "marking sled for evacuation")
            }
            SledEvacuationProgress::RegisteredVmms { count } => {
                let s = if count.get() == 1 { "" } else { "s" };
                write!(f, "{count} VMM{s} remaining")
            }
            SledEvacuationProgress::Other { message } => {
                write!(f, "{message}")
            }
        }
    }
}
