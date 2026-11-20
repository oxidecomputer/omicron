// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Summary of what the system is doing, intended for operators
//!
//! These types carry more detail than the external API exposes.  The external
//! API includes only the top-level kind of status (see
//! [`crate::external_api::update::OperatorStatus`]) plus the summary and
//! support details produced by [`OperatorStatus::describe()`].

use crate::deployment::MgsUpdateComponent;
use crate::deployment::planning_report::WaitingOnSledEvacuationDetails;
use crate::external_api::update as external_api_update;
use crate::internal_api::views::QuiesceState;
use itertools::Itertools;
use omicron_uuid_kinds::SledUuid;
use sled_agent_types::inventory::ZoneKind;
use sled_hardware_types::BaseboardId;
use std::collections::BTreeSet;

/// What the system is doing, as summarized for operators
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(
    test,
    derive(strum::EnumDiscriminants),
    strum_discriminants(derive(strum::EnumIter))
)]
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

impl OperatorStatus {
    /// Returns a human-readable description of this status
    pub fn describe(&self) -> OperatorStatusDescription {
        match self {
            OperatorStatus::AtRest => {
                OperatorStatusDescription::summary("No changes in progress")
            }
            OperatorStatus::Busy => {
                OperatorStatusDescription::summary("System is busy")
            }
            OperatorStatus::WaitingForMupdateResolution => {
                OperatorStatusDescription::summary(
                    "Waiting for MUPdate resolution",
                )
            }
            OperatorStatus::WaitingForUpdate(UpdateWait::ServiceRecovery {
                service,
            }) => OperatorStatusDescription::summary_with_details(
                "Waiting for internal services to recover after recent changes",
                format!(
                    "Internal service awaiting recovery: {}",
                    service.report_str()
                ),
            ),
            OperatorStatus::WaitingForUpdate(UpdateWait::SledEvacuation {
                sled_id,
                baseboard_id,
                progress,
            }) => describe_sled_evacuation(sled_id, baseboard_id, progress),
            OperatorStatus::WaitingForUpdate(UpdateWait::SledAvailable {
                sled_id,
                baseboard_id,
            }) => OperatorStatusDescription::summary(format!(
                "Waiting for sled to be returned to service after update \
                 (sled {sled_id}, baseboard {baseboard_id})",
            )),
            OperatorStatus::WaitingForUpdate(UpdateWait::Mgs {
                baseboard_id,
                component,
            }) => OperatorStatusDescription::summary(format!(
                "Waiting for update of {component} on {baseboard_id}"
            )),
            OperatorStatus::WaitingForUpdate(UpdateWait::Zones { kinds }) => {
                let summary =
                    "Waiting for a control plane zone update to finish";
                if kinds.is_empty() {
                    OperatorStatusDescription::summary(summary)
                } else {
                    let s = if kinds.len() == 1 { "" } else { "s" };
                    let kinds = kinds.iter().map(|k| k.report_str()).join(", ");
                    OperatorStatusDescription::summary_with_details(
                        summary,
                        format!(
                            "Control plane service{s} being updated: {kinds}"
                        ),
                    )
                }
            }
            OperatorStatus::WaitingForUpdate(UpdateWait::HandoffSagas) => {
                OperatorStatusDescription::summary(
                    "Waiting for in-progress control plane operations \
                     (sagas) to finish before handing off to the new \
                     control plane",
                )
            }
            OperatorStatus::WaitingForUpdate(UpdateWait::HandoffFinal) => {
                OperatorStatusDescription::summary(
                    "Waiting for the new control plane to take over",
                )
            }
            OperatorStatus::Unknown(UnknownReason::PlannerNotYetRun) => {
                OperatorStatusDescription::summary(
                    "Unknown: the system is determining next steps",
                )
            }
            OperatorStatus::Unknown(UnknownReason::PlannerFailed { error }) => {
                OperatorStatusDescription::summary_with_details(
                    "Unknown: planner error",
                    format!("planner error: {error}"),
                )
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

/// Textual description of [`OperatorStatus`]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperatorStatusDescription {
    /// describes what the system is doing or waiting for, in terms that a
    /// customer would be expected to know
    pub summary: String,

    /// additional details that refer to system internals that a customer is not
    /// necessarily expected to know about
    ///
    /// This is aimed at Oxide support, engineering, testers, etc. as well as
    /// sophisticated customers.  It's separated out from `summary` so that we
    /// can make it available without front-loading it to people for whom it's
    /// not actionable.
    pub support_details: Option<String>,
}

impl OperatorStatusDescription {
    /// Returns a description with the given summary and no support details
    fn summary(summary: impl Into<String>) -> Self {
        OperatorStatusDescription {
            summary: summary.into(),
            support_details: None,
        }
    }

    /// Returns a description with the given summary and support details
    fn summary_with_details(
        summary: impl Into<String>,
        support_details: impl Into<String>,
    ) -> Self {
        OperatorStatusDescription {
            summary: summary.into(),
            support_details: Some(support_details.into()),
        }
    }
}

/// Returns the description of a status waiting for a sled to be evacuated
fn describe_sled_evacuation(
    sled_id: &SledUuid,
    baseboard_id: &BaseboardId,
    progress: &SledEvacuationProgress,
) -> OperatorStatusDescription {
    use WaitingOnSledEvacuationDetails as Details;

    let summary = format!(
        "Evacuating instances from sled in preparation for update \
         (sled {sled_id}, baseboard {baseboard_id})"
    );
    match progress {
        SledEvacuationProgress::Starting => {
            OperatorStatusDescription::summary_with_details(
                summary,
                "marking sled for evacuation",
            )
        }
        SledEvacuationProgress::Waiting(
            Details::InstanceManagerRegisteredVmms { num_registered_vmms },
        ) => {
            let s = if num_registered_vmms.get() == 1 { "" } else { "s" };
            OperatorStatusDescription::summary(format!(
                "{summary}: {num_registered_vmms} instance{s} remaining"
            ))
        }
        SledEvacuationProgress::Waiting(
            details @ (Details::MissingFromInventory
            | Details::MissingLedgeredSledConfig
            | Details::WaitingForSledConfigGeneration { .. }
            | Details::InstanceManagerNoConfig
            | Details::InstanceManagerAvailable),
        ) => OperatorStatusDescription::summary_with_details(
            summary,
            details.to_string(),
        ),
    }
}

/// What the caller knows about whether Nexus is quiescing for handoff
///
/// Operator notes can only report that the system is waiting for handoff when
/// the caller knows Nexus's quiesce state.  Code that prints a planning report
/// by itself (e.g., from a saved blueprint) does not know it.
#[derive(Clone, Copy, Debug)]
pub enum QuiesceInfo<'a> {
    /// the caller knows Nexus's current quiesce state
    Known(&'a QuiesceState),
    /// the caller does not know whether Nexus is quiescing
    Unknown,
}

/// Why the current status could not be determined
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(
    test,
    derive(strum::EnumDiscriminants),
    strum_discriminants(derive(strum::EnumIter))
)]
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
#[cfg_attr(
    test,
    derive(strum::EnumDiscriminants),
    strum_discriminants(derive(strum::EnumIter))
)]
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
#[cfg_attr(
    test,
    derive(strum::EnumDiscriminants),
    strum_discriminants(derive(strum::EnumIter))
)]
pub enum SledEvacuationProgress {
    /// The planner is marking the sled for evacuation in this blueprint.
    ///
    /// The planner has not yet checked on the evacuation's progress, so it
    /// doesn't know how many VMMs are on the sled.
    Starting,
    /// The planner is waiting for the sled's evacuation to finish.
    Waiting(WaitingOnSledEvacuationDetails),
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::deployment::planning_report::WaitingOnSledEvacuationDetailsDiscriminants;
    use expectorate::assert_contents;
    use omicron_generation_kinds::SledConfigGeneration;
    use omicron_uuid_kinds::GenericUuid;
    use std::fmt::Write;
    use std::num::NonZeroUsize;
    use strum::IntoEnumIterator;
    use uuid::Uuid;

    // Prints an example of every kind of operator status.  This is useful to
    // have at a glance in the repo and also helps us notice when we're changing
    // user-facing messages.
    #[test]
    fn operator_status_messages() {
        let mut output = String::new();
        for status in OperatorStatusDiscriminants::iter()
            .flat_map(operator_status_examples)
        {
            let OperatorStatusDescription { summary, support_details } =
                status.describe();
            writeln!(output, "{status:?}").unwrap();
            writeln!(output, "    summary: {summary}").unwrap();
            if let Some(support_details) = support_details {
                writeln!(output, "    support details: {support_details}")
                    .unwrap();
            }
            writeln!(output).unwrap();
        }
        assert_contents("output/operator_status_messages.out", &output);
    }

    fn operator_status_examples(
        kind: OperatorStatusDiscriminants,
    ) -> Vec<OperatorStatus> {
        match kind {
            OperatorStatusDiscriminants::AtRest => vec![OperatorStatus::AtRest],
            OperatorStatusDiscriminants::Busy => vec![OperatorStatus::Busy],
            OperatorStatusDiscriminants::WaitingForMupdateResolution => {
                vec![OperatorStatus::WaitingForMupdateResolution]
            }
            OperatorStatusDiscriminants::WaitingForUpdate => {
                UpdateWaitDiscriminants::iter()
                    .flat_map(update_wait_examples)
                    .map(OperatorStatus::WaitingForUpdate)
                    .collect()
            }
            OperatorStatusDiscriminants::Unknown => {
                UnknownReasonDiscriminants::iter()
                    .map(unknown_reason_example)
                    .map(OperatorStatus::Unknown)
                    .collect()
            }
        }
    }

    fn unknown_reason_example(
        kind: UnknownReasonDiscriminants,
    ) -> UnknownReason {
        match kind {
            UnknownReasonDiscriminants::PlannerNotYetRun => {
                UnknownReason::PlannerNotYetRun
            }
            UnknownReasonDiscriminants::PlannerFailed => {
                UnknownReason::PlannerFailed {
                    error: String::from("blueprint planning is disabled"),
                }
            }
        }
    }

    fn update_wait_examples(kind: UpdateWaitDiscriminants) -> Vec<UpdateWait> {
        match kind {
            UpdateWaitDiscriminants::ServiceRecovery => {
                vec![UpdateWait::ServiceRecovery {
                    service: ZoneKind::InternalDns,
                }]
            }
            UpdateWaitDiscriminants::SledEvacuation => {
                SledEvacuationProgressDiscriminants::iter()
                    .flat_map(sled_evacuation_progress_examples)
                    .map(|progress| UpdateWait::SledEvacuation {
                        sled_id: example_sled_id(),
                        baseboard_id: example_baseboard_id(),
                        progress,
                    })
                    .collect()
            }
            UpdateWaitDiscriminants::SledAvailable => {
                vec![UpdateWait::SledAvailable {
                    sled_id: example_sled_id(),
                    baseboard_id: example_baseboard_id(),
                }]
            }
            UpdateWaitDiscriminants::Mgs => vec![UpdateWait::Mgs {
                baseboard_id: example_baseboard_id(),
                component: MgsUpdateComponent::RotBootloader,
            }],
            UpdateWaitDiscriminants::Zones => vec![
                UpdateWait::for_zones([]),
                UpdateWait::for_zones([ZoneKind::Nexus]),
                UpdateWait::for_zones([ZoneKind::Crucible, ZoneKind::Nexus]),
            ],
            UpdateWaitDiscriminants::HandoffSagas => {
                vec![UpdateWait::HandoffSagas]
            }
            UpdateWaitDiscriminants::HandoffFinal => {
                vec![UpdateWait::HandoffFinal]
            }
        }
    }

    fn sled_evacuation_progress_examples(
        kind: SledEvacuationProgressDiscriminants,
    ) -> Vec<SledEvacuationProgress> {
        match kind {
            SledEvacuationProgressDiscriminants::Starting => {
                vec![SledEvacuationProgress::Starting]
            }
            SledEvacuationProgressDiscriminants::Waiting => {
                WaitingOnSledEvacuationDetailsDiscriminants::iter()
                    .flat_map(waiting_on_sled_evacuation_examples)
                    .map(SledEvacuationProgress::Waiting)
                    .collect()
            }
        }
    }

    fn waiting_on_sled_evacuation_examples(
        kind: WaitingOnSledEvacuationDetailsDiscriminants,
    ) -> Vec<WaitingOnSledEvacuationDetails> {
        use WaitingOnSledEvacuationDetailsDiscriminants as Kind;
        match kind {
            Kind::MissingFromInventory => {
                vec![WaitingOnSledEvacuationDetails::MissingFromInventory]
            }
            Kind::MissingLedgeredSledConfig => {
                vec![WaitingOnSledEvacuationDetails::MissingLedgeredSledConfig]
            }
            Kind::WaitingForSledConfigGeneration => vec![
                WaitingOnSledEvacuationDetails::WaitingForSledConfigGeneration {
                    desired: SledConfigGeneration::from_u32(3),
                    current: SledConfigGeneration::from_u32(2),
                },
            ],
            Kind::InstanceManagerNoConfig => {
                vec![WaitingOnSledEvacuationDetails::InstanceManagerNoConfig]
            }
            Kind::InstanceManagerAvailable => {
                vec![WaitingOnSledEvacuationDetails::InstanceManagerAvailable]
            }
            // Show both the singular and the plural forms.
            Kind::InstanceManagerRegisteredVmms => [1, 3]
                .into_iter()
                .filter_map(NonZeroUsize::new)
                .map(|num_registered_vmms| {
                    WaitingOnSledEvacuationDetails::InstanceManagerRegisteredVmms {
                        num_registered_vmms,
                    }
                })
                .collect(),
        }
    }

    fn example_sled_id() -> SledUuid {
        SledUuid::from_untyped_uuid(Uuid::from_u128(1))
    }

    fn example_baseboard_id() -> BaseboardId {
        BaseboardId {
            part_number: String::from("913-0000019"),
            serial_number: String::from("serial0"),
        }
    }
}
