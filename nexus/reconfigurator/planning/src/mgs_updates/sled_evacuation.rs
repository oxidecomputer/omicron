// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Facilities for making decisions about sled evacuation during reboot-inducing
//! updates.

use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintSledUpdateDispositionKind;
use nexus_types::deployment::planning_report::WaitingOnSledEvacuationDetails;
use nexus_types::inventory::Collection;
use omicron_generation_kinds::SledConfigGeneration;
use omicron_uuid_kinds::SledUuid;
use sled_agent_types::inventory::CurrentUpdateDisposition;
use sled_agent_types::inventory::InstanceManagerStatus;
use sled_agent_types::inventory::OmicronSledUpdateDisposition;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use crate::mgs_updates::UpdateableBoard;

pub(super) enum EvacuationStatus {
    Evacuated,
    NeedsEvacuatingUpdateDisposition,
    WaitingOnEvacuation(WaitingOnSledEvacuationDetails),
}

/// Subset of a blueprint containing only the sleds that are currently marked
/// for evacuation (and their current config generation).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EvacuatingSleds {
    evacuating_sleds: BTreeMap<SledUuid, SledConfigGeneration>,
}

impl EvacuatingSleds {
    pub(crate) fn from_blueprint(blueprint: &Blueprint) -> Self {
        Self {
            evacuating_sleds: blueprint
                .active_sled_configs()
                .filter_map(|(sled_id, config)| {
                    match config.update_disposition.kind {
                        BlueprintSledUpdateDispositionKind::Available => None,
                        BlueprintSledUpdateDispositionKind::Evacuating {
                            ..
                        } => Some((sled_id, config.sled_agent_generation)),
                    }
                })
                .collect(),
        }
    }

    pub(super) fn contains(&self, sled_id: &SledUuid) -> bool {
        self.evacuating_sleds.contains_key(sled_id)
    }

    pub(super) fn contains_board(&self, board: &UpdateableBoard) -> bool {
        let Some(sled_id) = board.sled_id() else {
            return false;
        };
        self.contains(&sled_id)
    }

    pub(super) fn evacuation_status(
        &self,
        sled_id: SledUuid,
        inventory: &Collection,
    ) -> EvacuationStatus {
        let Some(&desired_generation) = self.evacuating_sleds.get(&sled_id)
        else {
            return EvacuationStatus::NeedsEvacuatingUpdateDisposition;
        };

        let Some(inventory) = inventory.sled_agents.get(&sled_id) else {
            return EvacuationStatus::WaitingOnEvacuation(
                WaitingOnSledEvacuationDetails::MissingFromInventory,
            );
        };

        // We have a sled that our parent blueprint marked as evacuating, and we
        // have an inventory collection from it. Check whether the sled is fully
        // evacuated:
        //
        // 1. Is the ledgered sled config's generation at least as far as the
        //    parent blueprint? (If it's _greater_ than the parent's blueprint,
        //    then our parent blueprint is out of date, and everything else we
        //    do doesn't matter anyway, except potentially in tests - the
        //    blueprint we emit can never become the target.)
        let Some(sled_config) = inventory.ledgered_sled_config.as_ref() else {
            return EvacuationStatus::WaitingOnEvacuation(
                WaitingOnSledEvacuationDetails::MissingLedgeredSledConfig,
            );
        };
        if sled_config.generation < desired_generation {
            return EvacuationStatus::WaitingOnEvacuation(
                WaitingOnSledEvacuationDetails::WaitingForSledConfigGeneration {
                    desired: desired_generation,
                    current: sled_config.generation,
                },
            );
        }

        // 2. Does the instance manager on the sled report a complete
        //    evacuation? This requires (a) it's acting on the evacuating
        //    state...
        let InstanceManagerStatus { update_disposition, num_registered_vmms } =
            inventory.instance_manager_status;
        match update_disposition {
            CurrentUpdateDisposition::ConfigNotAvailable => {
                EvacuationStatus::WaitingOnEvacuation(
                    WaitingOnSledEvacuationDetails::InstanceManagerNoConfig,
                )
            }
            CurrentUpdateDisposition::Known(
                OmicronSledUpdateDisposition::Available,
            ) => EvacuationStatus::WaitingOnEvacuation(
                WaitingOnSledEvacuationDetails::InstanceManagerAvailable,
            ),
            CurrentUpdateDisposition::Known(
                OmicronSledUpdateDisposition::Evacuating,
            ) => {
                // ...and (b) there are no registered VMMs.
                if let Some(num_registered_vmms) =
                    NonZeroUsize::new(num_registered_vmms)
                {
                    EvacuationStatus::WaitingOnEvacuation(
                        WaitingOnSledEvacuationDetails::InstanceManagerRegisteredVmms {
                            num_registered_vmms,
                        }
                    )
                } else {
                    EvacuationStatus::Evacuated
                }
            }
        }
    }
}

/// Describes a set of blueprint changes to the desired update dispositions
/// for a number of sleds
///
/// This is generated by the planning process whenever it also generates updates
/// that require sled reboots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingUpdateDispositionChanges {
    by_sled: BTreeMap<SledUuid, BlueprintSledUpdateDispositionKind>,
}

impl PendingUpdateDispositionChanges {
    pub(super) fn empty() -> Self {
        Self { by_sled: BTreeMap::new() }
    }

    pub(super) fn insert(
        &mut self,
        sled_id: SledUuid,
        kind: BlueprintSledUpdateDispositionKind,
    ) {
        let previous = self.by_sled.insert(sled_id, kind);
        assert_eq!(
            previous, None,
            "recorded multiple changes for sled {sled_id}"
        );
    }

    pub(crate) fn iter(
        &self,
    ) -> impl Iterator<Item = (&SledUuid, &BlueprintSledUpdateDispositionKind)>
    {
        self.by_sled.iter()
    }

    pub(crate) fn into_map(
        self,
    ) -> BTreeMap<SledUuid, BlueprintSledUpdateDispositionKind> {
        self.by_sled
    }
}
