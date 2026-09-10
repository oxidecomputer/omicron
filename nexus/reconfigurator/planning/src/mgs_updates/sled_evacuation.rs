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

#[derive(Debug, PartialEq, Eq)]
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

    #[cfg(test)]
    pub(crate) fn new_for_test(
        sleds: impl IntoIterator<Item = (SledUuid, SledConfigGeneration)>,
    ) -> Self {
        Self { evacuating_sleds: sleds.into_iter().collect() }
    }

    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        Self { evacuating_sleds: BTreeMap::new() }
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

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.by_sled.len()
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.by_sled.is_empty()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mgs_updates::test_helpers::TestBoards;
    use iddqd::IdOrdMap;

    #[test]
    fn test_evacuation_status() {
        let test_boards =
            TestBoards::new("planning_mgs_updates_evacuation_status");
        let sled_0_id = test_boards.sled_id(0).expect("have sled 0");
        let gen1 = SledConfigGeneration::new();
        let gen2 = gen1.next();
        let evacuating_at_gen1 =
            EvacuatingSleds::new_for_test([(sled_0_id, gen1)]);
        let evacuating_at_gen2 =
            EvacuatingSleds::new_for_test([(sled_0_id, gen2)]);

        // A sled that isn't marked for evacuation needs to be.
        let collection = test_boards.collection_builder().build();
        assert_eq!(
            EvacuatingSleds::empty().evacuation_status(sled_0_id, &collection),
            EvacuationStatus::NeedsEvacuatingUpdateDisposition,
        );

        // Sled missing from inventory entirely (e.g., mid-reboot).
        let mut collection = test_boards.collection_builder().build();
        collection.sled_agents = IdOrdMap::new();
        assert_eq!(
            evacuating_at_gen1.evacuation_status(sled_0_id, &collection),
            EvacuationStatus::WaitingOnEvacuation(
                WaitingOnSledEvacuationDetails::MissingFromInventory
            ),
        );

        // Sled present but hasn't ledgered a config yet.
        let mut collection = test_boards.collection_builder().build();
        collection
            .sled_agents
            .get_mut(&sled_0_id)
            .expect("sled 0 in inventory")
            .ledgered_sled_config = None;
        assert_eq!(
            evacuating_at_gen1.evacuation_status(sled_0_id, &collection),
            EvacuationStatus::WaitingOnEvacuation(
                WaitingOnSledEvacuationDetails::MissingLedgeredSledConfig
            ),
        );

        // Ledgered config is behind the generation that marked the sled
        // `Evacuating`.
        let collection =
            test_boards.collection_builder().sled_evacuated(0, gen1).build();
        assert_eq!(
            evacuating_at_gen2.evacuation_status(sled_0_id, &collection),
            EvacuationStatus::WaitingOnEvacuation(
                WaitingOnSledEvacuationDetails::WaitingForSledConfigGeneration {
                    desired: gen2,
                    current: gen1,
                }
            ),
        );

        // Ledgered config is current, but the instance manager hasn't
        // received any config at all (e.g., it's still starting up).
        let collection = test_boards
            .collection_builder()
            .instance_manager_status_exception(
                0,
                InstanceManagerStatus {
                    update_disposition:
                        CurrentUpdateDisposition::ConfigNotAvailable,
                    num_registered_vmms: 0,
                },
            )
            .build();
        assert_eq!(
            evacuating_at_gen1.evacuation_status(sled_0_id, &collection),
            EvacuationStatus::WaitingOnEvacuation(
                WaitingOnSledEvacuationDetails::InstanceManagerNoConfig
            ),
        );

        // Instance manager still reports `Available`.
        let collection = test_boards
            .collection_builder()
            .instance_manager_status_exception(
                0,
                InstanceManagerStatus {
                    update_disposition: CurrentUpdateDisposition::Known(
                        OmicronSledUpdateDisposition::Available,
                    ),
                    num_registered_vmms: 2,
                },
            )
            .build();
        assert_eq!(
            evacuating_at_gen1.evacuation_status(sled_0_id, &collection),
            EvacuationStatus::WaitingOnEvacuation(
                WaitingOnSledEvacuationDetails::InstanceManagerAvailable
            ),
        );

        // Instance manager is evacuating but still has VMMs registered.
        let collection = test_boards
            .collection_builder()
            .instance_manager_status_exception(
                0,
                InstanceManagerStatus {
                    update_disposition: CurrentUpdateDisposition::Known(
                        OmicronSledUpdateDisposition::Evacuating,
                    ),
                    num_registered_vmms: 2,
                },
            )
            .build();
        assert_eq!(
            evacuating_at_gen1.evacuation_status(sled_0_id, &collection),
            EvacuationStatus::WaitingOnEvacuation(
                WaitingOnSledEvacuationDetails::InstanceManagerRegisteredVmms {
                    num_registered_vmms: NonZeroUsize::new(2).unwrap(),
                }
            ),
        );

        // Fully evacuated.
        let collection =
            test_boards.collection_builder().sled_evacuated(0, gen1).build();
        assert_eq!(
            evacuating_at_gen1.evacuation_status(sled_0_id, &collection),
            EvacuationStatus::Evacuated,
        );

        // A ledgered config _newer_ than the one that marked the sled
        // `Evacuating` is also fine: it can only have been produced by a later
        // blueprint.
        let collection =
            test_boards.collection_builder().sled_evacuated(0, gen2).build();
        assert_eq!(
            evacuating_at_gen1.evacuation_status(sled_0_id, &collection),
            EvacuationStatus::Evacuated,
        );
    }
}
