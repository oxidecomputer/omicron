// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeSet;

use nexus_types::deployment::{
    BlueprintZoneConfig, BlueprintZoneDisposition, BlueprintZonesConfig,
};
use omicron_common::api::external::Generation;
use omicron_uuid_kinds::OmicronZoneUuid;
use thiserror::Error;

#[derive(Debug)]
#[must_use]
pub(super) struct BuilderZonesConfig {
    // The current generation -- this is bumped at blueprint build time and is
    // otherwise not exposed to callers.
    generation: Generation,

    // The list of zones, along with their state.
    zones: Vec<BuilderZoneConfig>,
}

impl BuilderZonesConfig {
    pub(super) fn new() -> Self {
        Self {
            // Note that the first generation is reserved to mean the one
            // containing no zones. See
            // OmicronZonesConfig::INITIAL_GENERATION.
            //
            // Since we're currently assuming that creating a new
            // `BuilderZonesConfig` means that we're going to add new zones
            // shortly, we start with Generation::new() here. It'll get
            // bumped up to the next one in `Self::build`.
            generation: Generation::new(),
            zones: vec![],
        }
    }

    pub(super) fn from_parent(parent: &BlueprintZonesConfig) -> Self {
        Self {
            // We'll bump this up at build time.
            generation: parent.generation,

            zones: parent
                .zones
                .iter()
                .map(|zone| BuilderZoneConfig {
                    zone: zone.clone(),
                    state: BuilderZoneState::Unchanged,
                })
                .collect(),
        }
    }

    pub(super) fn add_zone(
        &mut self,
        zone: BlueprintZoneConfig,
    ) -> Result<(), BuilderZonesConfigError> {
        if self.zones.iter().any(|z| z.zone.id == zone.id) {
            // We shouldn't be trying to add zones that already exist --
            // something went wrong in the planner logic.
            return Err(BuilderZonesConfigError::AddExistingZone {
                zone_id: zone.id,
            });
        };

        self.zones
            .push(BuilderZoneConfig { zone, state: BuilderZoneState::Added });
        Ok(())
    }

    pub(super) fn expunge_zones(
        &mut self,
        mut zones: BTreeSet<OmicronZoneUuid>,
    ) -> Result<(), BuilderZonesConfigError> {
        for zone in &mut self.zones {
            if zones.remove(&zone.zone.id) {
                // Check that the zone is expungeable. Typically, zones passed
                // in here should have had this check done to them already, but
                // in case they're not, or in case something else about those
                // zones changed in between, check again.
                is_already_expunged(&zone.zone, zone.state)?;
                zone.zone.disposition = BlueprintZoneDisposition::Expunged;
                zone.state = BuilderZoneState::Modified;
            }
        }

        // All zones passed in should have been found -- are there any left
        // over?
        if !zones.is_empty() {
            return Err(BuilderZonesConfigError::ExpungeUnmatchedZones {
                unmatched: zones,
            });
        }

        Ok(())
    }

    pub(super) fn iter_zones(
        &self,
    ) -> impl Iterator<Item = &BuilderZoneConfig> {
        self.zones.iter()
    }

    pub(super) fn build(self) -> BlueprintZonesConfig {
        let mut ret = BlueprintZonesConfig {
            // Something we could do here is to check if any zones have
            // actually been modified, and if not, return the parent's
            // generation. For now, we depend on callers to only call
            // `BlueprintZonesBuilder::change_sled_zones` when they really
            // mean it.
            generation: self.generation.next(),
            zones: self.zones.into_iter().map(|z| z.zone).collect(),
        };
        ret.sort();
        ret
    }
}

pub(super) fn is_already_expunged(
    zone: &BlueprintZoneConfig,
    state: BuilderZoneState,
) -> Result<bool, BuilderZonesConfigError> {
    match zone.disposition {
        BlueprintZoneDisposition::InService
        | BlueprintZoneDisposition::Quiesced => {
            if state != BuilderZoneState::Unchanged {
                // We shouldn't be trying to expunge zones that have also been
                // changed in this blueprint -- something went wrong in the planner
                // logic.
                return Err(BuilderZonesConfigError::ExpungeModifiedZone {
                    zone_id: zone.id,
                    state,
                });
            }
            Ok(false)
        }
        BlueprintZoneDisposition::Expunged => {
            // Treat expungement as idempotent.
            Ok(true)
        }
    }
}

#[derive(Debug)]
pub(super) struct BuilderZoneConfig {
    zone: BlueprintZoneConfig,
    state: BuilderZoneState,
}

impl BuilderZoneConfig {
    pub(super) fn zone(&self) -> &BlueprintZoneConfig {
        &self.zone
    }

    pub(super) fn state(&self) -> BuilderZoneState {
        self.state
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(super) enum BuilderZoneState {
    Unchanged,
    Modified,
    Added,
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub(super) enum BuilderZonesConfigError {
    #[error("attempted to add zone that already exists: {zone_id}")]
    AddExistingZone { zone_id: OmicronZoneUuid },
    #[error(
        "attempted to expunge zone {zone_id} that was in state {state:?} \
         (can only expunge unchanged zones)"
    )]
    ExpungeModifiedZone { zone_id: OmicronZoneUuid, state: BuilderZoneState },
    #[error(
        "while expunging zones, not all zones provided were found: {unmatched:?}"
    )]
    ExpungeUnmatchedZones { unmatched: BTreeSet<OmicronZoneUuid> },
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        net::{Ipv6Addr, SocketAddrV6},
    };

    use maplit::btreeset;
    use nexus_types::{
        deployment::{
            blueprint_zone_type, BlueprintZoneType, SledDetails, SledFilter,
            SledResources,
        },
        external_api::views::{SledPolicy, SledState},
    };
    use omicron_common::address::Ipv6Subnet;
    use omicron_test_utils::dev::test_setup_log;

    use crate::{
        blueprint_builder::{
            test::{verify_blueprint, DEFAULT_N_SLEDS},
            BlueprintBuilder, Ensure,
        },
        example::ExampleSystem,
    };

    use super::*;

    /// A test focusing on `BlueprintZonesBuilder` and its internal logic.
    #[test]
    fn test_builder_zones() {
        static TEST_NAME: &str = "blueprint_test_builder_zones";
        let logctx = test_setup_log(TEST_NAME);
        let mut example =
            ExampleSystem::new(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);
        let blueprint_initial = example.blueprint;

        // Add a completely bare sled to the input.
        let (new_sled_id, input2) = {
            let mut input = example.input.clone().into_builder();
            let new_sled_id = example.sled_rng.next();
            input
                .add_sled(
                    new_sled_id,
                    SledDetails {
                        policy: SledPolicy::provisionable(),
                        state: SledState::Active,
                        resources: SledResources {
                            subnet: Ipv6Subnet::new(
                                "fd00:1::".parse().unwrap(),
                            ),
                            zpools: BTreeMap::new(),
                        },
                    },
                )
                .expect("adding new sled");

            (new_sled_id, input.build())
        };

        let mut builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint_initial,
            &input2,
            "the_test",
        )
        .expect("creating blueprint builder");
        builder.set_rng_seed((TEST_NAME, "bp2"));

        // Test adding a new sled with an NTP zone.
        assert_eq!(
            builder.sled_ensure_zone_ntp(new_sled_id).unwrap(),
            Ensure::Added
        );

        // Iterate over the zones for the sled and ensure that the NTP zone is
        // present.
        {
            let mut zones = builder.zones.current_sled_zones(new_sled_id);
            let (_, state) = zones.next().expect("exactly one zone for sled");
            assert!(zones.next().is_none(), "exactly one zone for sled");
            assert_eq!(
                state,
                BuilderZoneState::Added,
                "NTP zone should have been added"
            );
        }

        // Now, test adding a new zone (Oximeter, picked arbitrarily) to an
        // existing sled.
        let existing_sled_id = example
            .input
            .all_sled_ids(SledFilter::Commissioned)
            .next()
            .expect("at least one sled present");
        let change = builder.zones.change_sled_zones(existing_sled_id);

        let new_zone_id = OmicronZoneUuid::new_v4();
        change
            .add_zone(BlueprintZoneConfig {
                disposition: BlueprintZoneDisposition::InService,
                id: new_zone_id,
                underlay_address: Ipv6Addr::UNSPECIFIED,
                zone_type: BlueprintZoneType::Oximeter(
                    blueprint_zone_type::Oximeter {
                        address: SocketAddrV6::new(
                            Ipv6Addr::UNSPECIFIED,
                            0,
                            0,
                            0,
                        ),
                    },
                ),
            })
            .expect("adding new zone");

        // Attempt to expunge one of the other zones on the sled.
        let existing_zone_id = change
            .iter_zones()
            .find(|z| z.zone.id != new_zone_id)
            .expect("at least one existing zone")
            .zone
            .id;
        change
            .expunge_zones(btreeset! { existing_zone_id })
            .expect("expunging existing zone");
        // Do it again to ensure that expunging an already-expunged zone is
        // idempotent, even within the same blueprint.
        change
            .expunge_zones(btreeset! { existing_zone_id })
            .expect("expunging already-expunged zone");
        // But expunging a zone that doesn't exist should fail.
        let non_existent_zone_id = OmicronZoneUuid::new_v4();
        let non_existent_set = btreeset! { non_existent_zone_id };
        let error = change
            .expunge_zones(non_existent_set.clone())
            .expect_err("expunging non-existent zone");
        assert_eq!(
            error,
            BuilderZonesConfigError::ExpungeUnmatchedZones {
                unmatched: non_existent_set
            }
        );

        {
            // Iterate over the zones and ensure that the Oximeter zone is
            // present, and marked added.
            let mut zones = builder.zones.current_sled_zones(existing_sled_id);
            zones
                .find_map(|(z, state)| {
                    if z.id == new_zone_id {
                        assert_eq!(
                            state,
                            BuilderZoneState::Added,
                            "new zone ID {new_zone_id} should be marked added"
                        );
                        Some(())
                    } else {
                        None
                    }
                })
                .expect("new zone ID should be present");
        }

        // Also call change_sled_zones without making any changes. This
        // currently bumps the generation number, but in the future might
        // become smarter and not do so (in which case this test will break).
        let control_sled_id = example
            .input
            .all_sled_ids(SledFilter::Commissioned)
            .nth(2)
            .expect("at least 2 sleds present");
        _ = builder.zones.change_sled_zones(control_sled_id);

        // Attempt to expunge the newly added Oximeter zone. This should fail
        // because we only support expunging zones that are unchanged from the
        // parent blueprint.
        let error = builder
            .zones
            .change_sled_zones(existing_sled_id)
            .expunge_zones(btreeset! { new_zone_id })
            .expect_err("expunging a new zone should fail");
        assert_eq!(
            error,
            BuilderZonesConfigError::ExpungeModifiedZone {
                zone_id: new_zone_id,
                state: BuilderZoneState::Added
            }
        );

        // Now build the blueprint and ensure that all the changes we described
        // above are present.
        let blueprint = builder.build();
        verify_blueprint(&blueprint);
        let diff = blueprint.diff_since_blueprint(&blueprint_initial).unwrap();
        println!("expecting new NTP and Oximeter zones:\n{}", diff.display());

        // No sleds were removed.
        assert_eq!(diff.sleds_removed().len(), 0);

        // One sled was added.
        let sleds: Vec<_> = diff.sleds_added().collect();
        assert_eq!(sleds.len(), 1);
        let (sled_id, new_sled_zones) = sleds[0];
        assert_eq!(sled_id, new_sled_id);
        // The generation number should be newer than the initial default.
        assert_eq!(new_sled_zones.generation, Generation::new().next());
        assert_eq!(new_sled_zones.zones.len(), 1);

        // Two sleds were modified: existing_sled_id and control_sled_id.
        let sleds = diff.sleds_modified();
        assert_eq!(sleds.len(), 2, "2 sleds modified");
        for (sled_id, sled_modified) in sleds {
            if sled_id == existing_sled_id {
                assert_eq!(
                    sled_modified.generation_after,
                    sled_modified.generation_before.next()
                );
                assert_eq!(sled_modified.zones_added().len(), 1);
                let added_zone = sled_modified.zones_added().next().unwrap();
                assert_eq!(added_zone.id, new_zone_id);

                assert_eq!(sled_modified.zones_removed().len(), 0);
                assert_eq!(sled_modified.zones_modified().count(), 1);
                let modified_zone =
                    sled_modified.zones_modified().next().unwrap();
                assert_eq!(modified_zone.zone_before.id(), existing_zone_id);
            } else {
                assert_eq!(sled_id, control_sled_id);

                // The generation number is bumped, but nothing else.
                assert_eq!(
                    sled_modified.generation_after,
                    sled_modified.generation_before.next(),
                    "control sled has generation number bumped"
                );
                assert_eq!(sled_modified.zones_added().len(), 0);
                assert_eq!(sled_modified.zones_removed().len(), 0);
                assert_eq!(sled_modified.zones_modified().count(), 0);
            }
        }

        logctx.cleanup_successful();
    }
}
