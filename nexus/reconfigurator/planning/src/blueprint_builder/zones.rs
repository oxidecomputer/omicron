// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{collections::BTreeSet, fmt};

use nexus_types::deployment::{
    BlueprintZoneConfig, BlueprintZoneDisposition, BlueprintZoneFilter,
    BlueprintZonesConfig,
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
    zones: Vec<BuilderZoneEntry>,
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
                .map(|zone| {
                    BuilderZoneEntry::Present(BuilderZoneConfig {
                        zone: zone.clone(),
                        state: BuilderZoneState::Unchanged,
                    })
                })
                .collect(),
        }
    }

    pub(super) fn add_zone(
        &mut self,
        zone: BlueprintZoneConfig,
    ) -> Result<(), BuilderZonesConfigError> {
        if self.zones.iter().any(|z| z.id() == zone.id) {
            // We shouldn't be trying to add zones that already exist (or were
            // even noted to be removed) -- something went wrong in the planner
            // logic.
            return Err(BuilderZonesConfigError::AddExistingZone {
                zone_id: zone.id,
            });
        };

        self.zones.push(BuilderZoneEntry::Present(BuilderZoneConfig {
            zone,
            state: BuilderZoneState::Added,
        }));
        Ok(())
    }

    pub(super) fn expunge_zone(
        &mut self,
        zone_id: OmicronZoneUuid,
    ) -> Result<(), BuilderZonesConfigError> {
        let zone =
            self.find_zone_config_mut(zone_id, BuilderZoneOperation::Expunge)?;

        // Check that the zone is expungeable. Typically, zones passed in here
        // should have had this check done to them already, but in case they're
        // not, or in case something else about those zones changed in between,
        // check again.
        is_already_expunged(&zone.zone, zone.state)?;
        zone.zone.disposition = BlueprintZoneDisposition::Expunged;
        zone.state = BuilderZoneState::Modified;

        Ok(())
    }

    pub(super) fn expunge_zones(
        &mut self,
        zones: BTreeSet<OmicronZoneUuid>,
    ) -> Result<(), BuilderZonesConfigError> {
        let mut unmatched = UnmatchedZones::default();
        // We'll remove zones from this set as we find them.
        unmatched.non_existent = zones;

        for entry in &mut self.zones {
            match entry {
                BuilderZoneEntry::Present(zone) => {
                    if unmatched.non_existent.remove(&zone.zone.id) {
                        // Check that the zone is expungeable. Typically, zones passed
                        // in here should have had this check done to them already, but
                        // in case they're not, or in case something else about those
                        // zones changed in between, check again.
                        is_already_expunged(&zone.zone, zone.state)?;
                        zone.zone.disposition =
                            BlueprintZoneDisposition::Expunged;
                        zone.state = BuilderZoneState::Modified;
                    }
                }
                BuilderZoneEntry::Removed(zone_id) => {
                    if unmatched.non_existent.remove(&zone_id) {
                        // Once a zone is removed, no more changes can be made to it.
                        unmatched.already_removed.insert(*zone_id);
                    }
                }
            }
        }

        // All zones passed in should have been found -- are there any left
        // over?
        if !unmatched.is_empty() {
            return Err(BuilderZonesConfigError::OperateOnUnmatchedZones {
                op: BuilderZoneOperation::Expunge,
                unmatched,
            });
        }

        Ok(())
    }

    pub(super) fn remove_zone_entry(
        &mut self,
        zone_id: OmicronZoneUuid,
    ) -> Result<(), BuilderZonesConfigError> {
        // We should only be removing zones that are already expunged. (This
        // should have been checked already, but check again just in case.)
        let entry =
            self.find_zone_entry_mut(zone_id, BuilderZoneOperation::Remove)?;
        check_removability(entry)?;

        *entry = BuilderZoneEntry::Removed(zone_id);

        Ok(())
    }

    pub(super) fn iter_zones(
        &self,
        filter: BlueprintZoneFilter,
    ) -> impl Iterator<Item = &BuilderZoneConfig> {
        self.zones.iter().filter_map(move |z| {
            let z = z.as_present()?;
            z.zone.disposition.matches(filter).then_some(z)
        })
    }

    fn find_zone_entry_mut(
        &mut self,
        zone_id: OmicronZoneUuid,
        op: BuilderZoneOperation,
    ) -> Result<&mut BuilderZoneEntry, BuilderZonesConfigError> {
        self.zones.iter_mut().find(|z| z.id() == zone_id).ok_or_else(|| {
            BuilderZonesConfigError::OperateOnUnmatchedZones {
                op,
                unmatched: UnmatchedZones::one_non_existent(zone_id),
            }
        })
    }

    fn find_zone_config_mut(
        &mut self,
        zone_id: OmicronZoneUuid,
        op: BuilderZoneOperation,
    ) -> Result<&mut BuilderZoneConfig, BuilderZonesConfigError> {
        let entry = self.find_zone_entry_mut(zone_id, op)?;

        match entry {
            BuilderZoneEntry::Present(zone) => Ok(zone),
            BuilderZoneEntry::Removed(_) => {
                Err(BuilderZonesConfigError::OperateOnUnmatchedZones {
                    op,
                    unmatched: UnmatchedZones::one_already_removed(zone_id),
                })
            }
        }
    }

    pub(super) fn build(self) -> BlueprintZonesConfig {
        // Only bump the generation if any zones have been changed.
        let generation = if self.zones.iter().any(|z| {
            if let BuilderZoneEntry::Present(z) = z {
                z.state != BuilderZoneState::Unchanged
            } else {
                // Removed zones don't count as changes because they're purely
                // for internal accounting.
                false
            }
        }) {
            self.generation.next()
        } else {
            self.generation
        };

        let mut ret = BlueprintZonesConfig {
            generation,
            zones: self
                .zones
                .into_iter()
                .filter_map(|z| Some(z.into_present()?.zone))
                .collect(),
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

fn check_removability(
    entry: &BuilderZoneEntry,
) -> Result<(), BuilderZonesConfigError> {
    match entry {
        BuilderZoneEntry::Present(zone) => match zone.zone.disposition {
            BlueprintZoneDisposition::InService
            | BlueprintZoneDisposition::Quiesced => {
                return Err(BuilderZonesConfigError::RemoveNonExpungedZone {
                    zone_id: zone.zone.id,
                    disposition: zone.zone.disposition,
                });
            }
            BlueprintZoneDisposition::Expunged => Ok(()),
        },
        BuilderZoneEntry::Removed(zone_id) => {
            Err(BuilderZonesConfigError::OperateOnUnmatchedZones {
                op: BuilderZoneOperation::Remove,
                unmatched: UnmatchedZones::one_already_removed(*zone_id),
            })
        }
    }
}

#[derive(Debug)]
enum BuilderZoneEntry {
    Present(BuilderZoneConfig),
    Removed(OmicronZoneUuid),
}

impl BuilderZoneEntry {
    fn id(&self) -> OmicronZoneUuid {
        match self {
            Self::Present(z) => z.zone.id,
            Self::Removed(id) => *id,
        }
    }

    fn as_present(&self) -> Option<&BuilderZoneConfig> {
        match self {
            Self::Present(z) => Some(z),
            Self::Removed(_) => None,
        }
    }

    fn into_present(self) -> Option<BuilderZoneConfig> {
        match self {
            Self::Present(z) => Some(z),
            Self::Removed(_) => None,
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
        "while attempting to {op} zones, not all zones provided\
         were found: {unmatched:?}"
    )]
    OperateOnUnmatchedZones {
        op: BuilderZoneOperation,
        unmatched: UnmatchedZones,
    },

    #[error(
        "attempted to expunge zone {zone_id} that was in state {state:?} \
         (can only expunge unchanged zones)"
    )]
    ExpungeModifiedZone { zone_id: OmicronZoneUuid, state: BuilderZoneState },

    #[error(
        "attempted to remove a non-expunged zone: {zone_id} \
         (disposition: {disposition:?})"
    )]
    RemoveNonExpungedZone {
        zone_id: OmicronZoneUuid,
        disposition: BlueprintZoneDisposition,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum BuilderZoneOperation {
    Expunge,
    Remove,
}

impl fmt::Display for BuilderZoneOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Expunge => write!(f, "expunge"),
            Self::Remove => write!(f, "remove"),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct UnmatchedZones {
    non_existent: BTreeSet<OmicronZoneUuid>,
    already_removed: BTreeSet<OmicronZoneUuid>,
}

impl UnmatchedZones {
    fn one_non_existent(zone_id: OmicronZoneUuid) -> Self {
        Self {
            non_existent: {
                let mut set = BTreeSet::new();
                set.insert(zone_id);
                set
            },
            already_removed: BTreeSet::new(),
        }
    }

    fn one_already_removed(zone_id: OmicronZoneUuid) -> Self {
        Self {
            non_existent: BTreeSet::new(),
            already_removed: {
                let mut set = BTreeSet::new();
                set.insert(zone_id);
                set
            },
        }
    }

    fn is_empty(&self) -> bool {
        self.non_existent.is_empty() && self.already_removed.is_empty()
    }
}

impl fmt::Display for UnmatchedZones {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.non_existent.is_empty() {
            write!(f, "non-existent: ")?;
            for (ix, zone_id) in self.non_existent.iter().enumerate() {
                write!(f, "{zone_id}")?;
                if ix < self.non_existent.len() - 1 {
                    write!(f, ", ")?;
                }
            }
        }

        if !self.already_removed.is_empty() {
            if !self.non_existent.is_empty() {
                write!(f, "; ")?;
            }
            write!(f, "already removed: ")?;
            for (ix, zone_id) in self.already_removed.iter().enumerate() {
                write!(f, "{zone_id}")?;
                if ix < self.already_removed.len() - 1 {
                    write!(f, ", ")?;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        net::{Ipv6Addr, SocketAddrV6},
    };

    use maplit::btreeset;
    use nexus_types::deployment::SledDisk;
    use nexus_types::external_api::views::PhysicalDiskPolicy;
    use nexus_types::external_api::views::PhysicalDiskState;
    use nexus_types::{
        deployment::{
            blueprint_zone_type, BlueprintZoneType, SledDetails, SledFilter,
            SledResources,
        },
        external_api::views::{SledPolicy, SledState},
    };
    use omicron_common::address::Ipv6Subnet;
    use omicron_common::disk::DiskIdentity;
    use omicron_common::zpool_name::ZpoolName;
    use omicron_test_utils::dev::test_setup_log;
    use omicron_uuid_kinds::PhysicalDiskUuid;
    use omicron_uuid_kinds::ZpoolUuid;

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
                            zpools: BTreeMap::from([(
                                ZpoolUuid::new_v4(),
                                SledDisk {
                                    disk_identity: DiskIdentity {
                                        vendor: String::from("fake-vendor"),
                                        serial: String::from("fake-serial"),
                                        model: String::from("fake-model"),
                                    },
                                    disk_id: PhysicalDiskUuid::new_v4(),
                                    policy: PhysicalDiskPolicy::InService,
                                    state: PhysicalDiskState::Active,
                                },
                            )]),
                        },
                    },
                )
                .expect("adding new sled");

            (new_sled_id, input.build())
        };

        let existing_sled_id = example
            .input
            .all_sled_ids(SledFilter::Commissioned)
            .next()
            .expect("at least one sled present");

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
            let mut zones = builder.zones.current_sled_zones(
                new_sled_id,
                BlueprintZoneFilter::ShouldBeRunning,
            );
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
        let change = builder.zones.change_sled_zones(existing_sled_id);

        let new_zone_id = OmicronZoneUuid::new_v4();
        // NOTE: This pool doesn't actually exist on the sled, but nothing is
        // checking for that in this test?
        let filesystem_pool = ZpoolName::new_external(ZpoolUuid::new_v4());
        change
            .add_zone(BlueprintZoneConfig {
                disposition: BlueprintZoneDisposition::InService,
                id: new_zone_id,
                underlay_address: Ipv6Addr::UNSPECIFIED,
                filesystem_pool: Some(filesystem_pool),
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
            .iter_zones(BlueprintZoneFilter::ShouldBeRunning)
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
            BuilderZonesConfigError::OperateOnUnmatchedZones {
                op: BuilderZoneOperation::Expunge,
                unmatched: UnmatchedZones {
                    non_existent: non_existent_set,
                    already_removed: BTreeSet::new(),
                }
            }
        );

        {
            // Iterate over the zones and ensure that the Oximeter zone is
            // present, and marked added.
            let mut zones = builder.zones.current_sled_zones(
                existing_sled_id,
                BlueprintZoneFilter::ShouldBeRunning,
            );
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
        let diff = blueprint.diff_since_blueprint(&blueprint_initial);
        println!("expecting new NTP and Oximeter zones:\n{}", diff.display());

        // No sleds were removed.
        assert_eq!(diff.sleds_removed.len(), 0);

        // One sled was added.
        assert_eq!(diff.sleds_added.len(), 1);
        let sled_id = diff.sleds_added.first().unwrap();
        assert_eq!(*sled_id, new_sled_id);
        let new_sled_zones = diff.zones.added.get(sled_id).unwrap();
        // The generation number should be newer than the initial default.
        assert_eq!(
            new_sled_zones.generation_after.unwrap(),
            Generation::new().next()
        );
        assert_eq!(new_sled_zones.zones.len(), 1);

        // TODO: AJS - See comment above - we don't actually use the control sled anymore
        // so the comparison was changed.
        // One sled was modified: existing_sled_id
        assert_eq!(diff.sleds_modified.len(), 1, "1 sled modified");
        for sled_id in &diff.sleds_modified {
            assert_eq!(*sled_id, existing_sled_id);
            let added = diff.zones.added.get(sled_id).unwrap();
            assert_eq!(
                added.generation_after.unwrap(),
                added.generation_before.unwrap().next()
            );
            assert_eq!(added.zones.len(), 1);
            let added_zone = &added.zones[0];
            assert_eq!(added_zone.id(), new_zone_id);

            assert!(!diff.zones.removed.contains_key(sled_id));
            let modified = diff.zones.modified.get(sled_id).unwrap();
            assert_eq!(modified.zones.len(), 1);
            let modified_zone = &modified.zones[0];
            assert_eq!(modified_zone.zone.id(), existing_zone_id);
        }

        // Test a no-op change.
        {
            let mut builder = BlueprintBuilder::new_based_on(
                &logctx.log,
                &blueprint,
                &input2,
                "the_test",
            )
            .expect("creating blueprint builder");
            builder.set_rng_seed((TEST_NAME, "bp2"));

            // This call by itself shouldn't bump the generation number.
            builder.zones.change_sled_zones(existing_sled_id);

            let blueprint_noop = builder.build();
            verify_blueprint(&blueprint_noop);
            let diff = blueprint_noop.diff_since_blueprint(&blueprint);
            println!("expecting a noop:\n{}", diff.display());

            assert!(diff.sleds_modified.is_empty(), "no sleds modified");
            assert!(diff.sleds_added.is_empty(), "no sleds added");
            assert!(diff.sleds_removed.is_empty(), "no sleds removed");
        }

        logctx.cleanup_successful();
    }
}
