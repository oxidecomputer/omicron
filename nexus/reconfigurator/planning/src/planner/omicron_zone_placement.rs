// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Omicron zone placement decisions

use nexus_types::deployment::BlueprintZoneType;
use omicron_uuid_kinds::SledUuid;
use sled_agent_types_migrations::latest::inventory::ZoneKind;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::mem;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub(crate) enum DiscretionaryOmicronZone {
    BoundaryNtp,
    Clickhouse,
    ClickhouseKeeper,
    ClickhouseServer,
    CockroachDb,
    CruciblePantry,
    InternalDns,
    ExternalDns,
    Nexus,
    Oximeter,
}

impl DiscretionaryOmicronZone {
    pub(super) fn from_zone_type(
        zone_type: &BlueprintZoneType,
    ) -> Option<Self> {
        match zone_type {
            BlueprintZoneType::BoundaryNtp(_) => Some(Self::BoundaryNtp),
            BlueprintZoneType::Clickhouse(_) => Some(Self::Clickhouse),
            BlueprintZoneType::ClickhouseKeeper(_) => {
                Some(Self::ClickhouseKeeper)
            }
            BlueprintZoneType::ClickhouseServer(_) => {
                Some(Self::ClickhouseServer)
            }
            BlueprintZoneType::CockroachDb(_) => Some(Self::CockroachDb),
            BlueprintZoneType::CruciblePantry(_) => Some(Self::CruciblePantry),
            BlueprintZoneType::InternalDns(_) => Some(Self::InternalDns),
            BlueprintZoneType::ExternalDns(_) => Some(Self::ExternalDns),
            BlueprintZoneType::Nexus(_) => Some(Self::Nexus),
            BlueprintZoneType::Oximeter(_) => Some(Self::Oximeter),
            // Zones that get special handling for placement (all sleds get
            // them, although internal NTP has some interactions with boundary
            // NTP that are handled separately).
            BlueprintZoneType::Crucible(_)
            | BlueprintZoneType::InternalNtp(_) => None,
        }
    }
}

impl From<DiscretionaryOmicronZone> for ZoneKind {
    fn from(zone: DiscretionaryOmicronZone) -> Self {
        match zone {
            DiscretionaryOmicronZone::BoundaryNtp => Self::BoundaryNtp,
            DiscretionaryOmicronZone::Clickhouse => Self::Clickhouse,
            DiscretionaryOmicronZone::ClickhouseKeeper => {
                Self::ClickhouseKeeper
            }
            DiscretionaryOmicronZone::ClickhouseServer => {
                Self::ClickhouseServer
            }
            DiscretionaryOmicronZone::CockroachDb => Self::CockroachDb,
            DiscretionaryOmicronZone::CruciblePantry => Self::CruciblePantry,
            DiscretionaryOmicronZone::InternalDns => Self::InternalDns,
            DiscretionaryOmicronZone::ExternalDns => Self::ExternalDns,
            DiscretionaryOmicronZone::Nexus => Self::Nexus,
            DiscretionaryOmicronZone::Oximeter => Self::Oximeter,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum PlacementError {
    #[error(
        "no sleds eligible for placement of new {} zone",
        ZoneKind::from(*zone_kind).report_str()
    )]
    NoSledsEligible { zone_kind: DiscretionaryOmicronZone },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct OmicronZonePlacementSledState {
    pub sled_id: SledUuid,
    pub num_zpools: usize,
    pub discretionary_zones: Vec<DiscretionaryOmicronZone>,
}

impl OmicronZonePlacementSledState {
    fn num_discretionary_zones_of_kind(
        &self,
        kind: DiscretionaryOmicronZone,
    ) -> usize {
        self.discretionary_zones.iter().filter(|&&z| z == kind).count()
    }
}

/// `OmicronZonePlacement` keeps an internal heap of sleds and their current
/// discretionary zones and chooses sleds for placement of additional
/// discretionary zones.
#[derive(Debug, Clone)]
pub(super) struct OmicronZonePlacement {
    sleds: OrderedSleds,
}

impl OmicronZonePlacement {
    /// Construct a new `OmicronZonePlacement` with a given set of eligible
    /// sleds.
    ///
    /// Sleds which are not eligible for discretionary services for reasons
    /// outside the knowledge of `OmicronZonePlacement` (e.g., sleds with a
    /// policy or state that makes them ineligible) should be omitted from this
    /// list of sleds. For now, sleds that are waiting for an NTP zone should be
    /// omitted as well, although that may change in the future when we add
    /// support for boundary NTP zone placement.
    pub(super) fn new(
        sleds: impl Iterator<Item = OmicronZonePlacementSledState>,
    ) -> Self {
        // We rebuild our heap whenever the zone type we're placing changes. We
        // need to pick _something_ to start; this only matters for performance,
        // not correctness (we don't have to rebuild the heap if `place_zone` is
        // called with a zone kind that matches the current sorting).
        let ordered_by = DiscretionaryOmicronZone::Nexus;
        Self { sleds: OrderedSleds::new(ordered_by, sleds) }
    }

    /// Attempt to place a new zone of kind `zone_kind` on one of the sleds
    /// provided when this `OmicronZonePlacement` was created.
    ///
    /// On success, the internal heap held by `self` is updated assuming that a
    /// new zone of the given kind was added to the sled returned by
    /// `place_zone()`. This allows one `OmicronZonePlacement` to be reused
    /// across multiple zone placement decisions, but requires the caller to
    /// accept its decisions. If the caller decides not to add a zone to the
    /// returned sled, the `OmicronZonePlacement` instance should be discarded
    /// and a new one should be created for future placement decisions.
    ///
    /// Placement is currently minimal. The only hard requirement we enforce is
    /// that a sled may only one run one instance of any given zone kind per
    /// zpool it has (e.g., a sled with 5 zpools could run 5 Nexus instances and
    /// 5 CockroachDb instances concurrently, but could not run 6 Nexus
    /// instances). If there is at least one sled that satisfies this
    /// requirement, this method will return `Ok(_)`. If there are multiple
    /// sleds that satisfy this requirement, this method will return a sled
    /// which has the fewest instances of `zone_kind`; if multiple sleds are
    /// tied, it will pick the one with the fewest total discretionary zones; if
    /// multiple sleds are still tied, it will pick deterministically (e.g.,
    /// choosing the lowest or highest sled ID).
    ///
    /// `OmicronZonePlacement` currently does not track _which_ zpools are
    /// assigned to services. This could lead to it being overly conservative if
    /// zpools that are not in service are hosting relevant zones. For example,
    /// imagine a sled with two zpools: zpool-a and zpool-b. The sled has a
    /// single Nexus instance with a transitory dataset on zpool-a. If zpool-a
    /// is in a degraded state and considered not-in-service,
    /// `OmicronZonePlacement` will be told by the planner that the sled has 1
    /// zpool. Our simple check of "at most one Nexus per zpool" would
    /// erroneously fail to realize we could still add a Nexus (backed by
    /// zpool-b), and would claim that the sled already has a Nexus for each
    /// zpool.
    ///
    /// We punt on this problem for multiple reasons:
    ///
    /// 1. It's overly conservative; if we get into this state, we may refuse to
    ///    start services when we ought to be able to, but this isn't the worst
    ///    failure mode. In practice we should have far more options for
    ///    placement than we need for any of our control plane services, so
    ///    skipping a sled in this state should be fine.
    /// 2. We don't yet track transitory datasets, so even if we wanted to know
    ///    which zpool Nexus was using (in the above example), we can't.
    /// 3. We don't (yet?) have a way for a zpool to be present, backing a zone,
    ///    and not considered to be in service. The only zpools that aren't in
    ///    service belong to expunged disks, which can't be backing live
    ///    services.
    pub(super) fn place_zone(
        &mut self,
        zone_kind: DiscretionaryOmicronZone,
    ) -> Result<SledUuid, PlacementError> {
        self.sleds.ensure_ordered_by(zone_kind);

        let mut sleds_skipped = Vec::new();
        let mut chosen_sled = None;
        while let Some(sled) = self.sleds.pop() {
            let num_existing = sled.num_discretionary_zones_of_kind(zone_kind);

            // For boundary NTP, a sled is only eligible if it does not already
            // hold a boundary NTP zone.
            let should_skip = zone_kind
                == DiscretionaryOmicronZone::BoundaryNtp
                && num_existing > 0;

            // For all zone kinds, a sled is only eligible if it has at
            // least one zpool more than the number of `zone_kind` zones
            // already placed on this sled.
            let should_skip = should_skip || num_existing >= sled.num_zpools;

            if should_skip {
                sleds_skipped.push(sled);
            } else {
                chosen_sled = Some(sled);
                break;
            }
        }

        // Push any skipped sleds back onto our heap.
        for sled in sleds_skipped {
            self.sleds.push(sled);
        }

        let mut sled =
            chosen_sled.ok_or(PlacementError::NoSledsEligible { zone_kind })?;
        let sled_id = sled.sled_id;

        // Update our internal state so future `place_zone` calls take the new
        // zone we just placed into account.
        sled.discretionary_zones.push(zone_kind);
        self.sleds.push(sled);

        Ok(sled_id)
    }
}

// Wrapper around a binary heap that allows us to change the ordering at runtime
// (so we can sort for particular types of zones to place).
#[derive(Debug, Clone)]
struct OrderedSleds {
    // The current zone type we're sorted to place. We maintain the invariant
    // that every element of `heap` has the same `ordered_by` value as this
    // field's current value.
    ordered_by: DiscretionaryOmicronZone,
    heap: BinaryHeap<OrderedSledState>,
}

impl OrderedSleds {
    fn new(
        ordered_by: DiscretionaryOmicronZone,
        sleds: impl Iterator<Item = OmicronZonePlacementSledState>,
    ) -> Self {
        Self {
            ordered_by,
            heap: sleds
                .map(|sled| OrderedSledState { ordered_by, sled })
                .collect(),
        }
    }

    fn ensure_ordered_by(&mut self, ordered_by: DiscretionaryOmicronZone) {
        if self.ordered_by == ordered_by {
            return;
        }

        // Rebuild our heap, sorting by a new zone kind, and maintaining the
        // invariant that all our heap members have the same `ordered_by` value
        // as we do.
        let mut sleds = mem::take(&mut self.heap).into_vec();
        for s in &mut sleds {
            s.ordered_by = ordered_by;
        }
        self.heap = BinaryHeap::from(sleds);
        self.ordered_by = ordered_by;
    }

    fn pop(&mut self) -> Option<OmicronZonePlacementSledState> {
        self.heap.pop().map(|ordered| ordered.sled)
    }

    fn push(&mut self, sled: OmicronZonePlacementSledState) {
        self.heap.push(OrderedSledState { ordered_by: self.ordered_by, sled })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OrderedSledState {
    ordered_by: DiscretionaryOmicronZone,
    sled: OmicronZonePlacementSledState,
}

impl Ord for OrderedSledState {
    fn cmp(&self, other: &Self) -> Ordering {
        // Invariant: We should never compare other entries with a different
        // `ordered_by`. This is enforced by `OrderedSleds`.
        assert_eq!(self.ordered_by, other.ordered_by);

        // Count how many zones of our ordering type are in each side.
        let our_zones_of_interest = self
            .sled
            .discretionary_zones
            .iter()
            .filter(|&&z| z == self.ordered_by)
            .count();
        let other_zones_of_interest = other
            .sled
            .discretionary_zones
            .iter()
            .filter(|&&z| z == self.ordered_by)
            .count();

        // BinaryHeap is a max heap, and we want to be on the top of the heap if
        // we have fewer zones of interest, so reverse the comparisons below.
        our_zones_of_interest
            .cmp(&other_zones_of_interest)
            .reverse()
            // If the zones of interest count is equal, we tiebreak by total
            // discretionary zones, again reversing the order for our max heap
            // to prioritize sleds with fewer total discretionary zones.
            .then_with(|| {
                self.sled
                    .discretionary_zones
                    .len()
                    .cmp(&other.sled.discretionary_zones.len())
                    .reverse()
            })
            // If we're still tied, tiebreak by sorting on sled ID for
            // determinism.
            .then_with(|| self.sled.sled_id.cmp(&other.sled.sled_id))
    }
}

impl PartialOrd for OrderedSledState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use omicron_uuid_kinds::GenericUuid;
    use proptest::arbitrary::any;
    use proptest::collection::btree_map;
    use proptest::sample::size_range;
    use std::collections::BTreeMap;
    use test_strategy::Arbitrary;
    use test_strategy::proptest;
    use uuid::Uuid;

    #[derive(Debug, Clone, Arbitrary)]
    struct ZonesToPlace {
        #[any(size_range(0..8).lift())]
        zones: Vec<DiscretionaryOmicronZone>,
    }

    #[derive(Debug, Clone, Arbitrary)]
    struct ExistingSled {
        zones: ZonesToPlace,
        #[strategy(0_usize..8)]
        num_zpools: usize,
    }

    #[derive(Debug, Arbitrary)]
    struct ArbitraryTestInput {
        #[strategy(btree_map(any::<[u8; 16]>(), any::<ExistingSled>(), 1..8))]
        existing_sleds: BTreeMap<[u8; 16], ExistingSled>,
        zones_to_place: ZonesToPlace,
    }

    #[derive(Debug)]
    struct TestInput {
        state: TestState,
        zones_to_place: Vec<DiscretionaryOmicronZone>,
    }

    impl From<ArbitraryTestInput> for TestInput {
        fn from(input: ArbitraryTestInput) -> Self {
            let mut sleds = BTreeMap::new();
            for (&raw_id, existing_sled) in input.existing_sleds.iter() {
                let sled_id =
                    SledUuid::from_untyped_uuid(Uuid::from_bytes(raw_id));
                sleds.insert(
                    sled_id,
                    TestSledState {
                        zones: existing_sled.zones.zones.clone(),
                        num_zpools: existing_sled.num_zpools,
                    },
                );
            }
            let state = TestState { sleds };
            Self { state, zones_to_place: input.zones_to_place.zones }
        }
    }

    #[derive(Debug)]
    struct TestSledState {
        zones: Vec<DiscretionaryOmicronZone>,
        num_zpools: usize,
    }

    impl TestSledState {
        fn count_zones_of_kind(&self, kind: DiscretionaryOmicronZone) -> usize {
            self.zones.iter().filter(|&&k| k == kind).count()
        }
    }

    #[derive(Debug)]
    struct TestState {
        sleds: BTreeMap<SledUuid, TestSledState>,
    }

    impl TestState {
        fn validate_sled_can_support_another_zone_of_kind(
            &self,
            sled_id: SledUuid,
            kind: DiscretionaryOmicronZone,
        ) -> Result<(), String> {
            let sled_state = self.sleds.get(&sled_id).expect("valid sled_id");
            let existing_zones = sled_state.count_zones_of_kind(kind);

            // Boundary NTP is special: there should be at most one instance per
            // sled, so placing a new boundary NTP zone is only legal if the
            // sled doesn't already have one.
            if kind == DiscretionaryOmicronZone::BoundaryNtp
                && existing_zones > 0
            {
                Err(format!("sled {sled_id} already has a boundary NTP zone"))
            } else if existing_zones >= sled_state.num_zpools {
                Err(format!(
                    "already have {existing_zones} \
                     {kind:?} instances but only {} zpools",
                    sled_state.num_zpools
                ))
            } else {
                Ok(())
            }
        }

        fn validate_placement(
            &mut self,
            sled_id: SledUuid,
            kind: DiscretionaryOmicronZone,
        ) -> Result<(), String> {
            // Ensure this sled is eligible for this kind at all: We have to
            // have at least one disk on which we can put the dataset for this
            // zone that isn't already holding another zone of this same kind
            // (i.e., at most one zone of any given kind per disk per sled).
            self.validate_sled_can_support_another_zone_of_kind(sled_id, kind)?;

            let sled_state = self.sleds.get(&sled_id).expect("valid sled_id");
            let existing_zones = sled_state.count_zones_of_kind(kind);

            // Ensure this sled is (at least tied for) the best choice for this
            // kind: it should have the minimum number of existing zones of this
            // kind, and of all sleds tied for the minimum, it should have the
            // fewest total discretionary services.
            for (&other_sled_id, other_sled_state) in &self.sleds {
                // Ignore other sleds that can't run another zone of `kind`.
                if self
                    .validate_sled_can_support_another_zone_of_kind(
                        other_sled_id,
                        kind,
                    )
                    .is_err()
                {
                    continue;
                }

                let other_zone_count =
                    other_sled_state.count_zones_of_kind(kind);
                if other_zone_count < existing_zones {
                    return Err(format!(
                        "sled {other_sled_id} would be a better choice \
                         (fewer existing {kind:?} instances: \
                         {other_zone_count} < {existing_zones})"
                    ));
                }
                if other_zone_count == existing_zones
                    && other_sled_state.zones.len() < sled_state.zones.len()
                {
                    return Err(format!(
                        "sled {other_sled_id} would be a better choice \
                         (same number of existing {kind:?} instances, but \
                          fewer total discretionary services: {} < {})",
                        other_sled_state.zones.len(),
                        sled_state.zones.len(),
                    ));
                }
            }

            // This placement is valid: update our state.
            self.sleds.get_mut(&sled_id).unwrap().zones.push(kind);
            Ok(())
        }

        fn validate_no_placement_possible(
            &self,
            kind: DiscretionaryOmicronZone,
        ) -> Result<(), String> {
            let max_this_kind_for_sled = |sled_state: &TestSledState| {
                // Boundary NTP zones should be placeable unless every sled
                // already has one. Other zone types should be placeable unless
                // every sled already has a zone of that kind on every disk.
                if kind == DiscretionaryOmicronZone::BoundaryNtp {
                    usize::min(1, sled_state.num_zpools)
                } else {
                    sled_state.num_zpools
                }
            };

            for (sled_id, sled_state) in self.sleds.iter() {
                if sled_state.count_zones_of_kind(kind)
                    < max_this_kind_for_sled(sled_state)
                {
                    return Err(format!(
                        "sled {sled_id} is eligible for {kind:?} placement"
                    ));
                }
            }
            Ok(())
        }
    }

    #[proptest]
    fn test_place_omicron_zones(input: ArbitraryTestInput) {
        let mut input = TestInput::from(input);

        let mut placer =
            OmicronZonePlacement::new(input.state.sleds.iter().map(
                |(&sled_id, sled_state)| OmicronZonePlacementSledState {
                    sled_id,
                    num_zpools: sled_state.num_zpools,
                    discretionary_zones: sled_state.zones.clone(),
                },
            ));

        for z in input.zones_to_place {
            println!("placing {z:?}");
            match placer.place_zone(z) {
                Ok(sled_id) => {
                    input
                        .state
                        .validate_placement(sled_id, z)
                        .expect("valid placement");
                }
                Err(PlacementError::NoSledsEligible { zone_kind }) => {
                    assert_eq!(zone_kind, z);
                    input
                        .state
                        .validate_no_placement_possible(z)
                        .expect("no placement possible");
                }
            }
        }
    }
}
