// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A mechanism for allocating clickhouse keeper nodes for clustered clickhouse setups

use clickhouse_admin_types::KeeperId;
use nexus_types::deployment::{
    blueprint_zone_type, Blueprint, BlueprintZoneFilter, BlueprintZoneType,
    PlanningInput,
};
use nexus_types::inventory::Collection;
use omicron_uuid_kinds::OmicronZoneUuid;
use slog::{error, Logger};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

struct KeeperZones {
    in_service:
        BTreeMap<OmicronZoneUuid, blueprint_zone_type::ClickhouseKeeper>,
    expunged: BTreeMap<OmicronZoneUuid, blueprint_zone_type::ClickhouseKeeper>,
}

/// Only a single keeper may be added or removed from the clickhouse keeper
/// cluster at a time. If an addition or removal is in progress, we must wait to
/// change our desired state of the cluster.
pub struct KeeperAllocator<'a> {
    log: Logger,
    parent_blueprint: &'a Blueprint,
    input: &'a PlanningInput,
    inventory: &'a Collection,
}

/// Errors encountered when trying to plan keeper deployments
#[derive(Debug, Error)]
pub enum KeeperAllocationError {
    #[error("a clickhouse cluster configuration has not been created")]
    NoConfig,
    #[error("failed to retrieve clickhouse keeper membership from inventory")]
    NoInventory,
    #[error("cannot add more than one keeper at a time: {added_keepers:?}")]
    BadMembershipChange { added_keepers: BTreeSet<KeeperId> },
}

impl<'a> KeeperAllocator<'a> {
    /// Generate the next configuration of our keeper cluster membership
    ///
    /// If there is a configuration change in progress or the configuration
    /// matches our desired state then the membership will not change, and we'll
    /// report `Ok(None)`. A configuration matches our desired state when all
    /// in service keeper zones have keepers in the clickhouse keeper cluster
    /// membership and all expunged zones do not have keepers in the clickhouse
    /// keeper cluster membership.
    pub fn plan(
        &self,
    ) -> Result<Option<BTreeSet<KeeperId>>, KeeperAllocationError> {
        let parent_keeper_zones = self.all_keeper_zones_in_parent_blueprint();
        let parent_keeper_membership = self
            .desired_keeper_membership_in_parent_blueprint(
                &parent_keeper_zones,
            )?;

        // Does inventory reflect our desired keeper membership? If not, the
        // executor will keep trying until it does. Note that this may end up
        // requiring support, but if so it's because of a bug in clickhouse and
        // so trying to automate that away at this stage is probably not worth
        // it.
        let Some((_, inventory_membership)) =
            self.inventory.latest_clickhouse_keeper_membership()
        else {
            // We can't get inventory so we assume that a reconfiguration must
            // be in progress for safety purposes.
            return Err(KeeperAllocationError::NoInventory);
        };

        let mut desired_membership: BTreeSet<_> =
            parent_keeper_membership.keys().cloned().collect();

        if desired_membership != inventory_membership.raft_config {
            // We're still trying to reach our desired state We want to ensure
            // however, that if we are currently trying to add a node, that we
            // have not expunged the zone of the keeper that we are trying to
            // add. This can happen for a number of reasons, and without this
            // check we would not be able to make forward progress.
            let added_keepers: BTreeSet<_> = desired_membership
                .difference(&inventory_membership.raft_config)
                .cloned()
                .collect();

            // We should only be adding or removing 1 keeper at a time
            if added_keepers.len() > 1 {
                error!(
                    self.log,
                    concat!(
                        "Keeper membership error: attempting to add",
                        "more than one keeper to cluster: {:?}"
                    ),
                    added_keepers
                );
                return Err(KeeperAllocationError::BadMembershipChange {
                    added_keepers,
                });
            }

            // If we are not adding a keeper than we are done.
            // The executor is trying to remove one from the cluster.
            //
            // This should always succeed eventually, barring a bug in
            // clickhouse.
            //
            // TODO: Should we ensure that we don't have a planner bug
            // by checking to see if we have more than one keeper to remove?
            if added_keepers.len() == 0 {
                return Ok(None);
            }

            // We must be adding a keeper. Let's make sure that the zone
            // for this keeper has not already been expunged.
            //
            // Unwraps are safe because we know there is one added keeper and we
            // generated it from the keys in `parent_keeper_membership`.
            let zone_id = parent_keeper_membership
                .get(added_keepers.first().unwrap())
                .unwrap();
            if parent_keeper_zones.expunged.get(zone_id).is_some() {
                // We need to change our desired state. A node we were trying
                // to add was expunged. Our desired state is equivalent to
                // the inventory so let's set it to that and go through one
                // more round of planning. If the sled was added to the keeper
                // cluster in the meantime this will allow it to get removed in
                // the next execution.
                return Ok(Some(inventory_membership.raft_config.clone()));
            } else {
                // We are still trying to add this keeper
                return Ok(None);
            }
        }

        // Our desired membership from the parent blueprint matches the
        // inventory.
        //
        // Do we need to add or remove any nodes? We expunge first, because
        // those nodes are already gone and don't help our quorum.
        for keeper_zone in parent_keeper_zones.expunged.values() {
            if desired_membership.contains(&keeper_zone.keeper_id) {
                // Let's expunge the first match we see
                desired_membership.remove(&keeper_zone.keeper_id);
                return Ok(Some(desired_membership));
            }
        }

        for keeper_zone in parent_keeper_zones.in_service.values() {
            if !desired_membership.contains(&keeper_zone.keeper_id) {
                // Let's add the first match we see
                desired_membership.insert(keeper_zone.keeper_id);
                return Ok(Some(desired_membership));
            }
        }

        // We have reached our desired state and there are no actions
        // to take.
        Ok(None)
    }

    /// Is there an ongoing membership change

    /// Get all `ClickhouseKeeper` zones from the parent blueprint
    fn all_keeper_zones_in_parent_blueprint(&self) -> KeeperZones {
        let in_service: BTreeMap<_, _> = self
            .parent_blueprint
            .all_omicron_zones(BlueprintZoneFilter::ShouldBeRunning)
            .filter_map(|(_, bp_zone_config)| {
                if let BlueprintZoneType::ClickhouseKeeper(keeper_zone_type) =
                    &bp_zone_config.zone_type
                {
                    Some((bp_zone_config.id, keeper_zone_type.clone()))
                } else {
                    None
                }
            })
            .collect();
        let expunged: BTreeMap<_, _> = self
            .parent_blueprint
            .all_omicron_zones(BlueprintZoneFilter::ShouldBeRunning)
            .filter_map(|(_, bp_zone_config)| {
                if let BlueprintZoneType::ClickhouseKeeper(keeper_zone_type) =
                    &bp_zone_config.zone_type
                {
                    Some((bp_zone_config.id, keeper_zone_type.clone()))
                } else {
                    None
                }
            })
            .collect();
        KeeperZones { in_service, expunged }
    }

    /// Get all `KeeperId`s that we wanted to be part of the clickhouse keeper
    /// cluster membership in the parent blueprint.
    ///
    /// This is our desired state, reflected in the parent blueprint, not what
    /// the actual membership is as reflected by the inventory.
    ///
    /// Also note that a zone may already be expunged, and yet we may still be
    /// trying to add it as a keeper member in the executor. We'll handle this in the
    /// `plan` method above.
    ///
    /// Panics if there is a clickhouse keeper zone id in
    /// `self.clickhouse_cluster_config.zones_with_keepers`, but not a
    /// corresponding omicron zone in `all_keeper_zones`.
    fn desired_keeper_membership_in_parent_blueprint(
        &self,
        all_keeper_zones: &KeeperZones,
    ) -> Result<BTreeMap<KeeperId, OmicronZoneUuid>, KeeperAllocationError>
    {
        // Get our current clickhouse cluster configuration if there is one
        let Some(clickhouse_cluster_config) =
            &self.parent_blueprint.clickhouse_cluster_config
        else {
            return Err(KeeperAllocationError::NoConfig);
        };

        Ok(clickhouse_cluster_config
            .zones_with_keepers
            .iter()
            .map(|zone_id| {
                if let Some(keeper) = all_keeper_zones.in_service.get(zone_id) {
                    (keeper.keeper_id, *zone_id)
                } else {
                    // It is an invariant violation/programmer error if there is
                    // not a keeper zone for each keeper id.
                    //
                    // We panic here as planning is broken at this point.
                    if let Some(keeper) = all_keeper_zones.expunged.get(zone_id)
                    {
                        (keeper.keeper_id, *zone_id)
                    } else {
                        let msg = format!(
                            concat!(
                                "Failed to find zone id {}",
                                "when looking up keeper id"
                            ),
                            zone_id
                        );
                        error!(self.log, "{msg}");
                        panic!("{msg}");
                    }
                }
            })
            .collect())
    }
}
