// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A mechanism for allocating clickhouse keeper nodes for clustered clickhouse setups

use nexus_types::deployment::{
    blueprint_zone_type, Blueprint, BlueprintZoneFilter, BlueprintZoneType,
    ClickhouseClusterConfig, PlanningInput,
};
use nexus_types::inventory::Collection;
use slog::Logger;
use thiserror::Error;

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
}

impl<'a> KeeperAllocator<'a> {
    /// Are any membership changes required for the keeper cluster desired state
    ///
    /// This method solely ensures that any zones that should have keepers in the blueprint do,
    /// and any that shouldn't do not. It does not ensure that this desired state has been
    /// reached. That is the goal of the executor, and is reflected in the current inventory.
    fn desired_state_is_valid(&self) -> Result<bool, KeeperAllocationError> {
        // Get our current clickhouse cluster configuration if there is one
        let Some(clickhouse_cluster_config) =
            &self.parent_blueprint.clickhouse_cluster_config
        else {
            return Err(KeeperAllocationError::NoConfig);
        };

        // Have we allocated a keeper process to each of our in-service keeper zones?
        let all_in_service_zones_have_keepers = self
            .parent_blueprint
            .all_omicron_zones(BlueprintZoneFilter::ShouldBeRunning)
            .all(|(_, bp_zone_config)| {
                clickhouse_cluster_config
                    .zones_with_keepers
                    .contains(&bp_zone_config.id)
            });

        // Do we have any allocated keeper processes for expunged zones?
        let all_expunged_zones_do_not_have_keepers = self
            .parent_blueprint
            .all_omicron_zones(BlueprintZoneFilter::Expunged)
            .all(|(_, bp_zone_config)| {
                !clickhouse_cluster_config
                    .zones_with_keepers
                    .contains(&bp_zone_config.id)
            });

        Ok(all_in_service_zones_have_keepers
            && all_expunged_zones_do_not_have_keepers)
    }

    ///  Is there currently a reconfiguration of the clickhouse keeper cluster
    /// membership in progress?
    ///
    // TODO: THIS IS GOING AWAY -mostly
    pub fn is_membership_change_in_progress(&self) -> bool {
        let Some(clickhouse_cluster_config) =
            &self.parent_blueprint.clickhouse_cluster_config
        else {
            // There's no membership change in progress because we haven't
            // deployed a clickhouse cluster at all yet.
            return false;
        };

        // Get the latest clickhouse cluster membership
        let Some((_, membership)) =
            self.inventory.latest_clickhouse_keeper_membership()
        else {
            // We can't retrieve the latest membership and so we assume
            // that a reconfiguration is underway for safety reasons. In
            // any case, if we can't talk to any `clickhouse-admin` servers
            // in the `ClickhouseKeeper` zones then we cannot attempt a
            // reconfiguration.
            return true;
        };

        // Ensure all in service keeper zones are part of the raft configuration
        if !self
            .parent_blueprint
            .all_omicron_zones(BlueprintZoneFilter::ShouldBeRunning)
            .all(|(_, bp_zone_config)| {
                if let BlueprintZoneType::ClickhouseKeeper(
                    blueprint_zone_type::ClickhouseKeeper { keeper_id, .. },
                ) = bp_zone_config.zone_type
                {
                    // Check the inventory to see if a keeper that should be
                    // running is not part of the raft configuration.
                    membership.raft_config.contains(&keeper_id)
                } else {
                    // We ignore other types of zones, so we don't want to fail
                    // our check
                    true
                }
            })
        {
            // We are still attempting to add this node
            //
            // TODO: We need a way to detect if the attempted addition actually
            // failed here. This will involve querying the keeper cluster via
            // inventory in case the configuration doesn't change for a while.
            // I don't know how to get that information out of the keeper yet,
            // except by reading the log files.
            return true;
        }

        // Ensure all expunged zones are no longer part of the configuration
        if !self
            .parent_blueprint
            .all_omicron_zones(BlueprintZoneFilter::Expunged)
            .all(|(_, bp_zone_config)| {
                if let BlueprintZoneType::ClickhouseKeeper(
                    blueprint_zone_type::ClickhouseKeeper { keeper_id, .. },
                ) = bp_zone_config.zone_type
                {
                    !membership.raft_config.contains(&keeper_id)
                } else {
                    // We ignore other types of zones, so we don't want to fail
                    // our check
                    true
                }
            })
        {
            // We are still attempting to remove keepers for expunged zones from
            // the configuration.
            return true;
        }

        // There is no ongoing reconfiguration of the keeper raft cluster
        false
    }
}
