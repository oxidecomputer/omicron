// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A mechanism for allocating clickhouse keeper and server nodes for clustered
//! clickhouse setups during blueprint planning

use clickhouse_admin_types::{ClickhouseKeeperClusterMembership, KeeperId};
use nexus_types::deployment::{
    Blueprint, BlueprintSledConfig, BlueprintZoneDisposition,
    BlueprintZoneType, ClickhouseClusterConfig,
};
use omicron_uuid_kinds::{OmicronZoneUuid, SledUuid};
use slog::{Logger, error};
use std::collections::BTreeSet;
use thiserror::Error;

// The set of clickhouse server and keeper zones that should be running as
// constructed by the `BlueprintBuilder` in the current planning iteration.
pub struct ClickhouseZonesThatShouldBeRunning {
    pub keepers: BTreeSet<OmicronZoneUuid>,
    pub servers: BTreeSet<OmicronZoneUuid>,
}

impl ClickhouseZonesThatShouldBeRunning {
    pub fn new<'a, I>(zones_by_sled_id: I) -> Self
    where
        I: Iterator<Item = (SledUuid, &'a BlueprintSledConfig)>,
    {
        let mut keepers = BTreeSet::new();
        let mut servers = BTreeSet::new();
        for (_, bp_zone_config) in Blueprint::filtered_zones(
            zones_by_sled_id,
            BlueprintZoneDisposition::is_in_service,
        ) {
            match bp_zone_config.zone_type {
                BlueprintZoneType::ClickhouseKeeper(_) => {
                    keepers.insert(bp_zone_config.id);
                }
                BlueprintZoneType::ClickhouseServer(_) => {
                    servers.insert(bp_zone_config.id);
                }
                _ => (),
            }
        }
        ClickhouseZonesThatShouldBeRunning { keepers, servers }
    }
}

/// A mechanism for allocating clickhouse server and keeper processes
/// to zones
///
/// This is to be used as part of the `BlueprintBuilder` after zones have been
/// allocated.
pub struct ClickhouseAllocator {
    log: Logger,
    parent_config: ClickhouseClusterConfig,
    // The latest clickhouse cluster membership from inventory
    inventory: Option<ClickhouseKeeperClusterMembership>,
}

/// Errors encountered when trying to plan keeper deployments
#[derive(Debug, Error)]
pub enum KeeperAllocationError {
    #[error("cannot add more than one keeper at a time: {added_keepers:?}")]
    BadMembershipChange { added_keepers: BTreeSet<KeeperId> },
}

impl ClickhouseAllocator {
    pub fn new(
        log: Logger,
        clickhouse_cluster_config: ClickhouseClusterConfig,
        inventory: Option<ClickhouseKeeperClusterMembership>,
    ) -> ClickhouseAllocator {
        ClickhouseAllocator {
            log,
            parent_config: clickhouse_cluster_config,
            inventory,
        }
    }

    /// Return a new clickhouse cluster configuration based
    /// on the parent blueprint and inventory
    pub fn plan(
        &self,
        active_clickhouse_zones: &ClickhouseZonesThatShouldBeRunning,
    ) -> Result<ClickhouseClusterConfig, KeeperAllocationError> {
        let mut new_config = self.parent_config.clone();

        // Bump the generation number if the config has changed
        // and return the new configuration.
        let bump_gen_if_necessary =
            |mut new_config: ClickhouseClusterConfig| {
                if new_config.needs_generation_bump(&self.parent_config) {
                    new_config.generation = new_config.generation.next();
                }
                Ok(new_config)
            };

        // First, remove the clickhouse servers that are no longer in service
        new_config.servers.retain(|zone_id, _| {
            active_clickhouse_zones.servers.contains(zone_id)
        });
        // Next, add any new clickhouse servers
        for zone_id in &active_clickhouse_zones.servers {
            if !new_config.servers.contains_key(zone_id) {
                // Allocate a new `ServerId` and map it to the server zone
                new_config.max_used_server_id += 1.into();
                new_config
                    .servers
                    .insert(*zone_id, new_config.max_used_server_id);
            }
        }

        // Now we need to configure the keepers. We can only add or remove
        // one keeper at a time during a reconfiguration.
        //
        // We need to see if we have any keeper inventory so we can compare it
        // with our current configuration and see if any changes are required.
        // If we fail to retrieve any inventory for keepers in the current
        // collection than we must not modify our keeper config, as we don't
        // know whether a configuration is ongoing or not.
        //
        // There is an exception to this rule: on *new* clusters that have
        // keeper zones deployed but do not have any keepers running we can
        // create a full cluster configuration unconditionally. We can add
        // more than one keeper because this is the initial configuration and
        // not a "reconfiguration" that only allows adding or removing one
        // node at a time. Furthermore, we have to start at last one keeper
        // unconditionally in this case because we cannot retrieve keeper
        // inventory if there are no keepers running. Without retrieving
        // inventory, we cannot make further progress.
        let current_keepers: BTreeSet<_> =
            self.parent_config.keepers.values().cloned().collect();
        let Some(inventory_membership) = &self.inventory else {
            // Are we a new cluster ?
            if new_config.max_used_keeper_id == 0.into() {
                // Generate our initial configuration
                for zone_id in &active_clickhouse_zones.keepers {
                    // Allocate a new `KeeperId` and map it to the zone_id
                    new_config.max_used_keeper_id += 1.into();
                    new_config
                        .keepers
                        .insert(*zone_id, new_config.max_used_keeper_id);
                }
            }
            return bump_gen_if_necessary(new_config);
        };

        let inventory_updated = inventory_membership.leader_committed_log_index
            > self.parent_config.highest_seen_keeper_leader_committed_log_index;

        if current_keepers != inventory_membership.raft_config {
            // We know that there is a reconfiguration in progress. If there has
            // been no inventory update to reflect the change yet, then skip the
            // rest of planning.
            if !inventory_updated {
                return bump_gen_if_necessary(new_config);
            }

            // Save the log index for next time through the loop
            new_config.highest_seen_keeper_leader_committed_log_index =
                inventory_membership.leader_committed_log_index;

            // We're still trying to reach our desired state. We want to ensure,
            // however, that if we are currently trying to add a node, that we
            // have not expunged the zone of the keeper that we are trying to
            // add. This can happen for a number of reasons, and without this
            // check we would not be able to make forward progress.
            let mut added_keepers: BTreeSet<_> = current_keepers
                .difference(&inventory_membership.raft_config)
                .cloned()
                .collect();

            // We should only be adding or removing 1 keeper at a time
            if added_keepers.len() > 1 {
                error!(
                    self.log,
                    concat!(
                        "Keeper membership error: attempting to add",
                        "more than one keeper to cluster: {:?}.",
                        "Did keeper membership change out from under us?"
                    ),
                    added_keepers
                );
                return Err(KeeperAllocationError::BadMembershipChange {
                    added_keepers,
                });
            }

            // If we are not adding a keeper then we are done.
            // The executor is trying to remove one from the cluster.
            //
            // This should always succeed eventually, barring a bug in
            // clickhouse.
            //
            if added_keepers.is_empty() {
                return bump_gen_if_necessary(new_config);
            }

            // Let's find the matching zone id for the keeper we are adding
            //
            // Unwrap is fine, because we know we have exactly one added keeper
            // at this point, and it is present in our `parent_config`.
            let added_keeper_id = added_keepers.pop_first().unwrap();
            let (added_zone_id, _) = self
                .parent_config
                .keepers
                .iter()
                .find(|(_, &keeper_id)| keeper_id == added_keeper_id)
                .unwrap();

            // Let's ensure that this zone has not been expunged yet. If it has that means
            // that adding the keeper will never succeed.
            if !active_clickhouse_zones.keepers.contains(added_zone_id) {
                // The zone has been expunged, so we must remove it from our configuration.
                new_config.keepers.remove(added_zone_id);

                // We are now done. We'll fall through to the return below.
                //
                // We only want to make one change to our keeper config at a
                // time. It is possible that there is a race where the keeper
                // was actually added to the rust configuration after the
                // inventory was read but before the zone was expunged. In that
                // case, we'll want the executor to go ahead and remove it from
                // the keeper cluster membershp when it runs next.
            }

            // We are done
            return bump_gen_if_necessary(new_config);
        }

        // Our desired membership from the parent blueprint matches the
        // inventory.

        // Do we need to remove any nodes for zones that have been expunged?
        //
        // We remove first, because the zones are already gone and therefore
        // don't help our quorum.
        for (zone_id, _) in &self.parent_config.keepers {
            if !active_clickhouse_zones.keepers.contains(&zone_id) {
                // Remove the keeper for the first expunged zone we see.
                // Remember, we only do one keeper membership change at time.
                new_config.keepers.remove(zone_id);

                if inventory_updated {
                    // Save the log from inventory because we are going to need
                    // to use it to see if inventory has changed next time through
                    // the planner.
                    new_config.highest_seen_keeper_leader_committed_log_index =
                        inventory_membership.leader_committed_log_index;
                }

                return bump_gen_if_necessary(new_config);
            }
        }

        // Do we need to add any nodes to in-service zones that don't have them
        for zone_id in &active_clickhouse_zones.keepers {
            if !new_config.keepers.contains_key(zone_id) {
                // Allocate a new `KeeperId` and map it to the keeper zone
                new_config.max_used_keeper_id += 1.into();
                new_config
                    .keepers
                    .insert(*zone_id, new_config.max_used_keeper_id);

                if inventory_updated {
                    // Save the log from inventory because we are going to need
                    // to use it to see if inventory has changed next time through
                    // the planner.
                    new_config.highest_seen_keeper_leader_committed_log_index =
                        inventory_membership.leader_committed_log_index;
                }

                return bump_gen_if_necessary(new_config);
            }
        }

        // We possibly added or removed clickhouse servers, but not keepers.
        bump_gen_if_necessary(new_config)
    }

    pub fn parent_config(&self) -> &ClickhouseClusterConfig {
        &self.parent_config
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use clickhouse_admin_types::ServerId;
    use omicron_common::api::external::Generation;
    use omicron_test_utils::dev::test_setup_log;
    use std::collections::BTreeMap;

    fn initial_config(
        n_keeper_zones: u64,
        n_server_zones: u64,
        n_keepers: u64,
        n_servers: u64,
    ) -> (ClickhouseZonesThatShouldBeRunning, ClickhouseClusterConfig) {
        // Generate the maximum number of (`ZoneId`, `KeeperId`) pairs
        // Note that we order by `KeeperId` so that we have determinism during
        // tests
        let all_keepers: BTreeMap<KeeperId, OmicronZoneUuid> =
            (1..=u64::max(n_keeper_zones, n_keepers))
                .map(|i| (KeeperId(i), OmicronZoneUuid::new_v4()))
                .collect();

        // Generate the maximum number of (`ZoneId`, `ServerId`) pairs
        let all_servers: BTreeMap<ServerId, OmicronZoneUuid> =
            (1..=u64::max(n_server_zones, n_servers))
                .map(|i| (ServerId(i), OmicronZoneUuid::new_v4()))
                .collect();

        // Pare the zones down to the actual number we want
        let keeper_zone_ids: BTreeSet<_> = all_keepers
            .values()
            .take(n_keeper_zones as usize)
            .cloned()
            .collect();
        let server_zone_ids: BTreeSet<_> = all_servers
            .values()
            .take(n_server_zones as usize)
            .cloned()
            .collect();

        let active_clickhouse_zones = ClickhouseZonesThatShouldBeRunning {
            keepers: keeper_zone_ids.clone(),
            servers: server_zone_ids.clone(),
        };

        // Pare the keeper and server configs down to the actual number we want
        let keepers: BTreeMap<_, _> = all_keepers
            .into_iter()
            .take(n_keepers as usize)
            .map(|(k, z)| (z, k))
            .collect();
        let servers = all_servers
            .into_iter()
            .take(n_servers as usize)
            .map(|(s, z)| (z, s))
            .collect();

        let parent_clickhouse_cluster_config = ClickhouseClusterConfig {
            generation: Generation::new(),
            max_used_server_id: ServerId(n_servers),
            max_used_keeper_id: KeeperId(n_keepers),
            cluster_name: "test_cluster".to_string(),
            cluster_secret: "test_secret".to_string(),
            highest_seen_keeper_leader_committed_log_index: 1,
            keepers,
            servers,
        };

        (active_clickhouse_zones, parent_clickhouse_cluster_config)
    }

    #[test]
    fn no_changes_needed() {
        static TEST_NAME: &str = "clickhouse_allocator_no_changes_needed";
        let logctx = test_setup_log(TEST_NAME);

        let (n_keeper_zones, n_server_zones, n_keepers, n_servers) =
            (3, 2, 3, 2);

        let (active_clickhouse_zones, parent_config) = initial_config(
            n_keeper_zones,
            n_server_zones,
            n_keepers,
            n_servers,
        );

        let inventory = Some(ClickhouseKeeperClusterMembership {
            queried_keeper: KeeperId(1),
            leader_committed_log_index: 2,
            raft_config: parent_config.keepers.values().cloned().collect(),
        });

        let mut allocator = ClickhouseAllocator {
            log: logctx.log.clone(),
            parent_config: parent_config.clone(),
            inventory,
        };

        // Our clickhouse cluster config should not have changed
        let new_config = allocator.plan(&active_clickhouse_zones).unwrap();

        // Note that we cannot directly check equality here and
        // in a bunch of the test cases below, because we bump the
        // `highest_seen_keeper_leader_committed_log_index` in the `new_config`
        // during `plan` calls, even though no configuration has actually
        // changed.
        assert!(!new_config.needs_generation_bump(&parent_config));

        // Running again without changing the inventory should be idempotent
        allocator.parent_config = new_config;
        let new_config = allocator.plan(&active_clickhouse_zones).unwrap();
        assert_eq!(new_config, allocator.parent_config);

        logctx.cleanup_successful();
    }

    #[test]
    fn move_from_3_to_5_keepers() {
        static TEST_NAME: &str =
            "clickhouse_allocator_move_from_3_to_5_keepers";
        let logctx = test_setup_log(TEST_NAME);
        let (n_keeper_zones, n_server_zones, n_keepers, n_servers) =
            (5, 2, 3, 2);

        let (active_clickhouse_zones, parent_config) = initial_config(
            n_keeper_zones,
            n_server_zones,
            n_keepers,
            n_servers,
        );

        let inventory = Some(ClickhouseKeeperClusterMembership {
            queried_keeper: KeeperId(1),
            leader_committed_log_index: 2,
            raft_config: parent_config.keepers.values().cloned().collect(),
        });

        // We have 5 in service keeper zones, but only three keepers in our
        // cluster configuration. All three show up in inventory, therefore the
        // allocator should allocate one more keeper.
        let mut allocator = ClickhouseAllocator {
            log: logctx.log.clone(),
            parent_config: parent_config.clone(),
            inventory,
        };

        // Did our new config change as we expect?
        let new_config = allocator.plan(&active_clickhouse_zones).unwrap();
        assert_eq!(new_config.generation, Generation::from_u32(2));
        assert_eq!(new_config.generation, parent_config.generation.next());
        assert_eq!(new_config.max_used_keeper_id, 4.into());
        assert_eq!(
            new_config.max_used_keeper_id,
            parent_config.max_used_keeper_id + 1.into()
        );
        assert_eq!(new_config.keepers.len(), 4);
        assert_eq!(new_config.keepers.len(), parent_config.keepers.len() + 1);
        assert_eq!(new_config.servers.len(), 2);
        assert_eq!(new_config.servers.len(), parent_config.servers.len());

        // Let's allocate again, but with the config output from our last round.
        // Our inventory still hasn't changed, and so the output from this round
        // should be identical to the last.
        //
        // Note that We mutate the allocator here and in subsequent tests, as
        // this is the shortest way to test what we want. However, planning
        // itself does not modify  the allocator and a new one is created by the
        // `BlueprintBuilder` on each planning round.
        allocator.parent_config = new_config;
        let new_config = allocator.plan(&active_clickhouse_zones).unwrap();
        assert_eq!(new_config, allocator.parent_config);

        // Now let's update our inventory to reflect the new keeper. This should
        // trigger the planner to add a 5th keeper.
        allocator.inventory.as_mut().unwrap().raft_config.insert(4.into());
        allocator.inventory.as_mut().unwrap().leader_committed_log_index += 1;
        let new_config = allocator.plan(&active_clickhouse_zones).unwrap();
        assert_eq!(new_config.generation, Generation::from_u32(3));
        assert_eq!(
            new_config.generation,
            allocator.parent_config.generation.next()
        );
        assert_eq!(new_config.max_used_keeper_id, 5.into());
        assert_eq!(
            new_config.max_used_keeper_id,
            allocator.parent_config.max_used_keeper_id + 1.into()
        );
        assert_eq!(new_config.keepers.len(), 5);
        assert_eq!(
            new_config.keepers.len(),
            allocator.parent_config.keepers.len() + 1
        );
        assert_eq!(new_config.servers.len(), 2);
        assert_eq!(
            new_config.servers.len(),
            allocator.parent_config.servers.len()
        );

        // Let's run that plan aginst the new config without changing the
        // inventory raft config. We should end up with the same config.
        allocator.parent_config = new_config;
        allocator.inventory.as_mut().unwrap().leader_committed_log_index += 1;
        let new_config = allocator.plan(&active_clickhouse_zones).unwrap();
        assert!(!new_config.needs_generation_bump(&allocator.parent_config));

        // Now let's modify the inventory to reflect that the 5th keeper node
        // was added to the cluster.
        //
        // We should see that the configuration doesn't change because all of
        // our keeper zones have a keeper that is part of the cluster.
        allocator.inventory.as_mut().unwrap().raft_config.insert(5.into());
        allocator.inventory.as_mut().unwrap().leader_committed_log_index += 1;
        let new_config = allocator.plan(&active_clickhouse_zones).unwrap();
        assert!(!new_config.needs_generation_bump(&allocator.parent_config));

        logctx.cleanup_successful();
    }

    #[test]
    fn expunge_2_of_5_keeper_zones() {
        static TEST_NAME: &str = "clickhouse_allocator_expunge_2_of_5_keepers";
        let logctx = test_setup_log(TEST_NAME);

        let (n_keeper_zones, n_server_zones, n_keepers, n_servers) =
            (5, 2, 5, 2);

        let (mut active_clickhouse_zones, parent_config) = initial_config(
            n_keeper_zones,
            n_server_zones,
            n_keepers,
            n_servers,
        );

        let inventory = Some(ClickhouseKeeperClusterMembership {
            queried_keeper: KeeperId(1),
            leader_committed_log_index: 2,
            raft_config: parent_config.keepers.values().cloned().collect(),
        });

        let mut allocator = ClickhouseAllocator {
            log: logctx.log.clone(),
            parent_config: parent_config.clone(),
            inventory,
        };

        // Our clickhouse cluster config should not have changed
        // We have 5 keepers and 5 zones and all of them are in the inventory
        let new_config = allocator.plan(&active_clickhouse_zones).unwrap();
        assert!(!new_config.needs_generation_bump(&parent_config));

        // Now expunge 2 of the 5 keeper zones by removing them from the
        // in-service zones
        active_clickhouse_zones.keepers.pop_first();
        active_clickhouse_zones.keepers.pop_first();

        // Running the planner should remove one of the keepers from the new config
        let new_config = allocator.plan(&active_clickhouse_zones).unwrap();
        assert_eq!(new_config.generation, Generation::from_u32(2));
        assert_eq!(
            new_config.generation,
            allocator.parent_config.generation.next()
        );
        assert_eq!(new_config.max_used_keeper_id, 5.into());
        assert_eq!(
            new_config.max_used_keeper_id,
            allocator.parent_config.max_used_keeper_id
        );
        assert_eq!(new_config.keepers.len(), 4);
        assert_eq!(
            new_config.keepers.len(),
            allocator.parent_config.keepers.len() - 1
        );
        assert_eq!(new_config.servers.len(), 2);
        assert_eq!(
            new_config.servers.len(),
            allocator.parent_config.servers.len()
        );

        // Planning with the new config should result in the same output,
        // since the inventory hasn't reflected the change
        allocator.parent_config = new_config;
        allocator.inventory.as_mut().unwrap().leader_committed_log_index += 1;
        let new_config = allocator.plan(&active_clickhouse_zones).unwrap();
        assert!(!new_config.needs_generation_bump(&allocator.parent_config));

        // Reflecting the new config in inventory should remove another keeper
        allocator.inventory.as_mut().unwrap().raft_config =
            new_config.keepers.values().cloned().collect();
        allocator.inventory.as_mut().unwrap().leader_committed_log_index += 1;
        let new_config = allocator.plan(&active_clickhouse_zones).unwrap();

        assert_eq!(new_config.generation, Generation::from_u32(3));
        assert_eq!(
            new_config.generation,
            allocator.parent_config.generation.next()
        );
        assert_eq!(new_config.max_used_keeper_id, 5.into());
        assert_eq!(
            new_config.max_used_keeper_id,
            allocator.parent_config.max_used_keeper_id
        );
        assert_eq!(new_config.keepers.len(), 3);
        assert_eq!(
            new_config.keepers.len(),
            allocator.parent_config.keepers.len() - 1
        );
        assert_eq!(new_config.servers.len(), 2);
        assert_eq!(
            new_config.servers.len(),
            allocator.parent_config.servers.len()
        );

        // Running with the same inventory and new config will result in no
        // change, because the inventory doesn't reflect the removed keeper
        allocator.parent_config = new_config;
        allocator.inventory.as_mut().unwrap().leader_committed_log_index += 1;
        let new_config = allocator.plan(&&active_clickhouse_zones).unwrap();
        assert!(!new_config.needs_generation_bump(&allocator.parent_config));

        // Reflecting the keeper removal in inventory should also result in no
        // change since the number of keepers matches the number of zones
        allocator.inventory.as_mut().unwrap().raft_config =
            new_config.keepers.values().cloned().collect();
        allocator.inventory.as_mut().unwrap().leader_committed_log_index += 1;
        allocator.parent_config = new_config;
        let new_config = allocator.plan(&active_clickhouse_zones).unwrap();
        assert!(!new_config.needs_generation_bump(&allocator.parent_config));

        logctx.cleanup_successful();
    }

    #[test]
    fn expunge_a_different_keeper_while_adding_keeper() {
        static TEST_NAME: &str = "clickhouse_allocator_expunge_a_different_keeper_while_adding_keeper";
        let logctx = test_setup_log(TEST_NAME);

        let (n_keeper_zones, n_server_zones, n_keepers, n_servers) =
            (5, 2, 4, 2);

        let (mut active_clickhouse_zones, parent_config) = initial_config(
            n_keeper_zones,
            n_server_zones,
            n_keepers,
            n_servers,
        );

        let inventory = Some(ClickhouseKeeperClusterMembership {
            queried_keeper: KeeperId(1),
            leader_committed_log_index: 2,
            raft_config: parent_config.keepers.values().cloned().collect(),
        });

        let mut allocator = ClickhouseAllocator {
            log: logctx.log.clone(),
            parent_config,
            inventory,
        };

        // First run the planner to add a 5th keeper to our config
        assert_eq!(allocator.parent_config.keepers.len(), 4);
        let new_config = allocator.plan(&active_clickhouse_zones).unwrap();
        assert_eq!(new_config.keepers.len(), 5);

        // Pick one of the keepers currently in our inventory, find the zone
        // for it, and expunge it
        let keeper_to_expunge = allocator
            .inventory
            .as_ref()
            .unwrap()
            .raft_config
            .first()
            .cloned()
            .unwrap();
        let zone_to_expunge = allocator
            .parent_config
            .keepers
            .iter()
            .find(|(_, &keeper_id)| keeper_id == keeper_to_expunge)
            .map(|(zone_id, _)| *zone_id)
            .unwrap();
        active_clickhouse_zones.keepers.remove(&zone_to_expunge);

        // Bump the inventory commit index so we guarantee we perform the keeper
        // checks
        allocator.inventory.as_mut().unwrap().leader_committed_log_index += 1;

        // Use our last new configuration as the parent
        allocator.parent_config = new_config;

        // Run the plan. Our configuration should stay the same because we can
        // only add or remove one keeper node from the cluster at a time and we
        // are already in the process of adding a node.
        let new_config = allocator.plan(&active_clickhouse_zones).unwrap();
        assert!(!new_config.needs_generation_bump(&allocator.parent_config));

        // Now we change the inventory to reflect the addition of the node to
        // the cluster. This should result in the expunged zone removing the
        // keeper from configuration.
        allocator.parent_config = new_config;
        allocator.inventory.as_mut().unwrap().leader_committed_log_index += 1;
        allocator.inventory.as_mut().unwrap().raft_config.insert(5.into());
        let new_config = allocator.plan(&active_clickhouse_zones).unwrap();
        assert_eq!(new_config.keepers.len(), 4);

        // Let's make sure that the right keeper was expunged.
        assert!(!new_config.keepers.contains_key(&zone_to_expunge));

        // Adding a new zone should allow a new keeper to get provisioned as
        // long as the inventory reflects the last one is gone. Let's set the
        // replicator to reflect that there are 4 keepers and the inventory
        // knows that, and that there is a new zone without a keeper;
        allocator.parent_config = new_config;
        allocator.inventory.as_mut().unwrap().leader_committed_log_index += 1;
        allocator
            .inventory
            .as_mut()
            .unwrap()
            .raft_config
            .remove(&keeper_to_expunge);
        let new_zone_id = OmicronZoneUuid::new_v4();
        active_clickhouse_zones.keepers.insert(new_zone_id);
        let new_config = allocator.plan(&active_clickhouse_zones).unwrap();
        assert_eq!(new_config.keepers.len(), 5);
        assert_eq!(*new_config.keepers.get(&new_zone_id).unwrap(), KeeperId(6));
        assert_eq!(new_config.max_used_keeper_id, 6.into());

        logctx.cleanup_successful();
    }

    #[test]
    fn expunge_keeper_being_added() {
        static TEST_NAME: &str =
            "clickhouse_allocator_expunge_keeper_being_added";
        let logctx = test_setup_log(TEST_NAME);

        let (n_keeper_zones, n_server_zones, n_keepers, n_servers) =
            (5, 2, 4, 2);

        let (mut active_clickhouse_zones, parent_config) = initial_config(
            n_keeper_zones,
            n_server_zones,
            n_keepers,
            n_servers,
        );

        let inventory = Some(ClickhouseKeeperClusterMembership {
            queried_keeper: KeeperId(1),
            leader_committed_log_index: 2,
            raft_config: parent_config.keepers.values().cloned().collect(),
        });

        let mut allocator = ClickhouseAllocator {
            log: logctx.log.clone(),
            parent_config,
            inventory,
        };

        // First run the planner to add a 5th keeper to our config
        assert_eq!(allocator.parent_config.keepers.len(), 4);
        let new_config = allocator.plan(&active_clickhouse_zones).unwrap();
        assert_eq!(new_config.keepers.len(), 5);

        // Find the zone for our new keeper and expunge it before it is
        // reflected in inventory. An expunged zone in most cases will not be
        // succesfully added. There can be a race here, but that is also ok,
        // because the next run of the executor will then remove the added
        // keeper.
        let zone_to_expunge = new_config
            .keepers
            .iter()
            .find(|(_, &keeper_id)| keeper_id == 5.into())
            .map(|(zone_id, _)| *zone_id)
            .unwrap();
        allocator.parent_config = new_config;
        active_clickhouse_zones.keepers.remove(&zone_to_expunge);

        // Bump the inventory commit index so we guarantee we perform the keeper
        // checks
        allocator.inventory.as_mut().unwrap().leader_committed_log_index += 1;
        let new_config = allocator.plan(&active_clickhouse_zones).unwrap();
        assert_eq!(new_config.keepers.len(), 4);
        assert!(!new_config.keepers.contains_key(&zone_to_expunge));

        logctx.cleanup_successful();
    }

    #[test]
    fn add_3_servers_and_expunge_one_simultaneously() {
        static TEST_NAME: &str =
            "clickhouse_allocator_add_3_servers_and_expunge_one_simultaneously";
        let logctx = test_setup_log(TEST_NAME);

        let (n_keeper_zones, n_server_zones, n_keepers, n_servers) =
            (3, 5, 3, 2);

        let (mut active_clickhouse_zones, parent_config) = initial_config(
            n_keeper_zones,
            n_server_zones,
            n_keepers,
            n_servers,
        );

        let inventory = Some(ClickhouseKeeperClusterMembership {
            queried_keeper: KeeperId(1),
            leader_committed_log_index: 2,
            raft_config: parent_config.keepers.values().cloned().collect(),
        });

        let mut allocator = ClickhouseAllocator {
            log: logctx.log.clone(),
            parent_config,
            inventory,
        };

        let zone_to_expunge =
            *allocator.parent_config.servers.keys().next().unwrap();

        active_clickhouse_zones.servers.remove(&zone_to_expunge);

        // After running the planner we should see 4 servers:
        // Start with 2, expunge 1, add 3 to reach the number of zones we have.
        assert_eq!(allocator.parent_config.servers.len(), 2);
        let new_config = allocator.plan(&active_clickhouse_zones).unwrap();
        assert_eq!(new_config.servers.len(), 4);
        assert_eq!(new_config.max_used_server_id, 5.into());
        assert_eq!(new_config.generation, Generation::from_u32(2));

        // Let's ensure that the zone to expunge isn't actually in the new
        // config
        assert!(!new_config.servers.contains_key(&zone_to_expunge));

        // We can add a new keeper and server at the same time
        let new_keeper_zone = OmicronZoneUuid::new_v4();
        let new_server_id = OmicronZoneUuid::new_v4();
        active_clickhouse_zones.keepers.insert(new_keeper_zone);
        active_clickhouse_zones.servers.insert(new_server_id);
        allocator.parent_config = new_config;
        allocator.inventory.as_mut().unwrap().leader_committed_log_index += 1;
        let new_config = allocator.plan(&active_clickhouse_zones).unwrap();
        assert_eq!(new_config.generation, Generation::from_u32(3));
        assert_eq!(new_config.max_used_server_id, 6.into());
        assert_eq!(new_config.max_used_keeper_id, 4.into());
        assert_eq!(new_config.keepers.len(), 4);
        assert_eq!(new_config.servers.len(), 5);

        logctx.cleanup_successful();
    }

    #[test]
    fn inventory_returns_unexpected_membership() {
        static TEST_NAME: &str =
            "clickhouse_allocator_inventory_returns_unexpected_membership";
        let logctx = test_setup_log(TEST_NAME);

        let (n_keeper_zones, n_server_zones, n_keepers, n_servers) =
            (3, 2, 3, 2);

        let (active_clickhouse_zones, parent_config) = initial_config(
            n_keeper_zones,
            n_server_zones,
            n_keepers,
            n_servers,
        );

        // Let's show an incorrect inventory, where either there
        // was a keeper bug or someone manually removed an extra node.
        let inventory = Some(ClickhouseKeeperClusterMembership {
            queried_keeper: KeeperId(1),
            leader_committed_log_index: 2,
            raft_config: [KeeperId(1)].into_iter().collect(),
        });

        let allocator = ClickhouseAllocator {
            log: logctx.log.clone(),
            parent_config,
            inventory,
        };

        // We expect to get an error back. This can be used by higher level
        // software to trigger alerts, etc... In practice the `BlueprintBuilder`
        // should not change it's config when it receives an error.
        assert!(allocator.plan(&active_clickhouse_zones).is_err());

        logctx.cleanup_successful();
    }
}
