// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::Activator;

/// Interface for activating various background tasks and read data that they
/// expose to Nexus at-large
pub struct BackgroundTasks {
    // Handles to activate specific background tasks
    pub task_internal_dns_config: Activator,
    pub task_internal_dns_servers: Activator,
    pub task_external_dns_config: Activator,
    pub task_external_dns_servers: Activator,
    pub task_metrics_producer_gc: Activator,
    pub task_external_endpoints: Activator,
    pub task_nat_cleanup: Activator,
    pub task_bfd_manager: Activator,
    pub task_inventory_collection: Activator,
    pub task_inventory_loader: Activator,
    pub task_support_bundle_collector: Activator,
    pub task_physical_disk_adoption: Activator,
    pub task_decommissioned_disk_cleaner: Activator,
    pub task_phantom_disks: Activator,
    pub task_blueprint_loader: Activator,
    pub task_blueprint_planner: Activator,
    pub task_blueprint_executor: Activator,
    pub task_blueprint_rendezvous: Activator,
    pub task_crdb_node_id_collector: Activator,
    pub task_service_zone_nat_tracker: Activator,
    pub task_switch_port_settings_manager: Activator,
    pub task_v2p_manager: Activator,
    pub task_region_replacement: Activator,
    pub task_region_replacement_driver: Activator,
    pub task_instance_watcher: Activator,
    pub task_instance_updater: Activator,
    pub task_instance_reincarnation: Activator,
    pub task_service_firewall_propagation: Activator,
    pub task_abandoned_vmm_reaper: Activator,
    pub task_vpc_route_manager: Activator,
    pub task_saga_recovery: Activator,
    pub task_lookup_region_port: Activator,
    pub task_region_snapshot_replacement_start: Activator,
    pub task_region_snapshot_replacement_garbage_collection: Activator,
    pub task_region_snapshot_replacement_step: Activator,
    pub task_region_snapshot_replacement_finish: Activator,
    pub task_tuf_artifact_replication: Activator,
    pub task_tuf_repo_pruner: Activator,
    pub task_read_only_region_replacement_start: Activator,
    pub task_alert_dispatcher: Activator,
    pub task_webhook_deliverator: Activator,
    pub task_sp_ereport_ingester: Activator,
    pub task_reconfigurator_config_loader: Activator,
    pub task_multicast_group_reconciler: Activator,

    // Handles to activate background tasks that do not get used by Nexus
    // at-large.  These background tasks are implementation details as far as
    // the rest of Nexus is concerned.  These handles don't even really need to
    // be here, but it's convenient.
    pub task_internal_dns_propagation: Activator,
    pub task_external_dns_propagation: Activator,
}

impl BackgroundTasks {
    /// Activate the specified background task
    ///
    /// If the task is currently running, it will be activated again when it
    /// finishes.
    pub fn activate(&self, task: &Activator) {
        task.activate();
    }
}
