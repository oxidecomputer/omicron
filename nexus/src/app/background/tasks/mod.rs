// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementations of specific background tasks

pub mod abandoned_vmm_reaper;
pub mod alert_dispatcher;
pub mod bfd;
pub mod blueprint_execution;
pub mod blueprint_load;
pub mod blueprint_planner;
pub mod blueprint_rendezvous;
pub mod crdb_node_id_collector;
pub mod decommissioned_disk_cleaner;
pub mod dns_config;
pub mod dns_propagation;
pub mod dns_servers;
pub mod ereport_ingester;
pub mod external_endpoints;
pub mod instance_reincarnation;
pub mod instance_updater;
pub mod instance_watcher;
pub mod inventory_collection;
pub mod inventory_load;
pub mod lookup_region_port;
pub mod metrics_producer_gc;
pub mod multicast;
pub mod nat_cleanup;
pub mod networking;
pub mod phantom_disks;
pub mod physical_disk_adoption;
pub mod read_only_region_replacement_start;
pub mod reconfigurator_config;
pub mod region_replacement;
pub mod region_replacement_driver;
pub mod region_snapshot_replacement_finish;
pub mod region_snapshot_replacement_garbage_collect;
pub mod region_snapshot_replacement_start;
pub mod region_snapshot_replacement_step;
pub mod saga_recovery;
pub mod service_firewall_rules;
pub mod support_bundle_collector;
pub mod sync_service_zone_nat;
pub mod sync_switch_configuration;
pub mod tuf_artifact_replication;
pub mod tuf_repo_pruner;
pub mod v2p_mappings;
pub mod vpc_routes;
pub mod webhook_deliverator;
