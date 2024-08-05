// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementations of specific background tasks

pub mod abandoned_vmm_reaper;
pub mod bfd;
pub mod blueprint_execution;
pub mod blueprint_load;
pub mod crdb_node_id_collector;
pub mod decommissioned_disk_cleaner;
pub mod dns_config;
pub mod dns_propagation;
pub mod dns_servers;
pub mod external_endpoints;
pub mod instance_watcher;
pub mod inventory_collection;
pub mod lookup_region_port;
pub mod metrics_producer_gc;
pub mod nat_cleanup;
pub mod networking;
pub mod phantom_disks;
pub mod physical_disk_adoption;
pub mod region_replacement;
pub mod region_replacement_driver;
pub mod saga_recovery;
pub mod service_firewall_rules;
pub mod sync_service_zone_nat;
pub mod sync_switch_configuration;
pub mod v2p_mappings;
pub mod vpc_routes;
