// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background tasks

mod abandoned_vmm_reaper;
mod bfd;
mod blueprint_execution;
mod blueprint_load;
mod common;
mod dns_config;
mod dns_propagation;
mod dns_servers;
mod external_endpoints;
mod init;
mod instance_updater;
mod instance_watcher;
mod inventory_collection;
mod metrics_producer_gc;
mod nat_cleanup;
mod networking;
mod phantom_disks;
mod physical_disk_adoption;
mod region_replacement;
mod service_firewall_rules;
mod status;
mod sync_service_zone_nat;
mod sync_switch_configuration;
mod v2p_mappings;

pub use init::BackgroundTasks;
