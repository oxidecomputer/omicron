// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background tasks

mod bfd;
mod blueprint_execution;
mod blueprint_load;
mod common;
mod dns_config;
mod dns_propagation;
mod dns_servers;
mod external_endpoints;
mod init;
mod inventory_collection;
mod nat_cleanup;
mod phantom_disks;
mod physical_storage_monitor;
mod region_replacement;
mod status;
mod sync_service_zone_nat;

pub use init::BackgroundTasks;
