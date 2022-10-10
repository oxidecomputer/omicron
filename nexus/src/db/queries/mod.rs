// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Specialized queries for inserting database records, usually to maintain
//! complex invariants that are most accurately expressed in a single query.

pub mod external_ip;
pub mod ip_pool;
#[macro_use]
mod next_item;
pub mod network_interface;
pub mod region_allocation;
pub mod virtual_resource_provisioning_update;
pub mod vpc;
pub mod vpc_subnet;
