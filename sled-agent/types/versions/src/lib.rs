// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Versioned types for the Sled Agent API.
//!
//! # Organization
//!
//! Types are organized based on the rules outlined in [RFD
//! 619](https://rfd.shared.oxide.computer/rfd/0619).

#[path = "bootstrap_initial/mod.rs"]
pub mod bootstrap_v1;
pub mod latest;
#[path = "initial/mod.rs"]
pub mod v1;
#[path = "add_dual_stack_shared_network_interfaces/mod.rs"]
pub mod v10;
#[path = "add_switch_zone_operator_policy/mod.rs"]
pub mod v3;
#[path = "add_nexus_lockstep_port_to_inventory/mod.rs"]
pub mod v4;
#[path = "add_probe_put_endpoint/mod.rs"]
pub mod v6;
#[path = "multicast_support/mod.rs"]
pub mod v7;
#[path = "delegate_zvol_to_propolis/mod.rs"]
pub mod v9;
