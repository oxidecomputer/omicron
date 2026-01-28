// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `ADD_DUAL_STACK_SHARED_NETWORK_INTERFACES` of the Sled Agent API.
//!
//! This version added dual-stack support to NetworkInterface, changing
//! from a single IP address to `PrivateIpConfig` which can hold both
//! IPv4 and IPv6 addresses.
//!
//! All types in this version use `NetworkInterface` v2 (dual-stack).

pub mod firewall_rules;
pub mod instance;
pub mod inventory;
pub mod probes;
