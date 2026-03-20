// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `ADD_ICMPV6_FIREWALL_SUPPORT` of the Sled Agent API.
//!
//! Adds `Icmp6` as a valid firewall rule protocol, mirroring the existing
//! `Icmp` variant which applies to IPv4 only.

pub mod firewall_rules;
pub mod instance;
