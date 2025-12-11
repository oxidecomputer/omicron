// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for Sled Agent API version 10.
//!
//! This version added dual-stack support to NetworkInterface, changing
//! from a single IP address to `PrivateIpConfig` which can hold both
//! IPv4 and IPv6 addresses.
//!
//! All types in this version use `NetworkInterface` v2 (dual-stack).

pub mod instance;
pub mod inventory;
pub mod probes;
