// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `DUAL_STACK_NICS` of the Nexus external API.
//!
//! This version (2026010300) introduces dual-stack network interface support,
//! allowing instances to have both IPv4 and IPv6 addresses on a single NIC.
//! It also adds `ip_version` support for IP pool selection.

pub mod floating_ip;
pub mod instance;
pub mod probe;
