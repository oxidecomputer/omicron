// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `RENAME_FIREWALL_ICMP_PROTOCOLS` of the Sled Agent API.
//!
//! Renames the firewall rule ICMP protocols from `icmp`/`icmp6` to
//! `icmp_v4`/`icmp_v6`, matching the external API. This re-versions the chain
//! of types that embed `ResolvedVpcFirewallRule`.

pub mod firewall_rules;
pub mod instance;
