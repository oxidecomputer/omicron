// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `MULTIPLE_ZONE_EXTERNAL_IPS` of the Sled Agent API.
//!
//! This version lets a service zone carry more than one external IP: Nexus and
//! external DNS zones now hold a set of external IPs (`ZoneExternalIps`), and
//! boundary NTP holds a single- or dual-stack SNAT configuration
//! (`ZoneSnatConfig`).

pub mod inventory;
