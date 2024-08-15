// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Feet policy related functionality used by both Reconfigurator and RSS.

/// The amount of redundancy for boundary NTP servers.
pub const BOUNDARY_NTP_REDUNDANCY: usize = 2;

/// The amount of redundancy for Nexus services.
///
/// This is used by both RSS (to distribute the initial set of services) and the
/// Reconfigurator (to know whether to add new Nexus zones)
pub const NEXUS_REDUNDANCY: usize = 3;

/// The amount of redundancy for CockroachDb services.
///
/// This is used by both RSS (to distribute the initial set of services) and the
/// Reconfigurator (to know whether to add new crdb zones)
pub const COCKROACHDB_REDUNDANCY: usize = 5;

/// The amount of redundancy for internal DNS servers.
///
/// Must be less than or equal to MAX_DNS_REDUNDANCY.
pub const DNS_REDUNDANCY: usize = 3;

/// The maximum amount of redundancy for DNS servers.
///
/// This determines the number of addresses which are reserved for DNS servers.
pub const MAX_DNS_REDUNDANCY: usize = 5;

/// The amount of redundancy for clickhouse servers
///
/// Clickhouse servers contain lazily replicated data
pub const CLICKHOUSE_SERVER_REDUNDANCY: usize = 3;

/// The amount of redundancy for clickhouse keepers
///
/// Keepers maintain strongly consistent metadata about data replication
pub const CLICKHOUSE_KEEPER_REDUNDANCY: usize = 5;
