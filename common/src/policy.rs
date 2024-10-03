// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Fleet policy related functionality used by both Reconfigurator and RSS.

/// The amount of redundancy for boundary NTP servers.
pub const BOUNDARY_NTP_REDUNDANCY: usize = 2;

/// The amount of redundancy for Nexus services.
///
/// This is used by both RSS (to distribute the initial set of services) and the
/// Reconfigurator (to know whether to add new Nexus zones)
pub const NEXUS_REDUNDANCY: usize = 3;

// The amount of redundancy for Oximeter services.
///
/// This is used by both RSS (to distribute the initial set of services) and the
/// Reconfigurator (to know whether to add new Oximeter zones)
pub const OXIMETER_REDUNDANCY: usize = 1;

/// The amount of redundancy for CockroachDb services.
///
/// This is used by both RSS (to distribute the initial set of services) and the
/// Reconfigurator (to know whether to add new crdb zones)
pub const COCKROACHDB_REDUNDANCY: usize = 5;

/// The amount of redundancy for internal DNS servers.
///
/// Must be less than or equal to RESERVED_INTERNAL_DNS_REDUNDANCY.
pub const INTERNAL_DNS_REDUNDANCY: usize = 3;

/// The potential number of internal DNS servers we hold reserved for future
/// growth.
///
/// Any consumers interacting with "the number of internal DNS servers" (e.g.,
/// to construct a DNS client) should operate in terms of
/// [`INTERNAL_DNS_REDUNDANCY`]. This constant should only be used to reserve
/// space where we could increase `INTERNAL_DNS_REDUNDANCY` up to at most this
/// value.
pub const RESERVED_INTERNAL_DNS_REDUNDANCY: usize = 5;

/// The amount of redundancy for clickhouse servers
///
/// Clickhouse servers contain lazily replicated data
pub const CLICKHOUSE_SERVER_REDUNDANCY: usize = 3;

/// The amount of redundancy for clickhouse keepers
///
/// Keepers maintain strongly consistent metadata about data replication
pub const CLICKHOUSE_KEEPER_REDUNDANCY: usize = 5;
