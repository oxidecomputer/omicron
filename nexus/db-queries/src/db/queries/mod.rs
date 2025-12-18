// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Specialized queries for inserting database records, usually to maintain
//! complex invariants that are most accurately expressed in a single query.

pub mod disk;
pub mod external_ip;
pub mod external_multicast_group;
pub mod ip_pool;
#[macro_use]
mod next_item;
pub mod network_interface;
pub mod oximeter;
pub mod region_allocation;
pub mod regions_hard_delete;
pub mod sled_reservation;
pub mod virtual_provisioning_collection_update;
pub mod vpc;
pub mod vpc_subnet;

/// SQL used to enable full table scans for the duration of the current
/// transaction.
///
/// We normally disallow table scans in effort to identify scalability issues
/// during development.  There are some rare cases where we do want to do full
/// scans on small tables.  This SQL can be used to re-enable table scans for
/// the duration of the current transaction.
///
/// This should only be used when we know a table will be very small, it's not
/// paginated (if it were paginated, we could scan an index), and there's no
/// good way to limit the query based on an index.
///
/// This SQL appears to have no effect when used outside of a transaction.
/// That's intentional.  We do not want to use `SET` (rather than `SET LOCAL`)
/// here because that would change the behavior for any code that happens to use
/// the same pooled connection after this SQL gets run.
///
/// **BE VERY CAREFUL WHEN USING THIS.**
pub const ALLOW_FULL_TABLE_SCAN_SQL: &str = "set local disallow_full_table_scans = off; \
     set local large_full_scan_rows = 1000;";
