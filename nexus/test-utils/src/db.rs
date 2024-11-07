// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database testing facilities.

/// See the definition of this constant in nexus_db_queries.
///
/// Besides the cases mentioned there, it's also preferable for some ad hoc
/// test-only queries to do table scans rather than add indexes that are only
/// used for the test suite.
pub const ALLOW_FULL_TABLE_SCAN_SQL: &str =
    nexus_db_queries::db::queries::ALLOW_FULL_TABLE_SCAN_SQL;

/// Refer to nexus_db_queries for additional documentation.
///
/// This structure enables callers to construct an arbitrarily
/// populated database, with or without a datastore on top.
pub use nexus_db_queries::db::pub_test_utils::TestDatabase;
