// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Specialized atomic operations for multicast group members.
//!
//! This module contains specialized database operations for managing multicast
//! group members with different concurrency patterns:
//!
//! ## Operations Provided
//!
//! - **member_attach**: Atomic CTE for initial attachment (addresses TOCTOU)
//!   - Used by instance create saga and instance reconfiguration
//!   - Handles idempotent reactivation from "Left" state
//!   - Validates group is "Active" before attaching
//!   - Uses CTE to atomically validate group + instance + upsert member
//!
//! - **member_reconcile**: Pure CAS operations for reconciliation
//!   - Used by RPW reconciler for background updates
//!   - Updates sled_id and/or transitions to "Left"
//!
//! ## Design
//!
//! - **member_attach uses CTE**: Addresses Time-of-Check-to-Time-of-Use (TOCTOU)
//!   race condition when callers validate group/instance state before creating
//!   member
//!
//! - **member_reconcile uses CAS**: Reconciler already reads instance state, so
//!   simpler CAS operations are sufficient and easier to maintain
//!
//! ## Common Utilities
//!
//! This module provides functions for converting state enums to SQL
//! literals with compile-time safety.

use nexus_db_model::{MulticastGroupMemberState, MulticastGroupState};

pub mod member_attach;
pub mod member_reconcile;

/// Returns the SQL literal representation of a group state for use in raw SQL
/// queries.
///
/// This provides compile-time safety by ensuring state names in SQL match
/// the enum definition. The returned string includes single quotes for direct
/// SQL interpolation (e.g., "'active'").
pub(super) const fn group_state_as_sql_literal(
    state: MulticastGroupState,
) -> &'static str {
    match state {
        MulticastGroupState::Creating => "'creating'",
        MulticastGroupState::Active => "'active'",
        MulticastGroupState::Deleting => "'deleting'",
        MulticastGroupState::Deleted => "'deleted'",
    }
}

/// Returns the SQL literal representation of a member state for use in raw SQL
/// queries.
///
/// This provides compile-time safety by ensuring state names in SQL match
/// the enum definition. The returned string includes single quotes for direct
/// SQL interpolation (e.g., "'joined'").
pub(super) const fn member_state_as_sql_literal(
    state: MulticastGroupMemberState,
) -> &'static str {
    match state {
        MulticastGroupMemberState::Joining => "'joining'",
        MulticastGroupMemberState::Joined => "'joined'",
        MulticastGroupMemberState::Left => "'left'",
    }
}
