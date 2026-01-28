// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Atomic database operations for multicast group members.
//!
//! ## Operations
//!
//! - **member_attach**: Atomic CTE for attaching instances to groups
//!   - Validates group is "Creating" or "Active" (rejects "Deleting"/"Deleted" groups)
//!   - Performs member upsert (insert or reactivate from "Left")
//!   - TOCTOU-safe: single atomic database operation
//!
//! - **member_reconcile**: CAS operations for RPW reconciler
//!   - Background sled_id updates during migration
//!   - Transitions to "Left" when instance stops
//!
//! ## Common Utils
//!
//! Helper functions convert state enums to SQL literals with compile-time
//! safety (ensures SQL strings match enum definitions).

use nexus_db_model::{MulticastGroupMemberState, MulticastGroupState};

pub mod member_attach;
pub mod member_reconcile;

/// Returns SQL literal for a group state (e.g., "'active'").
///
/// Compile-time safety: state names in SQL must match enum definition.
/// Returned string includes single quotes for direct SQL interpolation.
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

/// Returns SQL literal for a member state (e.g., "'joined'").
///
/// Compile-time safety: state names in SQL must match enum definition.
/// Returned string includes single quotes for direct SQL interpolation.
pub(super) const fn member_state_as_sql_literal(
    state: MulticastGroupMemberState,
) -> &'static str {
    match state {
        MulticastGroupMemberState::Joining => "'joining'",
        MulticastGroupMemberState::Joined => "'joined'",
        MulticastGroupMemberState::Left => "'left'",
    }
}
