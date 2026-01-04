// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Atomic CTE for attaching instances to multicast groups.
//!
//! Uses CTEs to atomically validate group exists (not in "Deleting" state)
//! and instance exists, then inserts or updates the member row. This
//! operation handles:
//!
//! - **No existing member**: Insert new row in "Joining" state
//! - **Member in "Left" (time_deleted=NULL)**: Transition to "Joining", update sled_id
//! - **Member in "Left" (time_deleted set)**: Insert new row (soft-delete ignored / not reactivated)
//! - **Member in "Joining"/"Joined"**: No-op (already attached)
//!
//! Upsert only runs if group exists ("Creating" or "Active") and instance exists
//! (validated by `valid_group` and `instance_sled` CTEs). The operation returns
//! the full member record in a single database round-trip.
//!
//! Prevents TOCTOU races: group validation, instance sled_id lookup, and member
//! upsert all happen in one atomic database operation.
//!
//! We use sentinel-based error handling (like [`network_interface`]): validation
//! failures trigger a CAST error with a sentinel string, which is decoded in
//! error handling to return the appropriate error type.
//!
//! [`network_interface`]: crate::db::datastore::network_interface

use std::fmt::Debug;

use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::{DateTime, Utc};
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::*;
use diesel::result::Error as DieselError;
use diesel::sql_types::{Array, Timestamptz};
use ipnetwork::IpNetwork;
use uuid::Uuid;

use nexus_db_lookup::DbConnection;
use nexus_db_model::{MulticastGroupMember, MulticastGroupMemberState};
use omicron_common::api::external;

use crate::db::true_or_cast_error::matches_sentinel;

// Sentinel strings for validation errors.
// These trigger a CAST error when validation fails, allowing us to decode
// the specific failure reason from the error message.
const GROUP_NOT_FOUND_SENTINEL: &str = "group-not-found";
const INSTANCE_NOT_FOUND_SENTINEL: &str = "instance-not-found";

/// Result of attaching an instance to a multicast group.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AttachMemberResult {
    /// Full member record for this (group, instance) pair.
    pub member: MulticastGroupMember,
}

/// Errors from attaching an instance to a multicast group.
#[derive(Debug)]
pub(crate) enum AttachMemberError {
    /// Multicast group doesn't exist or is being deleted
    GroupNotFound,
    /// Instance doesn't exist or has been deleted
    InstanceNotFound,
    /// Database constraint violation (unique index, etc.)
    ConstraintViolation(String),
    /// Other database error
    DatabaseError(DieselError),
}

impl AttachMemberError {
    /// Construct an [`AttachMemberError`] from a database error.
    ///
    /// This catches the sentinel errors that indicate validation failures
    /// (group not found, instance not found) as well as constraint violations.
    fn from_diesel(err: DieselError) -> Self {
        // Check for sentinel errors first
        let sentinels = [GROUP_NOT_FOUND_SENTINEL, INSTANCE_NOT_FOUND_SENTINEL];
        if let Some(sentinel) = matches_sentinel(&err, &sentinels) {
            return match sentinel {
                GROUP_NOT_FOUND_SENTINEL => AttachMemberError::GroupNotFound,
                INSTANCE_NOT_FOUND_SENTINEL => {
                    AttachMemberError::InstanceNotFound
                }
                _ => unreachable!("Unknown sentinel: {sentinel}"),
            };
        }

        // Check for constraint violations
        if let DieselError::DatabaseError(kind, info) = &err {
            if matches!(
                kind,
                diesel::result::DatabaseErrorKind::UniqueViolation
            ) {
                return AttachMemberError::ConstraintViolation(
                    info.message().to_string(),
                );
            }
        }

        AttachMemberError::DatabaseError(err)
    }
}

impl From<AttachMemberError> for external::Error {
    fn from(err: AttachMemberError) -> Self {
        match err {
            AttachMemberError::GroupNotFound => {
                external::Error::invalid_request(
                    "Multicast group not found or is being deleted",
                )
            }
            AttachMemberError::InstanceNotFound => {
                external::Error::invalid_request(
                    "Instance does not exist or has been deleted",
                )
            }
            AttachMemberError::ConstraintViolation(msg) => {
                external::Error::invalid_request(&format!(
                    "Constraint violation: {msg}"
                ))
            }
            AttachMemberError::DatabaseError(e) => {
                external::Error::internal_error(&format!(
                    "Database error: {e:?}"
                ))
            }
        }
    }
}

/// Atomically attach an instance to a multicast group.
///
/// Single database round-trip performs unconditional upsert:
///
/// - **Insert**: No member exists → create in "Joining" state
/// - **Reactivate**: Member in "Left" (time_deleted=NULL) → transition to
///   "Joining", update `sled_id`
/// - **Insert new**: Member in "Left" (time_deleted set) → create new row
/// - **Idempotent**: Member already "Joining" or "Joined" → no-op
///
/// Atomically validates group and instance exist, retrieves instance's current
/// sled_id, and performs member upsert. Returns member ID.
///
/// Source IPs handling:
/// - `None` → preserve existing source_ips on reactivation, empty for new inserts
/// - `Some([])` → clear source_ips (only valid for ASM addresses as SSM requires sources)
/// - `Some([a,b])` → set/replace with new source_ips
///
/// Note: The address range (not sources) determines SSM vs ASM mode. SSM
/// addresses (232/8, ff3x::/32) require sources and the app layer validates
/// this before calling.
#[must_use = "Queries must be executed"]
pub(crate) struct AttachMemberToGroupStatement {
    group_id: Uuid,
    instance_id: Uuid,
    new_member_id: Uuid,
    time_created: DateTime<Utc>,
    time_modified: DateTime<Utc>,
    /// Whether (or not) to update `source_ips` on reactivation
    update_source_ips_on_reactivation: bool,
    /// Source IPs for INSERT operation
    source_ips_for_insert: Vec<IpNetwork>,
}

impl AttachMemberToGroupStatement {
    /// Create an attach statement.
    ///
    /// # Arguments
    ///
    /// - `group_id`: Multicast group to attach to
    /// - `instance_id`: Instance being attached as member
    /// - `new_member_id`: UUID for new member row (if creating)
    /// - `source_ips`: Source IPs for filtering (`None` preserves existing on reactivation)
    ///
    /// CTEs atomically validate group is not in a "Deleting" state,
    /// that the instance exists, retrieves the current `sled_id` from
    /// VMM table, then performs the upsert.
    pub fn new(
        group_id: Uuid,
        instance_id: Uuid,
        new_member_id: Uuid,
        source_ips: Option<Vec<IpNetwork>>,
    ) -> Self {
        let now = Utc::now();
        Self {
            group_id,
            instance_id,
            new_member_id,
            time_created: now,
            time_modified: now,
            update_source_ips_on_reactivation: source_ips.is_some(),
            source_ips_for_insert: source_ips.unwrap_or_default(),
        }
    }

    /// Execute the statement and parse the result.
    pub async fn execute(
        self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<AttachMemberResult, AttachMemberError> {
        self.get_result_async::<MulticastGroupMember>(conn)
            .await
            .map_err(AttachMemberError::from_diesel)
            .map(|member| AttachMemberResult { member })
    }
}

impl QueryId for AttachMemberToGroupStatement {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl Query for AttachMemberToGroupStatement {
    // Return type matches the MulticastGroupMember model directly
    type SqlType = <<MulticastGroupMember as diesel::Selectable<Pg>>::SelectExpression as diesel::Expression>::SqlType;
}

impl RunQueryDsl<DbConnection> for AttachMemberToGroupStatement {}

/// Generates SQL for atomic member attachment via CTEs.
///
/// CTEs validate group and instance exist (triggering sentinel errors on failure),
/// retrieve instance's current sled_id, then perform unconditional upsert
/// (handles insert, reactivation, and idempotent cases). ON CONFLICT DO UPDATE
/// only modifies rows in "Left" state.
///
/// Prevents TOCTOU races by performing all validation and updates in one atomic
/// database operation.
impl AttachMemberToGroupStatement {
    /// Generates the `valid_group` CTE (checks group exists and is attachable).
    ///
    /// Returns id and multicast_ip for use in the member insert.
    /// Allows "Creating" and "Active" groups, but rejects "Deleting" groups.
    fn push_valid_group_cte<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> QueryResult<()> {
        use nexus_db_model::MulticastGroupState;
        out.push_sql(
            "SELECT id, multicast_ip FROM multicast_group WHERE id = ",
        );
        out.push_bind_param::<diesel::sql_types::Uuid, _>(&self.group_id)?;
        out.push_sql(" AND state != ");
        out.push_sql(super::group_state_as_sql_literal(
            MulticastGroupState::Deleting,
        ));
        out.push_sql(" AND time_deleted IS NULL");
        Ok(())
    }

    /// Generates the `instance_sled` CTE (validates instance and gets sled_id).
    ///
    /// Joins instance and VMM tables via active_propolis_id to get current `sled_id`.
    /// Returns one row with (`instance_id`, `sled_id`) if instance exists and not deleted.
    fn push_instance_sled_cte<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> QueryResult<()> {
        out.push_sql(
            "SELECT instance.id, vmm.sled_id \
             FROM instance \
             LEFT JOIN vmm ON instance.active_propolis_id = vmm.id \
             WHERE instance.id = ",
        );
        out.push_bind_param::<diesel::sql_types::Uuid, _>(&self.instance_id)?;
        out.push_sql(" AND instance.time_deleted IS NULL");
        Ok(())
    }

    /// Generates the `validation` CTE that triggers sentinel errors on failure.
    ///
    /// Uses CAST to trigger a predictable error when validation fails:
    /// - If group not found → CAST('group-not-found' AS BOOL) fails
    /// - If instance not found → CAST('instance-not-found' AS BOOL) fails
    /// - If both valid → CAST('TRUE' AS BOOL) succeeds
    ///
    /// This follows the pattern used in `network_interface.rs` and `external_ip.rs`.
    fn push_validation_cte<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> QueryResult<()> {
        // SELECT CAST(
        //   CASE
        //     WHEN NOT EXISTS (SELECT 1 FROM instance_sled) THEN 'instance-not-found'
        //     WHEN NOT EXISTS (SELECT 1 FROM valid_group) THEN 'group-not-found'
        //     ELSE 'TRUE'
        //   END AS BOOL
        // ) AS validated
        //
        // Instance is checked first to provide more those errors up front
        out.push_sql("SELECT CAST(CASE ");
        out.push_sql("WHEN NOT EXISTS (SELECT 1 FROM instance_sled) THEN '");
        out.push_sql(INSTANCE_NOT_FOUND_SENTINEL);
        out.push_sql("' ");
        out.push_sql("WHEN NOT EXISTS (SELECT 1 FROM valid_group) THEN '");
        out.push_sql(GROUP_NOT_FOUND_SENTINEL);
        out.push_sql("' ");
        out.push_sql("ELSE 'TRUE' END AS BOOL) AS validated");
        Ok(())
    }

    /// Generates the `upserted_member` CTE (performs unconditional upsert).
    ///
    /// SELECT joins with both `valid_group` and `instance_sled` CTEs to:
    /// 1. Ensure group exists and is attachable (FROM valid_group)
    /// 2. Retrieve group's multicast_ip (FROM valid_group)
    /// 3. Retrieve instance's current sled_id (CROSS JOIN instance_sled)
    ///
    /// ON CONFLICT clause uses partial unique index (only rows with time_deleted IS NULL):
    /// - Conflict only for members with time_deleted=NULL (active or stopped)
    /// - Members with time_deleted set ignored by constraint (INSERT new row)
    /// - UPDATE path preserves time_deleted=NULL for reactivated members
    ///
    /// Source IPs handling on conflict:
    /// - If `update_source_ips_on_reactivation` → use EXCLUDED.source_ips
    /// - Otherwise → preserve existing (no update)
    fn push_upserted_member_cte<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> QueryResult<()> {
        // Column order matches schema: id, time_created, time_modified,
        // external_group_id, multicast_ip, parent_id, sled_id, source_ips, state
        out.push_sql(
            "INSERT INTO multicast_group_member (\
                 id, time_created, time_modified, external_group_id, \
                 multicast_ip, parent_id, sled_id, source_ips, state) SELECT ",
        );
        out.push_bind_param::<diesel::sql_types::Uuid, _>(&self.new_member_id)?;
        out.push_sql(", ");
        out.push_bind_param::<Timestamptz, _>(&self.time_created)?;
        out.push_sql(", ");
        out.push_bind_param::<Timestamptz, _>(&self.time_modified)?;
        out.push_sql(", ");
        out.push_bind_param::<diesel::sql_types::Uuid, _>(&self.group_id)?;
        out.push_sql(", valid_group.multicast_ip, ");
        out.push_bind_param::<diesel::sql_types::Uuid, _>(&self.instance_id)?;
        out.push_sql(", instance_sled.sled_id, ");
        out.push_bind_param::<Array<diesel::sql_types::Inet>, _>(
            &self.source_ips_for_insert,
        )?;
        out.push_sql(", ");
        out.push_sql(super::member_state_as_sql_literal(
            MulticastGroupMemberState::Joining,
        ));
        out.push_sql(" FROM valid_group CROSS JOIN instance_sled ");

        // ON CONFLICT: only update "Left" members, preserve other states
        out.push_sql("ON CONFLICT (external_group_id, parent_id) WHERE time_deleted IS NULL DO UPDATE SET state = CASE WHEN multicast_group_member.state = ");
        out.push_sql(super::member_state_as_sql_literal(
            MulticastGroupMemberState::Left,
        ));
        out.push_sql(" THEN ");
        out.push_sql(super::member_state_as_sql_literal(
            MulticastGroupMemberState::Joining,
        ));
        out.push_sql(" ELSE multicast_group_member.state END, sled_id = CASE WHEN multicast_group_member.state = ");
        out.push_sql(super::member_state_as_sql_literal(
            MulticastGroupMemberState::Left,
        ));
        out.push_sql(" THEN EXCLUDED.sled_id ELSE multicast_group_member.sled_id END, time_modified = CASE WHEN multicast_group_member.state = ");
        out.push_sql(super::member_state_as_sql_literal(
            MulticastGroupMemberState::Left,
        ));
        out.push_sql(" THEN EXCLUDED.time_modified ELSE multicast_group_member.time_modified END, time_deleted = CASE WHEN multicast_group_member.state = ");
        out.push_sql(super::member_state_as_sql_literal(
            MulticastGroupMemberState::Left,
        ));
        out.push_sql(" THEN NULL ELSE multicast_group_member.time_deleted END");

        // source_ips: update on reactivation only if caller provided source_ips
        out.push_sql(
            ", source_ips = CASE WHEN multicast_group_member.state = ",
        );
        out.push_sql(super::member_state_as_sql_literal(
            MulticastGroupMemberState::Left,
        ));
        out.push_sql(" THEN ");
        if self.update_source_ips_on_reactivation {
            // source_ips was provided → use the new value
            out.push_sql("EXCLUDED.source_ips");
        } else {
            // source_ips was `None` → preserve existing
            out.push_sql("multicast_group_member.source_ips");
        }
        out.push_sql(" ELSE multicast_group_member.source_ips END");

        // Return all columns so caller gets full member record
        out.push_sql(
            " RETURNING id, time_created, time_modified, time_deleted, \
             external_group_id, multicast_ip, parent_id, sled_id, state, \
             source_ips, version_added, version_removed",
        );
        Ok(())
    }

    /// Generates the final SELECT (returns member columns directly).
    ///
    /// The validation CTE has already triggered an error if validation failed,
    /// so we can assume the upserted_member CTE returned exactly one row.
    fn push_final_select<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> QueryResult<()> {
        out.push_sql(
            "SELECT id, time_created, time_modified, time_deleted, \
             external_group_id, multicast_ip, parent_id, sled_id, state, \
             source_ips, version_added, version_removed \
             FROM upserted_member",
        );
        Ok(())
    }
}

impl QueryFragment<Pg> for AttachMemberToGroupStatement {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();

        // CTE: Check if group exists and is active
        out.push_sql("WITH valid_group AS (");
        self.push_valid_group_cte(out.reborrow())?;
        out.push_sql("), ");

        // CTE: Validate instance exists and get sled_id
        out.push_sql("instance_sled AS (");
        self.push_instance_sled_cte(out.reborrow())?;
        out.push_sql("), ");

        // CTE: Validation that triggers sentinel errors on failure
        out.push_sql("validation AS MATERIALIZED (");
        self.push_validation_cte(out.reborrow())?;
        out.push_sql("), ");

        // CTE: Unconditional upsert (INSERT or UPDATE)
        // This depends on validation CTE being evaluated first (MATERIALIZED ensures this)
        out.push_sql("upserted_member AS (");
        self.push_upserted_member_cte(out.reborrow())?;
        out.push_sql(") ");

        // Final SELECT: return member columns directly
        // The validation CTE already triggered an error if validation failed,
        // so upserted_member is guaranteed to have exactly one row.
        self.push_final_select(out.reborrow())?;

        Ok(())
    }
}
