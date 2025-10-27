// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! CTE for attaching an instance to a multicast group.
//!
//! This uses a CTE to atomically validate the group is "Active" and the instance
//! exists, then insert or update the member row. The operation is idempotent
//! and handles these cases:
//!
//! - **No existing member**: Insert new row in "Joining" state
//! - **Member in "Left" state with time_deleted=NULL**: Transition to "Joining"
//!     and update `sled_id`
//! - **Member in "Left" state with time_deleted set**: Insert new row
//!     (soft-deleted members not reactivated)
//! - **Member in "Joining"/"Joined"**: No-op (idempotent)
//!
//! The upsert only occurs if the group exists and is in "Active" state and the
//! instance exists (see `active_group` and `instance_sled` CTEs below).
//! Returns the member ID.
//!
//! This addresses TOCTOU concerns by performing group validation, instance
//! sled_id lookup, and member upsert in a single atomic database operation.

use std::fmt::Debug;

use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::{DateTime, Utc};
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::*;
use diesel::result::Error as DieselError;
use diesel::sql_types::{Bool, Nullable, Timestamptz, Uuid as SqlUuid};
use uuid::Uuid;

use nexus_db_lookup::DbConnection;
use nexus_db_model::MulticastGroupMemberState;
use omicron_common::api::external::Error as ExternalError;

/// True if the group exists and is in "Active" state.
type GroupIsActive = Option<bool>;

/// True if the instance exists and has not been deleted.
type InstanceExists = Option<bool>;

/// UUID of the member row (new or existing).
type MemberId = Option<Uuid>;

/// The raw result tuple returned by the CTE query before parsing.
///
/// All fields are `Option` because the CTEs may return zero rows if
/// validations fail (group not active, instance not found, etc.).
type RawAttachMemberResult = (GroupIsActive, InstanceExists, MemberId);

/// Result of attaching a member to a multicast group.
#[derive(Debug, Clone, PartialEq)]
pub struct AttachMemberResult {
    /// Member UUID for this `(group, instance)` pair. New on first attach,
    /// otherwise the existing id.
    pub member_id: Uuid,
}

/// Errors that can occur when attaching a member to a multicast group.
#[derive(Debug)]
pub enum AttachMemberError {
    /// The multicast group does not exist or is not "Active".
    GroupNotActive,
    /// The instance does not exist or has been deleted.
    InstanceNotFound,
    /// Database constraint violation (e.g., unique index violation).
    ConstraintViolation(String),
    /// Other database error
    DatabaseError(DieselError),
}

impl From<AttachMemberError> for ExternalError {
    fn from(err: AttachMemberError) -> Self {
        match err {
            AttachMemberError::GroupNotActive => {
                ExternalError::invalid_request(
                    "Multicast group is not active (may be creating, deleting, or deleted)",
                )
            }
            AttachMemberError::InstanceNotFound => {
                ExternalError::invalid_request(
                    "Instance does not exist or has been deleted",
                )
            }
            AttachMemberError::ConstraintViolation(msg) => {
                ExternalError::invalid_request(&format!(
                    "Constraint violation: {msg}"
                ))
            }
            AttachMemberError::DatabaseError(e) => {
                ExternalError::internal_error(&format!("Database error: {e:?}"))
            }
        }
    }
}

/// Atomically attach an instance to a multicast group.
///
/// This performs an unconditional upsert in a single database round-trip:
///
/// - **Insert**: If no member exists, create a new row in "Joining" state
/// - **Reactivate**: If member exists in "Left" state with time_deleted=NULL,
///   transition to "Joining" and update `sled_id`
/// - **Insert new**: If member in "Left" with time_deleted set, create new row
/// - **Idempotent**: If member is already "Joining" or "Joined", do nothing
///
/// The operation atomically validates that both the group and instance exist,
/// retrieves the instance's current sled_id, and performs the member upsert.
/// Returns the member ID.
#[must_use = "Queries must be executed"]
pub struct AttachMemberToGroupStatement {
    group_id: Uuid,
    instance_id: Uuid,
    new_member_id: Uuid,
    time_created: DateTime<Utc>,
    time_modified: DateTime<Utc>,
}

impl AttachMemberToGroupStatement {
    /// Create an attach statement.
    ///
    /// # Arguments
    ///
    /// - `group_id`: The multicast group to attach to
    /// - `instance_id`: The instance being attached as a member
    /// - `new_member_id`: UUID to use if creating a new member row
    ///
    /// The CTE will atomically validate that the instance exists and retrieve
    /// its current sled_id from the VMM table.
    pub fn new(group_id: Uuid, instance_id: Uuid, new_member_id: Uuid) -> Self {
        let now = Utc::now();
        Self {
            group_id,
            instance_id,
            new_member_id,
            time_created: now,
            time_modified: now,
        }
    }

    /// Execute the statement and parse the result.
    pub async fn execute(
        self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<AttachMemberResult, AttachMemberError> {
        self.get_result_async::<RawAttachMemberResult>(conn)
            .await
            .map_err(|e| match &e {
                DieselError::DatabaseError(kind, info) => match kind {
                    diesel::result::DatabaseErrorKind::UniqueViolation => {
                        AttachMemberError::ConstraintViolation(
                            info.message().to_string(),
                        )
                    }
                    _ => AttachMemberError::DatabaseError(e),
                },
                _ => AttachMemberError::DatabaseError(e),
            })
            .and_then(Self::parse_result)
    }

    fn parse_result(
        result: RawAttachMemberResult,
    ) -> Result<AttachMemberResult, AttachMemberError> {
        let (group_is_active, instance_exists, member_id) = result;

        // Check validations in priority order to provide the most helpful error
        // message when both validations fail. Instance errors are checked first
        // because users typically attach their own instances to groups, making
        // instance-not-found errors more actionable than group-state errors.
        if instance_exists != Some(true) {
            return Err(AttachMemberError::InstanceNotFound);
        }

        // Group must be active
        if group_is_active != Some(true) {
            return Err(AttachMemberError::GroupNotActive);
        }

        // If validations passed, we must have a member_id
        let member_id = member_id
            .ok_or(AttachMemberError::DatabaseError(DieselError::NotFound))?;
        Ok(AttachMemberResult { member_id })
    }
}

impl QueryId for AttachMemberToGroupStatement {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl Query for AttachMemberToGroupStatement {
    type SqlType = (
        // group_is_active: true if group exists and is Active
        Nullable<Bool>,
        // instance_exists: true if instance exists and not deleted
        Nullable<Bool>,
        // member_id: UUID of member row
        Nullable<SqlUuid>,
    );
}

impl RunQueryDsl<DbConnection> for AttachMemberToGroupStatement {}

/// Generates SQL for atomic member attachment via CTE.
///
/// The CTE validates that both the group and instance exist, retrieves the
/// instance's current sled_id, then performs an unconditional upsert that
/// handles insert, reactivation, and idempotent cases. The ON CONFLICT DO
/// UPDATE only modifies rows in "Left" state.
///
/// This addresses TOCTOU concerns by performing all validation and updates
/// in a single atomic database operation.
impl AttachMemberToGroupStatement {
    /// Generates the `active_group` CTE that checks if the group exists and is active.
    fn push_active_group_cte<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> QueryResult<()> {
        use nexus_db_model::MulticastGroupState;
        out.push_sql("SELECT id FROM multicast_group WHERE id = ");
        out.push_bind_param::<diesel::sql_types::Uuid, _>(&self.group_id)?;
        out.push_sql(" AND state = ");
        out.push_sql(super::group_state_as_sql_literal(
            MulticastGroupState::Active,
        ));
        out.push_sql(" AND time_deleted IS NULL");
        Ok(())
    }

    /// Generates the `instance_sled` CTE that validates instance and gets sled_id.
    ///
    /// Joins instance and VMM tables via active_propolis_id to get current sled_id.
    /// Returns one row with (instance_id, sled_id) if instance exists and is not deleted.
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

    /// Generates the `upserted_member` CTE that performs the unconditional upsert.
    ///
    /// This SELECT now joins with both `active_group` and `instance_sled` CTEs to:
    /// 1. Ensure the group is active (FROM active_group)
    /// 2. Retrieve the instance's current sled_id (CROSS JOIN instance_sled)
    ///
    /// The ON CONFLICT clause uses the partial unique index that only includes rows
    /// where `time_deleted IS NULL`. This means:
    /// - Conflict only occurs for members with time_deleted=NULL (active or stopped)
    /// - Members with time_deleted set are ignored by the constraint (INSERT new row)
    /// - The UPDATE path preserves time_deleted=NULL for reactivated members
    fn push_upserted_member_cte<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> QueryResult<()> {
        out.push_sql(
            "INSERT INTO multicast_group_member (\
                 id, time_created, time_modified, external_group_id, \
                 parent_id, sled_id, state) SELECT ",
        );
        out.push_bind_param::<diesel::sql_types::Uuid, _>(&self.new_member_id)?;
        out.push_sql(", ");
        out.push_bind_param::<Timestamptz, _>(&self.time_created)?;
        out.push_sql(", ");
        out.push_bind_param::<Timestamptz, _>(&self.time_modified)?;
        out.push_sql(", ");
        out.push_bind_param::<diesel::sql_types::Uuid, _>(&self.group_id)?;
        out.push_sql(", ");
        out.push_bind_param::<diesel::sql_types::Uuid, _>(&self.instance_id)?;
        out.push_sql(", instance_sled.sled_id, ");
        out.push_sql(super::member_state_as_sql_literal(
            MulticastGroupMemberState::Joining,
        ));
        out.push_sql(" FROM active_group CROSS JOIN instance_sled ");
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
        out.push_sql(" THEN NULL ELSE multicast_group_member.time_deleted END RETURNING id");
        Ok(())
    }

    /// Generates the final SELECT that always returns exactly one row.
    ///
    /// This uses a LEFT JOIN pattern to ensure we return a row even when
    /// the group is not active or instance doesn't exist (which would cause
    /// the `upserted_member` CTE to return zero rows).
    ///
    fn push_final_select<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> QueryResult<()> {
        out.push_sql(
            "SELECT \
               EXISTS(SELECT 1 FROM active_group) AS group_is_active, \
               EXISTS(SELECT 1 FROM instance_sled) AS instance_exists, \
               u.id AS member_id \
             FROM (SELECT 1) AS dummy \
             LEFT JOIN upserted_member u ON TRUE",
        );
        Ok(())
    }
}

impl QueryFragment<Pg> for AttachMemberToGroupStatement {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();

        // CTE: Check if group exists and is active
        out.push_sql("WITH active_group AS (");
        self.push_active_group_cte(out.reborrow())?;
        out.push_sql("), ");

        // CTE: Validate instance exists and get sled_id
        out.push_sql("instance_sled AS (");
        self.push_instance_sled_cte(out.reborrow())?;
        out.push_sql("), ");

        // CTE: Unconditional upsert (INSERT or UPDATE)
        out.push_sql("upserted_member AS (");
        self.push_upserted_member_cte(out.reborrow())?;
        out.push_sql(") ");

        // Final SELECT: always return a row with group validity check.
        //
        // We ensure that we are always returning a constant number of columns.
        //
        // In our case, the `upserted_member` CTE returns zero rows if the group
        // is not active (because `FROM active_group` returns nothing). Without
        // the LEFT JOIN, the final SELECT would return zero rows, which would be
        // unparseable by Diesel (it expects exactly one row).
        //
        // The pattern we use is:
        // - Start with a dummy scalar query `(SELECT 1)` to anchor the result
        // - LEFT JOIN the `upserted_member` CTE, which may have zero or one row
        // - Use `EXISTS(SELECT 1 FROM active_group)` to check group validity
        //
        // This ensures we always return exactly one row with a constant number
        // of columns, even when the group doesn't exist or the upsert CTE returns
        // nothing.
        self.push_final_select(out.reborrow())?;

        Ok(())
    }
}
