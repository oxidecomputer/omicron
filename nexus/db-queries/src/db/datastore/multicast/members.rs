//! Multicast group member management operations.
//!
//! Database operations for managing multicast group memberships - adding/
//! removing members and lifecycle coordination.

use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use slog::debug;
use uuid::Uuid;

use nexus_db_errors::{ErrorHandler, public_error_from_diesel};
use omicron_common::api::external::{
    self, CreateResult, DataPageParams, DeleteResult, ListResultVec,
    LookupType, ResourceType, UpdateResult,
};
use omicron_uuid_kinds::{
    GenericUuid, InstanceUuid, MulticastGroupUuid, SledKind,
};

use crate::context::OpContext;
use crate::db::datastore::DataStore;
use crate::db::datastore::multicast::ops;
use crate::db::model::{
    DbTypedUuid, MulticastGroupMember, MulticastGroupMemberState,
    MulticastGroupMemberValues,
};
use crate::db::on_conflict_ext::IncompleteOnConflictExt;
use crate::db::pagination::paginated;

impl DataStore {
    /// List members of a multicast group.
    pub async fn multicast_group_members_list(
        &self,
        opctx: &OpContext,
        group_id: MulticastGroupUuid,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<MulticastGroupMember> {
        self.multicast_group_members_list_by_id(opctx, group_id, pagparams)
            .await
    }

    /// Create a new multicast group member for an instance.
    ///
    /// Used by the HTTP API endpoint for explicit member attachment.
    /// Creates a member record in "Joining" state. Uses a Diesel
    /// upsert (not the CTE) since the HTTP endpoint validates separately.
    ///
    /// RPW reconciler programs the dataplane when the instance starts.
    ///
    /// Handles reactivation of "Left" members and preserves "Joined" state for
    /// idempotency.
    pub async fn multicast_group_member_add(
        &self,
        opctx: &OpContext,
        group_id: MulticastGroupUuid,
        instance_id: InstanceUuid,
    ) -> CreateResult<MulticastGroupMember> {
        use nexus_db_schema::schema::multicast_group::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        // Fetch the group's multicast_ip
        let group_multicast_ip: ipnetwork::IpNetwork = dsl::multicast_group
            .filter(dsl::id.eq(group_id.into_untyped_uuid()))
            .filter(dsl::time_deleted.is_null())
            .select(dsl::multicast_ip)
            .first_async(&*conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::MulticastGroup,
                        LookupType::ById(group_id.into_untyped_uuid()),
                    ),
                )
            })?;

        self.multicast_group_member_add_with_conn(
            opctx,
            &conn,
            group_id.into_untyped_uuid(),
            group_multicast_ip,
            instance_id.into_untyped_uuid(),
        )
        .await
    }

    /// Add an instance to a multicast group using provided connection.
    ///
    /// Internal helper that performs member attachment with state preservation.
    /// This only transitions "Left" members (with time_deleted=NULL) to "Joining"
    /// for reactivation, preserving "Joined" state if already active.
    ///
    /// State handling:
    /// - Member in "Left" with time_deleted=NULL → UPDATE to "Joining" (reactivation)
    /// - Member in "Left" with time_deleted set → not matched (soft-deleted, INSERT new)
    /// - Member in "Joining" → return existing (idempotent)
    /// - Member in "Joined" → return existing (preserve active state)
    /// - Member doesn't exist → INSERT as "Joining"
    async fn multicast_group_member_add_with_conn(
        &self,
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<nexus_db_lookup::DbConnection>,
        group_id: Uuid,
        multicast_ip: ipnetwork::IpNetwork,
        instance_id: Uuid,
    ) -> CreateResult<MulticastGroupMember> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        // Look up the sled_id for this instance (may be None for stopped instances)
        let sled_id = self
            .instance_get_sled_id(opctx, instance_id)
            .await?
            .map(DbTypedUuid::from_untyped_uuid);

        // Try UPDATE on "Left" members only (reactivation)
        let reactivation_result = diesel::update(dsl::multicast_group_member)
            .filter(dsl::external_group_id.eq(group_id))
            .filter(dsl::parent_id.eq(instance_id))
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::state.eq(MulticastGroupMemberState::Left))
            .set((
                dsl::state.eq(MulticastGroupMemberState::Joining),
                dsl::sled_id.eq(sled_id),
                dsl::time_modified.eq(Utc::now()),
            ))
            .returning(MulticastGroupMember::as_returning())
            .get_result_async(conn)
            .await;

        // Early return on member or error
        match reactivation_result {
            // Successfully reactivated Left → Joining
            Ok(member) => return Ok(member),
            Err(diesel::result::Error::NotFound) => {}
            Err(e) => {
                return Err(public_error_from_diesel(e, ErrorHandler::Server));
            }
        }

        // Try INSERT, but preserve existing state on conflict
        let new_member = MulticastGroupMemberValues {
            id: Uuid::new_v4(),
            parent_id: instance_id,
            external_group_id: group_id,
            multicast_ip,
            sled_id,
            state: MulticastGroupMemberState::Joining,
            time_created: Utc::now(),
            time_modified: Utc::now(),
            time_deleted: None,
        };

        // On conflict, perform a no-op update to return existing member.
        // This preserves "Joined"/"Joining" state while avoiding an extra SELECT.
        // CockroachDB requires `.as_partial_index()` for partial unique indexes.
        diesel::insert_into(dsl::multicast_group_member)
            .values(new_member)
            .on_conflict((dsl::external_group_id, dsl::parent_id))
            .as_partial_index()
            .do_update()
            .set(dsl::time_modified.eq(dsl::time_modified))
            .returning(MulticastGroupMember::as_returning())
            .get_result_async(conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Delete a multicast group member by group ID.
    ///
    /// This performs a hard delete of all members (both active and soft-deleted)
    /// for the specified group. Used during group cleanup operations.
    pub async fn multicast_group_members_delete_by_group(
        &self,
        opctx: &OpContext,
        group_id: MulticastGroupUuid,
    ) -> DeleteResult {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        // Delete all members for this group, including soft-deleted ones
        // We use a targeted query to leverage existing indexes
        diesel::delete(dsl::multicast_group_member)
            .filter(dsl::external_group_id.eq(group_id.into_untyped_uuid()))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
            .map(|_x| ())
    }

    /// Set the state of a multicast group member.
    pub async fn multicast_group_member_set_state(
        &self,
        opctx: &OpContext,
        external_group_id: MulticastGroupUuid,
        parent_id: InstanceUuid,
        new_state: MulticastGroupMemberState,
    ) -> UpdateResult<()> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let rows_updated = diesel::update(dsl::multicast_group_member)
            .filter(
                dsl::external_group_id
                    .eq(external_group_id.into_untyped_uuid()),
            )
            .filter(dsl::parent_id.eq(parent_id.into_untyped_uuid()))
            .filter(dsl::time_deleted.is_null())
            .set((dsl::state.eq(new_state), dsl::time_modified.eq(Utc::now())))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::MulticastGroupMember,
                        LookupType::ById(external_group_id.into_untyped_uuid()),
                    ),
                )
            })?;

        if rows_updated == 0 {
            return Err(external::Error::not_found_by_id(
                ResourceType::MulticastGroupMember,
                &external_group_id.into_untyped_uuid(),
            ));
        }

        Ok(())
    }

    /// Conditionally set the state of a multicast group member if the current
    /// state matches `expected_state`.
    ///
    /// Used by RPW reconciler.
    ///
    /// Returns `Ok(true)` if updated, `Ok(false)` if no row matched the filters
    /// (member not found, soft-deleted, or state mismatch).
    pub async fn multicast_group_member_set_state_if_current(
        &self,
        opctx: &OpContext,
        external_group_id: MulticastGroupUuid,
        parent_id: InstanceUuid,
        expected_state: MulticastGroupMemberState,
        new_state: MulticastGroupMemberState,
    ) -> UpdateResult<bool> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let rows_updated = diesel::update(dsl::multicast_group_member)
            .filter(
                dsl::external_group_id
                    .eq(external_group_id.into_untyped_uuid()),
            )
            .filter(dsl::parent_id.eq(parent_id.into_untyped_uuid()))
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::state.eq(expected_state))
            .set((dsl::state.eq(new_state), dsl::time_modified.eq(Utc::now())))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(rows_updated > 0)
    }

    /// Atomically transition from "Left" → "Joining" and set sled_id.
    ///
    /// Used by RPW reconciler.
    ///
    /// Returns Ok(true) if updated, Ok(false) if state was not "Left" or row missing.
    pub async fn multicast_group_member_left_to_joining_if_current(
        &self,
        opctx: &OpContext,
        external_group_id: MulticastGroupUuid,
        parent_id: InstanceUuid,
        sled_id: DbTypedUuid<SledKind>,
    ) -> UpdateResult<bool> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let rows_updated = diesel::update(dsl::multicast_group_member)
            .filter(
                dsl::external_group_id
                    .eq(external_group_id.into_untyped_uuid()),
            )
            .filter(dsl::parent_id.eq(parent_id.into_untyped_uuid()))
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::state.eq(MulticastGroupMemberState::Left))
            .set((
                dsl::state.eq(MulticastGroupMemberState::Joining),
                dsl::sled_id.eq(Some(sled_id)),
                dsl::time_modified.eq(Utc::now()),
            ))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(rows_updated > 0)
    }

    /// Atomically transition to "Left" and clear sled_id if current state
    /// matches `expected_state`.
    ///
    /// Used by RPW reconciler.
    ///
    /// Returns Ok(true) if updated, Ok(false) if state did not match or row missing.
    pub async fn multicast_group_member_to_left_if_current(
        &self,
        opctx: &OpContext,
        external_group_id: MulticastGroupUuid,
        parent_id: InstanceUuid,
        expected_state: MulticastGroupMemberState,
    ) -> UpdateResult<bool> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let rows_updated = diesel::update(dsl::multicast_group_member)
            .filter(
                dsl::external_group_id
                    .eq(external_group_id.into_untyped_uuid()),
            )
            .filter(dsl::parent_id.eq(parent_id.into_untyped_uuid()))
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::state.eq(expected_state))
            .set((
                dsl::state.eq(MulticastGroupMemberState::Left),
                dsl::sled_id.eq(Option::<DbTypedUuid<SledKind>>::None),
                dsl::time_modified.eq(Utc::now()),
            ))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(rows_updated > 0)
    }

    /// List members of a multicast group by ID.
    pub async fn multicast_group_members_list_by_id(
        &self,
        opctx: &OpContext,
        external_group_id: MulticastGroupUuid,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<MulticastGroupMember> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        paginated(dsl::multicast_group_member, dsl::id, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(
                dsl::external_group_id
                    .eq(external_group_id.into_untyped_uuid()),
            )
            .select(MulticastGroupMember::as_select())
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// List multicast group memberships for a specific instance.
    ///
    /// Only returns active (non-deleted) memberships.
    pub async fn multicast_group_members_list_by_instance(
        &self,
        opctx: &OpContext,
        instance_id: InstanceUuid,
    ) -> ListResultVec<MulticastGroupMember> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        dsl::multicast_group_member
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::parent_id.eq(instance_id.into_untyped_uuid()))
            .order(dsl::id.asc())
            .select(MulticastGroupMember::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Attach an instance to a multicast group atomically.
    ///
    /// Used by instance create saga and instance reconfiguration to ensure
    /// atomic validation and member creation. This CTE:
    /// - Verifies the group is "Active"
    /// - Validates instance exists
    /// - Retrieves instance's current sled_id from VMM table
    /// - Inserts "Joining" if no row exists
    /// - Reactivates "Left" → "Joining" (updates sled_id)
    /// - No-ops for "Joining"/"Joined" (idempotent)
    ///
    /// Returns the `member_id` for this `(group, instance)` pair.
    ///
    /// See `crate::db::datastore::multicast::ops::member_attach::AttachMemberToGroupStatement` for CTE implementation.
    pub async fn multicast_group_member_attach_to_instance(
        &self,
        opctx: &OpContext,
        group_id: MulticastGroupUuid,
        instance_id: InstanceUuid,
    ) -> Result<Uuid, external::Error> {
        let conn = self.pool_connection_authorized(opctx).await?;

        // Use the CTE to atomically validate group state, instance existence,
        // retrieve sled_id, and attach member - all in a single database operation.
        // This eliminates TOCTOU issues from separate instance validation.
        let statement = ops::member_attach::AttachMemberToGroupStatement::new(
            group_id.into_untyped_uuid(),
            instance_id.into_untyped_uuid(),
            Uuid::new_v4(), // new_member_id if we need to insert
        );

        let result = statement.execute(&conn).await?;
        Ok(result.member_id)
    }

    /// Atomically reconcile a member in "Joining" state.
    ///
    /// This combines sled_id updates and state transitions into a single atomic
    /// database operation to handle concurrent reconciliation by multiple Nexus
    /// instances.
    ///
    /// # Arguments
    ///
    /// - `group_id`: The multicast group
    /// - `instance_id`: The instance being reconciled
    /// - `instance_valid`: Whether the instance is in a valid state for multicast
    /// - `current_sled_id`: The instance's current sled_id from VMM lookup
    ///
    /// # Returns
    ///
    /// Returns the reconciliation result indicating what action was taken.
    ///
    /// # Example Usage (from RPW reconciler)
    ///
    /// ```rust,ignore
    /// // Fetch cached instance state and sled_id from reconciler's state map
    /// let (instance_valid, sled_id) = instance_states
    ///     .get(&member.parent_id)
    ///     .copied()
    ///     .unwrap_or((false, None));
    /// let current_sled_id = sled_id.map(|id| id.into());
    ///
    /// let result = self
    ///     .datastore
    ///     .multicast_group_member_reconcile_joining(
    ///         opctx,
    ///         MulticastGroupUuid::from_untyped_uuid(group.id()),
    ///         InstanceUuid::from_untyped_uuid(member.parent_id),
    ///         instance_valid,
    ///         current_sled_id,
    ///     )
    ///     .await?;
    ///
    /// match result.action {
    ///     ReconcileAction::TransitionedToLeft => { /* program dataplane to remove */ }
    ///     ReconcileAction::UpdatedSledId { .. } => { /* sled changed, stay "Joining" */ }
    ///     ReconcileAction::NoChange => { /* ready to transition to "Joined" */ }
    ///     ReconcileAction::NotFound => { /* member not in "Joining" state */ }
    /// }
    /// ```
    ///
    /// See [`ops::member_reconcile::reconcile_joining_member`] for atomic CTE implementation.
    pub async fn multicast_group_member_reconcile_joining(
        &self,
        opctx: &OpContext,
        group_id: MulticastGroupUuid,
        instance_id: InstanceUuid,
        instance_valid: bool,
        current_sled_id: Option<DbTypedUuid<SledKind>>,
    ) -> Result<ops::member_reconcile::ReconcileJoiningResult, external::Error>
    {
        let conn = self.pool_connection_authorized(opctx).await?;

        ops::member_reconcile::reconcile_joining_member(
            &conn,
            group_id.into_untyped_uuid(),
            instance_id.into_untyped_uuid(),
            instance_valid,
            current_sled_id,
        )
        .await
        .map_err(external::Error::from)
    }

    /// Detach all multicast group memberships for an instance.
    ///
    /// Transitions all non-Left members to "Left" state and clears sled_id.
    /// Used by instance lifecycle operations (stop, delete) to signal RPW
    /// that dataplane cleanup is needed.
    ///
    /// Note: This does not set `time_deleted`. For soft deletion of memberships,
    /// use [`Self::multicast_group_members_mark_for_removal`].
    ///
    /// See also [`Self::multicast_group_member_detach_by_group_and_instance`]
    /// for detaching a specific group membership.
    pub async fn multicast_group_members_detach_by_instance(
        &self,
        opctx: &OpContext,
        instance_id: InstanceUuid,
    ) -> Result<(), external::Error> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let now = Utc::now();

        // Transition members from "Joined/Joining" to "Left" state and clear
        // `sled_id`
        diesel::update(dsl::multicast_group_member)
            .filter(dsl::parent_id.eq(instance_id.into_untyped_uuid()))
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::state.ne(MulticastGroupMemberState::Left)) // Only update non-Left members
            .set((
                dsl::state.eq(MulticastGroupMemberState::Left),
                dsl::sled_id.eq(Option::<DbTypedUuid<SledKind>>::None),
                dsl::time_modified.eq(now),
            ))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
            .map(|_| ())
    }

    /// Get a specific multicast group member by group ID and instance ID.
    pub async fn multicast_group_member_get_by_group_and_instance(
        &self,
        opctx: &OpContext,
        group_id: MulticastGroupUuid,
        instance_id: InstanceUuid,
    ) -> Result<Option<MulticastGroupMember>, external::Error> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let member = dsl::multicast_group_member
            .filter(dsl::external_group_id.eq(group_id.into_untyped_uuid()))
            .filter(dsl::parent_id.eq(instance_id.into_untyped_uuid()))
            .filter(dsl::time_deleted.is_null())
            .select(MulticastGroupMember::as_select())
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(member)
    }

    /// Get a multicast group member by its unique ID.
    ///
    /// If `include_removed` is true, returns the member even if it has been
    /// soft-deleted (i.e., `time_deleted` is set). Otherwise filters out
    /// soft-deleted rows.
    pub async fn multicast_group_member_get_by_id(
        &self,
        opctx: &OpContext,
        member_id: Uuid,
        include_removed: bool,
    ) -> Result<Option<MulticastGroupMember>, external::Error> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let mut query = dsl::multicast_group_member.into_boxed();
        if !include_removed {
            query = query.filter(dsl::time_deleted.is_null());
        }

        let member = query
            .filter(dsl::id.eq(member_id))
            .select(MulticastGroupMember::as_select())
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(member)
    }

    /// Detach a specific multicast group member by group ID and instance ID.
    ///
    /// This transitions member to "Left" state, clears `sled_id`, and sets `time_deleted`
    /// (marking for permanent removal). Used by the HTTP API for explicit detach operations.
    /// Distinct from instance stop which only transitions to "Left" without `time_deleted`.
    ///
    /// See [`Self::multicast_group_members_detach_by_instance`] for detaching all
    /// memberships of an instance (used during instance stop).
    pub async fn multicast_group_member_detach_by_group_and_instance(
        &self,
        opctx: &OpContext,
        group_id: MulticastGroupUuid,
        instance_id: InstanceUuid,
    ) -> Result<bool, external::Error> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let now = Utc::now();

        // Mark member for removal (set time_deleted and state to "Left"), similar
        // to soft instance deletion
        let updated_rows = diesel::update(dsl::multicast_group_member)
            .filter(dsl::external_group_id.eq(group_id.into_untyped_uuid()))
            .filter(dsl::parent_id.eq(instance_id.into_untyped_uuid()))
            .filter(dsl::time_deleted.is_null())
            .set((
                dsl::state.eq(MulticastGroupMemberState::Left),
                dsl::sled_id.eq(Option::<DbTypedUuid<SledKind>>::None),
                dsl::time_deleted.eq(Some(now)), // Mark for deletion
                dsl::time_modified.eq(now),
            ))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(updated_rows > 0)
    }

    /// Update sled_id for all multicast group memberships of an instance.
    ///
    /// Used by instance sagas to update sled_id during lifecycle transitions:
    /// - Start: NULL → actual sled UUID
    /// - Stop: actual sled UUID → NULL
    /// - Migrate: old sled UUID → new sled UUID
    ///
    /// Only updates non-"Left" members. RPW detects the change and reprograms
    /// the dataplane accordingly.
    ///
    /// Note: This does not update members already in "Left" state. For instance
    /// stops, first transition memberships to "Left" and clear their `sled_id`
    /// via [`Self::multicast_group_members_detach_by_instance`].
    pub async fn multicast_group_member_update_sled_id(
        &self,
        opctx: &OpContext,
        instance_id: InstanceUuid,
        new_sled_id: Option<DbTypedUuid<SledKind>>,
    ) -> Result<(), external::Error> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let operation_type = match new_sled_id {
            Some(_) => "instance_start_or_migrate",
            None => "instance_stop",
        };

        debug!(
            opctx.log,
            "multicast member lifecycle transition: updating sled_id";
            "instance_id" => %instance_id,
            "operation" => operation_type,
            "new_sled_id" => ?new_sled_id
        );

        diesel::update(dsl::multicast_group_member)
            .filter(dsl::parent_id.eq(instance_id.into_untyped_uuid()))
            .filter(dsl::time_deleted.is_null())
            // Only update members not in "Left" state
            .filter(dsl::state.ne(MulticastGroupMemberState::Left))
            .set((
                dsl::sled_id.eq(new_sled_id),
                dsl::time_modified.eq(Utc::now()),
            ))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
            .map(|_| ())
    }

    /// Conditionally update sled_id only if it currently has the expected value.
    ///
    /// Used by RPW reconciler.
    ///
    /// Returns `Ok(true)` if updated, `Ok(false)` if the expected value didn't
    /// match (indicating concurrent modification).
    ///
    /// This prevents race conditions where multiple Nexus instances try to update
    /// the same member's sled_id concurrently. The update only proceeds if the
    /// current sled_id matches `expected_sled_id`, implementing a compare-and-swap
    /// (CAS) pattern.
    pub async fn multicast_group_member_update_sled_id_if_current(
        &self,
        opctx: &OpContext,
        instance_id: InstanceUuid,
        expected_sled_id: Option<DbTypedUuid<SledKind>>,
        new_sled_id: Option<DbTypedUuid<SledKind>>,
    ) -> UpdateResult<bool> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let rows_updated = diesel::update(dsl::multicast_group_member)
            .filter(dsl::parent_id.eq(instance_id.into_untyped_uuid()))
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::state.ne(MulticastGroupMemberState::Left))
            .filter(dsl::sled_id.eq(expected_sled_id)) // CAS condition
            .set((
                dsl::sled_id.eq(new_sled_id),
                dsl::time_modified.eq(Utc::now()),
            ))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(rows_updated > 0)
    }

    /// Set the sled_id for multicast members when an instance starts.
    ///
    /// This handles two scenarios:
    /// 1. **First-time start**: "Joining" (sled_id=NULL) → "Joining" (sled_id=actual)
    /// 2. **Restart after stop**: "Left" (sled_id=NULL) → "Joining" (sled_id=actual)
    ///
    /// After this operation, the RPW reconciler will detect the sled_id and
    /// transition "Joining" → "Joined" by programming the switch.
    ///
    /// # State Transitions
    ///
    /// - "Left" (sled_id=NULL) → "Joining" (sled_id=actual) - Instance restart
    /// - "Joining" (sled_id=NULL) → "Joining" (sled_id=actual) - First-time start
    /// - "Joined" - No change (already has sled_id, ignored)
    ///
    /// See also:
    /// - CAS-based reconciliation helpers for concurrent updates in
    ///   `nexus/db-queries/src/db/datastore/multicast/ops/member_reconcile.rs`.
    /// - Background reconciler docs discussing the CAS pattern in
    ///   `nexus/src/app/background/tasks/multicast/members.rs`.
    pub async fn multicast_group_member_set_instance_sled(
        &self,
        opctx: &OpContext,
        instance_id: InstanceUuid,
        sled_id: DbTypedUuid<SledKind>,
    ) -> Result<(), external::Error> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let now = Utc::now();

        // Update members in "Left" state (restart) or "Joining" state with NULL
        // sled_id (first start)
        // - "Left" → "Joining" + set sled_id (instance restart)
        // - "Joining" (sled_id=NULL) → "Joining" + set sled_id (first-time start)
        diesel::update(dsl::multicast_group_member)
            .filter(dsl::parent_id.eq(instance_id.into_untyped_uuid()))
            .filter(dsl::time_deleted.is_null())
            .filter(
                dsl::state.eq(MulticastGroupMemberState::Left).or(dsl::state
                    .eq(MulticastGroupMemberState::Joining)
                    .and(dsl::sled_id.is_null())),
            )
            .set((
                dsl::state.eq(MulticastGroupMemberState::Joining),
                dsl::sled_id.eq(Some(sled_id)),
                dsl::time_modified.eq(now),
            ))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
            .map(|_| ())
    }

    /// Permanently mark all multicast memberships for deletion when instance is deleted.
    ///
    /// Sets members to "Left" state with `time_deleted` timestamp, indicating
    /// permanent removal (not temporary like instance stop). This distinguishes
    /// permanent deletion from instance stop which only sets state="Left"
    /// without `time_deleted`, allowing later reactivation.
    ///
    /// After this operation:
    /// - Members cannot be reactivated (new attach creates new member record)
    /// - RPW reconciler will remove DPD configuration
    /// - Cleanup task will eventually hard-delete the database rows
    ///
    /// Compare with [`Self::multicast_group_members_detach_by_instance`] which leaves
    /// `time_deleted=NULL` for reactivation on instance restart.
    pub async fn multicast_group_members_mark_for_removal(
        &self,
        opctx: &OpContext,
        instance_id: InstanceUuid,
    ) -> Result<(), external::Error> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let now = Utc::now();

        diesel::update(dsl::multicast_group_member)
            .filter(dsl::parent_id.eq(instance_id.into_untyped_uuid()))
            .filter(dsl::time_deleted.is_null())
            .set((
                dsl::state.eq(MulticastGroupMemberState::Left), // Transition to Left state
                dsl::sled_id.eq(Option::<DbTypedUuid<SledKind>>::None), // Clear sled reference
                dsl::time_deleted.eq(Some(now)), // Mark for deletion
                dsl::time_modified.eq(now),
            ))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
            .map(|_| ())
    }

    /// Mark a multicast group member for deletion by ID.
    ///
    /// This performs a soft delete by setting the member to "Left" state and
    /// setting `time_deleted`. The RPW reconciler will remove the member from
    /// DPD, and later cleanup will hard-delete the database record.
    pub async fn multicast_group_member_delete_by_id(
        &self,
        opctx: &OpContext,
        member_id: Uuid,
    ) -> DeleteResult {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let now = Utc::now();

        let updated_rows = diesel::update(dsl::multicast_group_member)
            .filter(dsl::id.eq(member_id))
            .filter(dsl::time_deleted.is_null())
            .set((
                dsl::state.eq(MulticastGroupMemberState::Left),
                dsl::sled_id.eq(Option::<DbTypedUuid<SledKind>>::None),
                dsl::time_deleted.eq(Some(now)),
                dsl::time_modified.eq(now),
            ))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        if updated_rows == 0 {
            return Err(external::Error::not_found_by_id(
                ResourceType::MulticastGroupMember,
                &member_id,
            ));
        }

        debug!(
            opctx.log,
            "multicast group member marked for deletion";
            "member_id" => %member_id,
            "rows_updated" => updated_rows
        );

        Ok(())
    }

    /// Complete deletion of multicast group members that are in
    /// ["Left"](MulticastGroupMemberState::Left) state and `time_deleted` is
    /// set.
    ///
    /// Returns the number of members physically deleted.
    pub async fn multicast_group_members_complete_delete(
        &self,
        opctx: &OpContext,
    ) -> Result<usize, external::Error> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let deleted_rows = diesel::delete(dsl::multicast_group_member)
            .filter(dsl::state.eq(MulticastGroupMemberState::Left))
            .filter(dsl::time_deleted.is_not_null())
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        debug!(
            opctx.log,
            "multicast group member complete deletion finished";
            "left_and_time_deleted_members_deleted" => deleted_rows
        );

        Ok(deleted_rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use nexus_types::identity::Resource;
    use nexus_types::multicast::MulticastGroupCreate;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::SledUuid;

    use crate::db::pub_test_utils::helpers::{
        SledUpdateBuilder, attach_instance_to_vmm, create_instance_with_vmm,
        create_stopped_instance_record, create_vmm_for_instance,
    };
    use crate::db::pub_test_utils::{TestDatabase, multicast};

    // NOTE: These are datastore-level tests. They validate database state
    // transitions, validations, and query behavior for multicast members.
    // They purposefully do not exercise the reconciler (RPW) or dataplane (DPD)
    // components. End-to-end RPW/DPD behavior is covered by integration tests
    // under `nexus/tests/integration_tests/multicast`.

    // Lists all active multicast group members.
    impl DataStore {
        async fn multicast_group_members_list_active_test(
            &self,
            opctx: &OpContext,
        ) -> ListResultVec<MulticastGroupMember> {
            use nexus_db_schema::schema::multicast_group_member::dsl;

            dsl::multicast_group_member
                .filter(dsl::time_deleted.is_null())
                .filter(dsl::state.ne(MulticastGroupMemberState::Left))
                .order(dsl::id.asc())
                .select(MulticastGroupMember::as_select())
                .load_async(&*self.pool_connection_authorized(opctx).await?)
                .await
                .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
        }
    }

    #[tokio::test]
    async fn test_multicast_group_member_attach_to_instance() {
        let logctx = dev::test_setup_log(
            "test_multicast_group_member_attach_to_instance",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "attach-test-pool",
            "test-project-attach",
        )
        .await;

        // Create active group using helper
        let active_group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            &setup,
            "active-group",
            "224.10.1.5",
            true, // make_active
        )
        .await;

        // Create creating group manually (needs to stay in "Creating" state)
        let creating_group_params = MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "creating-group".parse().unwrap(),
                description: "Creating test group".to_string(),
            },
            multicast_ip: Some("224.10.1.6".parse().unwrap()),
            source_ips: None,
            // Pool resolved via authz_pool argument to datastore call
            mvlan: None,
        };

        let creating_group = datastore
            .multicast_group_create(
                &opctx,
                &creating_group_params,
                Some(setup.authz_pool.clone()),
            )
            .await
            .expect("Should create creating multicast group");

        // Create test instance
        let (instance, _vmm) = create_instance_with_vmm(
            &opctx,
            &datastore,
            &setup.authz_project,
            "attach-test-instance",
            setup.sled_id,
        )
        .await;
        let instance_id = *instance.as_untyped_uuid();

        // Cannot attach to group in "Creating" state (not "Active")
        let result = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(creating_group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await;
        assert!(result.is_err());
        match result.unwrap_err() {
            external::Error::InvalidRequest { .. } => (),
            other => panic!(
                "Expected InvalidRequest for 'Creating' group, got: {:?}",
                other
            ),
        }

        // First attach to active group should succeed and create new member
        let member_id = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(active_group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Should attach instance to active group");

        // Verify member was created in "Joining" state
        let member = datastore
            .multicast_group_member_get_by_group_and_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(active_group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Should get member")
            .expect("Member should exist");

        assert_eq!(member.id, member_id);
        assert_eq!(member.state, MulticastGroupMemberState::Joining);
        assert_eq!(member.sled_id, Some(setup.sled_id.into()));
        let time_after_first_attach = member.time_modified;

        // Second attach to same group with member in "Joining" state should be
        // idempotent
        let member_id2 = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(active_group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Should handle duplicate attach to 'Joining' member");

        assert_eq!(member_id, member_id2, "Should return same member ID");
        // Verify idempotency: time_modified unchanged
        let member_after_second = datastore
            .multicast_group_member_get_by_group_and_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(active_group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Should get member after second attach")
            .expect("Member should exist");
        assert_eq!(
            member_after_second.time_modified, time_after_first_attach,
            "Idempotent attach must not update time_modified"
        );

        // Transition member to "Joined" state and capture time_modified
        datastore
            .multicast_group_member_set_state(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(active_group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
                MulticastGroupMemberState::Joined,
            )
            .await
            .expect("Should transition member to 'Joined'");
        let member_joined = datastore
            .multicast_group_member_get_by_group_and_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(active_group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Should refetch member after Joined")
            .expect("Member should exist");
        let time_after_joined = member_joined.time_modified;

        // Attach to member in "Joined" state should be idempotent
        let member_id3 = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(active_group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Should handle attach to 'Joined' member");

        assert_eq!(member_id, member_id3, "Should return same member ID");
        // Verify idempotency in "Joined": time_modified unchanged
        let member_after_third = datastore
            .multicast_group_member_get_by_group_and_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(active_group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Should get member after third attach")
            .expect("Member should exist");
        assert_eq!(
            member_after_third.time_modified, time_after_joined,
            "Idempotent attach while Joined must not update time_modified"
        );

        // Transition member to "Left" state (simulating instance stop)
        datastore
            .multicast_group_member_set_state(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(active_group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
                MulticastGroupMemberState::Left,
            )
            .await
            .expect("Should transition member to 'Left'");

        // Update member to have no sled_id (simulating stopped instance)
        datastore
            .multicast_group_member_update_sled_id(
                &opctx,
                InstanceUuid::from_untyped_uuid(instance_id),
                None,
            )
            .await
            .expect("Should clear sled_id for stopped instance");
        let member_left = datastore
            .multicast_group_member_get_by_group_and_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(active_group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Should get member after Left")
            .expect("Member should exist");
        let time_after_left = member_left.time_modified;

        // Attach to member in "Left" state should reactivate it
        let member_id4 = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(active_group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Should reactivate 'Left' member");

        assert_eq!(member_id, member_id4, "Should return same member ID");

        // Verify member was reactivated to "Joining" state with updated sled_id
        let reactivated_member = datastore
            .multicast_group_member_get_by_group_and_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(active_group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Should get reactivated member")
            .expect("Reactivated member should exist");

        assert_eq!(
            reactivated_member.state,
            MulticastGroupMemberState::Joining
        );
        assert_eq!(reactivated_member.sled_id, Some(setup.sled_id.into()));
        assert!(
            reactivated_member.time_modified >= time_after_left,
            "Reactivation should advance time_modified"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_multicast_group_members_detach_by_instance() {
        let logctx = dev::test_setup_log(
            "test_multicast_group_members_detach_by_instance",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "test-pool",
            "test-project",
        )
        .await;

        // Create multiple multicast groups
        let group1 = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            &setup,
            "group1",
            "224.10.1.5",
            true, // make_active
        )
        .await;
        let group2 = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            &setup,
            "group2",
            "224.10.1.6",
            true, // make_active
        )
        .await;

        // Create test instances
        let instance1_record = create_stopped_instance_record(
            &opctx,
            &datastore,
            &setup.authz_project,
            "test-instance-1",
        )
        .await;
        let instance1_id = instance1_record.as_untyped_uuid();
        let instance2_record = create_stopped_instance_record(
            &opctx,
            &datastore,
            &setup.authz_project,
            "test-instance-2",
        )
        .await;
        let instance2_id = instance2_record.as_untyped_uuid();

        // Create VMMs and associate instances with sled (required for multicast membership)
        let vmm1_id = create_vmm_for_instance(
            &opctx,
            &datastore,
            instance1_record,
            setup.sled_id,
        )
        .await;
        attach_instance_to_vmm(
            &opctx,
            &datastore,
            &setup.authz_project,
            instance1_record,
            vmm1_id,
        )
        .await;

        let vmm2_id = create_vmm_for_instance(
            &opctx,
            &datastore,
            instance2_record,
            setup.sled_id,
        )
        .await;
        attach_instance_to_vmm(
            &opctx,
            &datastore,
            &setup.authz_project,
            instance2_record,
            vmm2_id,
        )
        .await;

        // Add instance1 to both groups and instance2 to only group1
        let member1_1 = datastore
            .multicast_group_member_add(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group1.id()),
                InstanceUuid::from_untyped_uuid(*instance1_id),
            )
            .await
            .expect("Should add instance1 to group1");

        let member1_2 = datastore
            .multicast_group_member_add(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group2.id()),
                InstanceUuid::from_untyped_uuid(*instance1_id),
            )
            .await
            .expect("Should add instance1 to group2");

        let member2_1 = datastore
            .multicast_group_member_add(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group1.id()),
                InstanceUuid::from_untyped_uuid(*instance2_id),
            )
            .await
            .expect("Should add instance2 to group1");

        // Verify all memberships exist
        assert_eq!(member1_1.parent_id, *instance1_id);
        assert_eq!(member1_2.parent_id, *instance1_id);
        assert_eq!(member2_1.parent_id, *instance2_id);

        // Detach all memberships for instance1 (transitions to "Left", does not set time_deleted)
        datastore
            .multicast_group_members_detach_by_instance(
                &opctx,
                InstanceUuid::from_untyped_uuid(*instance1_id),
            )
            .await
            .expect("Should detach all memberships for instance1");

        // Verify time_deleted was not set (members still exist, just in "Left" state)
        let detached_member1 = datastore
            .multicast_group_member_get_by_id(&opctx, member1_1.id, false)
            .await
            .expect("Should fetch member")
            .expect("Member should still exist");
        assert_eq!(detached_member1.state, MulticastGroupMemberState::Left);
        assert!(
            detached_member1.time_deleted.is_none(),
            "detach_by_instance should not set time_deleted"
        );
        assert!(
            detached_member1.sled_id.is_none(),
            "sled_id should be cleared"
        );

        // Verify instance1 memberships transitioned to Left state
        datastore
            .multicast_group_members_list_by_id(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group1.id()),
                &external::DataPageParams::max_page(),
            )
            .await
            .expect("Should list group1 members");

        datastore
            .multicast_group_members_list_by_id(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group2.id()),
                &external::DataPageParams::max_page(),
            )
            .await
            .expect("Should list group2 members");

        // Use list_active_test to get only active members (excludes "Left" state)
        let active_group1_members = datastore
            .multicast_group_members_list_active_test(&opctx)
            .await
            .expect("Should list active members")
            .into_iter()
            .filter(|m| m.external_group_id == group1.id())
            .collect::<Vec<_>>();
        assert_eq!(active_group1_members.len(), 1);
        assert_eq!(active_group1_members[0].parent_id, *instance2_id);

        let active_group2_members = datastore
            .multicast_group_members_list_active_test(&opctx)
            .await
            .expect("Should list active members")
            .into_iter()
            .filter(|m| m.external_group_id == group2.id())
            .collect::<Vec<_>>();
        assert_eq!(active_group2_members.len(), 0);

        // Test idempotency - detaching again should be idempotent
        datastore
            .multicast_group_members_detach_by_instance(
                &opctx,
                InstanceUuid::from_untyped_uuid(*instance1_id),
            )
            .await
            .expect("Should handle detaching instance1 again");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_multicast_group_member_operations_with_parent_id() {
        let logctx = dev::test_setup_log(
            "test_multicast_group_member_operations_with_parent_id",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let setup = multicast::create_test_setup_with_range(
            &opctx,
            &datastore,
            "parent-id-test-pool",
            "test-project2",
            (224, 0, 2, 1),
            (224, 0, 2, 254),
        )
        .await;
        let group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            &setup,
            "parent-id-test-group",
            "224.0.2.5",
            true,
        )
        .await;

        // Create test instance
        let instance_record = create_stopped_instance_record(
            &opctx,
            &datastore,
            &setup.authz_project,
            "test-instance-parent",
        )
        .await;
        let instance_id = instance_record.as_untyped_uuid();

        // Create VMM and associate instance with sled (required for multicast membership)
        let vmm_id = create_vmm_for_instance(
            &opctx,
            &datastore,
            instance_record,
            setup.sled_id,
        )
        .await;
        attach_instance_to_vmm(
            &opctx,
            &datastore,
            &setup.authz_project,
            instance_record,
            vmm_id,
        )
        .await;

        // Add member using parent_id (instance_id)
        let member = datastore
            .multicast_group_member_add(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(*instance_id),
            )
            .await
            .expect("Should add instance as member");

        // Verify member has correct parent_id
        assert_eq!(member.parent_id, *instance_id);
        assert_eq!(member.external_group_id, group.id());
        assert_eq!(member.state, MulticastGroupMemberState::Joining);

        // Test member lookup by parent_id
        let member_memberships = datastore
            .multicast_group_members_list_by_instance(
                &opctx,
                InstanceUuid::from_untyped_uuid(*instance_id),
            )
            .await
            .expect("Should list memberships for instance");

        assert_eq!(member_memberships.len(), 1);
        assert_eq!(member_memberships[0].parent_id, *instance_id);
        assert_eq!(member_memberships[0].external_group_id, group.id());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_multicast_group_member_duplicate_prevention() {
        let logctx = dev::test_setup_log(
            "test_multicast_group_member_duplicate_prevention",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "duplicate-test-pool",
            "test-project3",
        )
        .await;
        let group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            &setup,
            "duplicate-test-group",
            "224.10.1.5",
            true,
        )
        .await;

        // Create test instance
        let instance_id = create_stopped_instance_record(
            &opctx,
            &datastore,
            &setup.authz_project,
            "test-instance-dup",
        )
        .await;

        // Create VMM and associate instance with sled (required for multicast membership)
        let vmm_id = create_vmm_for_instance(
            &opctx,
            &datastore,
            instance_id,
            setup.sled_id,
        )
        .await;
        attach_instance_to_vmm(
            &opctx,
            &datastore,
            &setup.authz_project,
            instance_id,
            vmm_id,
        )
        .await;

        // Add member first time - should succeed
        let member1 = datastore
            .multicast_group_member_add(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                instance_id,
            )
            .await
            .expect("Should add instance as member first time");

        // Try to add same instance again - should return existing member
        let member2 = datastore
            .multicast_group_member_add(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                instance_id,
            )
            .await
            .expect("Should handle duplicate add idempotently");

        // Should return the same member
        assert_eq!(member1.id, member2.id);
        assert_eq!(member1.parent_id, member2.parent_id);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_multicast_member_sled_id_lifecycle() {
        let logctx =
            dev::test_setup_log("test_multicast_member_sled_id_lifecycle");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "lifecycle-test-pool",
            "test-project-lifecycle",
        )
        .await;
        let group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            &setup,
            "lifecycle-test-group",
            "224.10.1.5",
            true,
        )
        .await;

        // Create additional test sleds for migration testing
        let sled1_id = SledUuid::new_v4();
        let sled1_update = SledUpdateBuilder::new().sled_id(sled1_id).build();
        datastore.sled_upsert(sled1_update).await.unwrap();

        let sled2_id = SledUuid::new_v4();
        let sled2_update = SledUpdateBuilder::new().sled_id(sled2_id).build();
        datastore.sled_upsert(sled2_update).await.unwrap();

        // Create test instance
        let instance_id = create_stopped_instance_record(
            &opctx,
            &datastore,
            &setup.authz_project,
            "lifecycle-test-instance",
        )
        .await;
        let test_instance_id = instance_id.into_untyped_uuid();

        // Create member record in "Joining" state (no sled_id initially)
        let member = datastore
            .multicast_group_member_add(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(test_instance_id),
            )
            .await
            .expect("Should create member record");

        // Member initially has no sled_id (created in "Joining" state)
        assert_eq!(member.sled_id, None);

        // Instance start - Update sled_id from NULL to actual sled
        datastore
            .multicast_group_member_update_sled_id(
                &opctx,
                InstanceUuid::from_untyped_uuid(test_instance_id),
                Some(sled1_id.into()),
            )
            .await
            .expect("Should update sled_id for instance start");

        // Verify sled_id was updated
        let updated_member = datastore
            .multicast_group_member_get_by_group_and_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(test_instance_id),
            )
            .await
            .expect("Should fetch updated member")
            .expect("Member should exist");

        assert_eq!(updated_member.sled_id, Some(sled1_id.into()));

        // Instance migration - Update sled_id from sled1 to sled2
        datastore
            .multicast_group_member_update_sled_id(
                &opctx,
                InstanceUuid::from_untyped_uuid(test_instance_id),
                Some(sled2_id.into()),
            )
            .await
            .expect("Should update sled_id for instance migration");

        // Verify sled_id was updated to new sled
        let migrated_member = datastore
            .multicast_group_member_get_by_group_and_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(test_instance_id),
            )
            .await
            .expect("Should fetch migrated member")
            .expect("Member should exist");

        assert_eq!(migrated_member.sled_id, Some(sled2_id.into()));

        // Instance stop - Clear sled_id (set to NULL)
        datastore
            .multicast_group_members_detach_by_instance(
                &opctx,
                InstanceUuid::from_untyped_uuid(test_instance_id),
            )
            .await
            .expect("Should clear sled_id for instance stop");

        // Verify sled_id was cleared
        let stopped_member = datastore
            .multicast_group_member_get_by_group_and_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(test_instance_id),
            )
            .await
            .expect("Should fetch stopped member")
            .expect("Member should exist");

        assert_eq!(stopped_member.sled_id, None);

        // Idempotency - Clearing again should be idempotent
        datastore
            .multicast_group_members_detach_by_instance(
                &opctx,
                InstanceUuid::from_untyped_uuid(test_instance_id),
            )
            .await
            .expect("Should handle clearing sled_id again");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    /// Datastore-only verification of member state transitions.
    async fn test_multicast_group_member_state_transitions_datastore() {
        let logctx = dev::test_setup_log(
            "test_multicast_group_member_state_transitions_datastore",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let setup = multicast::create_test_setup_with_range(
            &opctx,
            &datastore,
            "state-test-pool",
            "test-project4",
            (224, 2, 1, 1),
            (224, 2, 1, 254),
        )
        .await;
        let group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            &setup,
            "state-test-group",
            "224.2.1.5",
            true,
        )
        .await;

        // Create test instance (datastore-only)
        let (instance, _vmm) = create_instance_with_vmm(
            &opctx,
            &datastore,
            &setup.authz_project,
            "state-test-instance",
            setup.sled_id,
        )
        .await;
        let test_instance_id = instance.into_untyped_uuid();

        // Create member record directly in "Joining" state
        datastore
            .multicast_group_member_add(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(test_instance_id),
            )
            .await
            .expect("Should create member record");

        // Complete the attach operation
        datastore
            .multicast_group_member_set_state(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(test_instance_id),
                MulticastGroupMemberState::Joined,
            )
            .await
            .expect("Should complete attach operation");

        // Complete the operation and leave
        datastore
            .multicast_group_member_set_state(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(test_instance_id),
                MulticastGroupMemberState::Left,
            )
            .await
            .expect("Should complete detach operation");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_multicast_group_members_complete_delete() {
        let logctx =
            dev::test_setup_log("test_multicast_group_members_complete_delete");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "complete-delete-test-pool",
            "test-project-cleanup",
        )
        .await;
        let group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            &setup,
            "cleanup-test-group",
            "224.10.1.5",
            true,
        )
        .await;

        // Create real instances for the test
        let (instance1, _vmm1) = create_instance_with_vmm(
            &opctx,
            &datastore,
            &setup.authz_project,
            "delete-test-instance1",
            setup.sled_id,
        )
        .await;
        let instance1_id = instance1.into_untyped_uuid();

        let (instance2, _vmm2) = create_instance_with_vmm(
            &opctx,
            &datastore,
            &setup.authz_project,
            "delete-test-instance2",
            setup.sled_id,
        )
        .await;
        let instance2_id = instance2.into_untyped_uuid();

        let (instance3, _vmm3) = create_instance_with_vmm(
            &opctx,
            &datastore,
            &setup.authz_project,
            "delete-test-instance3",
            setup.sled_id,
        )
        .await;
        let instance3_id = instance3.into_untyped_uuid();

        // Create member records in different states
        let conn = datastore
            .pool_connection_authorized(&opctx)
            .await
            .expect("Get connection");
        use nexus_db_schema::schema::multicast_group_member::dsl;

        // Member 1: "Left" + `time_deleted` (should be deleted)
        let member1: MulticastGroupMember =
            diesel::insert_into(dsl::multicast_group_member)
                .values(MulticastGroupMemberValues {
                    id: Uuid::new_v4(),
                    time_created: Utc::now(),
                    time_modified: Utc::now(),
                    time_deleted: Some(Utc::now()),
                    external_group_id: group.id(),
                    multicast_ip: group.multicast_ip,
                    parent_id: instance1_id,
                    sled_id: Some(setup.sled_id.into()),
                    state: MulticastGroupMemberState::Left,
                })
                .returning(MulticastGroupMember::as_returning())
                .get_result_async(&*conn)
                .await
                .expect("Should create member1 record");

        // Member 2: "Left" but no `time_deleted` (should not be deleted)
        let member2: MulticastGroupMember =
            diesel::insert_into(dsl::multicast_group_member)
                .values(MulticastGroupMemberValues {
                    id: Uuid::new_v4(),
                    time_created: Utc::now(),
                    time_modified: Utc::now(),
                    time_deleted: None,
                    external_group_id: group.id(),
                    multicast_ip: group.multicast_ip,
                    parent_id: instance2_id,
                    sled_id: Some(setup.sled_id.into()),
                    state: MulticastGroupMemberState::Left,
                })
                .returning(MulticastGroupMember::as_returning())
                .get_result_async(&*conn)
                .await
                .expect("Should create member2 record");

        // Member 3: "Joined" state (should not be deleted, even if it had time_deleted)
        let member3: MulticastGroupMember =
            diesel::insert_into(dsl::multicast_group_member)
                .values(MulticastGroupMemberValues {
                    id: Uuid::new_v4(),
                    time_created: Utc::now(),
                    time_modified: Utc::now(),
                    time_deleted: Some(Utc::now()), // Has time_deleted but is Joined, so won't be cleaned up
                    external_group_id: group.id(),
                    multicast_ip: group.multicast_ip,
                    parent_id: instance3_id,
                    sled_id: Some(setup.sled_id.into()),
                    state: MulticastGroupMemberState::Joined,
                })
                .returning(MulticastGroupMember::as_returning())
                .get_result_async(&*conn)
                .await
                .expect("Should create member3 record");

        // Since we created exactly 3 member records above, we can verify by
        // checking that each member was created successfully (no need for a
        // full table scan) member1: "Left" + `time_deleted`, member2: "Left" +
        // no `time_deleted`, member3: "Joined" + `time_deleted`

        // Run complete delete
        let deleted_count = datastore
            .multicast_group_members_complete_delete(&opctx)
            .await
            .expect("Should run complete delete");

        // Should only delete member1 ("Left" + `time_deleted`)
        assert_eq!(deleted_count, 1);

        // Verify member1 was deleted by trying to find it directly
        let member1_result = datastore
            .multicast_group_member_get_by_group_and_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(member1.parent_id),
            )
            .await
            .expect("Should query for member1");
        assert!(member1_result.is_none(), "member1 should be deleted");

        // Verify member2 still exists
        let member2_result = datastore
            .multicast_group_member_get_by_group_and_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(member2.parent_id),
            )
            .await
            .expect("Should query for member2");
        assert!(member2_result.is_some(), "member2 should still exist");

        // Verify member3 still exists (time_deleted set but not cleaned up yet)
        let member3_result = datastore
            .multicast_group_member_get_by_id(&opctx, member3.id, true)
            .await
            .expect("Should query for member3");
        assert!(
            member3_result.is_some(),
            "member3 should still exist in database (not cleaned up due to 'Joined' state)"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_instance_get_sled_id() {
        let logctx = dev::test_setup_log("test_instance_get_sled_id");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "sled-test-pool",
            "test-project-sled",
        )
        .await;

        // Non-existent instance should return NotFound error
        let fake_instance_id = Uuid::new_v4();
        let result =
            datastore.instance_get_sled_id(&opctx, fake_instance_id).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            external::Error::ObjectNotFound { .. } => (),
            other => panic!("Expected ObjectNotFound, got: {:?}", other),
        }

        // Stopped instance (no active VMM) should return None
        let stopped_instance = create_stopped_instance_record(
            &opctx,
            &datastore,
            &setup.authz_project,
            "stopped-instance",
        )
        .await;
        let stopped_instance_id = stopped_instance.as_untyped_uuid();

        let result = datastore
            .instance_get_sled_id(&opctx, *stopped_instance_id)
            .await
            .expect("Should get sled_id for stopped instance");
        assert_eq!(result, None);

        // Running instance (with active VMM) should return the sled_id
        let (running_instance, _vmm) = create_instance_with_vmm(
            &opctx,
            &datastore,
            &setup.authz_project,
            "running-instance",
            setup.sled_id,
        )
        .await;
        let running_instance_id = running_instance.as_untyped_uuid();

        let result = datastore
            .instance_get_sled_id(&opctx, *running_instance_id)
            .await
            .expect("Should get sled_id for running instance");
        assert_eq!(result, Some(setup.sled_id.into_untyped_uuid()));

        // Instance with VMM but no active_propolis_id should return None
        let inactive_instance = create_stopped_instance_record(
            &opctx,
            &datastore,
            &setup.authz_project,
            "inactive-instance",
        )
        .await;
        let inactive_instance_id = inactive_instance.as_untyped_uuid();

        // Create VMM but don't attach it (no active_propolis_id)
        create_vmm_for_instance(
            &opctx,
            &datastore,
            inactive_instance,
            setup.sled_id,
        )
        .await;

        let result = datastore
            .instance_get_sled_id(&opctx, *inactive_instance_id)
            .await
            .expect("Should get sled_id for inactive instance");
        assert_eq!(result, None);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_multicast_group_member_database_error_handling() {
        let logctx = dev::test_setup_log(
            "test_multicast_group_member_database_error_handling",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "error-test-pool",
            "test-project-errors",
        )
        .await;
        let group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            &setup,
            "error-test-group",
            "224.10.1.6",
            true,
        )
        .await;

        // Create test instance
        let (instance, _vmm) = create_instance_with_vmm(
            &opctx,
            &datastore,
            &setup.authz_project,
            "error-test-instance",
            setup.sled_id,
        )
        .await;
        let instance_id = *instance.as_untyped_uuid();

        // Operations on non-existent groups should return appropriate errors
        let fake_group_id = Uuid::new_v4();

        // Try to add member to non-existent group
        let result = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(fake_group_id),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await;
        assert!(result.is_err(), "Attach to non-existent group should fail");

        // Try to set state for non-existent member
        let result = datastore
            .multicast_group_member_set_state(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(fake_group_id),
                InstanceUuid::from_untyped_uuid(instance_id),
                MulticastGroupMemberState::Joined,
            )
            .await;
        assert!(
            result.is_err(),
            "Set state for non-existent member should fail"
        );

        // Try to get member from non-existent group
        let result = datastore
            .multicast_group_member_get_by_group_and_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(fake_group_id),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Query should succeed");
        assert!(result.is_none(), "Non-existent member should return None");

        // Operations on non-existent instances should handle errors appropriately
        let fake_instance_id = Uuid::new_v4();

        // Try to get sled_id for non-existent instance
        let result =
            datastore.instance_get_sled_id(&opctx, fake_instance_id).await;
        assert!(
            result.is_err(),
            "Get sled_id for non-existent instance should fail"
        );

        // Try to attach non-existent instance to group
        let result = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(fake_instance_id),
            )
            .await;
        assert!(result.is_err(), "Attach non-existent instance should fail");

        // Successfully create a member for further testing
        datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Should create member");

        // Invalid state transitions should be handled gracefully
        datastore
            .multicast_group_member_set_state(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
                MulticastGroupMemberState::Left,
            )
            .await
            .expect("Should allow transition to 'Left'");

        datastore
            .multicast_group_member_set_state(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
                MulticastGroupMemberState::Joined,
            )
            .await
            .expect("Should allow transition back to 'Joined'");

        // Test idempotent operations work correctly
        datastore
            .multicast_group_members_detach_by_instance(
                &opctx,
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("First detach should succeed");

        datastore
            .multicast_group_members_detach_by_instance(
                &opctx,
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Second detach should be idempotent");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_multicast_group_member_set_instance_sled() {
        let logctx = dev::test_setup_log(
            "test_multicast_group_member_set_instance_sled",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create test setup
        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "start-test-pool",
            "test-project",
        )
        .await;

        // Create multicast group
        let group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            &setup,
            "start-test-group",
            "224.10.1.100",
            true,
        )
        .await;

        let initial_sled = SledUuid::new_v4();
        let new_sled = SledUuid::new_v4();

        // Create sled records
        datastore
            .sled_upsert(SledUpdateBuilder::new().sled_id(initial_sled).build())
            .await
            .unwrap();
        datastore
            .sled_upsert(SledUpdateBuilder::new().sled_id(new_sled).build())
            .await
            .unwrap();

        // Create test instance
        let instance_record = create_stopped_instance_record(
            &opctx,
            &datastore,
            &setup.authz_project,
            "start-test-instance",
        )
        .await;
        let instance_id =
            InstanceUuid::from_untyped_uuid(*instance_record.as_untyped_uuid());

        // Add member in "Joining" state (typical after instance create)
        let member = datastore
            .multicast_group_member_add(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                instance_id,
            )
            .await
            .expect("Should add member");

        // Verify initial state: "Joining" with no sled_id
        assert_eq!(member.state, MulticastGroupMemberState::Joining);
        assert!(member.sled_id.is_none());

        // Simulate first-time instance start - use update_sled_id for "Joining" members
        datastore
            .multicast_group_member_update_sled_id(
                &opctx,
                instance_id,
                Some(initial_sled.into()),
            )
            .await
            .expect("Should update sled_id on first start");

        // Verify member is still "Joining" but now has sled_id
        let updated_member = datastore
            .multicast_group_member_get_by_group_and_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                instance_id,
            )
            .await
            .expect("Should find updated member")
            .expect("Member should exist");

        assert_eq!(updated_member.state, MulticastGroupMemberState::Joining);
        assert_eq!(updated_member.sled_id, Some(initial_sled.into()));
        assert!(updated_member.time_modified > member.time_modified);

        // Simulate instance stop by transitioning to "Left" state
        datastore
            .multicast_group_members_detach_by_instance(&opctx, instance_id)
            .await
            .expect("Should stop instance");

        // Verify member is "Left" with no sled_id
        let stopped_member = datastore
            .multicast_group_member_get_by_group_and_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                instance_id,
            )
            .await
            .expect("Should find stopped member")
            .expect("Member should exist");

        assert_eq!(stopped_member.state, MulticastGroupMemberState::Left);
        assert!(stopped_member.sled_id.is_none());

        // Simulate instance restart on new sled - should transition "Left" → "Joining"
        datastore
            .multicast_group_member_set_instance_sled(
                &opctx,
                instance_id,
                new_sled.into(),
            )
            .await
            .expect("Should restart instance on new sled");

        // Verify member is back to "Joining" with new sled_id
        let restarted_member = datastore
            .multicast_group_member_get_by_group_and_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                instance_id,
            )
            .await
            .expect("Should find restarted member")
            .expect("Member should exist");

        assert_eq!(restarted_member.state, MulticastGroupMemberState::Joining);
        assert_eq!(restarted_member.sled_id, Some(new_sled.into()));
        assert!(restarted_member.time_modified > stopped_member.time_modified);

        // Test that starting instance with "Joined" members works correctly
        // First transition to "Joined" state (simulate RPW reconciler)
        datastore
            .multicast_group_member_set_state(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                instance_id,
                MulticastGroupMemberState::Joined,
            )
            .await
            .expect("Should transition to 'Joined'");

        // Verify member is now "Joined"
        let joined_member = datastore
            .multicast_group_member_get_by_group_and_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                instance_id,
            )
            .await
            .expect("Should find joined member")
            .expect("Member should exist");

        assert_eq!(joined_member.state, MulticastGroupMemberState::Joined);

        // Start instance again - "Joined" members should remain unchanged
        let before_modification = joined_member.time_modified;
        datastore
            .multicast_group_member_set_instance_sled(
                &opctx,
                instance_id,
                new_sled.into(),
            )
            .await
            .expect("Should handle start on already-running instance");

        // Verify "Joined" member remains unchanged (no state transition)
        let unchanged_member = datastore
            .multicast_group_member_get_by_group_and_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                instance_id,
            )
            .await
            .expect("Should find unchanged member")
            .expect("Member should exist");

        assert_eq!(unchanged_member.state, MulticastGroupMemberState::Joined);
        assert_eq!(unchanged_member.time_modified, before_modification);

        // Test starting instance that has no multicast memberships (should be no-op)
        let non_member_instance = InstanceUuid::new_v4();
        datastore
            .multicast_group_member_set_instance_sled(
                &opctx,
                non_member_instance,
                new_sled.into(),
            )
            .await
            .expect("Should handle start on instance with no memberships");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_multicast_group_members_mark_for_removal() {
        let logctx = dev::test_setup_log(
            "test_multicast_group_members_mark_for_removal",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create test setup
        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "removal-test-pool",
            "test-project",
        )
        .await;

        // Create multicast groups
        let group1 = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            &setup,
            "removal-group1",
            "224.10.1.100",
            true,
        )
        .await;

        let group2 = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            &setup,
            "removal-group2",
            "224.10.1.101",
            true,
        )
        .await;

        // Create test instances
        let instance1_record = create_stopped_instance_record(
            &opctx,
            &datastore,
            &setup.authz_project,
            "removal-test-instance1",
        )
        .await;
        let instance1_id = InstanceUuid::from_untyped_uuid(
            *instance1_record.as_untyped_uuid(),
        );

        let instance2_record = create_stopped_instance_record(
            &opctx,
            &datastore,
            &setup.authz_project,
            "removal-test-instance2",
        )
        .await;
        let instance2_id = InstanceUuid::from_untyped_uuid(
            *instance2_record.as_untyped_uuid(),
        );

        // Add instance1 to both groups
        let member1_1 = datastore
            .multicast_group_member_add(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group1.id()),
                instance1_id,
            )
            .await
            .expect("Should add instance1 to group1");

        let member1_2 = datastore
            .multicast_group_member_add(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group2.id()),
                instance1_id,
            )
            .await
            .expect("Should add instance1 to group2");

        // Add instance2 to only group1
        let member2_1 = datastore
            .multicast_group_member_add(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group1.id()),
                instance2_id,
            )
            .await
            .expect("Should add instance2 to group1");

        // Verify all members exist and are not marked for removal
        assert!(member1_1.time_deleted.is_none());
        assert!(member1_2.time_deleted.is_none());
        assert!(member2_1.time_deleted.is_none());

        // Mark all memberships for instance1 for removal
        datastore
            .multicast_group_members_mark_for_removal(&opctx, instance1_id)
            .await
            .expect("Should mark instance1 memberships for removal");

        // Verify instance1 memberships are marked for removal
        let marked_member1_1 = datastore
            .multicast_group_member_get_by_id(&opctx, member1_1.id, true)
            .await
            .expect("Should query member1_1")
            .expect("Member1_1 should exist");
        assert!(marked_member1_1.time_deleted.is_some());

        let marked_member1_2 = datastore
            .multicast_group_member_get_by_id(&opctx, member1_2.id, true)
            .await
            .expect("Should query member1_2")
            .expect("Member1_2 should exist");
        assert!(marked_member1_2.time_deleted.is_some());

        // Verify instance2 membership is not marked for removal
        let unmarked_member2_1 = datastore
            .multicast_group_member_get_by_id(&opctx, member2_1.id, true)
            .await
            .expect("Should query member2_1")
            .expect("Member2_1 should exist");
        assert!(unmarked_member2_1.time_deleted.is_none());

        // Verify marked members are not returned by normal queries (time_deleted filter)
        let visible_member1_1 = datastore
            .multicast_group_member_get_by_id(&opctx, member1_1.id, false)
            .await
            .expect("Should query member1_1");
        assert!(
            visible_member1_1.is_none(),
            "Marked member should not be visible"
        );

        let visible_member2_1 = datastore
            .multicast_group_member_get_by_id(&opctx, member2_1.id, false)
            .await
            .expect("Should query member2_1");
        assert!(
            visible_member2_1.is_some(),
            "Unmarked member should be visible"
        );

        // Test idempotency - marking again should be safe
        datastore
            .multicast_group_members_mark_for_removal(&opctx, instance1_id)
            .await
            .expect("Should handle duplicate mark for removal");

        // Test marking instance with no memberships (should be no-op)
        let non_member_instance = InstanceUuid::new_v4();
        datastore
            .multicast_group_members_mark_for_removal(
                &opctx,
                non_member_instance,
            )
            .await
            .expect("Should handle marking instance with no memberships");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_multicast_group_members_delete_by_group() {
        let logctx =
            dev::test_setup_log("test_multicast_group_members_delete_by_group");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create test setup
        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "delete-group-test-pool",
            "test-project",
        )
        .await;

        // Create multicast groups
        let group1 = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            &setup,
            "delete-group1",
            "224.10.1.100",
            true,
        )
        .await;

        let group2 = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            &setup,
            "delete-group2",
            "224.10.1.101",
            true,
        )
        .await;

        // Create test instances
        let instance1_record = create_stopped_instance_record(
            &opctx,
            &datastore,
            &setup.authz_project,
            "delete-test-instance1",
        )
        .await;
        let instance1_id = InstanceUuid::from_untyped_uuid(
            *instance1_record.as_untyped_uuid(),
        );

        let instance2_record = create_stopped_instance_record(
            &opctx,
            &datastore,
            &setup.authz_project,
            "delete-test-instance2",
        )
        .await;
        let instance2_id = InstanceUuid::from_untyped_uuid(
            *instance2_record.as_untyped_uuid(),
        );

        let instance3_record = create_stopped_instance_record(
            &opctx,
            &datastore,
            &setup.authz_project,
            "delete-test-instance3",
        )
        .await;
        let instance3_id = InstanceUuid::from_untyped_uuid(
            *instance3_record.as_untyped_uuid(),
        );

        // Add members to group1
        let member1_1 = datastore
            .multicast_group_member_add(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group1.id()),
                instance1_id,
            )
            .await
            .expect("Should add instance1 to group1");

        let member1_2 = datastore
            .multicast_group_member_add(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group1.id()),
                instance2_id,
            )
            .await
            .expect("Should add instance2 to group1");

        // Add members to group2
        let member2_1 = datastore
            .multicast_group_member_add(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group2.id()),
                instance1_id,
            )
            .await
            .expect("Should add instance1 to group2");

        let member2_2 = datastore
            .multicast_group_member_add(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group2.id()),
                instance3_id,
            )
            .await
            .expect("Should add instance3 to group2");

        // Verify all members exist
        assert!(
            datastore
                .multicast_group_member_get_by_id(&opctx, member1_1.id, false)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            datastore
                .multicast_group_member_get_by_id(&opctx, member1_2.id, false)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            datastore
                .multicast_group_member_get_by_id(&opctx, member2_1.id, false)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            datastore
                .multicast_group_member_get_by_id(&opctx, member2_2.id, false)
                .await
                .unwrap()
                .is_some()
        );

        // Delete all members of group1
        datastore
            .multicast_group_members_delete_by_group(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group1.id()),
            )
            .await
            .expect("Should delete all group1 members");

        // Verify group1 members are gone
        assert!(
            datastore
                .multicast_group_member_get_by_id(&opctx, member1_1.id, true)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            datastore
                .multicast_group_member_get_by_id(&opctx, member1_2.id, true)
                .await
                .unwrap()
                .is_none()
        );

        // Verify group2 members still exist
        assert!(
            datastore
                .multicast_group_member_get_by_id(&opctx, member2_1.id, false)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            datastore
                .multicast_group_member_get_by_id(&opctx, member2_2.id, false)
                .await
                .unwrap()
                .is_some()
        );

        // Verify group1 member list is empty
        let group1_members = datastore
            .multicast_group_members_list_by_id(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group1.id()),
                &external::DataPageParams::max_page(),
            )
            .await
            .expect("Should list group1 members");
        assert_eq!(group1_members.len(), 0);

        // Verify group2 still has its members
        let group2_members = datastore
            .multicast_group_members_list_by_id(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group2.id()),
                &external::DataPageParams::max_page(),
            )
            .await
            .expect("Should list group2 members");
        assert_eq!(group2_members.len(), 2);

        // Test deleting from group with no members (should be no-op)
        datastore
            .multicast_group_members_delete_by_group(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group1.id()),
            )
            .await
            .expect("Should handle deleting from empty group");

        // Test deleting from nonexistent group (should be no-op)
        let fake_group_id = Uuid::new_v4();
        datastore
            .multicast_group_members_delete_by_group(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(fake_group_id),
            )
            .await
            .expect("Should handle deleting from nonexistent group");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_member_attach_concurrent_same_member() {
        let logctx =
            dev::test_setup_log("test_member_attach_concurrent_same_member");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "concurrent-test-pool",
            "concurrent-test-project",
        )
        .await;

        // Create active group
        let group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            &setup,
            "test-group",
            "224.10.1.5",
            true, // make_active
        )
        .await;

        // Create instance
        let (instance, _vmm) = create_instance_with_vmm(
            &opctx,
            &datastore,
            &setup.authz_project,
            "test-instance",
            setup.sled_id,
        )
        .await;
        let instance_id = *instance.as_untyped_uuid();

        // Simulate two Nexus instances concurrently attaching the same member
        let group_id = group.id();
        let datastore1 = datastore.clone();
        let datastore2 = datastore.clone();
        let opctx1 = opctx.child(std::collections::BTreeMap::new());
        let opctx2 = opctx.child(std::collections::BTreeMap::new());

        let handle1 = tokio::spawn(async move {
            datastore1
                .multicast_group_member_attach_to_instance(
                    &opctx1,
                    MulticastGroupUuid::from_untyped_uuid(group_id),
                    InstanceUuid::from_untyped_uuid(instance_id),
                )
                .await
        });

        let handle2 = tokio::spawn(async move {
            datastore2
                .multicast_group_member_attach_to_instance(
                    &opctx2,
                    MulticastGroupUuid::from_untyped_uuid(group_id),
                    InstanceUuid::from_untyped_uuid(instance_id),
                )
                .await
        });

        // Both operations should succeed
        let (result1, result2) = tokio::join!(handle1, handle2);
        let member_id1 = result1
            .expect("Task 1 should complete")
            .expect("Attach 1 should succeed");
        let member_id2 = result2
            .expect("Task 2 should complete")
            .expect("Attach 2 should succeed");

        // Both should return the same member_id
        assert_eq!(member_id1, member_id2);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_member_attach_invalid_group_or_instance() {
        let logctx =
            dev::test_setup_log("test_member_attach_invalid_group_or_instance");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "invalid-test-pool",
            "invalid-test-project",
        )
        .await;

        // Create a valid instance
        let (instance, _vmm) = create_instance_with_vmm(
            &opctx,
            &datastore,
            &setup.authz_project,
            "test-instance",
            setup.sled_id,
        )
        .await;
        let instance_id = *instance.as_untyped_uuid();

        // Attach to non-existent group
        let fake_group_id = Uuid::new_v4();
        let result = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(fake_group_id),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await;

        // Should fail with GroupNotActive (group doesn't exist)
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, external::Error::InvalidRequest { .. }));

        // Create a valid active group
        let group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            &setup,
            "test-group",
            "224.10.1.6",
            true, // make_active
        )
        .await;

        // Attach non-existent instance
        let fake_instance_id = Uuid::new_v4();
        let result = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(fake_instance_id),
            )
            .await;

        // Should fail because CTE validates instance exists atomically
        assert!(result.is_err());
        let err = result.unwrap_err();
        // The error will be InvalidRequest from the CTE (instance not found)
        assert!(matches!(err, external::Error::InvalidRequest { .. }));
        assert!(err.to_string().contains("does not exist"));

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_member_attach_requires_active_group() {
        let logctx =
            dev::test_setup_log("test_member_attach_requires_active_group");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "active-check-pool",
            "active-check-project",
        )
        .await;

        // Create group that stays in Creating state (don't activate)
        let creating_group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            &setup,
            "creating-group",
            "224.10.1.7",
            false, // leave in Creating state
        )
        .await;

        // Create instance
        let (instance, _vmm) = create_instance_with_vmm(
            &opctx,
            &datastore,
            &setup.authz_project,
            "test-instance",
            setup.sled_id,
        )
        .await;
        let instance_id = *instance.as_untyped_uuid();

        // Attempt to attach to non-active group should fail
        let result = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(creating_group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, external::Error::InvalidRequest { .. }));

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_member_attach_idempotency() {
        let logctx = dev::test_setup_log("test_member_attach_idempotency");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "idempotent-test-pool",
            "idempotent-test-project",
        )
        .await;

        // Create active group
        let group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            &setup,
            "test-group",
            "224.10.1.8",
            true, // make_active
        )
        .await;

        // Create instance
        let (instance, _vmm) = create_instance_with_vmm(
            &opctx,
            &datastore,
            &setup.authz_project,
            "test-instance",
            setup.sled_id,
        )
        .await;
        let instance_id = *instance.as_untyped_uuid();

        // First attach
        let member_id1 = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("First attach should succeed");
        // Capture time_modified after first attach
        let member_after_first = datastore
            .multicast_group_member_get_by_group_and_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Should fetch member after first attach")
            .expect("Member should exist");
        let time_after_first = member_after_first.time_modified;

        // Second attach
        let member_id2 = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Second attach should succeed");

        assert_eq!(member_id1, member_id2, "Should return same member ID");
        let member_after_second = datastore
            .multicast_group_member_get_by_group_and_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Should fetch member after second attach")
            .expect("Member should exist");
        assert_eq!(
            member_after_second.time_modified, time_after_first,
            "Idempotent attach must not update time_modified"
        );

        // Third attach (still idempotent)
        let member_id3 = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Third attach should succeed");

        assert_eq!(member_id1, member_id3, "Should return same member ID");
        let member_after_third = datastore
            .multicast_group_member_get_by_group_and_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Should fetch member after third attach")
            .expect("Member should exist");
        assert_eq!(
            member_after_third.time_modified, time_after_first,
            "Idempotent attach must not update time_modified (third call)"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_member_attach_reactivation_from_left() {
        let logctx =
            dev::test_setup_log("test_member_attach_reactivation_from_left");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "reactivation-test-pool",
            "reactivation-test-project",
        )
        .await;

        // Create active group
        let group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            &setup,
            "test-group",
            "224.10.1.9",
            true, // make_active
        )
        .await;

        // Create instance
        let (instance, _vmm) = create_instance_with_vmm(
            &opctx,
            &datastore,
            &setup.authz_project,
            "test-instance",
            setup.sled_id,
        )
        .await;
        let instance_id = *instance.as_untyped_uuid();

        // First attach
        let member_id1 = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("First attach should succeed");

        // Transition member to "Left" state and clear sled_id (simulating instance stop)
        // This does not set time_deleted - only stopped instances can be reactivated
        datastore
            .multicast_group_members_detach_by_instance(
                &opctx,
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Should transition member to 'Left' and clear sled_id");

        // Verify member is now in Left state WITHOUT time_deleted
        let member_stopped = datastore
            .multicast_group_member_get_by_id(&opctx, member_id1, false)
            .await
            .expect("Should get member")
            .expect("Member should still exist (not soft-deleted)");
        assert_eq!(member_stopped.state, MulticastGroupMemberState::Left);
        assert!(
            member_stopped.time_deleted.is_none(),
            "time_deleted should not be set for stopped instances"
        );
        assert!(member_stopped.sled_id.is_none(), "sled_id should be cleared");

        // Reactivate by attaching again (simulating instance restart)
        let member_id2 = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Reactivation should succeed");

        // Should return same member ID (reactivated existing member)
        assert_eq!(member_id1, member_id2, "Should reactivate same member");

        // Verify member is back in "Joining" state with time_deleted still NULL
        let member = datastore
            .multicast_group_member_get_by_group_and_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Should get member")
            .expect("Member should exist");

        assert_eq!(member.state, MulticastGroupMemberState::Joining);
        assert_eq!(member.id, member_id1);
        assert!(
            member.time_deleted.is_none(),
            "time_deleted should remain NULL (never set by detach_by_instance)"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_member_attach_partial_index_behavior() {
        let logctx =
            dev::test_setup_log("test_member_attach_partial_index_behavior");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "partial-index-test-pool",
            "partial-index-test-project",
        )
        .await;

        // Create active group
        let group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            &setup,
            "test-group",
            "224.10.1.10",
            true, // make_active
        )
        .await;

        // Create instance
        let (instance, _vmm) = create_instance_with_vmm(
            &opctx,
            &datastore,
            &setup.authz_project,
            "test-instance",
            setup.sled_id,
        )
        .await;
        let instance_id = *instance.as_untyped_uuid();

        // Create member
        let member_id1 = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Attach should succeed");

        // Transition through states: "Joining" -> "Joined" -> "Left"
        datastore
            .multicast_group_member_set_state(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
                MulticastGroupMemberState::Joined,
            )
            .await
            .expect("Transition to Joined should succeed");

        datastore
            .multicast_group_member_set_state(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
                MulticastGroupMemberState::Left,
            )
            .await
            .expect("Transition to Left should succeed");

        // The partial unique index with predicate (time_deleted IS NULL)
        // works with ON CONFLICT to reactivate an existing row that is in
        // state 'Left' with time_deleted=NULL. In this case, ON CONFLICT
        // updates the row (Left → Joining) instead of inserting a new one.
        let member_id2 = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Should allow reattach of Left member");

        // Should reactivate the same member (not create a new one)
        assert_eq!(member_id1, member_id2);

        // Verify only one member exists for this (group, instance) pair
        let members = datastore
            .multicast_group_members_list_by_instance(
                &opctx,
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("List members should succeed");

        // Filter to our group
        let our_members: Vec<_> = members
            .iter()
            .filter(|m| m.external_group_id == group.id())
            .collect();

        assert_eq!(our_members.len(), 1, "Should have exactly one member");
        assert_eq!(our_members[0].id, member_id1);
        assert_eq!(our_members[0].state, MulticastGroupMemberState::Joining);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_member_attach_error_priority_both_invalid() {
        let logctx = dev::test_setup_log(
            "test_member_attach_error_priority_both_invalid",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let fake_group_id = Uuid::new_v4();
        let fake_instance_id = Uuid::new_v4();

        // Attempt to attach non-existent instance to non-existent group
        let result = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(fake_group_id),
                InstanceUuid::from_untyped_uuid(fake_instance_id),
            )
            .await;

        // Should fail with InstanceNotFound (checked first), not GroupNotActive
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, external::Error::InvalidRequest { .. }));
        assert!(
            err.to_string().contains("Instance does not exist"),
            "Expected InstanceNotFound error, got: {err}"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_member_attach_stopped_instance() {
        let logctx = dev::test_setup_log("test_member_attach_stopped_instance");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "stopped-test-pool",
            "stopped-test-project",
        )
        .await;

        // Create active group
        let group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            &setup,
            "test-group",
            "224.10.1.11",
            true, // make_active
        )
        .await;

        // Create stopped instance (no VMM)
        let instance_id = create_stopped_instance_record(
            &opctx,
            &datastore,
            &setup.authz_project,
            "stopped-instance",
        )
        .await;

        // Attach stopped instance should succeed
        let member_id = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                instance_id,
            )
            .await
            .expect("Should attach stopped instance");

        // Verify member created with sled_id = NULL (no active VMM)
        let member = datastore
            .multicast_group_member_get_by_group_and_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                instance_id,
            )
            .await
            .expect("Should get member")
            .expect("Member should exist");

        assert_eq!(member.id, member_id);
        assert_eq!(member.state, MulticastGroupMemberState::Joining);
        assert_eq!(
            member.sled_id, None,
            "Stopped instance should have sled_id = NULL"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
