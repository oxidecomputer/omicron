//! Multicast group member management operations.
//!
//! Database operations for managing multicast group memberships, including
//! adding/removing members and lifecycle coordination.

use std::collections::{BTreeSet, HashMap};
use std::net::IpAddr;

use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use ipnetwork::IpNetwork;
use serde::{Deserialize, Serialize};
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
};
use crate::db::pagination::paginated;

/// Aggregated source filtering state for a multicast group.
///
/// Captures both the union of specific source IPs and whether any member
/// wants "any source". Switch-level filtering behavior depends on address type:
///
/// - **SSM (232.0.0.0/8, ff3x::/32)**: Always use `specific_sources` per RFC 4607.
///   The `has_any_source_member` flag is ignored because API validation
///   prevents SSM joins without sources.
/// - **ASM**: Currently always passes `None` to DPD (Dendrite doesn't support
///   ASM filtering yet). TODO: if `has_any_source_member` is true, skip
///   switch-level filtering; otherwise use `specific_sources`.
/// - **OPTE**: Always uses per-member source lists for fine-grained filtering,
///   regardless of switch-level behavior.
///
/// This follows the (S,G) model where the switch does coarse filtering
/// and OPTE does fine-grained per-member filtering.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourceFilterState {
    /// Union of all specific source IPs from members (deduplicated).
    ///
    /// Contains only explicitly specified sources. Members with empty
    /// `source_ips` (ASM members wanting any source) don't affect this field.
    pub specific_sources: BTreeSet<IpAddr>,

    /// True if any member has empty `source_ips` (wants any source).
    ///
    /// For ASM groups: currently unused (Dendrite doesn't support ASM filtering).
    /// TODO: when true, switch-level filtering will be disabled.
    /// For SSM groups: ignored per RFC 4607 (API validation prevents SSM joins
    /// without sources).
    pub has_any_source_member: bool,
}

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

    /// Attach an instance to a multicast group as a member.
    ///
    /// Uses an atomic CTE to validate group/instance and perform upsert in a
    /// single database round-trip. The CTE is TOCTOU-safe, i.e., group state
    /// and instance existence are validated atomically with the upsert.
    ///
    /// Creates a member record in "Joining" state. The RPW reconciler
    /// programs the dataplane when the instance starts.
    ///
    /// Handles reactivation of "Left" members and preserves "Joined" state for
    /// idempotency.
    ///
    /// Source IPs handling on reactivation:
    /// - `None` → preserve existing `source_ips` (rejoin without changes)
    /// - `Some([])` → clear `source_ips` (switch to ASM)
    /// - `Some([a,b])` → replace with new `source_ips` (update sources)
    pub async fn multicast_group_member_attach_to_instance(
        &self,
        opctx: &OpContext,
        group_id: MulticastGroupUuid,
        instance_id: InstanceUuid,
        source_ips: Option<&[IpAddr]>,
    ) -> CreateResult<MulticastGroupMember> {
        let conn = self.pool_connection_authorized(opctx).await?;

        // Convert IpAddr to IpNetwork for storage
        let source_networks: Option<Vec<IpNetwork>> = source_ips
            .map(|ips| ips.iter().copied().map(IpNetwork::from).collect());

        // Execute atomic CTE that validates group (not "Deleting"), validates
        // instance, gets `sled_id`, performs upsert, and returns full member
        // record
        let attach_result =
            ops::member_attach::AttachMemberToGroupStatement::new(
                group_id.into_untyped_uuid(),
                instance_id.into_untyped_uuid(),
                Uuid::new_v4(),
                source_networks,
            )
            .execute(&conn)
            .await
            .map_err(external::Error::from)?;

        Ok(attach_result.member)
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

    /// Delete a specific multicast group member by group and instance ID.
    ///
    /// This performs a hard delete of the specific member (both active and soft-deleted)
    /// for the given (group, instance) pair. Used during saga undo operations to
    /// clean up only the member created by that saga, not affecting other instances'
    /// memberships in the same group.
    pub async fn multicast_group_member_delete_by_group_and_instance(
        &self,
        opctx: &OpContext,
        group_id: MulticastGroupUuid,
        instance_id: InstanceUuid,
    ) -> DeleteResult {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        diesel::delete(dsl::multicast_group_member)
            .filter(dsl::external_group_id.eq(group_id.into_untyped_uuid()))
            .filter(dsl::parent_id.eq(instance_id.into_untyped_uuid()))
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
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<MulticastGroupMember> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        paginated(dsl::multicast_group_member, dsl::id, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::parent_id.eq(instance_id.into_untyped_uuid()))
            .select(MulticastGroupMember::as_select())
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Compute source filtering state for one or more groups in a single query.
    ///
    /// Returns a map from `group_id` to [`SourceFilterState`] containing:
    /// - `specific_sources`: Union of all explicitly specified source IPs
    /// - `has_any_source_member`: `true` if any member has empty `source_ips`
    ///
    /// Groups with no members will have empty `specific_sources` and
    /// `has_any_source_member: false`.
    ///
    /// # Batch Usage
    ///
    /// Designed for batch lookups to avoid n+1 query patterns. Pass multiple
    /// group IDs to fetch in a single database round-trip.
    ///
    /// # DPD Source Filtering
    ///
    /// When `has_any_source_member` is true, pass `None` to DPD for sources
    /// (disabling switch-level filtering). Otherwise, use `specific_sources`.
    pub async fn multicast_groups_source_filter_state(
        &self,
        opctx: &OpContext,
        group_ids: &[MulticastGroupUuid],
    ) -> Result<HashMap<Uuid, SourceFilterState>, external::Error> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        if group_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let group_uuids: Vec<Uuid> =
            group_ids.iter().map(|id| id.into_untyped_uuid()).collect();

        let mut res: HashMap<Uuid, SourceFilterState> = group_uuids
            .iter()
            .map(|id| (*id, SourceFilterState::default()))
            .collect();

        let rows: Vec<(Uuid, Vec<IpNetwork>)> = dsl::multicast_group_member
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::external_group_id.eq_any(group_uuids))
            .select((dsl::external_group_id, dsl::source_ips))
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        for (group_id, source_ips) in rows {
            if let Some(state) = res.get_mut(&group_id) {
                if source_ips.is_empty() {
                    // Member wants any source (ASM behavior)
                    state.has_any_source_member = true;
                } else {
                    // Member has specific sources
                    state
                        .specific_sources
                        .extend(source_ips.iter().map(|ip| ip.ip()));
                }
            }
        }

        Ok(res)
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
    use omicron_common::api::external::DataPageParams;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::SledUuid;

    use crate::db::model::MulticastGroupMemberValues;
    use crate::db::pub_test_utils::helpers::{
        SledUpdateBuilder, attach_instance_to_vmm, create_instance_with_vmm,
        create_stopped_instance_record, create_vmm_for_instance,
    };
    use crate::db::pub_test_utils::multicast::NO_SOURCE_IPS;
    use crate::db::pub_test_utils::{TestDatabase, multicast};

    // Note: These are datastore-level tests. They validate database state
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
            // Pool resolved via authz_pool argument to datastore call
            mvlan: None,
            has_sources: false,
            ip_version: None,
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

        // Attaching to "Creating" group should succeed (implicit lifecycle model)
        // Members start in "Joining" and wait for RPW to activate the group
        let creating_member = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(creating_group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should attach to 'Creating' group");
        assert_eq!(creating_member.state, MulticastGroupMemberState::Joining);

        // Attach to active group should also succeed
        let member = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(active_group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should attach instance to active group");

        assert_eq!(member.state, MulticastGroupMemberState::Joining);
        assert_eq!(member.sled_id, Some(setup.sled_id.into()));
        assert_eq!(member.external_group_id, active_group.id());
        assert_eq!(member.multicast_ip, active_group.multicast_ip);
        assert_eq!(member.parent_id, instance_id);
        assert!(member.time_deleted.is_none());
        assert!(member.time_created <= member.time_modified);
        assert_eq!(member.source_ips, Vec::<ipnetwork::IpNetwork>::new());
        assert!(member.version_removed.is_none());

        let time_after_first_attach = member.time_modified;

        // Second attach to same group with member in "Joining" state should be
        // idempotent
        let member2 = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(active_group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should handle duplicate attach to 'Joining' member");

        assert_eq!(member.id, member2.id, "Should return same member ID");
        assert_eq!(
            member2.time_modified, time_after_first_attach,
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
        let member3 = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(active_group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should handle attach to 'Joined' member");

        assert_eq!(member.id, member3.id, "Should return same member ID");
        assert_eq!(
            member3.time_modified, time_after_joined,
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

        // Attach to member in "Left" state should reactivate it with new sources
        let reactivation_sources: Vec<IpAddr> =
            vec!["10.0.0.1".parse().unwrap(), "10.0.0.2".parse().unwrap()];
        let reactivated_member = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(active_group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
                Some(reactivation_sources.as_slice()),
            )
            .await
            .expect("Should reactivate 'Left' member");

        assert_eq!(
            member.id, reactivated_member.id,
            "Should return same member ID"
        );
        assert_eq!(
            reactivated_member.state,
            MulticastGroupMemberState::Joining
        );
        assert_eq!(reactivated_member.sled_id, Some(setup.sled_id.into()));
        assert!(
            reactivated_member.time_modified >= time_after_left,
            "Reactivation should advance time_modified"
        );
        // Verify `source_ips` were updated on reactivation
        // Database stores IpNetwork, so convert for comparison
        let stored_ips: Vec<IpAddr> =
            reactivated_member.source_ips.iter().map(|n| n.ip()).collect();
        assert_eq!(
            stored_ips, reactivation_sources,
            "Reactivation should update source_ips"
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
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group1.id()),
                InstanceUuid::from_untyped_uuid(*instance1_id),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should add instance1 to group1");

        let member1_2 = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group2.id()),
                InstanceUuid::from_untyped_uuid(*instance1_id),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should add instance1 to group2");

        let member2_1 = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group1.id()),
                InstanceUuid::from_untyped_uuid(*instance2_id),
                Some(NO_SOURCE_IPS),
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

        // Verify instance1 memberships transitioned to "Left" state
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

        // Test idempotency: detaching again should be idempotent
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
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(*instance_id),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should add instance as member");

        // Verify member has correct parent_id
        assert_eq!(member.parent_id, *instance_id);
        assert_eq!(member.external_group_id, group.id());
        assert_eq!(member.state, MulticastGroupMemberState::Joining);

        // Test member lookup by parent_id
        let pagparams = &DataPageParams {
            marker: None,
            limit: std::num::NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        };
        let member_memberships = datastore
            .multicast_group_members_list_by_instance(
                &opctx,
                InstanceUuid::from_untyped_uuid(*instance_id),
                pagparams,
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
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                instance_id,
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should add instance as member first time");

        // Try to add same instance again - should return existing member
        let member2 = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                instance_id,
                Some(NO_SOURCE_IPS),
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
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(test_instance_id),
                Some(NO_SOURCE_IPS),
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
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(test_instance_id),
                Some(NO_SOURCE_IPS),
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
                    source_ips: vec![],
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
                    source_ips: vec![],
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
                    source_ips: vec![],
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
            other => panic!("Expected ObjectNotFound, got: {other:?}"),
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
                Some(NO_SOURCE_IPS),
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
                Some(NO_SOURCE_IPS),
            )
            .await;
        assert!(result.is_err(), "Attach non-existent instance should fail");

        // Successfully create a member for further testing
        datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
                Some(NO_SOURCE_IPS),
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
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                instance_id,
                Some(NO_SOURCE_IPS),
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
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group1.id()),
                instance1_id,
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should add instance1 to group1");

        let member1_2 = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group2.id()),
                instance1_id,
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should add instance1 to group2");

        // Add instance2 to only group1
        let member2_1 = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group1.id()),
                instance2_id,
                Some(NO_SOURCE_IPS),
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
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group1.id()),
                instance1_id,
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should add instance1 to group1");

        let member1_2 = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group1.id()),
                instance2_id,
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should add instance2 to group1");

        // Add members to group2
        let member2_1 = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group2.id()),
                instance1_id,
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should add instance1 to group2");

        let member2_2 = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group2.id()),
                instance3_id,
                Some(NO_SOURCE_IPS),
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
                    Some(NO_SOURCE_IPS),
                )
                .await
        });

        let handle2 = tokio::spawn(async move {
            datastore2
                .multicast_group_member_attach_to_instance(
                    &opctx2,
                    MulticastGroupUuid::from_untyped_uuid(group_id),
                    InstanceUuid::from_untyped_uuid(instance_id),
                    Some(NO_SOURCE_IPS),
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
                Some(NO_SOURCE_IPS),
            )
            .await;

        // Should fail with GroupNotFound (group doesn't exist)
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
                Some(NO_SOURCE_IPS),
            )
            .await;

        // Should fail because CTE validates instance exists atomically
        assert!(result.is_err());
        let err = result.unwrap_err();
        // The error will be InvalidRequest from the CTE (instance not found)
        assert!(matches!(err, external::Error::InvalidRequest { .. }));

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_member_attach_allows_creating_rejects_deleting() {
        let logctx = dev::test_setup_log(
            "test_member_attach_allows_creating_rejects_deleting",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "group-state-pool",
            "group-state-project",
        )
        .await;

        // Create group in "Creating" state
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

        // Attaching to "Creating" group should succeed (not just "Active")
        let member = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(creating_group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should allow attach to 'Creating' group");
        assert_eq!(member.state, MulticastGroupMemberState::Joining);

        // Create a separate group for testing "Deleting" state rejection
        let deleting_group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            &setup,
            "deleting-group",
            "224.10.1.8",
            true, // make_active first
        )
        .await;

        // Transition to "Deleting" state (works because group has no members yet)
        // Note: `mark_multicast_group_for_removal_if_no_members` also sets time_deleted,
        // so the group becomes soft-deleted and cannot be fetched via normal methods.
        let marked = datastore
            .mark_multicast_group_for_removal_if_no_members(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(deleting_group.id()),
            )
            .await
            .expect("Should transition to Deleting");
        assert!(marked, "Group should be marked for deletion");

        // Attaching to soft-deleted ("Deleting") group should fail
        let res = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(deleting_group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
                Some(NO_SOURCE_IPS),
            )
            .await;
        assert!(res.is_err(), "Should reject attach to 'Deleting' group");
        let err = res.unwrap_err();
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

        // First attach with source IPs
        let initial_sources: Vec<IpAddr> = vec!["192.168.1.1".parse().unwrap()];
        let member1 = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
                Some(initial_sources.as_slice()),
            )
            .await
            .expect("First attach should succeed");
        let time_after_first = member1.time_modified;

        // Second attach (idempotent, should not update sources)
        let member2 = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
                None, // None preserves existing sources
            )
            .await
            .expect("Second attach should succeed");

        assert_eq!(member1.id, member2.id, "Should return same member ID");
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
        // Verify  `source_ips` preserved after idempotent attach
        // Database stores IpNetwork, so convert for comparison
        let stored_ips: Vec<IpAddr> =
            member_after_second.source_ips.iter().map(|n| n.ip()).collect();
        assert_eq!(
            stored_ips, initial_sources,
            "Idempotent attach must preserve source_ips"
        );

        // Third attach (still idempotent)
        let member3 = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Third attach should succeed");

        assert_eq!(member1.id, member3.id, "Should return same member ID");
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
    async fn test_member_attach_reactivation_source_handling() {
        let logctx = dev::test_setup_log(
            "test_member_attach_reactivation_source_handling",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "reactivation-test-pool",
            "reactivation-test-project",
        )
        .await;

        let group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            &setup,
            "test-group",
            "224.10.1.9",
            true,
        )
        .await;
        let group_id = MulticastGroupUuid::from_untyped_uuid(group.id());

        // Preserve sources: None keeps existing source_ips
        {
            let instance = create_stopped_instance_record(
                &opctx,
                &datastore,
                &setup.authz_project,
                "preserve-instance",
            )
            .await;

            let original_sources: Vec<IpAddr> =
                vec!["10.1.1.1".parse().unwrap(), "10.1.1.2".parse().unwrap()];
            let member = datastore
                .multicast_group_member_attach_to_instance(
                    &opctx,
                    group_id,
                    instance,
                    Some(original_sources.as_slice()),
                )
                .await
                .expect("Should attach");

            datastore
                .multicast_group_members_detach_by_instance(&opctx, instance)
                .await
                .expect("Should detach");

            let reactivated = datastore
                .multicast_group_member_attach_to_instance(
                    &opctx, group_id, instance,
                    None, // Preserve existing sources
                )
                .await
                .expect("Reactivation should succeed");

            assert_eq!(
                member.id, reactivated.id,
                "Should reactivate same member"
            );
            let stored_ips: Vec<IpAddr> =
                reactivated.source_ips.iter().map(|n| n.ip()).collect();
            assert_eq!(
                stored_ips, original_sources,
                "None should preserve existing source_ips"
            );
            assert_eq!(reactivated.state, MulticastGroupMemberState::Joining);
        }

        // Replace sources: Some([new]) replaces existing source_ips
        {
            let instance = create_stopped_instance_record(
                &opctx,
                &datastore,
                &setup.authz_project,
                "replace-instance",
            )
            .await;

            let original_sources: Vec<IpAddr> =
                vec!["10.0.0.1".parse().unwrap(), "10.0.0.2".parse().unwrap()];
            let member = datastore
                .multicast_group_member_attach_to_instance(
                    &opctx,
                    group_id,
                    instance,
                    Some(original_sources.as_slice()),
                )
                .await
                .expect("Should attach");

            datastore
                .multicast_group_members_detach_by_instance(&opctx, instance)
                .await
                .expect("Should detach");

            let replacement_sources: Vec<IpAddr> =
                vec!["10.0.0.3".parse().unwrap(), "10.0.0.4".parse().unwrap()];
            let reactivated = datastore
                .multicast_group_member_attach_to_instance(
                    &opctx,
                    group_id,
                    instance,
                    Some(replacement_sources.as_slice()),
                )
                .await
                .expect("Reactivation should succeed");

            assert_eq!(
                member.id, reactivated.id,
                "Should reactivate same member when replacing sources"
            );
            let stored_ips: Vec<IpAddr> =
                reactivated.source_ips.iter().map(|n| n.ip()).collect();
            assert_eq!(
                stored_ips, replacement_sources,
                "Some([new]) should replace existing sources"
            );
            assert_ne!(stored_ips, original_sources);
        }

        // Clear sources: Some([]) clears source_ips (switch to ASM)
        {
            let instance = create_stopped_instance_record(
                &opctx,
                &datastore,
                &setup.authz_project,
                "clear-instance",
            )
            .await;

            let original_sources: Vec<IpAddr> =
                vec!["10.5.5.1".parse().unwrap(), "10.5.5.2".parse().unwrap()];
            let member = datastore
                .multicast_group_member_attach_to_instance(
                    &opctx,
                    group_id,
                    instance,
                    Some(original_sources.as_slice()),
                )
                .await
                .expect("Should attach");

            datastore
                .multicast_group_members_detach_by_instance(&opctx, instance)
                .await
                .expect("Should detach");

            let reactivated = datastore
                .multicast_group_member_attach_to_instance(
                    &opctx,
                    group_id,
                    instance,
                    Some(NO_SOURCE_IPS), // Clear sources
                )
                .await
                .expect("Reactivation should succeed");

            assert_eq!(
                member.id, reactivated.id,
                "Should reactivate same member when clearing sources"
            );
            assert_eq!(
                reactivated.source_ips.len(),
                0,
                "Some([]) should clear source_ips"
            );
            assert_eq!(reactivated.state, MulticastGroupMemberState::Joining);
        }

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
        let member1 = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
                Some(NO_SOURCE_IPS),
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
        let member2 = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should allow reattach of Left member");

        // Should reactivate the same member (not create a new one)
        assert_eq!(member1.id, member2.id);

        // Verify only one member exists for this (group, instance) pair
        let pagparams = &DataPageParams {
            marker: None,
            limit: std::num::NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        };
        let members = datastore
            .multicast_group_members_list_by_instance(
                &opctx,
                InstanceUuid::from_untyped_uuid(instance_id),
                pagparams,
            )
            .await
            .expect("List members should succeed");

        // Filter to our group
        let our_members: Vec<_> = members
            .iter()
            .filter(|m| m.external_group_id == group.id())
            .collect();

        assert_eq!(our_members.len(), 1, "Should have exactly one member");
        assert_eq!(our_members[0].id, member1.id);
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
                Some(NO_SOURCE_IPS),
            )
            .await;

        // Should fail with InstanceNotFound (checked first), not GroupNotFound
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
        let attached_member = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                instance_id,
                Some(NO_SOURCE_IPS),
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

        assert_eq!(member.id, attached_member.id);
        assert_eq!(member.state, MulticastGroupMemberState::Joining);
        assert_eq!(
            member.sled_id, None,
            "Stopped instance should have sled_id = NULL"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_source_ips_union_across_members() {
        let logctx =
            dev::test_setup_log("test_source_ips_union_across_members");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "union-test-pool",
            "union-test-project",
        )
        .await;

        // Create active group
        let group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            &setup,
            "union-group",
            "224.10.1.100",
            true, // make_active
        )
        .await;
        let group_id = MulticastGroupUuid::from_untyped_uuid(group.id());

        // Add member1 with source IPs [10.0.0.1, 10.0.0.2]
        let instance1 = create_stopped_instance_record(
            &opctx,
            &datastore,
            &setup.authz_project,
            "instance-1",
        )
        .await;

        let member1_sources: Vec<IpAddr> =
            vec!["10.0.0.1".parse().unwrap(), "10.0.0.2".parse().unwrap()];
        datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                group_id,
                instance1,
                Some(member1_sources.as_slice()),
            )
            .await
            .expect("Should add member1");

        // Verify filter state with single member
        let state_map = datastore
            .multicast_groups_source_filter_state(&opctx, &[group_id])
            .await
            .expect("Should get filter state");
        let state = state_map.get(&group.id()).cloned().unwrap_or_default();
        assert_eq!(
            state.specific_sources.len(),
            2,
            "Should have 2 IPs from member1"
        );
        assert!(!state.has_any_source_member, "No ASM member yet");

        // Add member2 with source IPs [10.0.0.2, 10.0.0.3] (10.0.0.2 overlaps)
        let instance2 = create_stopped_instance_record(
            &opctx,
            &datastore,
            &setup.authz_project,
            "instance-2",
        )
        .await;

        let member2_sources: Vec<IpAddr> =
            vec!["10.0.0.2".parse().unwrap(), "10.0.0.3".parse().unwrap()];
        datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                group_id,
                instance2,
                Some(member2_sources.as_slice()),
            )
            .await
            .expect("Should add member2");

        // Verify filter state deduplicates overlapping IPs
        let state_map = datastore
            .multicast_groups_source_filter_state(&opctx, &[group_id])
            .await
            .expect("Should get filter state");
        let state = state_map.get(&group.id()).cloned().unwrap_or_default();
        assert_eq!(
            state.specific_sources.len(),
            3,
            "Should have 3 unique IPs (10.0.0.1, 10.0.0.2, 10.0.0.3)"
        );
        assert!(!state.has_any_source_member, "Still no ASM member");

        // Add member3 with no source IPs (ASM member)
        let instance3 = create_stopped_instance_record(
            &opctx,
            &datastore,
            &setup.authz_project,
            "instance-3",
        )
        .await;

        datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                group_id,
                instance3,
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should add ASM member");

        // specific_sources should still be 3 (ASM member contributes nothing)
        // but has_any_source_member=true
        let state_map = datastore
            .multicast_groups_source_filter_state(&opctx, &[group_id])
            .await
            .expect("Should get filter state");
        let state = state_map.get(&group.id()).cloned().unwrap_or_default();
        assert_eq!(
            state.specific_sources.len(),
            3,
            "specific_sources should still be 3 (ASM member contributes nothing)"
        );
        assert!(
            state.has_any_source_member,
            "ASM member joined with empty sources"
        );

        // Verify actual IPs in specific_sources
        assert!(state.specific_sources.contains(&"10.0.0.1".parse().unwrap()));
        assert!(state.specific_sources.contains(&"10.0.0.2".parse().unwrap()));
        assert!(state.specific_sources.contains(&"10.0.0.3".parse().unwrap()));

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Test that empty group IDs returns empty map without DB query.
    #[tokio::test]
    async fn test_source_ips_union_empty_input() {
        let logctx = dev::test_setup_log("test_source_ips_union_empty_input");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Call with empty slice (should return empty map without hitting DB)
        let result = datastore
            .multicast_groups_source_filter_state(&opctx, &[])
            .await
            .expect("Empty input should succeed");

        assert!(result.is_empty(), "Empty input should return empty map");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_source_ips_union_group_with_no_members() {
        let logctx =
            dev::test_setup_log("test_source_ips_union_group_with_no_members");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "no-members-pool",
            "no-members-project",
        )
        .await;

        // Create active group with no members
        let group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            &setup,
            "empty-group",
            "224.10.1.1",
            true, // make_active
        )
        .await;
        let group_id = MulticastGroupUuid::from_untyped_uuid(group.id());

        // Query source IPs for group with no members
        let result = datastore
            .multicast_groups_source_filter_state(&opctx, &[group_id])
            .await
            .expect("Should succeed for group with no members");

        // Group should be in result map with default state (not missing)
        assert!(
            result.contains_key(&group.id()),
            "Group should be present in result map"
        );
        let state = result.get(&group.id()).unwrap();
        assert!(
            state.specific_sources.is_empty(),
            "Group with no members should have empty specific_sources"
        );
        assert!(
            !state.has_any_source_member,
            "Group with no members should have has_any_source_member=false"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
