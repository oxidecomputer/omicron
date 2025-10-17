//! Multicast group member management operations.
//!
//! This module provides database operations for managing multicast group memberships,
//! including adding/removing members and coordinating with saga operations.

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
        self.multicast_group_members_list_by_id(
            opctx,
            group_id.into_untyped_uuid(),
            pagparams,
        )
        .await
    }

    /// Get all multicast group memberships for a specific instance.
    ///
    /// This method returns all multicast groups that contain the specified
    /// instance, which is useful for updating multicast membership when
    /// instances change state.
    pub async fn multicast_group_members_list_for_instance(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
    ) -> ListResultVec<MulticastGroupMember> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        diesel::QueryDsl::filter(
            diesel::QueryDsl::order(
                diesel::QueryDsl::select(
                    dsl::multicast_group_member,
                    MulticastGroupMember::as_select(),
                ),
                dsl::id.asc(),
            ),
            dsl::parent_id.eq(instance_id).and(dsl::time_deleted.is_null()),
        )
        .get_results_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Look up the sled hosting an instance via its active VMM.
    /// Returns None if the instance exists but has no active VMM
    /// (stopped instance).
    pub async fn instance_get_sled_id(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
    ) -> Result<Option<Uuid>, external::Error> {
        use nexus_db_schema::schema::{instance, vmm};
        let maybe_row: Option<Option<Uuid>> = instance::table
            .left_join(
                vmm::table
                    .on(instance::active_propolis_id.eq(vmm::id.nullable())),
            )
            .filter(instance::id.eq(instance_id))
            .filter(instance::time_deleted.is_null())
            .select(vmm::sled_id.nullable())
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        match maybe_row {
            None => Err(external::Error::not_found_by_id(
                ResourceType::Instance,
                &instance_id,
            )),
            Some(sled) => Ok(sled),
        }
    }

    /// Create a new multicast group member for an instance.
    ///
    /// This creates a member record in the ["Joining"](MulticastGroupMemberState::Joining)
    /// state, which indicates the member exists but its dataplane configuration
    /// (via DPD) has not yet been applied on switches.
    ///
    /// The RPW reconciler applies the DPD configuration in response to instance
    /// lifecycle (e.g., when the instance starts).
    pub async fn multicast_group_member_add(
        &self,
        opctx: &OpContext,
        group_id: MulticastGroupUuid,
        instance_id: InstanceUuid,
    ) -> CreateResult<MulticastGroupMember> {
        let conn = self.pool_connection_authorized(opctx).await?;
        self.multicast_group_member_add_with_conn(
            opctx,
            &conn,
            group_id.into_untyped_uuid(),
            instance_id.into_untyped_uuid(),
        )
        .await
    }

    /// Add an instance to a multicast group using provided connection.
    async fn multicast_group_member_add_with_conn(
        &self,
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<nexus_db_lookup::DbConnection>,
        group_id: Uuid,
        instance_id: Uuid,
    ) -> CreateResult<MulticastGroupMember> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        // Look up the sled_id for this instance (may be None for stopped instances)
        let sled_id = self
            .instance_get_sled_id(opctx, instance_id)
            .await?
            .map(DbTypedUuid::from_untyped_uuid);

        // Create new member with fields
        let new_member = MulticastGroupMemberValues {
            id: Uuid::new_v4(),
            parent_id: instance_id,
            external_group_id: group_id,
            sled_id,
            state: MulticastGroupMemberState::Joining,
            time_created: Utc::now(),
            time_modified: Utc::now(),
            time_deleted: None,
        };

        // Upsert using the partial unique index on (external_group_id, parent_id)
        // WHERE time_deleted IS NULL. CockroachDB requires that ON CONFLICT
        // targets for partial unique indexes include a predicate; the helper
        // `.as_partial_index()` decorates the target so Cockroach infers the
        // partial predicate. Do NOT use `ON CONSTRAINT` here: Cockroach rejects
        // partial indexes as arbiters with that syntax.
        diesel::insert_into(dsl::multicast_group_member)
            .values(new_member)
            .on_conflict((dsl::external_group_id, dsl::parent_id))
            .as_partial_index()
            .do_update()
            .set((
                dsl::state.eq(MulticastGroupMemberState::Joining),
                dsl::sled_id.eq(sled_id),
                dsl::time_deleted.eq::<Option<chrono::DateTime<Utc>>>(None),
                dsl::time_modified.eq(Utc::now()),
            ))
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
        group_id: Uuid,
    ) -> DeleteResult {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        // Delete all members for this group, including soft-deleted ones
        // We use a targeted query to leverage existing indexes
        diesel::delete(dsl::multicast_group_member)
            .filter(dsl::external_group_id.eq(group_id))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
            .map(|_x| ())
    }

    /// Set the state of a multicast group member.
    pub async fn multicast_group_member_set_state(
        &self,
        opctx: &OpContext,
        external_group_id: Uuid,
        parent_id: Uuid,
        new_state: MulticastGroupMemberState,
    ) -> UpdateResult<()> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let rows_updated = diesel::update(dsl::multicast_group_member)
            .filter(dsl::external_group_id.eq(external_group_id))
            .filter(dsl::parent_id.eq(parent_id))
            .filter(dsl::time_deleted.is_null())
            .set((dsl::state.eq(new_state), dsl::time_modified.eq(Utc::now())))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::MulticastGroupMember,
                        LookupType::ById(external_group_id),
                    ),
                )
            })?;

        if rows_updated == 0 {
            return Err(external::Error::not_found_by_id(
                ResourceType::MulticastGroupMember,
                &external_group_id,
            ));
        }

        Ok(())
    }

    /// List members of an multicast group by ID.
    pub async fn multicast_group_members_list_by_id(
        &self,
        opctx: &OpContext,
        external_group_id: Uuid,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<MulticastGroupMember> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        paginated(dsl::multicast_group_member, dsl::id, pagparams)
            .filter(
                dsl::time_deleted
                    .is_null()
                    .and(dsl::external_group_id.eq(external_group_id)),
            )
            .select(MulticastGroupMember::as_select())
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// List all members of an external multicast group (whichever state).
    pub async fn multicast_group_members_list_all(
        &self,
        opctx: &OpContext,
        external_group_id: Uuid,
        pagparams: &external::DataPageParams<'_, Uuid>,
    ) -> ListResultVec<MulticastGroupMember> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        paginated(dsl::multicast_group_member, dsl::id, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::external_group_id.eq(external_group_id))
            .select(MulticastGroupMember::as_select())
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Lists all active multicast group members.
    pub async fn multicast_group_members_list_active(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<MulticastGroupMember> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        dsl::multicast_group_member
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::state.ne(MulticastGroupMemberState::Left))
            .select(MulticastGroupMember::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// List multicast group memberships for a specific instance.
    ///
    /// If `include_removed` is true, includes memberships that have been
    /// marked removed (i.e., rows with `time_deleted` set). Otherwise only
    /// returns active memberships.
    pub async fn multicast_group_members_list_by_instance(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
        include_removed: bool,
    ) -> ListResultVec<MulticastGroupMember> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let mut query = dsl::multicast_group_member.into_boxed();

        if !include_removed {
            query = query.filter(dsl::time_deleted.is_null());
        }

        query
            .filter(dsl::parent_id.eq(instance_id))
            .order(dsl::id.asc())
            .select(MulticastGroupMember::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Begin attaching an instance to a multicast group.
    pub async fn multicast_group_member_attach_to_instance(
        &self,
        opctx: &OpContext,
        group_id: Uuid,
        instance_id: Uuid,
    ) -> Result<(Uuid, bool), external::Error> {
        use nexus_db_schema::schema::multicast_group_member::dsl;
        let conn = self.pool_connection_authorized(opctx).await?;

        // Validate the group is still active
        if !self.multicast_group_is_active(&conn, group_id).await? {
            return Err(external::Error::invalid_request(&format!(
                "cannot add members to multicast group {group_id}, group must be 'Active'"
            )));
        }

        // Check for existing membership (active or recently deleted)
        let existing = dsl::multicast_group_member
            .filter(dsl::external_group_id.eq(group_id))
            .filter(dsl::parent_id.eq(instance_id))
            .filter(dsl::time_deleted.is_null())
            .select(MulticastGroupMember::as_select())
            .first_async::<MulticastGroupMember>(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        // Handle existing membership if present, otherwise create new member
        let Some(existing_member) = existing else {
            // No existing membership - create new member using existing connection
            let member = self
                .multicast_group_member_add_with_conn(
                    opctx,
                    &conn,
                    group_id,
                    instance_id,
                )
                .await?;

            return Ok((member.id, true));
        };

        match existing_member.state {
            MulticastGroupMemberState::Joined => {
                // Already attached - no saga needed
                Ok((existing_member.id, false))
            }
            MulticastGroupMemberState::Joining => {
                // Already in progress - no saga needed
                Ok((existing_member.id, false))
            }
            MulticastGroupMemberState::Left => {
                // Get current sled_id for this instance
                let sled_id = self
                    .instance_get_sled_id(opctx, instance_id)
                    .await?
                    .map(DbTypedUuid::<SledKind>::from_untyped_uuid);

                // Reactivate this formerly "Left" member, as it's being "Joined" again
                diesel::update(dsl::multicast_group_member)
                    .filter(dsl::id.eq(existing_member.id))
                    .filter(dsl::state.eq(MulticastGroupMemberState::Left))
                    .set((
                        dsl::state.eq(MulticastGroupMemberState::Joining), // update state
                        dsl::time_modified.eq(Utc::now()),
                        dsl::sled_id.eq(sled_id), // Update sled_id
                    ))
                    .returning(MulticastGroupMember::as_returning())
                    .get_result_async(&*conn)
                    .await
                    .optional()
                    .map_err(|e| {
                        public_error_from_diesel(e, ErrorHandler::Server)
                    })?;

                Ok((existing_member.id, true))
            }
        }
    }

    /// Detach all multicast group memberships for an instance.
    ///
    /// This sets state to ["Left"](MulticastGroupMemberState::Left) and clears
    /// `sled_id` for members of the stopped instance.
    ///
    /// This transitions members from ["Joined"](MulticastGroupMemberState::Joined)
    /// or ["Joining"](MulticastGroupMemberState::Joining) to
    /// ["Left"](MulticastGroupMemberState::Left) state, effectively detaching
    /// the instance from all multicast groups.
    pub async fn multicast_group_members_detach_by_instance(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
    ) -> Result<(), external::Error> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let now = Utc::now();

        // Transition members from "Joined/Joining" to "Left" state and clear
        // `sled_id`
        diesel::update(dsl::multicast_group_member)
            .filter(dsl::parent_id.eq(instance_id))
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
    /// This sets the member's state to ["Left"](MulticastGroupMemberState::Left)
    /// and clears sled_id.
    pub async fn multicast_group_member_detach_by_group_and_instance(
        &self,
        opctx: &OpContext,
        group_id: Uuid,
        instance_id: Uuid,
    ) -> Result<bool, external::Error> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let now = Utc::now();

        // Mark member for removal (set time_deleted and state to "Left"), similar
        // to soft instance deletion
        let updated_rows = diesel::update(dsl::multicast_group_member)
            .filter(dsl::external_group_id.eq(group_id))
            .filter(dsl::parent_id.eq(instance_id))
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
    /// This function is used during instance lifecycle transitions (start/stop/migrate)
    /// to keep multicast member sled_id values consistent with instance placement.
    ///
    /// - When instances start: sled_id changes from NULL to actual sled UUID
    /// - When instances stop: sled_id changes from actual sled UUID to NULL
    /// - When instances migrate: sled_id changes from old sled UUID to new sled UUID
    pub async fn multicast_group_member_update_sled_id(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
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
            .filter(dsl::parent_id.eq(instance_id))
            .filter(dsl::time_deleted.is_null())
            // Only update active members (not in "Left" state)
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

    /// Transition multicast memberships to ["Joining"](MulticastGroupMemberState::Joining) state when instance starts.
    /// Updates ["Left"](MulticastGroupMemberState::Left) members back to ["Joining"](MulticastGroupMemberState::Joining) state and sets sled_id for the new location.
    pub async fn multicast_group_member_start_instance(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
        sled_id: DbTypedUuid<SledKind>,
    ) -> Result<(), external::Error> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let now = Utc::now();

        // Update "Left" members (stopped instances) or still-"Joining" members
        diesel::update(dsl::multicast_group_member)
            .filter(dsl::parent_id.eq(instance_id))
            .filter(dsl::time_deleted.is_null())
            .filter(
                dsl::state
                    .eq(MulticastGroupMemberState::Left)
                    .or(dsl::state.eq(MulticastGroupMemberState::Joining)),
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

    /// Mark instance's multicast group members for removal.
    ///
    /// This soft-deletes all member records for the specified instance by
    /// setting their `time_deleted` timestamp and transitioning to "Left" state.
    ///
    /// The RPW reconciler removes corresponding DPD configuration when activated.
    pub async fn multicast_group_members_mark_for_removal(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
    ) -> Result<(), external::Error> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let now = Utc::now();

        diesel::update(dsl::multicast_group_member)
            .filter(dsl::parent_id.eq(instance_id))
            .filter(dsl::time_deleted.is_null())
            .set((
                dsl::state.eq(MulticastGroupMemberState::Left), // Transition to Left state
                dsl::time_deleted.eq(Some(now)), // Mark for deletion
                dsl::time_modified.eq(now),
            ))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
            .map(|_| ())
    }

    /// Permanently delete a multicast group member by ID.
    pub async fn multicast_group_member_delete_by_id(
        &self,
        opctx: &OpContext,
        member_id: Uuid,
    ) -> DeleteResult {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let deleted_rows = diesel::delete(dsl::multicast_group_member)
            .filter(dsl::id.eq(member_id))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        if deleted_rows == 0 {
            return Err(external::Error::not_found_by_id(
                ResourceType::MulticastGroupMember,
                &member_id,
            ));
        }

        debug!(
            opctx.log,
            "multicast group member deletion completed";
            "member_id" => %member_id,
            "rows_deleted" => deleted_rows
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

    use nexus_types::external_api::params;
    use nexus_types::identity::Resource;
    use omicron_common::api::external::{self, IdentityMetadataCreateParams};
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::SledUuid;

    use crate::db::pub_test_utils::helpers::{self, SledUpdateBuilder};
    use crate::db::pub_test_utils::{TestDatabase, multicast};

    // NOTE: These are datastore-level tests. They validate database state
    // transitions, validations, and query behavior for multicast members.
    // They purposefully do not exercise the reconciler (RPW) or dataplane (DPD)
    // components. End-to-end RPW/DPD behavior is covered by integration tests
    // under `nexus/tests/integration_tests/multicast`.

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
        let creating_group_params = params::MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "creating-group".parse().unwrap(),
                description: "Creating test group".to_string(),
            },
            multicast_ip: Some("224.10.1.6".parse().unwrap()),
            source_ips: None,
            // Pool resolved via authz_pool argument to datastore call
            pool: None,
            mvlan: None,
        };

        let creating_group = datastore
            .multicast_group_create(
                &opctx,
                Uuid::new_v4(),
                &creating_group_params,
                Some(setup.authz_pool.clone()),
            )
            .await
            .expect("Should create creating multicast group");

        // Create test instance
        let (instance, _vmm) = helpers::create_instance_with_vmm(
            &opctx,
            &datastore,
            &setup.authz_project,
            "attach-test-instance",
            setup.sled_id,
        )
        .await;
        let instance_id = instance.as_untyped_uuid();

        // Cannot attach to group in "Creating" state (not "Active")
        let result = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                creating_group.id(),
                *instance_id,
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
        let (member_id, saga_needed) = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                active_group.id(),
                *instance_id,
            )
            .await
            .expect("Should attach instance to active group");

        assert!(saga_needed, "First attach should need saga");

        // Verify member was created in "Joining" state
        let member = datastore
            .multicast_group_member_get_by_group_and_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(active_group.id()),
                InstanceUuid::from_untyped_uuid(*instance_id),
            )
            .await
            .expect("Should get member")
            .expect("Member should exist");

        assert_eq!(member.id, member_id);
        assert_eq!(member.state, MulticastGroupMemberState::Joining);
        assert_eq!(member.sled_id, Some(setup.sled_id.into()));

        // Second attach to same group with member in "Joining" state should be
        // idempotent
        let (member_id2, saga_needed2) = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                active_group.id(),
                *instance_id,
            )
            .await
            .expect("Should handle duplicate attach to 'Joining' member");

        assert_eq!(member_id, member_id2, "Should return same member ID");
        assert!(!saga_needed2, "Second attach should not need saga");

        // Transition member to "Joined" state
        datastore
            .multicast_group_member_set_state(
                &opctx,
                active_group.id(),
                *instance_id,
                MulticastGroupMemberState::Joined,
            )
            .await
            .expect("Should transition member to 'Joined'");

        // Attach to member in "Joined" state should be idempotent
        let (member_id3, saga_needed3) = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                active_group.id(),
                *instance_id,
            )
            .await
            .expect("Should handle attach to 'Joined' member");

        assert_eq!(member_id, member_id3, "Should return same member ID");
        assert!(!saga_needed3, "Attach to Joined member should not need saga");

        // Transition member to "Left" state (simulating instance stop)
        datastore
            .multicast_group_member_set_state(
                &opctx,
                active_group.id(),
                *instance_id,
                MulticastGroupMemberState::Left,
            )
            .await
            .expect("Should transition member to 'Left'");

        // Update member to have no sled_id (simulating stopped instance)
        datastore
            .multicast_group_member_update_sled_id(&opctx, *instance_id, None)
            .await
            .expect("Should clear sled_id for stopped instance");

        // Attach to member in "Left" state should reactivate it
        let (member_id4, saga_needed4) = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                active_group.id(),
                *instance_id,
            )
            .await
            .expect("Should reactivate 'Left' member");

        assert_eq!(member_id, member_id4, "Should return same member ID");
        assert!(saga_needed4, "Reactivating Left member should need saga");

        // Verify member was reactivated to "Joining" state with updated sled_id
        let reactivated_member = datastore
            .multicast_group_member_get_by_group_and_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(active_group.id()),
                InstanceUuid::from_untyped_uuid(*instance_id),
            )
            .await
            .expect("Should get reactivated member")
            .expect("Reactivated member should exist");

        assert_eq!(
            reactivated_member.state,
            MulticastGroupMemberState::Joining
        );
        assert_eq!(reactivated_member.sled_id, Some(setup.sled_id.into()));

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
        let instance1_record = helpers::create_stopped_instance_record(
            &opctx,
            &datastore,
            &setup.authz_project,
            "test-instance-1",
        )
        .await;
        let instance1_id = instance1_record.as_untyped_uuid();
        let instance2_record = helpers::create_stopped_instance_record(
            &opctx,
            &datastore,
            &setup.authz_project,
            "test-instance-2",
        )
        .await;
        let instance2_id = instance2_record.as_untyped_uuid();

        // Create VMMs and associate instances with sled (required for multicast membership)
        let vmm1_id = helpers::create_vmm_for_instance(
            &opctx,
            &datastore,
            instance1_record,
            setup.sled_id,
        )
        .await;
        helpers::attach_instance_to_vmm(
            &opctx,
            &datastore,
            &setup.authz_project,
            instance1_record,
            vmm1_id,
        )
        .await;

        let vmm2_id = helpers::create_vmm_for_instance(
            &opctx,
            &datastore,
            instance2_record,
            setup.sled_id,
        )
        .await;
        helpers::attach_instance_to_vmm(
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

        // Remove all memberships for instance1
        datastore
            .multicast_group_members_detach_by_instance(&opctx, *instance1_id)
            .await
            .expect("Should remove all memberships for instance1");

        // Verify instance1 memberships are gone but instance2 membership remains
        datastore
            .multicast_group_members_list_all(
                &opctx,
                group1.id(),
                &external::DataPageParams::max_page(),
            )
            .await
            .expect("Should list group1 members");

        datastore
            .multicast_group_members_list_all(
                &opctx,
                group2.id(),
                &external::DataPageParams::max_page(),
            )
            .await
            .expect("Should list group2 members");

        // Use list_active to get only active members (excludes "Left" state)
        let active_group1_members = datastore
            .multicast_group_members_list_active(&opctx)
            .await
            .expect("Should list active members")
            .into_iter()
            .filter(|m| m.external_group_id == group1.id())
            .collect::<Vec<_>>();
        assert_eq!(active_group1_members.len(), 1);
        assert_eq!(active_group1_members[0].parent_id, *instance2_id);

        let active_group2_members = datastore
            .multicast_group_members_list_active(&opctx)
            .await
            .expect("Should list active members")
            .into_iter()
            .filter(|m| m.external_group_id == group2.id())
            .collect::<Vec<_>>();
        assert_eq!(active_group2_members.len(), 0);

        // Test idempotency - running again should be idempotent
        datastore
            .multicast_group_members_detach_by_instance(&opctx, *instance1_id)
            .await
            .expect("Should handle removing memberships for instance1 again");

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
        let instance_record = helpers::create_stopped_instance_record(
            &opctx,
            &datastore,
            &setup.authz_project,
            "test-instance-parent",
        )
        .await;
        let instance_id = instance_record.as_untyped_uuid();

        // Create VMM and associate instance with sled (required for multicast membership)
        let vmm_id = helpers::create_vmm_for_instance(
            &opctx,
            &datastore,
            instance_record,
            setup.sled_id,
        )
        .await;
        helpers::attach_instance_to_vmm(
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
            .multicast_group_members_list_for_instance(&opctx, *instance_id)
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
        let instance_id = helpers::create_stopped_instance_record(
            &opctx,
            &datastore,
            &setup.authz_project,
            "test-instance-dup",
        )
        .await;

        // Create VMM and associate instance with sled (required for multicast membership)
        let vmm_id = helpers::create_vmm_for_instance(
            &opctx,
            &datastore,
            instance_id,
            setup.sled_id,
        )
        .await;
        helpers::attach_instance_to_vmm(
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

        // Try to add same instance again - should return existing member (idempotent)
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
        let instance_id = helpers::create_stopped_instance_record(
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
                test_instance_id,
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
                test_instance_id,
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
                test_instance_id,
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
                test_instance_id,
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
        let (instance, _vmm) = helpers::create_instance_with_vmm(
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
                group.id(),
                test_instance_id,
                MulticastGroupMemberState::Joined,
            )
            .await
            .expect("Should complete attach operation");

        // Complete the operation and leave
        datastore
            .multicast_group_member_set_state(
                &opctx,
                group.id(),
                test_instance_id,
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
        let (instance1, _vmm1) = helpers::create_instance_with_vmm(
            &opctx,
            &datastore,
            &setup.authz_project,
            "delete-test-instance1",
            setup.sled_id,
        )
        .await;
        let instance1_id = instance1.into_untyped_uuid();

        let (instance2, _vmm2) = helpers::create_instance_with_vmm(
            &opctx,
            &datastore,
            &setup.authz_project,
            "delete-test-instance2",
            setup.sled_id,
        )
        .await;
        let instance2_id = instance2.into_untyped_uuid();

        let (instance3, _vmm3) = helpers::create_instance_with_vmm(
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
                    parent_id: instance1_id,
                    sled_id: Some(setup.sled_id.into()),
                    state: MulticastGroupMemberState::Left,
                })
                .returning(MulticastGroupMember::as_returning())
                .get_result_async(&*conn)
                .await
                .expect("Should create member1 record");

        // Member 2: "Left" but no `time_deleted` (should NOT be deleted)
        let member2: MulticastGroupMember =
            diesel::insert_into(dsl::multicast_group_member)
                .values(MulticastGroupMemberValues {
                    id: Uuid::new_v4(),
                    time_created: Utc::now(),
                    time_modified: Utc::now(),
                    time_deleted: None,
                    external_group_id: group.id(),
                    parent_id: instance2_id,
                    sled_id: Some(setup.sled_id.into()),
                    state: MulticastGroupMemberState::Left,
                })
                .returning(MulticastGroupMember::as_returning())
                .get_result_async(&*conn)
                .await
                .expect("Should create member2 record");

        // Member 3: "Joined" state (should NOT be deleted, even if it had time_deleted)
        let member3: MulticastGroupMember =
            diesel::insert_into(dsl::multicast_group_member)
                .values(MulticastGroupMemberValues {
                    id: Uuid::new_v4(),
                    time_created: Utc::now(),
                    time_modified: Utc::now(),
                    time_deleted: Some(Utc::now()), // Has time_deleted but is Joined, so won't be cleaned up
                    external_group_id: group.id(),
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
        let stopped_instance = helpers::create_stopped_instance_record(
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
        let (running_instance, _vmm) = helpers::create_instance_with_vmm(
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
        let inactive_instance = helpers::create_stopped_instance_record(
            &opctx,
            &datastore,
            &setup.authz_project,
            "inactive-instance",
        )
        .await;
        let inactive_instance_id = inactive_instance.as_untyped_uuid();

        // Create VMM but don't attach it (no active_propolis_id)
        helpers::create_vmm_for_instance(
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
        let (instance, _vmm) = helpers::create_instance_with_vmm(
            &opctx,
            &datastore,
            &setup.authz_project,
            "error-test-instance",
            setup.sled_id,
        )
        .await;
        let instance_id = instance.as_untyped_uuid();

        // Operations on non-existent groups should return appropriate errors
        let fake_group_id = Uuid::new_v4();

        // Try to add member to non-existent group
        let result = datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                fake_group_id,
                *instance_id,
            )
            .await;
        assert!(result.is_err(), "Attach to non-existent group should fail");

        // Try to set state for non-existent member
        let result = datastore
            .multicast_group_member_set_state(
                &opctx,
                fake_group_id,
                *instance_id,
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
                InstanceUuid::from_untyped_uuid(*instance_id),
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
                group.id(),
                fake_instance_id,
            )
            .await;
        assert!(result.is_err(), "Attach non-existent instance should fail");

        // Successfully create a member for further testing
        datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                group.id(),
                *instance_id,
            )
            .await
            .expect("Should create member");

        // Invalid state transitions should be handled gracefully
        // (Note: The current implementation doesn't validate state transitions,
        // but we test that the operations complete without panicking)
        datastore
            .multicast_group_member_set_state(
                &opctx,
                group.id(),
                *instance_id,
                MulticastGroupMemberState::Left,
            )
            .await
            .expect("Should allow transition to 'Left'");

        datastore
            .multicast_group_member_set_state(
                &opctx,
                group.id(),
                *instance_id,
                MulticastGroupMemberState::Joined,
            )
            .await
            .expect("Should allow transition back to 'Joined'");

        // Test idempotent operations work correctly
        datastore
            .multicast_group_members_detach_by_instance(&opctx, *instance_id)
            .await
            .expect("First detach should succeed");

        datastore
            .multicast_group_members_detach_by_instance(&opctx, *instance_id)
            .await
            .expect("Second detach should be idempotent");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_multicast_group_member_start_instance() {
        let logctx =
            dev::test_setup_log("test_multicast_group_member_start_instance");
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
        let instance_record = helpers::create_stopped_instance_record(
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

        // Simulate instance start - should transition "Joining" → "Joining" with sled_id
        datastore
            .multicast_group_member_start_instance(
                &opctx,
                instance_id.into_untyped_uuid(),
                initial_sled.into(),
            )
            .await
            .expect("Should start instance");

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
            .multicast_group_members_detach_by_instance(
                &opctx,
                instance_id.into_untyped_uuid(),
            )
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
            .multicast_group_member_start_instance(
                &opctx,
                instance_id.into_untyped_uuid(),
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
                group.id(),
                instance_id.into_untyped_uuid(),
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
            .multicast_group_member_start_instance(
                &opctx,
                instance_id.into_untyped_uuid(),
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
            .multicast_group_member_start_instance(
                &opctx,
                non_member_instance.into_untyped_uuid(),
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
        let instance1_record = helpers::create_stopped_instance_record(
            &opctx,
            &datastore,
            &setup.authz_project,
            "removal-test-instance1",
        )
        .await;
        let instance1_id = InstanceUuid::from_untyped_uuid(
            *instance1_record.as_untyped_uuid(),
        );

        let instance2_record = helpers::create_stopped_instance_record(
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
            .multicast_group_members_mark_for_removal(
                &opctx,
                instance1_id.into_untyped_uuid(),
            )
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

        // Verify instance2 membership is NOT marked for removal
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
            .multicast_group_members_mark_for_removal(
                &opctx,
                instance1_id.into_untyped_uuid(),
            )
            .await
            .expect("Should handle duplicate mark for removal");

        // Test marking instance with no memberships (should be no-op)
        let non_member_instance = InstanceUuid::new_v4();
        datastore
            .multicast_group_members_mark_for_removal(
                &opctx,
                non_member_instance.into_untyped_uuid(),
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
        let instance1_record = helpers::create_stopped_instance_record(
            &opctx,
            &datastore,
            &setup.authz_project,
            "delete-test-instance1",
        )
        .await;
        let instance1_id = InstanceUuid::from_untyped_uuid(
            *instance1_record.as_untyped_uuid(),
        );

        let instance2_record = helpers::create_stopped_instance_record(
            &opctx,
            &datastore,
            &setup.authz_project,
            "delete-test-instance2",
        )
        .await;
        let instance2_id = InstanceUuid::from_untyped_uuid(
            *instance2_record.as_untyped_uuid(),
        );

        let instance3_record = helpers::create_stopped_instance_record(
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
            .multicast_group_members_delete_by_group(&opctx, group1.id())
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
            .multicast_group_members_list_all(
                &opctx,
                group1.id(),
                &external::DataPageParams::max_page(),
            )
            .await
            .expect("Should list group1 members");
        assert_eq!(group1_members.len(), 0);

        // Verify group2 still has its members
        let group2_members = datastore
            .multicast_group_members_list_all(
                &opctx,
                group2.id(),
                &external::DataPageParams::max_page(),
            )
            .await
            .expect("Should list group2 members");
        assert_eq!(group2_members.len(), 2);

        // Test deleting from group with no members (should be no-op)
        datastore
            .multicast_group_members_delete_by_group(&opctx, group1.id())
            .await
            .expect("Should handle deleting from empty group");

        // Test deleting from nonexistent group (should be no-op)
        let fake_group_id = Uuid::new_v4();
        datastore
            .multicast_group_members_delete_by_group(&opctx, fake_group_id)
            .await
            .expect("Should handle deleting from nonexistent group");

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
