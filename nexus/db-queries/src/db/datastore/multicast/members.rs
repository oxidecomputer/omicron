// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

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
use nexus_db_lookup::DbConnection;
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
use crate::db::datastore::multicast::ops::member_attach::AttachMemberResult;
use crate::db::model::{
    DbTypedUuid, MemberParentRef, MulticastGroupMember,
    MulticastGroupMemberParentKind, MulticastGroupMemberState,
    MulticastGroupState,
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
/// - **ASM**: If `has_any_source_member` is true, passes `None` to DPD
///   (no switch-level filtering). Otherwise, it uses `specific_sources`.
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
    /// For ASM groups: when true, switch-level source filtering is disabled
    /// (sources passed as `None` to Dendrite).
    /// For SSM groups: ignored per RFC 4607 (API validation prevents SSM joins
    /// without sources).
    pub has_any_source_member: bool,
}

enum MulticastGroupMemberGroupKey {
    Id(MulticastGroupUuid),
    MulticastIp(IpNetwork),
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

    /// List members that were soft-deleted with `sled_id` still set. These
    /// rows identify multicast state that may need to be withdrawn from a VMM.
    ///
    /// Every producer that sets `time_deleted` also transitions the row to
    /// "Left" in the same update. The state filter keeps this listing aligned
    /// with [`Self::multicast_group_members_complete_delete`], which only
    /// reaps "Left" rows.
    pub async fn multicast_group_members_list_pending_cleanup(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<MulticastGroupMember> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        paginated(dsl::multicast_group_member, dsl::id, pagparams)
            .filter(dsl::state.eq(MulticastGroupMemberState::Left))
            .filter(dsl::time_deleted.is_not_null())
            .filter(dsl::sled_id.is_not_null())
            .select(MulticastGroupMember::as_select())
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Attach a parent (instance or probe) to a multicast group as a member.
    ///
    /// Uses an atomic CTE to validate group and parent existence and perform
    /// the member upsert in a single database round-trip. The CTE is
    /// TOCTOU-safe, i.e., group state and parent existence are validated
    /// atomically with the upsert. The parent kind is persisted on the
    /// member row so the reconciler can dispatch on it without re-querying
    /// the parent tables.
    ///
    /// Creates a member record in "Joining" state. The RPW reconciler
    /// programs the dataplane when the parent comes up.
    ///
    /// Handles reactivation of "Left" members and preserves "Joined" state for
    /// idempotency.
    ///
    /// Source IPs handling, applied to a reactivated member and to one that is
    /// already live:
    /// - `None` → preserve existing `source_ips` (rejoin without changes)
    /// - `Some([])` → clear `source_ips` (switch to ASM)
    /// - `Some([a,b])` → replace with new `source_ips` (update sources)
    ///
    /// Atomically enforces the per-group source IP union cap
    /// ([`omicron_common::address::MAX_SOURCE_IPS_PER_GROUP`]) when a
    /// non-empty source list is being applied.
    ///
    /// The returned [`AttachMemberResult`] carries an
    /// [`AttachOutcome`](ops::member_attach::AttachOutcome) naming which
    /// upsert branch ran, so callers compensating for a failure can
    /// distinguish a row this call created from one that already existed.
    pub async fn multicast_group_member_attach(
        &self,
        opctx: &OpContext,
        group_id: MulticastGroupUuid,
        parent: MemberParentRef,
        source_ips: Option<&[IpAddr]>,
    ) -> CreateResult<AttachMemberResult> {
        let conn = self.pool_connection_authorized(opctx).await?;
        self.multicast_group_member_attach_on_conn(
            &conn, group_id, parent, source_ips,
        )
        .await
        .map_err(external::Error::from)
    }

    /// Attach a parent using a caller-supplied member ID.
    ///
    /// Saga actions use a stable ID here so a repeated forward invocation can
    /// identify the member it established and compensate it safely.
    pub async fn multicast_group_member_attach_with_id(
        &self,
        opctx: &OpContext,
        group_id: MulticastGroupUuid,
        parent: MemberParentRef,
        member_id: Uuid,
        source_ips: Option<&[IpAddr]>,
    ) -> CreateResult<MulticastGroupMember> {
        let conn = self.pool_connection_authorized(opctx).await?;
        self.multicast_group_member_attach_on_conn_with_id(
            &conn, group_id, parent, member_id, source_ips,
        )
        .await
        .map(|result| result.member)
        .map_err(external::Error::from)
    }

    /// Attach a parent using an existing database connection.
    pub(crate) async fn multicast_group_member_attach_on_conn(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        group_id: MulticastGroupUuid,
        parent: MemberParentRef,
        source_ips: Option<&[IpAddr]>,
    ) -> Result<AttachMemberResult, ops::member_attach::AttachMemberError> {
        self.multicast_group_member_attach_on_conn_with_id(
            conn,
            group_id,
            parent,
            Uuid::new_v4(),
            source_ips,
        )
        .await
    }

    /// Attach a parent using an existing connection and caller-supplied
    /// member ID.
    pub(crate) async fn multicast_group_member_attach_on_conn_with_id(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        group_id: MulticastGroupUuid,
        parent: MemberParentRef,
        member_id: Uuid,
        source_ips: Option<&[IpAddr]>,
    ) -> Result<AttachMemberResult, ops::member_attach::AttachMemberError> {
        // Convert IpAddr to IpNetwork for storage
        let source_networks: Option<Vec<IpNetwork>> = source_ips
            .map(|ips| ips.iter().copied().map(IpNetwork::from).collect());

        // Execute atomic CTE that validates group (not "Deleting"), validates
        // the parent (instance or probe), gets `sled_id`, performs upsert,
        // and returns the full member record.
        ops::member_attach::AttachMemberToGroupStatement::new(
            group_id.into_untyped_uuid(),
            parent,
            member_id,
            source_networks,
        )
        .execute(conn)
        .await
    }

    /// Hard delete every member of a group that is still marked for removal.
    ///
    /// Covers active and soft-deleted member rows without a sled assignment,
    /// and is used by the reconciler when tearing a group down. Rows holding
    /// a `sled_id` are left for the member reconciler to unsubscribe first.
    ///
    /// The delete declines unless the parent group is still soft-deleted in
    /// "Deleting" state. A reaped group can come back to life through
    /// [`DataStore::multicast_group_resurrect_if_unactivated`] while a teardown
    /// pass is blocked on its dataplane call, and the members attached to the
    /// restored group after that point belong to the saga that owns it, not to
    /// the teardown that is still unwinding. Without the guard the resumed pass
    /// would sweep those rows and leave a live but memberless group, which the
    /// orphan pass then reaps again.
    pub async fn multicast_group_members_delete_by_group(
        &self,
        opctx: &OpContext,
        group_id: MulticastGroupUuid,
    ) -> DeleteResult {
        use nexus_db_schema::schema::multicast_group;
        use nexus_db_schema::schema::multicast_group_member;

        diesel::delete(multicast_group_member::table)
            .filter(
                multicast_group_member::external_group_id
                    .eq(group_id.into_untyped_uuid()),
            )
            // A pending OPTE cleanup has `sled_id` set. Leave that row for
            // the member reconciler. Deleting it here would discard the only
            // durable handle for retrying the unsubscribe.
            .filter(multicast_group_member::sled_id.is_null())
            .filter(diesel::dsl::exists(
                multicast_group::table
                    .filter(
                        multicast_group::id.eq(group_id.into_untyped_uuid()),
                    )
                    .filter(
                        multicast_group::state
                            .eq(MulticastGroupState::Deleting),
                    )
                    .filter(multicast_group::time_deleted.is_not_null()),
            ))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
            .map(|_x| ())
    }

    /// Delete a multicast member by `(group, parent)` pair.
    ///
    /// Hard-deletes the row (active or soft-deleted) for the given group and
    /// parent (instance or probe). Used during saga undo to clean up only the
    /// member created by that saga, leaving other parents' memberships in the
    /// same group intact.
    pub async fn multicast_group_member_delete_by_group_and_parent(
        &self,
        opctx: &OpContext,
        group_id: MulticastGroupUuid,
        parent: MemberParentRef,
    ) -> DeleteResult {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        diesel::delete(dsl::multicast_group_member)
            .filter(dsl::external_group_id.eq(group_id.into_untyped_uuid()))
            .filter(dsl::parent_id.eq(parent.as_uuid()))
            .filter(dsl::parent_kind.eq(parent.kind()))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
            .map(|_x| ())
    }

    /// Set the state of a multicast group member identified by its
    /// `(group, parent)` pair, regardless of parent kind.
    pub async fn multicast_group_member_set_state_for_parent(
        &self,
        opctx: &OpContext,
        external_group_id: MulticastGroupUuid,
        parent: MemberParentRef,
        new_state: MulticastGroupMemberState,
    ) -> UpdateResult<()> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let rows_updated = diesel::update(dsl::multicast_group_member)
            .filter(
                dsl::external_group_id
                    .eq(external_group_id.into_untyped_uuid()),
            )
            .filter(dsl::parent_id.eq(parent.as_uuid()))
            .filter(dsl::parent_kind.eq(parent.kind()))
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

    /// Conditionally set the state of a multicast group member if the
    /// current state matches `expected_state`, regardless of parent kind.
    ///
    /// Used by the RPW reconciler.
    ///
    /// Returns `Ok(true)` if the row was updated and `Ok(false)` if no row
    /// matched the filters (member not found, soft-deleted, or state
    /// mismatch).
    pub async fn multicast_group_member_set_state_if_current_for_parent(
        &self,
        opctx: &OpContext,
        external_group_id: MulticastGroupUuid,
        parent: MemberParentRef,
        expected_state: MulticastGroupMemberState,
        new_state: MulticastGroupMemberState,
    ) -> UpdateResult<bool> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let rows_updated = diesel::update(dsl::multicast_group_member)
            .filter(
                dsl::external_group_id
                    .eq(external_group_id.into_untyped_uuid()),
            )
            .filter(dsl::parent_id.eq(parent.as_uuid()))
            .filter(dsl::parent_kind.eq(parent.kind()))
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::state.eq(expected_state))
            .set((dsl::state.eq(new_state), dsl::time_modified.eq(Utc::now())))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(rows_updated > 0)
    }

    /// Atomically transition from "Left" → "Joining" and set sled_id,
    /// regardless of parent kind.
    ///
    /// Used by the RPW reconciler.
    ///
    /// Returns `Ok(true)` if the row was updated and `Ok(false)` if the row
    /// was missing or its state was not "Left".
    pub async fn multicast_group_member_left_to_joining_if_current_for_parent(
        &self,
        opctx: &OpContext,
        external_group_id: MulticastGroupUuid,
        parent: MemberParentRef,
        sled_id: DbTypedUuid<SledKind>,
    ) -> UpdateResult<bool> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let rows_updated = diesel::update(dsl::multicast_group_member)
            .filter(
                dsl::external_group_id
                    .eq(external_group_id.into_untyped_uuid()),
            )
            .filter(dsl::parent_id.eq(parent.as_uuid()))
            .filter(dsl::parent_kind.eq(parent.kind()))
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
    /// matches `expected_state`, regardless of parent kind.
    ///
    /// Used by the RPW reconciler.
    ///
    /// Returns `Ok(true)` if the row was updated and `Ok(false)` if the row
    /// was missing or its state did not match `expected_state`.
    pub async fn multicast_group_member_to_left_if_current_for_parent(
        &self,
        opctx: &OpContext,
        external_group_id: MulticastGroupUuid,
        parent: MemberParentRef,
        expected_state: MulticastGroupMemberState,
    ) -> UpdateResult<bool> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let rows_updated = diesel::update(dsl::multicast_group_member)
            .filter(
                dsl::external_group_id
                    .eq(external_group_id.into_untyped_uuid()),
            )
            .filter(dsl::parent_id.eq(parent.as_uuid()))
            .filter(dsl::parent_kind.eq(parent.kind()))
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

    /// List multicast group memberships for a parent (instance or probe).
    ///
    /// Only returns active (non-deleted) memberships.
    pub async fn multicast_group_members_list_by_parent(
        &self,
        opctx: &OpContext,
        parent: MemberParentRef,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<MulticastGroupMember> {
        let conn = self.pool_connection_authorized(opctx).await?;
        self.multicast_group_members_list_by_parent_on_conn(
            &conn, parent, pagparams,
        )
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// List parent memberships using an existing database connection.
    ///
    /// Returns the raw diesel error so transactional callers can hand
    /// retryable errors back to the retry wrapper.
    pub(crate) async fn multicast_group_members_list_by_parent_on_conn(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        parent: MemberParentRef,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> Result<Vec<MulticastGroupMember>, diesel::result::Error> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        paginated(dsl::multicast_group_member, dsl::id, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::parent_id.eq(parent.as_uuid()))
            .filter(dsl::parent_kind.eq(parent.kind()))
            .select(MulticastGroupMember::as_select())
            .get_results_async(conn)
            .await
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
    /// Members in every state contribute, including "Left". Membership
    /// persists across instance stop, so a stopped member's source
    /// preferences stay in the switch filter rather than flapping on
    /// stop/start. Only soft-deleted rows are excluded.
    ///
    /// # Batch Usage
    ///
    /// Designed for batch lookups to avoid n+1 query patterns. Pass multiple
    /// group IDs to fetch in a single database round-trip.
    ///
    /// # DPD Source Filtering
    ///
    /// When `has_any_source_member` is true, pass `None` to DPD for sources
    /// (disabling switch-level filtering). Otherwise, use
    /// `specific_sources`.
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

    /// Union of the specific source IPs contributed by members other than
    /// `parent` for `group_id`.
    ///
    /// A repeat join replaces the existing member's filter, so excluding that
    /// member here matches the atomic attach validation.
    pub async fn multicast_group_source_union_excluding_parent(
        &self,
        opctx: &OpContext,
        group_id: MulticastGroupUuid,
        parent: MemberParentRef,
    ) -> Result<BTreeSet<IpAddr>, external::Error> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let rows: Vec<Vec<IpNetwork>> = dsl::multicast_group_member
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::external_group_id.eq(group_id.into_untyped_uuid()))
            // Members are keyed by (parent_kind, parent_id), matching the
            // atomic attach CTE. Excluding on parent_id alone would also
            // drop an other-kind member that happens to share the UUID.
            .filter(
                dsl::parent_kind
                    .ne(parent.kind())
                    .or(dsl::parent_id.ne(parent.as_uuid())),
            )
            .select(dsl::source_ips)
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(rows.into_iter().flatten().map(|ip| ip.ip()).collect())
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
    /// - `parent`: The member parent (instance or probe) being reconciled
    /// - `parent_valid`: Whether the parent is in a valid state for multicast
    /// - `current_sled_id`: The parent's current sled_id from VMM lookup
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
    ///     .multicast_group_member_reconcile_joining_for_parent(
    ///         opctx,
    ///         MulticastGroupUuid::from_untyped_uuid(group.id()),
    ///         MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(member.parent_id)),
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
    pub async fn multicast_group_member_reconcile_joining_for_parent(
        &self,
        opctx: &OpContext,
        group_id: MulticastGroupUuid,
        parent: MemberParentRef,
        parent_valid: bool,
        current_sled_id: Option<DbTypedUuid<SledKind>>,
    ) -> Result<ops::member_reconcile::ReconcileJoiningResult, external::Error>
    {
        let conn = self.pool_connection_authorized(opctx).await?;

        ops::member_reconcile::reconcile_joining_member(
            &conn,
            group_id.into_untyped_uuid(),
            parent,
            parent_valid,
            current_sled_id,
        )
        .await
        .map_err(external::Error::from)
    }

    /// Detach a parent (instance or probe) from all of its multicast group
    /// memberships.
    ///
    /// This is polymorphic over parent kind. Transitions non-"Left" members to
    /// "Left" and clears `sled_id`. The dataplane cleanup is then driven
    /// by the reconciler.
    pub async fn multicast_group_members_detach_by_parent(
        &self,
        opctx: &OpContext,
        parent: MemberParentRef,
    ) -> Result<(), external::Error> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;
        let now = Utc::now();

        diesel::update(dsl::multicast_group_member)
            .filter(dsl::parent_id.eq(parent.as_uuid()))
            .filter(dsl::parent_kind.eq(parent.kind()))
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::state.ne(MulticastGroupMemberState::Left))
            .set((
                dsl::state.eq(MulticastGroupMemberState::Left),
                dsl::sled_id.eq(Option::<DbTypedUuid<SledKind>>::None),
                dsl::time_modified.eq(now),
            ))
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
            .map(|_| ())
    }

    /// Get a multicast member by `(group, parent)` pair.
    pub async fn multicast_group_member_get_by_group_and_parent(
        &self,
        opctx: &OpContext,
        group_id: MulticastGroupUuid,
        parent: MemberParentRef,
    ) -> Result<Option<MulticastGroupMember>, external::Error> {
        self.multicast_group_member_get_by_group_key_and_parent(
            opctx,
            MulticastGroupMemberGroupKey::Id(group_id),
            parent,
        )
        .await
    }

    /// Get the live member for a multicast IP and parent.
    ///
    /// This lookup deliberately uses the IP rather than the external group
    /// UUID so cleanup remains safe if the group row has already been reaped
    /// and the address has been allocated to a replacement group.
    pub async fn multicast_group_member_get_by_multicast_ip_and_parent(
        &self,
        opctx: &OpContext,
        multicast_ip: IpNetwork,
        parent: MemberParentRef,
    ) -> Result<Option<MulticastGroupMember>, external::Error> {
        self.multicast_group_member_get_by_group_key_and_parent(
            opctx,
            MulticastGroupMemberGroupKey::MulticastIp(multicast_ip),
            parent,
        )
        .await
    }

    async fn multicast_group_member_get_by_group_key_and_parent(
        &self,
        opctx: &OpContext,
        group_key: MulticastGroupMemberGroupKey,
        parent: MemberParentRef,
    ) -> Result<Option<MulticastGroupMember>, external::Error> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let mut query = dsl::multicast_group_member.into_boxed();
        query = query
            .filter(dsl::parent_id.eq(parent.as_uuid()))
            .filter(dsl::parent_kind.eq(parent.kind()))
            .filter(dsl::time_deleted.is_null());
        query = match group_key {
            MulticastGroupMemberGroupKey::Id(group_id) => query.filter(
                dsl::external_group_id.eq(group_id.into_untyped_uuid()),
            ),
            MulticastGroupMemberGroupKey::MulticastIp(multicast_ip) => {
                query.filter(dsl::multicast_ip.eq(multicast_ip))
            }
        };

        let member = query
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
    /// soft-deleted (i.e., `time_deleted` is set). Otherwise, it filters out
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

    /// Detach a multicast member by (group, parent) pair.
    ///
    /// This transitions member to "Left" state, sets `time_deleted` (marking
    /// for permanent removal), and retains `sled_id` for OPTE cleanup retry.
    /// Used by the HTTP API for explicit detach operations. Distinct from
    /// instance stop, which only transitions to "Left" without `time_deleted`.
    ///
    /// `sled_id` remains unchanged so the member RPW knows which sled has the
    /// VMM's OPTE subscription.
    ///
    /// See [`Self::multicast_group_members_detach_by_parent`] for detaching
    /// all memberships of a parent (used during the instance stop event).
    pub async fn multicast_group_member_detach_by_group_and_parent(
        &self,
        opctx: &OpContext,
        group_id: MulticastGroupUuid,
        parent: MemberParentRef,
    ) -> Result<bool, external::Error> {
        let conn = self.pool_connection_authorized(opctx).await?;
        self.multicast_group_member_detach_by_group_and_parent_on_conn(
            &conn, group_id, parent,
        )
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Detach a parent's membership using an existing database connection.
    ///
    /// Returns the raw diesel error so transactional callers can hand
    /// retryable errors back to the retry wrapper.
    pub(crate) async fn multicast_group_member_detach_by_group_and_parent_on_conn(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        group_id: MulticastGroupUuid,
        parent: MemberParentRef,
    ) -> Result<bool, diesel::result::Error> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let now = Utc::now();

        // Mark member for removal (set time_deleted and state to "Left"),
        // similar to soft deletion of the parent resource. Retain `sled_id`
        // so OPTE cleanup can retry against the assigned sled.
        let updated_rows = diesel::update(dsl::multicast_group_member)
            .filter(dsl::external_group_id.eq(group_id.into_untyped_uuid()))
            .filter(dsl::parent_id.eq(parent.as_uuid()))
            .filter(dsl::parent_kind.eq(parent.kind()))
            .filter(dsl::time_deleted.is_null())
            .set((
                dsl::state.eq(MulticastGroupMemberState::Left),
                dsl::time_deleted.eq(Some(now)), // Mark for deletion
                dsl::time_modified.eq(now),
            ))
            .execute_async(conn)
            .await?;

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
    /// via [`Self::multicast_group_members_detach_by_parent`].
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
            .filter(
                dsl::parent_kind.eq(MulticastGroupMemberParentKind::Instance),
            )
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
            .filter(
                dsl::parent_kind.eq(MulticastGroupMemberParentKind::Instance),
            )
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
    /// - "Left" (sled_id=NULL) → "Joining" (sled_id=actual): instance restart
    /// - "Joining" (sled_id=NULL) → "Joining" (sled_id=actual): first-time start
    /// - "Joined": no change (already has sled_id, ignored)
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
            .filter(
                dsl::parent_kind.eq(MulticastGroupMemberParentKind::Instance),
            )
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

    /// Permanently mark all multicast memberships for deletion when the
    /// parent (instance or probe) is deleted.
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
    /// Compare with [`Self::multicast_group_members_detach_by_parent`] which
    /// leaves `time_deleted=NULL` for reactivation on parent restart.
    ///
    /// `sled_id` survives the soft delete. A row that still records a sled is
    /// picked up by [`Self::multicast_group_members_list_pending_cleanup`], so
    /// any OPTE subscription left behind can be withdrawn before the
    /// hard-delete sweep reaps the row.
    pub async fn multicast_group_members_mark_for_removal_by_parent(
        &self,
        opctx: &OpContext,
        parent: MemberParentRef,
    ) -> Result<(), external::Error> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let now = Utc::now();

        diesel::update(dsl::multicast_group_member)
            .filter(dsl::parent_id.eq(parent.as_uuid()))
            .filter(dsl::parent_kind.eq(parent.kind()))
            .filter(dsl::time_deleted.is_null())
            .set((
                dsl::state.eq(MulticastGroupMemberState::Left),
                dsl::time_deleted.eq(Some(now)),
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
    ///
    /// `false` means the row was already deleted, either by a concurrent
    /// request or by an earlier run of the same undo action.
    pub async fn multicast_group_member_delete_by_id(
        &self,
        opctx: &OpContext,
        member_id: Uuid,
    ) -> Result<bool, external::Error> {
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
            return Ok(false);
        }

        debug!(
            opctx.log,
            "multicast group member marked for deletion";
            "member_id" => %member_id,
            "rows_updated" => updated_rows
        );

        Ok(true)
    }

    /// Mark a multicast group member for deletion while retaining `sled_id` so
    /// the RPW can retry the OPTE unsubscribe.
    pub async fn multicast_group_member_delete_by_id_preserving_sled_id(
        &self,
        opctx: &OpContext,
        member_id: Uuid,
    ) -> Result<bool, external::Error> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let now = Utc::now();
        let updated_rows = diesel::update(dsl::multicast_group_member)
            .filter(dsl::id.eq(member_id))
            .filter(dsl::time_deleted.is_null())
            .set((
                dsl::state.eq(MulticastGroupMemberState::Left),
                dsl::time_deleted.eq(Some(now)),
                dsl::time_modified.eq(now),
            ))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        if updated_rows == 0 {
            return Ok(false);
        }

        debug!(
            opctx.log,
            "multicast group member marked for deletion with sled assignment retained";
            "member_id" => %member_id,
            "rows_updated" => updated_rows
        );

        Ok(true)
    }

    /// Clear a pending member's sled assignment after its OPTE unsubscribe
    /// succeeds. The expected sled ID makes this a compare-and-swap so a
    /// concurrent operation cannot clear a newer assignment.
    pub async fn multicast_group_member_clear_sled_id_if_current(
        &self,
        opctx: &OpContext,
        member_id: Uuid,
        expected_sled_id: DbTypedUuid<SledKind>,
    ) -> Result<bool, external::Error> {
        use nexus_db_schema::schema::multicast_group_member::dsl;

        let updated_rows = diesel::update(dsl::multicast_group_member)
            .filter(dsl::id.eq(member_id))
            .filter(dsl::time_deleted.is_not_null())
            .filter(dsl::sled_id.eq(expected_sled_id))
            .set((
                dsl::sled_id.eq(Option::<DbTypedUuid<SledKind>>::None),
                dsl::time_modified.eq(Utc::now()),
            ))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(updated_rows > 0)
    }

    /// Complete deletion of multicast group members that are in
    /// ["Left"](MulticastGroupMemberState::Left) state, have `time_deleted`
    /// set, and have `sled_id` cleared.
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
            .filter(dsl::sled_id.is_null())
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        debug!(
            opctx.log,
            "multicast group member complete deletion finished";
            "sledless_time_deleted_members_deleted" => deleted_rows
        );

        Ok(deleted_rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::Ipv4Addr;

    use nexus_types::identity::Resource;
    use nexus_types::multicast::MulticastGroupCreate;
    use omicron_common::address::{
        MAX_SOURCE_IPS_PER_GROUP, MAX_SOURCE_IPS_PER_MEMBER,
    };
    use omicron_common::api::external::DataPageParams;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::{ProbeUuid, SledUuid};

    use crate::db::datastore::multicast::ops::member_attach::AttachOutcome;
    use crate::db::model::{
        MulticastGroupMemberParentKind, MulticastGroupMemberValues,
    };
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
    async fn test_multicast_group_member_attach_instance_parent() {
        let logctx = dev::test_setup_log(
            "test_multicast_group_member_attach_instance_parent",
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
            has_sources: false,
            ip_version: None,
        };

        let creating_group = datastore
            .multicast_group_create(&opctx, &creating_group_params)
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
        let creating_attach = datastore
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(creating_group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should attach to 'Creating' group");
        assert_eq!(
            creating_attach.outcome,
            AttachOutcome::Created,
            "First attach to a 'Creating' group should create a member row"
        );
        let creating_member = creating_attach.member;
        assert_eq!(creating_member.state, MulticastGroupMemberState::Joining);

        // Attach to active group should also succeed
        let attach = datastore
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(active_group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should attach instance to active group");
        assert_eq!(attach.outcome, AttachOutcome::Created);
        let member = attach.member;

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
        let attach2 = datastore
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(active_group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should handle duplicate attach to 'Joining' member");
        assert_eq!(attach2.outcome, AttachOutcome::Existing);
        let member2 = attach2.member;

        assert_eq!(member.id, member2.id, "Should return same member ID");
        assert_eq!(
            member2.time_modified, time_after_first_attach,
            "Idempotent attach must not update time_modified"
        );

        // Transition member to "Joined" state and capture time_modified
        datastore
            .multicast_group_member_set_state_for_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(active_group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
                MulticastGroupMemberState::Joined,
            )
            .await
            .expect("Should transition member to 'Joined'");
        let member_joined = datastore
            .multicast_group_member_get_by_group_and_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(active_group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
            )
            .await
            .expect("Should refetch member after Joined")
            .expect("Member should exist");
        let time_after_joined = member_joined.time_modified;

        // Attach to member in "Joined" state should be idempotent
        let member3 = datastore
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(active_group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should handle attach to 'Joined' member")
            .member;

        assert_eq!(member.id, member3.id, "Should return same member ID");
        assert_eq!(
            member3.time_modified, time_after_joined,
            "Idempotent attach while Joined must not update time_modified"
        );

        // Transition member to "Left" state (simulating instance stop)
        datastore
            .multicast_group_member_set_state_for_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(active_group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
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
            .multicast_group_member_get_by_group_and_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(active_group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
            )
            .await
            .expect("Should get member after Left")
            .expect("Member should exist");
        let time_after_left = member_left.time_modified;

        // Attach to member in "Left" state should reactivate it with new sources
        let reactivation_sources: Vec<IpAddr> =
            vec!["10.0.0.1".parse().unwrap(), "10.0.0.2".parse().unwrap()];
        let reactivated_attach = datastore
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(active_group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
                Some(reactivation_sources.as_slice()),
            )
            .await
            .expect("Should reactivate 'Left' member");
        assert_eq!(reactivated_attach.outcome, AttachOutcome::Existing);
        let reactivated_member = reactivated_attach.member;

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

        // A stale undo must not delete a replacement row for the same
        // group/instance pair.
        let original_member_id = reactivated_member.id;
        let deleted = datastore
            .multicast_group_member_delete_by_id(&opctx, original_member_id)
            .await
            .expect("Should delete original member row");
        assert!(deleted, "Original member row should be deleted");

        let replacement = datastore
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(active_group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should create replacement member row");
        assert_eq!(replacement.outcome, AttachOutcome::Created);
        assert_ne!(
            replacement.member.id, original_member_id,
            "Replacement attach should mint a new member row rather than \
             reuse the deleted one"
        );

        let stale_delete = datastore
            .multicast_group_member_delete_by_id(&opctx, original_member_id)
            .await
            .expect("Stale undo should not error");
        assert!(!stale_delete, "Stale undo should not delete a row");

        let current_member = datastore
            .multicast_group_member_get_by_group_and_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(active_group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
            )
            .await
            .expect("Should find replacement member")
            .expect("Replacement member should exist");
        assert_eq!(
            current_member.id, replacement.member.id,
            "Lookup should return the replacement member row"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_multicast_group_member_attach_probe_parent() {
        let logctx = dev::test_setup_log(
            "test_multicast_group_member_attach_probe_parent",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "attach-probe-test-pool",
            "test-project-attach-probe",
        )
        .await;

        let active_group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            "active-probe-group",
            "224.10.1.7",
            true,
        )
        .await;

        // Insert a probe row directly. `multicast_group_member_attach` only
        // reads `probe.id`, `probe.sled`, and `probe.time_deleted`.
        let probe_id = Uuid::new_v4();
        let probe = nexus_db_model::Probe {
            identity: nexus_db_model::ProbeIdentity::new(
                probe_id,
                IdentityMetadataCreateParams {
                    name: "attach-test-probe".parse().unwrap(),
                    description: "attach probe test fixture".to_string(),
                },
            ),
            project_id: setup.project_id,
            sled: setup.sled_id.into(),
        };
        {
            use nexus_db_schema::schema::probe::dsl as probe_dsl;
            let conn = datastore
                .pool_connection_authorized(&opctx)
                .await
                .expect("Get connection");
            diesel::insert_into(probe_dsl::probe)
                .values(probe.clone())
                .execute_async(&*conn)
                .await
                .expect("Should insert probe row");
        }
        let parent =
            MemberParentRef::Probe(ProbeUuid::from_untyped_uuid(probe_id));

        // First attach: probe enters "Joining" with `sled_id` read directly
        // from the probe row (not via VMM lookup).
        let attach = datastore
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(active_group.id()),
                parent,
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should attach probe to active group");
        assert_eq!(attach.outcome, AttachOutcome::Created);
        let member = attach.member;

        assert_eq!(member.state, MulticastGroupMemberState::Joining);
        assert_eq!(member.sled_id, Some(setup.sled_id.into()));
        assert_eq!(member.external_group_id, active_group.id());
        assert_eq!(member.multicast_ip, active_group.multicast_ip);
        assert_eq!(member.parent_id, probe_id);
        assert_eq!(
            member.parent_kind,
            MulticastGroupMemberParentKind::Probe,
            "parent_kind must persist as Probe"
        );
        assert!(member.time_deleted.is_none());
        assert!(member.time_created <= member.time_modified);
        assert_eq!(member.source_ips, Vec::<ipnetwork::IpNetwork>::new());
        assert!(member.version_removed.is_none());

        // Verify the persisted row round-trips through the lookup path and
        // reconstructs as `MemberParentRef::Probe`.
        let fetched = datastore
            .multicast_group_member_get_by_group_and_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(active_group.id()),
                parent,
            )
            .await
            .expect("Should fetch probe member")
            .expect("Probe member should exist");
        assert_eq!(fetched.id, member.id);
        assert!(matches!(fetched.parent_ref(), MemberParentRef::Probe(_)));

        let time_after_first_attach = member.time_modified;

        // Re-attach while still "Joining" should be idempotent: same row, no
        // `time_modified` advance.
        let attach2 = datastore
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(active_group.id()),
                parent,
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should handle duplicate probe attach idempotently");
        assert_eq!(attach2.outcome, AttachOutcome::Existing);
        let member2 = attach2.member;

        assert_eq!(member.id, member2.id);
        assert_eq!(
            member2.time_modified, time_after_first_attach,
            "Idempotent probe attach must not update time_modified"
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
            "group1",
            "224.10.1.5",
            true, // make_active
        )
        .await;
        let group2 = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
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
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group1.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    *instance1_id,
                )),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should add instance1 to group1")
            .member;

        let member1_2 = datastore
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group2.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    *instance1_id,
                )),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should add instance1 to group2")
            .member;

        let member2_1 = datastore
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group1.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    *instance2_id,
                )),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should add instance2 to group1")
            .member;

        // Verify all memberships exist
        assert_eq!(member1_1.parent_id, *instance1_id);
        assert_eq!(member1_2.parent_id, *instance1_id);
        assert_eq!(member2_1.parent_id, *instance2_id);

        // Detach all memberships for instance1 (transitions to "Left", does not set time_deleted)
        datastore
            .multicast_group_members_detach_by_parent(
                &opctx,
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    *instance1_id,
                )),
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
            .multicast_group_members_detach_by_parent(
                &opctx,
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    *instance1_id,
                )),
            )
            .await
            .expect("Should handle detaching instance1 again");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_multicast_group_members_detach_by_parent_probe() {
        let logctx = dev::test_setup_log(
            "test_multicast_group_members_detach_by_parent_probe",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "detach-probe-pool",
            "test-project-detach-probe",
        )
        .await;

        let group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            "detach-probe-group",
            "224.10.1.8",
            true,
        )
        .await;

        // Create one instance-parented member.
        let (instance, _vmm) = create_instance_with_vmm(
            &opctx,
            &datastore,
            &setup.authz_project,
            "detach-probe-coexist-instance",
            setup.sled_id,
        )
        .await;
        let instance_id = *instance.as_untyped_uuid();
        let instance_parent = MemberParentRef::Instance(
            InstanceUuid::from_untyped_uuid(instance_id),
        );

        // Insert a probe row directly.
        let probe_id = Uuid::new_v4();
        let probe = nexus_db_model::Probe {
            identity: nexus_db_model::ProbeIdentity::new(
                probe_id,
                IdentityMetadataCreateParams {
                    name: "detach-test-probe".parse().unwrap(),
                    description: "detach probe test fixture".to_string(),
                },
            ),
            project_id: setup.project_id,
            sled: setup.sled_id.into(),
        };
        {
            use nexus_db_schema::schema::probe::dsl as probe_dsl;
            let conn = datastore
                .pool_connection_authorized(&opctx)
                .await
                .expect("Get connection");
            diesel::insert_into(probe_dsl::probe)
                .values(probe.clone())
                .execute_async(&*conn)
                .await
                .expect("Should insert probe row");
        }
        let probe_parent =
            MemberParentRef::Probe(ProbeUuid::from_untyped_uuid(probe_id));

        // Attach both to the same group and drive both to "Joined" so we can
        // confirm `detach_by_parent` only touches the targeted parent.
        let instance_member = datastore
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                instance_parent,
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should attach instance to group")
            .member;
        let probe_member = datastore
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                probe_parent,
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should attach probe to group")
            .member;

        datastore
            .multicast_group_member_set_state_for_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                instance_parent,
                MulticastGroupMemberState::Joined,
            )
            .await
            .expect("Should mark instance member Joined");
        datastore
            .multicast_group_member_set_state_for_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                probe_parent,
                MulticastGroupMemberState::Joined,
            )
            .await
            .expect("Should mark probe member Joined");

        // Detach by probe parent only.
        datastore
            .multicast_group_members_detach_by_parent(&opctx, probe_parent)
            .await
            .expect("Should detach probe memberships");

        // Probe row transitions to "Left" with cleared `sled_id`, no
        // `time_deleted` (membership row remains for the reconciler).
        let probe_after = datastore
            .multicast_group_member_get_by_id(&opctx, probe_member.id, false)
            .await
            .expect("Should fetch probe member after detach")
            .expect("Probe member row should still exist");
        assert_eq!(probe_after.state, MulticastGroupMemberState::Left);
        assert!(
            probe_after.sled_id.is_none(),
            "Probe detach must clear sled_id"
        );
        assert!(
            probe_after.time_deleted.is_none(),
            "detach_by_parent should not set time_deleted"
        );
        assert_eq!(
            probe_after.parent_kind,
            MulticastGroupMemberParentKind::Probe,
        );

        // Instance row in the same group is untouched, so still "Joined" with its
        // `sled_id` intact.
        let instance_after = datastore
            .multicast_group_member_get_by_id(&opctx, instance_member.id, false)
            .await
            .expect("Should fetch instance member after probe detach")
            .expect("Instance member row should still exist");
        assert_eq!(instance_after.state, MulticastGroupMemberState::Joined);
        assert_eq!(instance_after.sled_id, Some(setup.sled_id.into()));
        assert_eq!(
            instance_after.parent_kind,
            MulticastGroupMemberParentKind::Instance,
        );

        // Detaching the probe again is a noop.
        datastore
            .multicast_group_members_detach_by_parent(&opctx, probe_parent)
            .await
            .expect("Repeat probe detach should be idempotent");

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
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    *instance_id,
                )),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should add instance as member")
            .member;

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
            .multicast_group_members_list_by_parent(
                &opctx,
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    *instance_id,
                )),
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
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(instance_id),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should add instance as member first time")
            .member;

        // Try to add same instance again - should return existing member
        let member2 = datastore
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(instance_id),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should handle duplicate add idempotently")
            .member;

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
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    test_instance_id,
                )),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should create member record")
            .member;

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
            .multicast_group_member_get_by_group_and_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    test_instance_id,
                )),
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
            .multicast_group_member_get_by_group_and_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    test_instance_id,
                )),
            )
            .await
            .expect("Should fetch migrated member")
            .expect("Member should exist");

        assert_eq!(migrated_member.sled_id, Some(sled2_id.into()));

        // Instance stop - Clear sled_id (set to NULL)
        datastore
            .multicast_group_members_detach_by_parent(
                &opctx,
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    test_instance_id,
                )),
            )
            .await
            .expect("Should clear sled_id for instance stop");

        // Verify sled_id was cleared
        let stopped_member = datastore
            .multicast_group_member_get_by_group_and_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    test_instance_id,
                )),
            )
            .await
            .expect("Should fetch stopped member")
            .expect("Member should exist");

        assert_eq!(stopped_member.sled_id, None);

        // Idempotency - Clearing again should be idempotent
        datastore
            .multicast_group_members_detach_by_parent(
                &opctx,
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    test_instance_id,
                )),
            )
            .await
            .expect("Should handle clearing sled_id again");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Datastore-only verification of member state transitions.
    #[tokio::test]
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
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    test_instance_id,
                )),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should create member record");

        // Complete the attach operation
        datastore
            .multicast_group_member_set_state_for_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    test_instance_id,
                )),
                MulticastGroupMemberState::Joined,
            )
            .await
            .expect("Should complete attach operation");

        // Complete the operation and leave
        datastore
            .multicast_group_member_set_state_for_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    test_instance_id,
                )),
                MulticastGroupMemberState::Left,
            )
            .await
            .expect("Should complete detach operation");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Datastore-only verification of probe-parented member state transitions.
    ///
    /// Walks "Joining" -> "Joined" -> "Left" via the polymorphic
    /// `_for_parent` operations and asserts probe-side transitions never
    /// write `sled_id`.
    #[tokio::test]
    async fn test_multicast_group_member_state_transitions_probe_parent() {
        let logctx = dev::test_setup_log(
            "test_multicast_group_member_state_transitions_probe_parent",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let setup = multicast::create_test_setup_with_range(
            &opctx,
            &datastore,
            "state-probe-pool",
            "test-project-probe-state",
            (224, 2, 2, 1),
            (224, 2, 2, 254),
        )
        .await;
        let group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            "state-probe-group",
            "224.2.2.5",
            true,
        )
        .await;

        // Insert a probe row directly.
        let probe_id = Uuid::new_v4();
        let probe = nexus_db_model::Probe {
            identity: nexus_db_model::ProbeIdentity::new(
                probe_id,
                IdentityMetadataCreateParams {
                    name: "state-test-probe".parse().unwrap(),
                    description: "state probe test fixture".to_string(),
                },
            ),
            project_id: setup.project_id,
            sled: setup.sled_id.into(),
        };
        {
            use nexus_db_schema::schema::probe::dsl as probe_dsl;
            let conn = datastore
                .pool_connection_authorized(&opctx)
                .await
                .expect("Get connection");
            diesel::insert_into(probe_dsl::probe)
                .values(probe.clone())
                .execute_async(&*conn)
                .await
                .expect("Should insert probe row");
        }
        let parent =
            MemberParentRef::Probe(ProbeUuid::from_untyped_uuid(probe_id));

        // Attach: row lands in "Joining" with `sled_id` pulled from the
        // probe row by the CTE.
        let attached = datastore
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                parent,
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should attach probe member")
            .member;
        assert_eq!(attached.state, MulticastGroupMemberState::Joining);
        assert_eq!(attached.sled_id, Some(setup.sled_id.into()));

        let sled_id_after_attach = attached.sled_id;

        // Joining -> Joined via the polymorphic setter.
        datastore
            .multicast_group_member_set_state_for_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                parent,
                MulticastGroupMemberState::Joined,
            )
            .await
            .expect("Should transition probe to Joined");
        let joined = datastore
            .multicast_group_member_get_by_group_and_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                parent,
            )
            .await
            .expect("Fetch after Joined")
            .expect("Probe member should exist");
        assert_eq!(joined.state, MulticastGroupMemberState::Joined);
        assert_eq!(
            joined.sled_id, sled_id_after_attach,
            "Joined transition must not rewrite sled_id"
        );

        // "Joined" -> "Left" via the conditional
        // `multicast_group_member_to_left_if_current_for_parent` used by
        // the RPW reconciler. `sled_id` is cleared as part of "Left"
        // semantics. This is the only path on the probe side that mutates
        // `sled_id`, and it does so by clearing (not by writing a new
        // value).
        let transitioned = datastore
            .multicast_group_member_to_left_if_current_for_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                parent,
                MulticastGroupMemberState::Joined,
            )
            .await
            .expect("Conditional transition should succeed");

        assert!(transitioned, "Joined -> Left must update one row");

        let left = datastore
            .multicast_group_member_get_by_group_and_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                parent,
            )
            .await
            .expect("Fetch after Left")
            .expect("Probe member should exist");
        assert_eq!(left.state, MulticastGroupMemberState::Left);
        assert!(
            left.sled_id.is_none(),
            "Left transition clears sled_id; probe path never writes a new sled"
        );

        // Conditional re-transition with the wrong `expected_state` must
        // not affect the row (idempotency / TOCTOU guard).
        let no_op = datastore
            .multicast_group_member_to_left_if_current_for_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                parent,
                MulticastGroupMemberState::Joined,
            )
            .await
            .expect("Conditional with wrong expected_state should not error");
        assert!(!no_op, "No row should match a stale expected_state");

        // "Left" -> "Joining" via the polymorphic
        // `multicast_group_member_left_to_joining_if_current_for_parent`.
        // It re-populates `sled_id` from the passed parameter (RPW
        // reconciler-driven rejoin). Pass a sled distinct from the probe's
        // own sled so the assertion proves the setter writes the parameter
        // rather than re-deriving the probe's row value.
        let rejoin_sled = SledUuid::new_v4();
        assert_ne!(rejoin_sled, setup.sled_id);
        let rejoined = datastore
            .multicast_group_member_left_to_joining_if_current_for_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                parent,
                rejoin_sled.into(),
            )
            .await
            .expect("Should rejoin probe");
        assert!(rejoined, "Left -> Joining must update one row");
        let rejoined_row = datastore
            .multicast_group_member_get_by_group_and_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                parent,
            )
            .await
            .expect("Fetch after rejoin")
            .expect("Probe member should exist");
        assert_eq!(rejoined_row.state, MulticastGroupMemberState::Joining);
        assert_eq!(rejoined_row.sled_id, Some(rejoin_sled.into()));
        assert_eq!(
            rejoined_row.parent_kind,
            MulticastGroupMemberParentKind::Probe,
        );

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

        let (instance4, _vmm4) = create_instance_with_vmm(
            &opctx,
            &datastore,
            &setup.authz_project,
            "delete-test-instance4",
            setup.sled_id,
        )
        .await;
        let instance4_id = instance4.into_untyped_uuid();

        // Create member records in different states
        let conn = datastore
            .pool_connection_authorized(&opctx)
            .await
            .expect("Get connection");
        use nexus_db_schema::schema::multicast_group_member::dsl;

        // Member 1: "Left" + `time_deleted` + no sled assignment (should be
        // deleted).
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
                    parent_kind: MulticastGroupMemberParentKind::Instance,
                    sled_id: None,
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
                    parent_kind: MulticastGroupMemberParentKind::Instance,
                    sled_id: Some(setup.sled_id.into()),
                    state: MulticastGroupMemberState::Left,
                    source_ips: vec![],
                })
                .returning(MulticastGroupMember::as_returning())
                .get_result_async(&*conn)
                .await
                .expect("Should create member2 record");

        // Member 3: "Joined" state (should not be deleted, even if it had
        // `time_deleted`).
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
                    parent_kind: MulticastGroupMemberParentKind::Instance,
                    sled_id: Some(setup.sled_id.into()),
                    state: MulticastGroupMemberState::Joined,
                    source_ips: vec![],
                })
                .returning(MulticastGroupMember::as_returning())
                .get_result_async(&*conn)
                .await
                .expect("Should create member3 record");

        // Member 4: "Left" + `time_deleted` + sled assignment (pending OPTE
        // cleanup, so it must not be hard-deleted yet).
        let member4: MulticastGroupMember =
            diesel::insert_into(dsl::multicast_group_member)
                .values(MulticastGroupMemberValues {
                    id: Uuid::new_v4(),
                    time_created: Utc::now(),
                    time_modified: Utc::now(),
                    time_deleted: Some(Utc::now()),
                    external_group_id: group.id(),
                    multicast_ip: group.multicast_ip,
                    parent_id: instance4_id,
                    parent_kind: MulticastGroupMemberParentKind::Instance,
                    sled_id: Some(setup.sled_id.into()),
                    state: MulticastGroupMemberState::Left,
                    source_ips: vec![],
                })
                .returning(MulticastGroupMember::as_returning())
                .get_result_async(&*conn)
                .await
                .expect("Should create member4 record");

        // Run complete delete
        let deleted_count = datastore
            .multicast_group_members_complete_delete(&opctx)
            .await
            .expect("Should run complete delete");

        // Should only delete member1, because member4 still has an OPTE
        // cleanup handle.
        assert_eq!(deleted_count, 1);

        // Verify member1 was deleted by including soft-deleted rows in the
        // lookup.
        let member1_result = datastore
            .multicast_group_member_get_by_id(&opctx, member1.id, true)
            .await
            .expect("Should query for member1");
        assert!(member1_result.is_none(), "member1 should be deleted");

        // Verify member2 still exists
        let member2_result = datastore
            .multicast_group_member_get_by_group_and_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    member2.parent_id,
                )),
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

        assert!(
            datastore
                .multicast_group_member_delete_by_id_preserving_sled_id(
                    &opctx, member2.id,
                )
                .await
                .expect("Should mark member2 for deletion"),
            "member2 should be marked for deletion"
        );
        let member2_pending = datastore
            .multicast_group_member_get_by_id(&opctx, member2.id, true)
            .await
            .expect("Should query pending member2")
            .expect("member2 should remain available for cleanup");
        assert!(member2_pending.time_deleted.is_some());
        assert_eq!(member2_pending.sled_id, Some(setup.sled_id.into()));

        // A replacement member for the same multicast address and instance
        // must be visible to the pending-cleanup guard.
        let replacement4 = datastore
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance4_id,
                )),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should attach a replacement member4")
            .member;
        assert_ne!(replacement4.id, member4.id);
        let replacement4_lookup = datastore
            .multicast_group_member_get_by_multicast_ip_and_parent(
                &opctx,
                group.multicast_ip,
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance4_id,
                )),
            )
            .await
            .expect("Should look up replacement member4")
            .expect("Replacement member4 should be live");
        assert_eq!(replacement4_lookup.id, replacement4.id);

        let pending = datastore
            .multicast_group_members_list_pending_cleanup(
                &opctx,
                &DataPageParams::max_page(),
            )
            .await
            .expect("Should list pending cleanup members");
        assert_eq!(pending.len(), 2);
        assert!(pending.iter().any(|member| member.id == member2.id));
        assert!(pending.iter().any(|member| member.id == member4.id));

        assert!(
            !datastore
                .multicast_group_member_clear_sled_id_if_current(
                    &opctx,
                    member4.id,
                    SledUuid::new_v4().into(),
                )
                .await
                .expect("Should compare pending sled assignment"),
            "A stale sled assignment must not clear the row"
        );
        assert!(
            datastore
                .multicast_group_member_clear_sled_id_if_current(
                    &opctx,
                    member4.id,
                    setup.sled_id.into(),
                )
                .await
                .expect("Should clear pending sled assignment"),
            "The current sled assignment should clear"
        );
        assert!(
            datastore
                .multicast_group_member_clear_sled_id_if_current(
                    &opctx,
                    member2.id,
                    setup.sled_id.into(),
                )
                .await
                .expect("Should clear member2 sled assignment"),
            "member2's current sled assignment should clear"
        );

        let deleted_count = datastore
            .multicast_group_members_complete_delete(&opctx)
            .await
            .expect("Should run complete delete after OPTE cleanup");
        assert_eq!(deleted_count, 2);
        assert!(
            datastore
                .multicast_group_member_get_by_id(&opctx, member4.id, true)
                .await
                .expect("Should query cleaned member")
                .is_none(),
            "member4 should be deleted after its sled assignment is cleared"
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
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(fake_group_id),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
                Some(NO_SOURCE_IPS),
            )
            .await;
        assert!(result.is_err(), "Attach to non-existent group should fail");

        // Try to set state for non-existent member
        let result = datastore
            .multicast_group_member_set_state_for_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(fake_group_id),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
                MulticastGroupMemberState::Joined,
            )
            .await;
        assert!(
            result.is_err(),
            "Set state for non-existent member should fail"
        );

        // Try to get member from non-existent group
        let result = datastore
            .multicast_group_member_get_by_group_and_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(fake_group_id),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
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
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    fake_instance_id,
                )),
                Some(NO_SOURCE_IPS),
            )
            .await;
        assert!(result.is_err(), "Attach non-existent instance should fail");

        // Successfully create a member for further testing
        datastore
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should create member");

        // Invalid state transitions should be handled gracefully
        datastore
            .multicast_group_member_set_state_for_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
                MulticastGroupMemberState::Left,
            )
            .await
            .expect("Should allow transition to 'Left'");

        datastore
            .multicast_group_member_set_state_for_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
                MulticastGroupMemberState::Joined,
            )
            .await
            .expect("Should allow transition back to 'Joined'");

        // Test idempotent operations work correctly
        datastore
            .multicast_group_members_detach_by_parent(
                &opctx,
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
            )
            .await
            .expect("First detach should succeed");

        datastore
            .multicast_group_members_detach_by_parent(
                &opctx,
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
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
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(instance_id),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should add member")
            .member;

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
            .multicast_group_member_get_by_group_and_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(instance_id),
            )
            .await
            .expect("Should find updated member")
            .expect("Member should exist");

        assert_eq!(updated_member.state, MulticastGroupMemberState::Joining);
        assert_eq!(updated_member.sled_id, Some(initial_sled.into()));
        assert!(updated_member.time_modified > member.time_modified);

        // Simulate instance stop by transitioning to "Left" state
        datastore
            .multicast_group_members_detach_by_parent(
                &opctx,
                MemberParentRef::Instance(instance_id),
            )
            .await
            .expect("Should stop instance");

        // Verify member is "Left" with no sled_id
        let stopped_member = datastore
            .multicast_group_member_get_by_group_and_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(instance_id),
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
            .multicast_group_member_get_by_group_and_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(instance_id),
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
            .multicast_group_member_set_state_for_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(instance_id),
                MulticastGroupMemberState::Joined,
            )
            .await
            .expect("Should transition to 'Joined'");

        // Verify member is now "Joined"
        let joined_member = datastore
            .multicast_group_member_get_by_group_and_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(instance_id),
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
            .multicast_group_member_get_by_group_and_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(instance_id),
            )
            .await
            .expect("Should find unchanged member")
            .expect("Member should exist");

        assert_eq!(unchanged_member.state, MulticastGroupMemberState::Joined);
        assert_eq!(unchanged_member.time_modified, before_modification);

        // Test starting instance that has no multicast memberships (should be noop)
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
            "removal-group1",
            "224.10.1.100",
            true,
        )
        .await;

        let group2 = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
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
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group1.id()),
                MemberParentRef::Instance(instance1_id),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should add instance1 to group1")
            .member;

        let member1_2 = datastore
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group2.id()),
                MemberParentRef::Instance(instance1_id),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should add instance1 to group2")
            .member;

        // Add instance2 to only group1
        let member2_1 = datastore
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group1.id()),
                MemberParentRef::Instance(instance2_id),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should add instance2 to group1")
            .member;

        // Verify all members exist and are not marked for removal
        assert!(member1_1.time_deleted.is_none());
        assert!(member1_2.time_deleted.is_none());
        assert!(member2_1.time_deleted.is_none());

        // Assign a sled so removal marking can demonstrate retention.
        datastore
            .multicast_group_member_update_sled_id(
                &opctx,
                instance1_id,
                Some(setup.sled_id.into()),
            )
            .await
            .expect("Should assign a sled before marking for removal");

        // Mark all memberships for instance1 for removal
        datastore
            .multicast_group_members_mark_for_removal_by_parent(
                &opctx,
                MemberParentRef::Instance(instance1_id),
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
        assert_eq!(
            marked_member1_1.sled_id,
            Some(setup.sled_id.into()),
            "Removal marking must retain sled_id for OPTE cleanup"
        );

        let marked_member1_2 = datastore
            .multicast_group_member_get_by_id(&opctx, member1_2.id, true)
            .await
            .expect("Should query member1_2")
            .expect("Member1_2 should exist");
        assert!(marked_member1_2.time_deleted.is_some());
        assert_eq!(
            marked_member1_2.sled_id,
            Some(setup.sled_id.into()),
            "Removal marking must retain sled_id for OPTE cleanup"
        );

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
            .multicast_group_members_mark_for_removal_by_parent(
                &opctx,
                MemberParentRef::Instance(instance1_id),
            )
            .await
            .expect("Should handle duplicate mark for removal");

        // Test marking instance with no memberships (should be noop)
        let non_member_instance = InstanceUuid::new_v4();
        datastore
            .multicast_group_members_mark_for_removal_by_parent(
                &opctx,
                MemberParentRef::Instance(non_member_instance),
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
            "delete-group1",
            "224.10.1.100",
            true,
        )
        .await;

        let group2 = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
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
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group1.id()),
                MemberParentRef::Instance(instance1_id),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should add instance1 to group1")
            .member;

        let member1_2 = datastore
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group1.id()),
                MemberParentRef::Instance(instance2_id),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should add instance2 to group1")
            .member;

        // Add members to group2
        let member2_1 = datastore
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group2.id()),
                MemberParentRef::Instance(instance1_id),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should add instance1 to group2")
            .member;

        let member2_2 = datastore
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group2.id()),
                MemberParentRef::Instance(instance3_id),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should add instance3 to group2")
            .member;

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

        // Reach the state the reconciler sees when it tears a group down:
        // every member soft-deleted, then the group itself marked for removal.
        // The hard delete declines unless the group is still in that state.
        for instance_id in [instance1_id, instance2_id] {
            datastore
                .multicast_group_member_update_sled_id(
                    &opctx,
                    instance_id,
                    Some(setup.sled_id.into()),
                )
                .await
                .expect("Should assign a sled before detaching");

            let detached = datastore
                .multicast_group_member_detach_by_group_and_parent(
                    &opctx,
                    MulticastGroupUuid::from_untyped_uuid(group1.id()),
                    MemberParentRef::Instance(instance_id),
                )
                .await
                .expect("Should detach group1 member");
            assert!(
                detached,
                "Received an untouched row instead of a soft-deleted \
                 group1 member"
            );
        }

        for member_id in [member1_1.id, member1_2.id] {
            let detached_member = datastore
                .multicast_group_member_get_by_id(&opctx, member_id, true)
                .await
                .expect("Should fetch detached group1 member")
                .expect("Detached group1 member should remain");
            assert_eq!(
                detached_member.sled_id,
                Some(setup.sled_id.into()),
                "Explicit detach must retain sled_id for OPTE cleanup"
            );
            assert!(
                datastore
                    .multicast_group_member_clear_sled_id_if_current(
                        &opctx,
                        member_id,
                        setup.sled_id.into(),
                    )
                    .await
                    .expect("Should clear detached member sled assignment"),
                "The current sled assignment should clear after cleanup"
            );
        }

        let marked = datastore
            .multicast_group_mark_removal_if_empty(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group1.id()),
            )
            .await
            .expect("Should mark group1 for removal");
        assert!(
            marked,
            "Received an unmarked group instead of group1 in 'Deleting'"
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

        // Test deleting from group with no members (should be noop)
        datastore
            .multicast_group_members_delete_by_group(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group1.id()),
            )
            .await
            .expect("Should handle deleting from empty group");

        // Test deleting from nonexistent group (should be noop)
        let fake_group_id = Uuid::new_v4();
        datastore
            .multicast_group_members_delete_by_group(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(fake_group_id),
            )
            .await
            .expect("Should handle deleting from nonexistent group");

        // A resurrected group keeps its members. This is the interleaving where
        // a teardown pass reads a group as "Deleting", blocks on its dataplane
        // call, and resumes after a saga has restored the group and attached
        // its own member.
        let group3 = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            "delete-group3",
            "224.10.1.102",
            false,
        )
        .await;
        let group3_id = MulticastGroupUuid::from_untyped_uuid(group3.id());

        let marked = datastore
            .multicast_group_mark_removal_if_empty(&opctx, group3_id)
            .await
            .expect("Should mark empty group3 for removal");
        assert!(
            marked,
            "Received an unmarked group instead of group3 in 'Deleting'"
        );

        let resurrected = datastore
            .multicast_group_resurrect_if_unactivated(&opctx, group3_id)
            .await
            .expect("Should resurrect group3")
            .expect("Received no row instead of a resurrected group3");
        assert_eq!(
            resurrected.state,
            MulticastGroupState::Creating,
            "Received '{:?}' instead of 'Creating' after resurrection",
            resurrected.state
        );

        let member3 = datastore
            .multicast_group_member_attach(
                &opctx,
                group3_id,
                MemberParentRef::Instance(instance1_id),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should add instance1 to resurrected group3")
            .member;

        datastore
            .multicast_group_members_delete_by_group(&opctx, group3_id)
            .await
            .expect("Should handle deleting from a group that is not deleting");

        assert!(
            datastore
                .multicast_group_member_get_by_id(&opctx, member3.id, true)
                .await
                .unwrap()
                .is_some(),
            "Received a swept member instead of a member surviving on a \
             resurrected 'Creating' group"
        );

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
                .multicast_group_member_attach(
                    &opctx1,
                    MulticastGroupUuid::from_untyped_uuid(group_id),
                    MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                        instance_id,
                    )),
                    Some(NO_SOURCE_IPS),
                )
                .await
        });

        let handle2 = tokio::spawn(async move {
            datastore2
                .multicast_group_member_attach(
                    &opctx2,
                    MulticastGroupUuid::from_untyped_uuid(group_id),
                    MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                        instance_id,
                    )),
                    Some(NO_SOURCE_IPS),
                )
                .await
        });

        // Both operations should succeed
        let (result1, result2) = tokio::join!(handle1, handle2);
        let result1 = result1
            .expect("Task 1 should complete")
            .expect("Attach 1 should succeed");
        let result2 = result2
            .expect("Task 2 should complete")
            .expect("Attach 2 should succeed");

        // Both should return the same member row, and exactly one call should
        // observe the insert. The partial unique index admits no second live
        // row for a (group, instance) pair, so exactly-one is the invariant
        // that matters.
        assert_eq!(result1.member, result2.member);
        assert!(
            (result1.outcome == AttachOutcome::Created)
                ^ (result2.outcome == AttachOutcome::Created),
            "Exactly one concurrent attach should insert the member row"
        );

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
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(fake_group_id),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
                Some(NO_SOURCE_IPS),
            )
            .await;

        // Should fail with GroupNotFound (group doesn't exist), which the
        // caller sees as a conflict with a concurrent teardown
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, external::Error::Conflict { .. }));

        // Create a valid active group
        let group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            "test-group",
            "224.10.1.6",
            true, // make_active
        )
        .await;

        // Attach non-existent instance
        let fake_instance_id = Uuid::new_v4();
        let result = datastore
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    fake_instance_id,
                )),
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
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(creating_group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should allow attach to 'Creating' group")
            .member;
        assert_eq!(member.state, MulticastGroupMemberState::Joining);

        // Create a separate group for testing "Deleting" state rejection
        let deleting_group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            "deleting-group",
            "224.10.1.8",
            true, // make_active first
        )
        .await;

        // Transition to "Deleting" state (works because group has no members yet)
        // Note: `multicast_group_mark_removal_if_empty` also sets time_deleted,
        // so the group becomes soft-deleted and cannot be fetched via normal methods.
        let marked = datastore
            .multicast_group_mark_removal_if_empty(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(deleting_group.id()),
            )
            .await
            .expect("Should transition to Deleting");
        assert!(marked, "Group should be marked for deletion");

        // Attaching to soft-deleted ("Deleting") group should fail
        let res = datastore
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(deleting_group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
                Some(NO_SOURCE_IPS),
            )
            .await;
        assert!(res.is_err(), "Should reject attach to 'Deleting' group");
        let err = res.unwrap_err();
        assert!(matches!(err, external::Error::Conflict { .. }));

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
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
                Some(initial_sources.as_slice()),
            )
            .await
            .expect("First attach should succeed")
            .member;
        let time_after_first = member1.time_modified;

        // Second attach (idempotent, should not update sources)
        let member2 = datastore
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
                None, // None preserves existing sources
            )
            .await
            .expect("Second attach should succeed")
            .member;

        assert_eq!(member1.id, member2.id, "Should return same member ID");
        let member_after_second = datastore
            .multicast_group_member_get_by_group_and_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
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

        // Third attach names a different filter (empty), which rewrites
        // source_ips on the live member and advances time_modified
        let member3 = datastore
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Third attach should succeed")
            .member;

        assert_eq!(member1.id, member3.id, "Should return same member ID");
        let member_after_third = datastore
            .multicast_group_member_get_by_group_and_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
            )
            .await
            .expect("Should fetch member after third attach")
            .expect("Member should exist");
        assert!(
            member_after_third.source_ips.is_empty(),
            "Attach naming an empty filter must clear source_ips"
        );
        assert!(
            member_after_third.time_modified > time_after_first,
            "Filter rewrite must advance time_modified"
        );

        // Fourth attach repeats the same filter, so the row is unchanged
        let time_after_third = member_after_third.time_modified;
        let member4 = datastore
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Fourth attach should succeed")
            .member;

        assert_eq!(member1.id, member4.id, "Should return same member ID");
        let member_after_fourth = datastore
            .multicast_group_member_get_by_group_and_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
            )
            .await
            .expect("Should fetch member after fourth attach")
            .expect("Member should exist");
        assert_eq!(
            member_after_fourth.time_modified, time_after_third,
            "Attach repeating the same filter must not update time_modified"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // A supplied source filter is written on every attach path: new inserts,
    // "Left" reactivations, and rewrites on live members. Each path that
    // would grow the per-group source IP union past the cap must fail, while
    // an attach restating the stored filter leaves the union unchanged and
    // must pass.
    #[tokio::test]
    async fn test_member_attach_union_cap_bounds_filter_rewrites() {
        let logctx = dev::test_setup_log(
            "test_member_attach_union_cap_bounds_filter_rewrites",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "union-cap-test-pool",
            "union-cap-test-project",
        )
        .await;

        let group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            "union-cap-group",
            "224.10.1.101",
            true, // make_active
        )
        .await;
        let group_id = MulticastGroupUuid::from_untyped_uuid(group.id());

        // Fill the union to exactly the group cap with members that each
        // stay within the per-member cap, mirroring what the app layer
        // permits: 256 / 32 = 8 members with 32 distinct sources apiece.
        let filler_count = MAX_SOURCE_IPS_PER_GROUP / MAX_SOURCE_IPS_PER_MEMBER;
        let mut first_source: Option<IpAddr> = None;
        for m in 0..filler_count {
            let filler = create_stopped_instance_record(
                &opctx,
                &datastore,
                &setup.authz_project,
                &format!("cap-instance-fill-{m}"),
            )
            .await;
            let sources: Vec<IpAddr> = (0..MAX_SOURCE_IPS_PER_MEMBER)
                .map(|i| IpAddr::V4(Ipv4Addr::new(10, 0, m as u8, i as u8)))
                .collect();
            first_source.get_or_insert(sources[0]);
            datastore
                .multicast_group_member_attach(
                    &opctx,
                    group_id,
                    MemberParentRef::Instance(filler),
                    Some(sources.as_slice()),
                )
                .await
                .expect("Filling the union to the cap should succeed");
        }
        let first_source = first_source.unwrap();

        // Attach a second member with a source already in the union,
        // keeping the union at the cap.
        let instance2 = create_stopped_instance_record(
            &opctx,
            &datastore,
            &setup.authz_project,
            "cap-instance-2",
        )
        .await;
        let existing_source: Vec<IpAddr> = vec![first_source];
        let member2 = datastore
            .multicast_group_member_attach(
                &opctx,
                group_id,
                MemberParentRef::Instance(instance2),
                Some(existing_source.as_slice()),
            )
            .await
            .expect("Attach with an existing source should succeed");

        // A live member's supplied filter is rewritten, so a rewrite that
        // would grow the union past the cap must be rejected.
        let over_cap: Vec<IpAddr> = vec!["10.0.9.9".parse().unwrap()];
        let err = datastore
            .multicast_group_member_attach(
                &opctx,
                group_id,
                MemberParentRef::Instance(instance2),
                Some(over_cap.as_slice()),
            )
            .await
            .expect_err("Over-cap filter rewrite on a live member must fail");
        assert!(
            err.to_string().contains("source IP union cap"),
            "Received unexpected error: {err}"
        );

        // Restating the stored filter leaves the union unchanged and the
        // row untouched.
        let member2_again = datastore
            .multicast_group_member_attach(
                &opctx,
                group_id,
                MemberParentRef::Instance(instance2),
                Some(existing_source.as_slice()),
            )
            .await
            .expect("Restating the stored filter must not trip the cap");
        assert_eq!(
            member2.id, member2_again.id,
            "Received a different member on reattach"
        );
        assert_eq!(
            member2.time_modified, member2_again.time_modified,
            "Restating the stored filter must not update time_modified"
        );
        let stored: Vec<IpAddr> =
            member2_again.source_ips.iter().map(|n| n.ip()).collect();
        assert_eq!(
            stored, existing_source,
            "Restating the stored filter must preserve source_ips"
        );

        // Reactivating a "Left" member does rewrite its sources, so the
        // same list must still trip the cap.
        datastore
            .multicast_group_members_detach_by_parent(
                &opctx,
                MemberParentRef::Instance(instance2),
            )
            .await
            .expect("Detach should succeed");
        let err = datastore
            .multicast_group_member_attach(
                &opctx,
                group_id,
                MemberParentRef::Instance(instance2),
                Some(over_cap.as_slice()),
            )
            .await
            .expect_err("Left reactivation over the cap must fail");
        assert!(
            err.to_string().contains("source IP union cap"),
            "Received unexpected error: {err}"
        );

        // A new insert is likewise still guarded.
        let instance3 = create_stopped_instance_record(
            &opctx,
            &datastore,
            &setup.authz_project,
            "cap-instance-3",
        )
        .await;
        let err = datastore
            .multicast_group_member_attach(
                &opctx,
                group_id,
                MemberParentRef::Instance(instance3),
                Some(over_cap.as_slice()),
            )
            .await
            .expect_err("New insert over the cap must fail");
        assert!(
            err.to_string().contains("source IP union cap"),
            "Received unexpected error: {err}"
        );

        // Reactivation within the cap still succeeds and rewrites sources.
        let member2_react = datastore
            .multicast_group_member_attach(
                &opctx,
                group_id,
                MemberParentRef::Instance(instance2),
                Some(existing_source.as_slice()),
            )
            .await
            .expect("'Left' reactivation within the cap should succeed");
        assert_eq!(
            member2.id, member2_react.id,
            "Reactivation should reuse the existing row"
        );
        assert_eq!(
            member2_react.state,
            MulticastGroupMemberState::Joining,
            "Reactivation should transition back to 'Joining'"
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
                .multicast_group_member_attach(
                    &opctx,
                    group_id,
                    MemberParentRef::Instance(instance),
                    Some(original_sources.as_slice()),
                )
                .await
                .expect("Should attach")
                .member;

            datastore
                .multicast_group_members_detach_by_parent(
                    &opctx,
                    MemberParentRef::Instance(instance),
                )
                .await
                .expect("Should detach");

            let reactivated = datastore
                .multicast_group_member_attach(
                    &opctx,
                    group_id,
                    MemberParentRef::Instance(instance),
                    None, // Preserve existing sources
                )
                .await
                .expect("Reactivation should succeed")
                .member;

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
                .multicast_group_member_attach(
                    &opctx,
                    group_id,
                    MemberParentRef::Instance(instance),
                    Some(original_sources.as_slice()),
                )
                .await
                .expect("Should attach")
                .member;

            datastore
                .multicast_group_members_detach_by_parent(
                    &opctx,
                    MemberParentRef::Instance(instance),
                )
                .await
                .expect("Should detach");

            let replacement_sources: Vec<IpAddr> =
                vec!["10.0.0.3".parse().unwrap(), "10.0.0.4".parse().unwrap()];
            let reactivated = datastore
                .multicast_group_member_attach(
                    &opctx,
                    group_id,
                    MemberParentRef::Instance(instance),
                    Some(replacement_sources.as_slice()),
                )
                .await
                .expect("Reactivation should succeed")
                .member;

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
                .multicast_group_member_attach(
                    &opctx,
                    group_id,
                    MemberParentRef::Instance(instance),
                    Some(original_sources.as_slice()),
                )
                .await
                .expect("Should attach")
                .member;

            datastore
                .multicast_group_members_detach_by_parent(
                    &opctx,
                    MemberParentRef::Instance(instance),
                )
                .await
                .expect("Should detach");

            let reactivated = datastore
                .multicast_group_member_attach(
                    &opctx,
                    group_id,
                    MemberParentRef::Instance(instance),
                    Some(NO_SOURCE_IPS), // Clear sources
                )
                .await
                .expect("Reactivation should succeed")
                .member;

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
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Attach should succeed")
            .member;

        // Transition through states: "Joining" -> "Joined" -> "Left"
        datastore
            .multicast_group_member_set_state_for_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
                MulticastGroupMemberState::Joined,
            )
            .await
            .expect("Transition to Joined should succeed");

        datastore
            .multicast_group_member_set_state_for_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
                MulticastGroupMemberState::Left,
            )
            .await
            .expect("Transition to Left should succeed");

        // The partial unique index with predicate (time_deleted IS NULL)
        // works with ON CONFLICT to reactivate an existing row that is in
        // state 'Left' with time_deleted=NULL. In this case, ON CONFLICT
        // updates the row (Left → Joining) instead of inserting a new one.
        let member2 = datastore
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should allow reattach of Left member")
            .member;

        // Should reactivate the same member (not create a new one)
        assert_eq!(member1.id, member2.id);

        // Verify only one member exists for this (group, instance) pair
        let pagparams = &DataPageParams {
            marker: None,
            limit: std::num::NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        };
        let members = datastore
            .multicast_group_members_list_by_parent(
                &opctx,
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    instance_id,
                )),
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
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(fake_group_id),
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(
                    fake_instance_id,
                )),
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
            .multicast_group_member_attach(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(instance_id),
                Some(NO_SOURCE_IPS),
            )
            .await
            .expect("Should attach stopped instance")
            .member;

        // Verify member created with sled_id = NULL (no active VMM)
        let member = datastore
            .multicast_group_member_get_by_group_and_parent(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                MemberParentRef::Instance(instance_id),
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
            .multicast_group_member_attach(
                &opctx,
                group_id,
                MemberParentRef::Instance(instance1),
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
            .multicast_group_member_attach(
                &opctx,
                group_id,
                MemberParentRef::Instance(instance2),
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
            .multicast_group_member_attach(
                &opctx,
                group_id,
                MemberParentRef::Instance(instance3),
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

        multicast::create_test_setup(
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

    #[tokio::test]
    async fn test_multicast_group_member_attach_source_union_cap() {
        let logctx = dev::test_setup_log(
            "test_multicast_group_member_attach_source_union_cap",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "union-cap-pool",
            "test-project",
        )
        .await;

        let group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            "union-cap-group",
            "224.10.1.50",
            true,
        )
        .await;
        let group_id = MulticastGroupUuid::from_untyped_uuid(group.id());

        // Members that fill the group cap with full per-member blocks,
        // plus one more for the over-cap attempt.
        let full_members = MAX_SOURCE_IPS_PER_GROUP / MAX_SOURCE_IPS_PER_MEMBER;
        assert_eq!(
            full_members * MAX_SOURCE_IPS_PER_MEMBER,
            MAX_SOURCE_IPS_PER_GROUP,
            "Received a cap ratio the test layout no longer matches"
        );

        let mut instances = Vec::new();
        for i in 0..=full_members {
            let record = create_stopped_instance_record(
                &opctx,
                &datastore,
                &setup.authz_project,
                &format!("union-cap-inst{i}"),
            )
            .await;
            instances.push(InstanceUuid::from_untyped_uuid(
                *record.as_untyped_uuid(),
            ));
        }

        // Disjoint source blocks: 10.<block>.<i / 256>.<i % 256>.
        let sources = |block: u8, count: usize| -> Vec<IpAddr> {
            (0..count)
                .map(|i| {
                    IpAddr::from(std::net::Ipv4Addr::new(
                        10,
                        block,
                        (i / 256) as u8,
                        (i % 256) as u8,
                    ))
                })
                .collect()
        };

        // Each attach carries with it a full per-member block, hitting the
        // union on the group cap exactly. The check is strictly greater-than,
        // so every attach succeeds.
        for (i, instance) in instances.iter().take(full_members).enumerate() {
            let block = sources(i as u8, MAX_SOURCE_IPS_PER_MEMBER);
            datastore
                .multicast_group_member_attach(
                    &opctx,
                    group_id,
                    MemberParentRef::Instance(*instance),
                    Some(block.as_slice()),
                )
                .await
                .expect("Should attach member with union at or under the cap");
        }

        // A single new source pushes the union past the cap and trips the
        // CTE sentinel.
        let over = sources(full_members as u8, 1);
        let err = datastore
            .multicast_group_member_attach(
                &opctx,
                group_id,
                MemberParentRef::Instance(instances[full_members]),
                Some(over.as_slice()),
            )
            .await
            .expect_err("Received an attach success for an over-cap union");
        assert!(
            err.to_string().contains("source IP union cap"),
            "Received unexpected error for over-cap union: {err}"
        );

        // A repeat join replaces this member's list, so the union is
        // measured without its stored sources: swapping a full per-member
        // block for 10 new sources stays under the cap.
        let replacement = sources(full_members as u8 + 1, 10);
        let replaced = datastore
            .multicast_group_member_attach(
                &opctx,
                group_id,
                MemberParentRef::Instance(instances[0]),
                Some(replacement.as_slice()),
            )
            .await
            .expect("Should replace member sources during a repeat join");
        assert_eq!(
            replaced.member.source_ips.len(),
            10,
            "Received a merged source list instead of a replacement"
        );

        // A repeat join that omits sources preserves the stored list and
        // skips the cap check entirely.
        datastore
            .multicast_group_member_attach(
                &opctx,
                group_id,
                MemberParentRef::Instance(instances[1]),
                None,
            )
            .await
            .expect("Should preserve sources on a repeat join without a list");

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
