// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! CAS operations for reconciling "Joining" state members.
//!
//! Compare-And-Swap operations for the "Joining" member state. Unlike the atomic
//! CTE in member_attach (handles initial attachment), these simpler CAS operations
//! work for reconciliation since:
//!
//! - Instance state is fetched before calling
//! - Multiple reconcilers on same member is safe (idempotent)
//!
//! "Joining" is the handoff point from control plane to RPW, with the most
//! complex state transitions:
//!
//! - Multiple possible next states (→ "Joined" or → "Left")
//! - Multi-field updates (state + sled_id) must be atomic
//! - Conditional logic based on instance_valid and sled_id changes
//!
//! Other states ("Joined", "Left") have simpler transitions using direct datastore
//! methods (e.g., `multicast_group_member_to_left_if_current`).
//!
//! ## Operations
//!
//! 1. Instance invalid → transition to "Left", clear sled_id
//! 2. sled_id changed → update to new sled (migration)
//! 3. No change → return current state
//!
//! ## Usage
//!
//! Callers maintain member state from batch fetches and use returned `ReconcileAction`
//! to decide what happened. The `current_state` and `current_sled_id` fields may be
//! stale after failed CAS, so callers should use their own state view for decisions.

use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use diesel::result::Error as DieselError;
use uuid::Uuid;

use nexus_db_lookup::DbConnection;
use nexus_db_model::{
    DbTypedUuid, MulticastGroupMember, MulticastGroupMemberState,
};
use nexus_db_schema::schema::multicast_group_member::dsl;
use omicron_common::api::external::Error as ExternalError;
use omicron_uuid_kinds::SledKind;

/// Result of reconciling a "Joining" state member.
#[derive(Debug, Clone, PartialEq)]
pub struct ReconcileJoiningResult {
    /// Action taken during reconciliation
    pub action: ReconcileAction,
    /// Current state after operation (None if member not found)
    pub current_state: Option<MulticastGroupMemberState>,
    /// Current sled_id after operation (None if member not found or has no sled)
    pub current_sled_id: Option<DbTypedUuid<SledKind>>,
}

/// Actions taken when reconciling a "Joining" member.
#[derive(Debug, Clone, PartialEq)]
pub enum ReconcileAction {
    /// Transitioned to "Left" because instance became invalid
    TransitionedToLeft,
    /// Updated sled_id to new value (stayed in "Joining")
    UpdatedSledId {
        old: Option<DbTypedUuid<SledKind>>,
        new: Option<DbTypedUuid<SledKind>>,
    },
    /// No change made (member not in "Joining", or already correct)
    NoChange,
    /// Member not found or not in "Joining" state
    NotFound,
}

/// Errors from reconciling a multicast group member.
#[derive(Debug)]
pub enum ReconcileMemberError {
    /// Database constraint violation (unique index, etc.)
    ConstraintViolation(String),
    /// Other database error
    DatabaseError(DieselError),
}

impl From<ReconcileMemberError> for ExternalError {
    fn from(err: ReconcileMemberError) -> Self {
        match err {
            ReconcileMemberError::ConstraintViolation(msg) => {
                ExternalError::invalid_request(&format!(
                    "Constraint violation: {msg}"
                ))
            }
            ReconcileMemberError::DatabaseError(e) => {
                ExternalError::internal_error(&format!("Database error: {e:?}"))
            }
        }
    }
}

/// Reconcile a "Joining" state member using simple CAS operations.
///
/// Takes instance validity and desired sled_id as inputs (from separate
/// instance/VMM lookups) and performs appropriate CAS operation to update
/// member state.
///
/// # Arguments
///
/// - `conn`: Database connection
/// - `group_id`: Multicast group
/// - `instance_id`: Instance being reconciled
/// - `instance_valid`: Whether instance is in valid state for multicast
/// - `current_sled_id`: Instance's current sled_id (from VMM lookup)
pub async fn reconcile_joining_member(
    conn: &async_bb8_diesel::Connection<DbConnection>,
    group_id: Uuid,
    instance_id: Uuid,
    instance_valid: bool,
    current_sled_id: Option<DbTypedUuid<SledKind>>,
) -> Result<ReconcileJoiningResult, ReconcileMemberError> {
    // First, read the current member state
    let member_opt: Option<MulticastGroupMember> = dsl::multicast_group_member
        .filter(dsl::external_group_id.eq(group_id))
        .filter(dsl::parent_id.eq(instance_id))
        .filter(dsl::time_deleted.is_null())
        .filter(dsl::state.eq(MulticastGroupMemberState::Joining))
        .first_async(conn)
        .await
        .optional()
        .map_err(|e| ReconcileMemberError::DatabaseError(e))?;

    let Some(member) = member_opt else {
        return Ok(ReconcileJoiningResult {
            action: ReconcileAction::NotFound,
            current_state: None,
            current_sled_id: None,
        });
    };

    let prior_sled_id = member.sled_id;

    // Determine what action to take based on instance validity
    if !instance_valid {
        // Instance is invalid - transition to "Left"
        let updated = diesel::update(dsl::multicast_group_member)
            .filter(dsl::id.eq(member.id))
            .filter(dsl::state.eq(MulticastGroupMemberState::Joining))
            .set((
                dsl::state.eq(MulticastGroupMemberState::Left),
                dsl::sled_id.eq(None::<DbTypedUuid<SledKind>>),
                dsl::time_modified.eq(Utc::now()),
            ))
            .execute_async(conn)
            .await
            .map_err(|e| match &e {
                DieselError::DatabaseError(kind, info) => match kind {
                    diesel::result::DatabaseErrorKind::UniqueViolation => {
                        ReconcileMemberError::ConstraintViolation(
                            info.message().to_string(),
                        )
                    }
                    _ => ReconcileMemberError::DatabaseError(e),
                },
                _ => ReconcileMemberError::DatabaseError(e),
            })?;

        if updated > 0 {
            Ok(ReconcileJoiningResult {
                action: ReconcileAction::TransitionedToLeft,
                current_state: Some(MulticastGroupMemberState::Left),
                current_sled_id: None,
            })
        } else {
            // Member changed state between read and update
            Ok(ReconcileJoiningResult {
                action: ReconcileAction::NoChange,
                current_state: Some(member.state),
                current_sled_id: prior_sled_id,
            })
        }
    } else if prior_sled_id != current_sled_id {
        // Instance is valid but sled_id needs updating
        let updated = diesel::update(dsl::multicast_group_member)
            .filter(dsl::id.eq(member.id))
            .filter(dsl::state.eq(MulticastGroupMemberState::Joining))
            .set((
                dsl::sled_id.eq(current_sled_id),
                dsl::time_modified.eq(Utc::now()),
            ))
            .execute_async(conn)
            .await
            .map_err(|e| match &e {
                DieselError::DatabaseError(kind, info) => match kind {
                    diesel::result::DatabaseErrorKind::UniqueViolation => {
                        ReconcileMemberError::ConstraintViolation(
                            info.message().to_string(),
                        )
                    }
                    _ => ReconcileMemberError::DatabaseError(e),
                },
                _ => ReconcileMemberError::DatabaseError(e),
            })?;

        if updated > 0 {
            Ok(ReconcileJoiningResult {
                action: ReconcileAction::UpdatedSledId {
                    old: prior_sled_id,
                    new: current_sled_id,
                },
                current_state: Some(MulticastGroupMemberState::Joining),
                current_sled_id,
            })
        } else {
            // Member changed state between read and update
            Ok(ReconcileJoiningResult {
                action: ReconcileAction::NoChange,
                current_state: Some(member.state),
                current_sled_id: prior_sled_id,
            })
        }
    } else {
        // No change needed
        Ok(ReconcileJoiningResult {
            action: ReconcileAction::NoChange,
            current_state: Some(MulticastGroupMemberState::Joining),
            current_sled_id: prior_sled_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use nexus_types::identity::Resource;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::{
        GenericUuid, InstanceUuid, MulticastGroupUuid, SledUuid,
    };

    use crate::db::pub_test_utils::helpers::{
        SledUpdateBuilder, create_instance_with_vmm,
    };
    use crate::db::pub_test_utils::{TestDatabase, multicast};

    #[tokio::test]
    async fn test_reconcile_joining_instance_invalid() {
        let logctx =
            dev::test_setup_log("test_reconcile_joining_instance_invalid");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "reconcile-invalid-pool",
            "reconcile-invalid-project",
        )
        .await;

        let group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            &setup,
            "test-group",
            "224.10.1.12",
            true,
        )
        .await;

        let (instance, _vmm) = create_instance_with_vmm(
            &opctx,
            &datastore,
            &setup.authz_project,
            "test-instance",
            setup.sled_id,
        )
        .await;
        let instance_id = *instance.as_untyped_uuid();

        // Attach instance to create member in Joining state
        datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Should attach instance");

        // Reconcile with instance_valid=false (instance stopped/deleted)
        let conn = datastore.pool_connection_authorized(&opctx).await.unwrap();
        let result = reconcile_joining_member(
            &conn,
            group.id(),
            instance_id,
            false, // instance_valid=false
            Some(setup.sled_id.into()),
        )
        .await
        .expect("Should reconcile");

        assert_eq!(result.action, ReconcileAction::TransitionedToLeft);
        assert_eq!(result.current_state, Some(MulticastGroupMemberState::Left));
        assert_eq!(result.current_sled_id, None);

        // Verify database state
        let member = datastore
            .multicast_group_member_get_by_group_and_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Should get member")
            .expect("Member should exist");

        assert_eq!(member.state, MulticastGroupMemberState::Left);
        assert_eq!(member.sled_id, None);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_reconcile_joining_sled_id_changed() {
        let logctx =
            dev::test_setup_log("test_reconcile_joining_sled_id_changed");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "reconcile-migrate-pool",
            "reconcile-migrate-project",
        )
        .await;

        // Create second sled for migration
        let sled_id_new = SledUuid::new_v4();
        let sled_update2 =
            SledUpdateBuilder::default().sled_id(sled_id_new).build();
        datastore
            .sled_upsert(sled_update2)
            .await
            .expect("Should insert second sled");

        let group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            &setup,
            "test-group",
            "224.10.1.13",
            true,
        )
        .await;

        let (instance, _vmm) = create_instance_with_vmm(
            &opctx,
            &datastore,
            &setup.authz_project,
            "test-instance",
            setup.sled_id,
        )
        .await;
        let instance_id = *instance.as_untyped_uuid();

        // Attach instance
        datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Should attach instance");

        // Reconcile with new sled_id (simulating migration)
        let conn = datastore.pool_connection_authorized(&opctx).await.unwrap();
        let result = reconcile_joining_member(
            &conn,
            group.id(),
            instance_id,
            true, // instance_valid=true
            Some(sled_id_new.into()),
        )
        .await
        .expect("Should reconcile");

        match result.action {
            ReconcileAction::UpdatedSledId { old, new } => {
                assert_eq!(old, Some(setup.sled_id.into()));
                assert_eq!(new, Some(sled_id_new.into()));
            }
            other => panic!("Expected UpdatedSledId, got {other:?}"),
        }
        assert_eq!(
            result.current_state,
            Some(MulticastGroupMemberState::Joining)
        );
        assert_eq!(result.current_sled_id, Some(sled_id_new.into()));

        // Verify database state
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
        assert_eq!(member.sled_id, Some(sled_id_new.into()));

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_reconcile_joining_no_change_needed() {
        let logctx =
            dev::test_setup_log("test_reconcile_joining_no_change_needed");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "reconcile-nochange-pool",
            "reconcile-nochange-project",
        )
        .await;

        let group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            &setup,
            "test-group",
            "224.10.1.14",
            true,
        )
        .await;

        let (instance, _vmm) = create_instance_with_vmm(
            &opctx,
            &datastore,
            &setup.authz_project,
            "test-instance",
            setup.sled_id,
        )
        .await;
        let instance_id = *instance.as_untyped_uuid();

        // Attach instance
        datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Should attach instance");

        let member_before = datastore
            .multicast_group_member_get_by_group_and_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Should get member")
            .expect("Member should exist");
        let time_modified_before = member_before.time_modified;

        // Reconcile with same sled_id and valid instance
        let conn = datastore.pool_connection_authorized(&opctx).await.unwrap();
        let result = reconcile_joining_member(
            &conn,
            group.id(),
            instance_id,
            true, // instance_valid=true
            Some(setup.sled_id.into()),
        )
        .await
        .expect("Should reconcile");

        assert_eq!(result.action, ReconcileAction::NoChange);
        assert_eq!(
            result.current_state,
            Some(MulticastGroupMemberState::Joining)
        );
        assert_eq!(result.current_sled_id, Some(setup.sled_id.into()));

        // Verify time_modified unchanged (no database update)
        let member_after = datastore
            .multicast_group_member_get_by_group_and_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Should get member")
            .expect("Member should exist");

        assert_eq!(member_after.time_modified, time_modified_before);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_reconcile_joining_member_not_found() {
        let logctx =
            dev::test_setup_log("test_reconcile_joining_member_not_found");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "reconcile-notfound-pool",
            "reconcile-notfound-project",
        )
        .await;

        let group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            &setup,
            "test-group",
            "224.10.1.15",
            true,
        )
        .await;

        // Create instance but don't attach it
        let (instance, _vmm) = create_instance_with_vmm(
            &opctx,
            &datastore,
            &setup.authz_project,
            "test-instance",
            setup.sled_id,
        )
        .await;
        let instance_id = *instance.as_untyped_uuid();

        // Reconcile non-existent member
        let conn = datastore.pool_connection_authorized(&opctx).await.unwrap();
        let result = reconcile_joining_member(
            &conn,
            group.id(),
            instance_id,
            true,
            Some(setup.sled_id.into()),
        )
        .await
        .expect("Should reconcile");

        assert_eq!(result.action, ReconcileAction::NotFound);
        assert_eq!(result.current_state, None);
        assert_eq!(result.current_sled_id, None);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_reconcile_joining_concurrent_state_change() {
        let logctx = dev::test_setup_log(
            "test_reconcile_joining_concurrent_state_change",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "reconcile-concurrent-pool",
            "reconcile-concurrent-project",
        )
        .await;

        let group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            &setup,
            "test-group",
            "224.10.1.16",
            true,
        )
        .await;

        let (instance, _vmm) = create_instance_with_vmm(
            &opctx,
            &datastore,
            &setup.authz_project,
            "test-instance",
            setup.sled_id,
        )
        .await;
        let instance_id = *instance.as_untyped_uuid();

        // Attach instance
        datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Should attach instance");

        // Transition member to Joined state before reconciliation
        datastore
            .multicast_group_member_set_state_if_current(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
                MulticastGroupMemberState::Joining,
                MulticastGroupMemberState::Joined,
            )
            .await
            .expect("Should transition to Joined");

        // Attempt to reconcile - should return NotFound since not in Joining
        let conn = datastore.pool_connection_authorized(&opctx).await.unwrap();
        let result = reconcile_joining_member(
            &conn,
            group.id(),
            instance_id,
            false, // Would transition to Left if still Joining
            Some(setup.sled_id.into()),
        )
        .await
        .expect("Should reconcile");

        // Should return NotFound because member is not in Joining state
        assert_eq!(result.action, ReconcileAction::NotFound);

        // Verify member is still in Joined state
        let member = datastore
            .multicast_group_member_get_by_group_and_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Should get member")
            .expect("Member should exist");

        assert_eq!(member.state, MulticastGroupMemberState::Joined);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_reconcile_joining_migration_scenario() {
        let logctx =
            dev::test_setup_log("test_reconcile_joining_migration_scenario");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "reconcile-migration-pool",
            "reconcile-migration-project",
        )
        .await;

        // Create two sleds for migration scenario
        let sled_id_a = setup.sled_id;

        let sled_id_b = SledUuid::new_v4();
        let sled_update_b =
            SledUpdateBuilder::default().sled_id(sled_id_b).build();
        datastore
            .sled_upsert(sled_update_b)
            .await
            .expect("Should insert sled B");

        let group = multicast::create_test_group_with_state(
            &opctx,
            &datastore,
            &setup,
            "test-group",
            "224.10.1.17",
            true,
        )
        .await;

        let (instance, _vmm) = create_instance_with_vmm(
            &opctx,
            &datastore,
            &setup.authz_project,
            "test-instance",
            sled_id_a,
        )
        .await;
        let instance_id = *instance.as_untyped_uuid();

        // Attach instance (starts on sled_a)
        datastore
            .multicast_group_member_attach_to_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(instance_id),
            )
            .await
            .expect("Should attach instance");

        // Simulate migration: reconcile with sled_id_b
        let conn = datastore.pool_connection_authorized(&opctx).await.unwrap();
        let result = reconcile_joining_member(
            &conn,
            group.id(),
            instance_id,
            true,
            Some(sled_id_b.into()),
        )
        .await
        .expect("Should reconcile migration");

        // Should update sled_id but remain in Joining
        match result.action {
            ReconcileAction::UpdatedSledId { old, new } => {
                assert_eq!(old, Some(sled_id_a.into()));
                assert_eq!(new, Some(sled_id_b.into()));
            }
            other => panic!("Expected UpdatedSledId, got {:?}", other),
        }
        assert_eq!(
            result.current_state,
            Some(MulticastGroupMemberState::Joining)
        );

        // Verify member remains in Joining state with new sled_id
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
        assert_eq!(member.sled_id, Some(sled_id_b.into()));

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
