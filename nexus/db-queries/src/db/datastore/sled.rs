// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Sled`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::model::to_db_sled_policy;
use crate::db::model::Sled;
use crate::db::model::SledResource;
use crate::db::model::SledState;
use crate::db::model::SledUpdate;
use crate::db::pagination::paginated;
use crate::db::update_and_check::UpdateAndCheck;
use crate::transaction_retry::OptionalError;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_types::external_api::views::SledPolicy;
use nexus_types::external_api::views::SledProvisionPolicy;
use omicron_common::api::external;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::ResourceType;
use uuid::Uuid;

impl DataStore {
    /// Stores a new sled in the database.
    pub async fn sled_upsert(
        &self,
        sled_update: SledUpdate,
    ) -> CreateResult<Sled> {
        use db::schema::sled::dsl;
        diesel::insert_into(dsl::sled)
            .values(sled_update.clone().into_insertable())
            .on_conflict(dsl::id)
            .do_update()
            .set((
                dsl::time_modified.eq(Utc::now()),
                dsl::ip.eq(sled_update.ip),
                dsl::port.eq(sled_update.port),
                dsl::rack_id.eq(sled_update.rack_id),
                dsl::is_scrimlet.eq(sled_update.is_scrimlet()),
                dsl::usable_hardware_threads
                    .eq(sled_update.usable_hardware_threads),
                dsl::usable_physical_ram.eq(sled_update.usable_physical_ram),
                dsl::reservoir_size.eq(sled_update.reservoir_size),
            ))
            .returning(Sled::as_returning())
            .get_result_async(&*self.pool_connection_unauthorized().await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::Sled,
                        &sled_update.id().to_string(),
                    ),
                )
            })
    }

    pub async fn sled_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Sled> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        use db::schema::sled::dsl;
        paginated(dsl::sled, dsl::id, pagparams)
            .select(Sled::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn sled_reservation_create(
        &self,
        opctx: &OpContext,
        resource_id: Uuid,
        resource_kind: db::model::SledResourceKind,
        resources: db::model::Resources,
        constraints: db::model::SledReservationConstraints,
    ) -> CreateResult<db::model::SledResource> {
        #[derive(Debug)]
        enum SledReservationError {
            NotFound,
        }

        let err = OptionalError::new();

        let conn = self.pool_connection_authorized(opctx).await?;

        self.transaction_retry_wrapper("sled_reservation_create")
            .transaction(&conn, |conn| {
                // Clone variables into retryable function
                let err = err.clone();
                let constraints = constraints.clone();
                let resources = resources.clone();

                async move {
                    use db::schema::sled_resource::dsl as resource_dsl;
                    // Check if resource ID already exists - if so, return it.
                    let old_resource = resource_dsl::sled_resource
                        .filter(resource_dsl::id.eq(resource_id))
                        .select(SledResource::as_select())
                        .limit(1)
                        .load_async(&conn)
                        .await?;

                    if !old_resource.is_empty() {
                        return Ok(old_resource[0].clone());
                    }

                    // If it doesn't already exist, find a sled with enough space
                    // for the resources we're requesting.
                    use db::schema::sled::dsl as sled_dsl;
                    // This answers the boolean question:
                    // "Does the SUM of all hardware thread usage, plus the one we're trying
                    // to allocate, consume less threads than exists on the sled?"
                    let sled_has_space_for_threads =
                        (diesel::dsl::sql::<diesel::sql_types::BigInt>(
                            &format!(
                                "COALESCE(SUM(CAST({} as INT8)), 0)",
                                resource_dsl::hardware_threads::NAME
                            ),
                        ) + resources.hardware_threads)
                            .le(sled_dsl::usable_hardware_threads);

                    // This answers the boolean question:
                    // "Does the SUM of all RAM usage, plus the one we're trying
                    // to allocate, consume less RAM than exists on the sled?"
                    let sled_has_space_for_rss =
                        (diesel::dsl::sql::<diesel::sql_types::BigInt>(
                            &format!(
                                "COALESCE(SUM(CAST({} as INT8)), 0)",
                                resource_dsl::rss_ram::NAME
                            ),
                        ) + resources.rss_ram)
                            .le(sled_dsl::usable_physical_ram);

                    // Determine whether adding this service's reservoir allocation
                    // to what's allocated on the sled would avoid going over quota.
                    let sled_has_space_in_reservoir =
                        (diesel::dsl::sql::<diesel::sql_types::BigInt>(
                            &format!(
                                "COALESCE(SUM(CAST({} as INT8)), 0)",
                                resource_dsl::reservoir_ram::NAME
                            ),
                        ) + resources.reservoir_ram)
                            .le(sled_dsl::reservoir_size);

                    // Generate a query describing all of the sleds that have space
                    // for this reservation.
                    let mut sled_targets =
                        sled_dsl::sled
                            .left_join(
                                resource_dsl::sled_resource
                                    .on(resource_dsl::sled_id.eq(sled_dsl::id)),
                            )
                            .group_by(sled_dsl::id)
                            .having(
                                sled_has_space_for_threads
                                    .and(sled_has_space_for_rss)
                                    .and(sled_has_space_in_reservoir),
                            )
                            .filter(sled_dsl::time_deleted.is_null())
                            // Ensure that the sled is in-service and active.
                            .filter(sled_dsl::sled_policy.eq(
                                to_db_sled_policy(SledPolicy::provisionable()),
                            ))
                            .filter(sled_dsl::sled_state.eq(SledState::Active))
                            .select(sled_dsl::id)
                            .into_boxed();

                    // Further constrain the sled IDs according to any caller-
                    // supplied constraints.
                    if let Some(must_select_from) =
                        constraints.must_select_from()
                    {
                        sled_targets = sled_targets.filter(
                            sled_dsl::id.eq_any(must_select_from.to_vec()),
                        );
                    }

                    sql_function!(fn random() -> diesel::sql_types::Float);
                    let sled_targets = sled_targets
                        .order(random())
                        .limit(1)
                        .get_results_async::<Uuid>(&conn)
                        .await?;

                    if sled_targets.is_empty() {
                        return Err(err.bail(SledReservationError::NotFound));
                    }

                    // Create a SledResource record, associate it with the target
                    // sled.
                    let resource = SledResource::new(
                        resource_id,
                        sled_targets[0],
                        resource_kind,
                        resources,
                    );

                    diesel::insert_into(resource_dsl::sled_resource)
                        .values(resource)
                        .returning(SledResource::as_returning())
                        .get_result_async(&conn)
                        .await
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    match err {
                        SledReservationError::NotFound => {
                            return external::Error::insufficient_capacity(
                                "No sleds can fit the requested instance",
                                "No sled targets found that had enough \
                                 capacity to fit the requested instance.",
                            );
                        }
                    }
                }
                public_error_from_diesel(e, ErrorHandler::Server)
            })
    }

    pub async fn sled_reservation_delete(
        &self,
        opctx: &OpContext,
        resource_id: Uuid,
    ) -> DeleteResult {
        use db::schema::sled_resource::dsl as resource_dsl;
        diesel::delete(resource_dsl::sled_resource)
            .filter(resource_dsl::id.eq(resource_id))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        Ok(())
    }

    /// Sets the provision policy for this sled.
    ///
    /// Errors if the sled is not in service.
    ///
    /// Returns the previous policy.
    pub async fn sled_set_provision_policy(
        &self,
        opctx: &OpContext,
        authz_sled: &authz::Sled,
        policy: SledProvisionPolicy,
    ) -> Result<SledProvisionPolicy, external::Error> {
        use db::schema::sled::dsl;

        opctx.authorize(authz::Action::Modify, authz_sled).await?;

        let new_policy = SledPolicy::InService { provision_policy: policy };
        // The sled policy can only be changed if the current policy is one of
        // the `in_service` ones. There are only two in_service policies
        // possible at the moment.
        let valid_old_policy =
            SledPolicy::InService { provision_policy: policy.invert() };

        let sled_id = authz_sled.id();
        let query = diesel::update(dsl::sled)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(sled_id))
            .filter(dsl::sled_policy.eq(to_db_sled_policy(valid_old_policy)))
            // Ensure that the sled is active.
            .filter(dsl::sled_state.eq(SledState::Active))
            .set((
                dsl::sled_policy.eq(to_db_sled_policy(new_policy)),
                dsl::time_modified.eq(Utc::now()),
            ))
            .check_if_exists::<Sled>(sled_id);
        let result = query
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        // There are three possibilities here:
        // 1. The sled policy was the same as the valid old one, and was
        //    updated.
        // 2. The sled policy was the same as the updated one. The policy was
        //    not updated, but this is fine because this method is idempotent.
        // 3. The sled policy was something else. In that case, we should
        //    return an error.
        match result.found.policy() {
            SledPolicy::InService { provision_policy } => Ok(provision_policy),
            other @ SledPolicy::Expunged => {
                Err(external::Error::conflict(format!(
                    "the sled is {other}, and its \
                     provision state cannot be changed"
                )))
            }
        }
    }

    /// Marks a sled as expunged, as directed by the operator.
    ///
    /// This is an irreversible process! It should only be called after
    /// sufficient warning to the operator.
    ///
    /// This is idempotent, and it returns the old policy of the sled.
    ///
    /// XXX: This, or the code that's calling it, needs to kick off the
    /// blueprint planner.
    pub async fn sled_set_policy_to_expunged(
        &self,
        opctx: &OpContext,
        authz_sled: &authz::Sled,
    ) -> Result<SledPolicy, external::Error> {
        use db::schema::sled::dsl;

        opctx.authorize(authz::Action::Modify, authz_sled).await?;

        let new_policy = SledPolicy::Expunged;
        // The valid policies to transition from are the two in-service ones.
        let valid_old_policies = [
            SledPolicy::InService {
                provision_policy: SledProvisionPolicy::Provisionable,
            },
            SledPolicy::InService {
                provision_policy: SledProvisionPolicy::NonProvisionable,
            },
        ]
        .into_iter()
        .map(to_db_sled_policy);

        let sled_id = authz_sled.id();
        let query = diesel::update(dsl::sled)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(sled_id))
            .filter(dsl::sled_policy.eq_any(valid_old_policies))
            // Ensure that the sled is active.
            .filter(dsl::sled_state.eq(SledState::Active))
            .set((
                dsl::sled_policy.eq(to_db_sled_policy(new_policy)),
                dsl::time_modified.eq(Utc::now()),
            ))
            .check_if_exists::<Sled>(sled_id);

        let result = query
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        // There are two possibilities here:
        // 1. The sled policy was one of the valid old ones (in-service), and
        //    was updated.
        // 2. The sled policy was already expunged. The policy was not updated,
        //    but this is fine because this method is idempotent.
        //
        // In the future, when we add graceful sled removal, we'll need to
        // ensure that graceful removal <-> expunged transitions are
        // disallowed.
        Ok(result.found.policy())
    }

    /// Marks the state of the sled as decommissioned, as believed by Nexus.
    ///
    /// This is an irreversible process! It should only be called after all
    /// resources previously on the sled have been migrated over.
    ///
    /// This is idempotent, and it returns the old state of the sled.
    ///
    /// # Errors
    ///
    /// This method returns an error if the sled policy is not a state that is
    /// valid to decommission from (i.e. if, for the current sled policy,
    /// [`SledPolicy::is_decommissionable`] returns `false`).
    pub async fn sled_set_state_to_decommissioned(
        &self,
        opctx: &OpContext,
        authz_sled: &authz::Sled,
    ) -> Result<SledState, external::Error> {
        self.sled_set_state_to_decommissioned_inner(opctx, authz_sled, true)
            .await
    }

    async fn sled_set_state_to_decommissioned_inner(
        &self,
        opctx: &OpContext,
        authz_sled: &authz::Sled,
        // check_decommissionable = false means illegal state transitions are
        // possible -- this must only be called from test-only code.
        check_decommissionable: bool,
    ) -> Result<SledState, external::Error> {
        use db::schema::sled::dsl;

        #[cfg(not(test))]
        {
            if !check_decommissionable {
                panic!("check_decommissionable = false is only allowed in test code")
            }
        }

        opctx.authorize(authz::Action::Modify, authz_sled).await?;

        let new_state = SledState::Decommissioned;
        let sled_id = authz_sled.id();
        let query = diesel::update(dsl::sled)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(sled_id))
            .filter(dsl::sled_state.eq(SledState::Active));

        let query = if check_decommissionable {
            query
                .filter(
                    dsl::sled_policy.eq_any(
                        SledPolicy::all_decommissionable()
                            .into_iter()
                            .map(|p| to_db_sled_policy(*p)),
                    ),
                )
                .into_boxed()
        } else {
            query.into_boxed()
        };

        let query = query
            .set((
                dsl::sled_state.eq(new_state),
                dsl::time_modified.eq(Utc::now()),
            ))
            .check_if_exists::<Sled>(sled_id);

        let result = query
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        // There are three possibilities here:
        // 1. The sled state was active, and was updated.
        // 2. The sled state was already decommissioned. The state was not
        //    updated, but this is fine because this method is idempotent.
        // 3. The sled policy was not expunged. If so, return an error.
        match result.found.policy() {
            SledPolicy::Expunged => Ok(result.found.state()),
            other @ SledPolicy::InService { .. } => {
                if check_decommissionable {
                    Err(external::Error::conflict(format!(
                        "the sled is {other}, and its state cannot be \
                         decommissioned"
                    )))
                } else {
                    // This is test-only code, so don't check if it is
                    // in-service.
                    Ok(result.found.state())
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::datastore::datastore_test;
    use crate::db::datastore::test::{
        sled_baseboard_for_test, sled_system_hardware_for_test,
    };
    use crate::db::lookup::LookupPath;
    use crate::db::model::ByteCount;
    use crate::db::model::SqlU32;
    use nexus_test_utils::db::test_setup_database;
    use nexus_types::identity::Asset;
    use omicron_common::api::external;
    use omicron_test_utils::dev;
    use std::net::{Ipv6Addr, SocketAddrV6};
    use std::num::NonZeroU32;

    fn rack_id() -> Uuid {
        Uuid::parse_str(nexus_test_utils::RACK_UUID).unwrap()
    }

    #[tokio::test]
    async fn upsert_sled_updates_hardware() {
        let logctx = dev::test_setup_log("upsert_sled");
        let mut db = test_setup_database(&logctx.log).await;
        let (_opctx, datastore) = datastore_test(&logctx, &db).await;

        let mut sled_update = test_new_sled_update();
        let observed_sled =
            datastore.sled_upsert(sled_update.clone()).await.unwrap();
        assert_eq!(
            observed_sled.usable_hardware_threads,
            sled_update.usable_hardware_threads
        );
        assert_eq!(
            observed_sled.usable_physical_ram,
            sled_update.usable_physical_ram
        );
        assert_eq!(observed_sled.reservoir_size, sled_update.reservoir_size);

        // Modify the sizes of hardware
        sled_update.usable_hardware_threads =
            SqlU32::new(sled_update.usable_hardware_threads.0 + 1);
        const MIB: u64 = 1024 * 1024;
        sled_update.usable_physical_ram = ByteCount::from(
            external::ByteCount::try_from(
                sled_update.usable_physical_ram.0.to_bytes() + MIB,
            )
            .unwrap(),
        );
        sled_update.reservoir_size = ByteCount::from(
            external::ByteCount::try_from(
                sled_update.reservoir_size.0.to_bytes() + MIB,
            )
            .unwrap(),
        );

        // Test that upserting the sled propagates those changes to the DB.
        let observed_sled = datastore
            .sled_upsert(sled_update.clone())
            .await
            .expect("Could not upsert sled during test prep");
        assert_eq!(
            observed_sled.usable_hardware_threads,
            sled_update.usable_hardware_threads
        );
        assert_eq!(
            observed_sled.usable_physical_ram,
            sled_update.usable_physical_ram
        );
        assert_eq!(observed_sled.reservoir_size, sled_update.reservoir_size);

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    /// Test that new reservations aren't created on non-provisionable sleds.
    #[tokio::test]
    async fn sled_reservation_create_non_provisionable() {
        let logctx =
            dev::test_setup_log("sled_reservation_create_non_provisionable");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // Define some sleds that resources cannot be provisioned on.
        let non_provisionable_sled =
            datastore.sled_upsert(test_new_sled_update()).await.unwrap();
        set_provision_policy(
            &opctx,
            &datastore,
            non_provisionable_sled.id(),
            SledProvisionPolicy::NonProvisionable,
            Ok(SledProvisionPolicy::Provisionable),
        )
        .await;

        let expunged_sled =
            datastore.sled_upsert(test_new_sled_update()).await.unwrap();
        set_policy_to_expunged(
            &opctx,
            &datastore,
            expunged_sled.id(),
            Ok(SledPolicy::provisionable()),
        )
        .await;

        let decommissioned_sled =
            datastore.sled_upsert(test_new_sled_update()).await.unwrap();
        // Legally, we must set the policy to expunged before setting the state
        // to decommissioned.
        set_policy_to_expunged(
            &opctx,
            &datastore,
            decommissioned_sled.id(),
            Ok(SledPolicy::provisionable()),
        )
        .await;
        set_state_to_decommissioned(
            &opctx,
            &datastore,
            decommissioned_sled.id(),
            true,
            Ok(SledState::Active),
        )
        .await;

        // This is _not_ a legal state, BUT we test it out to ensure that if
        // the system somehow enters this state anyway, we don't try and
        // provision resources on it.
        let illegal_decommissioned_sled =
            datastore.sled_upsert(test_new_sled_update()).await.unwrap();
        set_state_to_decommissioned(
            &opctx,
            &datastore,
            illegal_decommissioned_sled.id(),
            false,
            Ok(SledState::Active),
        )
        .await;

        // This should be an error since there are no provisionable sleds.
        let resources = db::model::Resources::new(
            1,
            // Just require the bare non-zero amount of RAM.
            ByteCount::try_from(1024).unwrap(),
            ByteCount::try_from(1024).unwrap(),
        );
        let constraints = db::model::SledReservationConstraints::none();
        let error = datastore
            .sled_reservation_create(
                &opctx,
                Uuid::new_v4(),
                db::model::SledResourceKind::Instance,
                resources.clone(),
                constraints,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, external::Error::InsufficientCapacity { .. }));

        // Now add a provisionable sled and try again.
        let sled_update = test_new_sled_update();
        let provisionable_sled =
            datastore.sled_upsert(sled_update.clone()).await.unwrap();

        let sleds = datastore
            .sled_list(&opctx, &first_page(NonZeroU32::new(10).unwrap()))
            .await
            .unwrap();
        println!("sleds: {:?}", sleds);

        // Try a few times to ensure that resources never get allocated to the
        // non-provisionable sled.
        for _ in 0..10 {
            let constraints = db::model::SledReservationConstraints::none();
            let resource = datastore
                .sled_reservation_create(
                    &opctx,
                    Uuid::new_v4(),
                    db::model::SledResourceKind::Instance,
                    resources.clone(),
                    constraints,
                )
                .await
                .unwrap();
            assert_eq!(
                resource.sled_id,
                provisionable_sled.id(),
                "resource is always allocated to the provisionable sled"
            );

            datastore
                .sled_reservation_delete(&opctx, resource.id)
                .await
                .unwrap();
        }

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    fn test_new_sled_update() -> SledUpdate {
        let sled_id = Uuid::new_v4();
        let addr = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0);
        SledUpdate::new(
            sled_id,
            addr,
            sled_baseboard_for_test(),
            sled_system_hardware_for_test(),
            rack_id(),
        )
    }

    async fn set_provision_policy(
        opctx: &OpContext,
        datastore: &DataStore,
        sled_id: Uuid,
        new_policy: SledProvisionPolicy,
        expected_old_policy: Result<SledProvisionPolicy, SetStateError>,
    ) {
        let (authz_sled, _) = LookupPath::new(&opctx, &datastore)
            .sled_id(sled_id)
            .fetch_for(authz::Action::Modify)
            .await
            .unwrap();

        let res = datastore
            .sled_set_provision_policy(&opctx, &authz_sled, new_policy)
            .await;
        match expected_old_policy {
            Ok(expected_old_policy) => {
                assert_eq!(
                    res,
                    Ok(expected_old_policy),
                    "actual old policy is not the same as expected"
                );
            }
            Err(SetStateError::InvalidTransition) => {
                let error = res
                    .expect_err("expected an invalid state transition error");
                // Invalid transitions are represented as conflicts.
                assert!(matches!(error, external::Error::Conflict { .. }), "");
            }
        }
    }

    async fn set_policy_to_expunged(
        opctx: &OpContext,
        datastore: &DataStore,
        sled_id: Uuid,
        expected_old_policy: Result<SledPolicy, SetStateError>,
    ) {
        let (authz_sled, _) = LookupPath::new(&opctx, &datastore)
            .sled_id(sled_id)
            .fetch_for(authz::Action::Modify)
            .await
            .unwrap();

        let res =
            datastore.sled_set_policy_to_expunged(&opctx, &authz_sled).await;
        match expected_old_policy {
            Ok(expected_old_policy) => {
                assert_eq!(
                    res,
                    Ok(expected_old_policy),
                    "actual old policy is not the same as expected"
                );
            }
            Err(SetStateError::InvalidTransition) => {
                let error = res
                    .expect_err("expected an invalid state transition error");
                // Invalid transitions are represented as conflicts.
                assert!(matches!(error, external::Error::Conflict { .. }), "");
            }
        }
    }

    async fn set_state_to_decommissioned(
        opctx: &OpContext,
        datastore: &DataStore,
        sled_id: Uuid,
        check_decommissionable: bool,
        expected_old_state: Result<SledState, SetStateError>,
    ) {
        let (authz_sled, _) = LookupPath::new(&opctx, &datastore)
            .sled_id(sled_id)
            .fetch_for(authz::Action::Modify)
            .await
            .unwrap();

        let res = datastore
            .sled_set_state_to_decommissioned_inner(
                &opctx,
                &authz_sled,
                check_decommissionable,
            )
            .await;
        match expected_old_state {
            Ok(expected_old_state) => {
                assert_eq!(
                    res,
                    Ok(expected_old_state),
                    "actual old policy is not the same as expected"
                );
            }
            Err(SetStateError::InvalidTransition) => {
                let error = res
                    .expect_err("expected an invalid state transition error");
                // Invalid transitions are represented as conflicts.
                assert!(matches!(error, external::Error::Conflict { .. }), "");
            }
        }
    }

    /// Returns pagination parameters to fetch the first page of results for a
    /// paginated endpoint
    fn first_page<'a, T>(limit: NonZeroU32) -> DataPageParams<'a, T> {
        DataPageParams {
            marker: None,
            direction: dropshot::PaginationOrder::Ascending,
            limit,
        }
    }

    #[derive(Copy, Clone, Debug)]
    enum SetStateError {
        InvalidTransition,
    }
}
