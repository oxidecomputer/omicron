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
use crate::db::update_and_check::{UpdateAndCheck, UpdateStatus};
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
use strum::IntoEnumIterator;
use thiserror::Error;
use uuid::Uuid;

impl DataStore {
    /// Stores a new sled in the database.
    ///
    /// Produces `None` if the sled is decommissioned.
    pub async fn sled_upsert(
        &self,
        sled_update: SledUpdate,
    ) -> CreateResult<SledUpsertOutput> {
        use db::schema::sled::dsl;
        // required for conditional upsert
        use diesel::query_dsl::methods::FilterDsl;

        // TODO: figure out what to do with time_deleted. We want to replace it
        // with a time_decommissioned, most probably.

        let query = diesel::insert_into(dsl::sled)
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
            .filter(dsl::sled_state.ne(SledState::Decommissioned))
            .returning(Sled::as_returning());

        let sled: Option<Sled> = query
            .get_result_async(&*self.pool_connection_unauthorized().await?)
            .await
            .optional()
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::Sled,
                        &sled_update.id().to_string(),
                    ),
                )
            })?;

        // The only situation in which a sled is not returned is if the
        // `.filter(dsl::sled_state.ne(SledState::Decommissioned))` is not
        // satisfied.
        //
        // If we want to return a sled even if it's decommissioned here, we may
        // have to do something more complex. See
        // https://stackoverflow.com/q/34708509.
        match sled {
            Some(sled) => Ok(SledUpsertOutput::Updated(sled)),
            None => Ok(SledUpsertOutput::Decommissioned),
        }
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

    #[cfg(test)]
    async fn sled_fetch_by_id(
        &self,
        opctx: &OpContext,
        sled_id: Uuid,
    ) -> Result<Sled, external::Error> {
        use db::schema::sled::dsl;
        dsl::sled
            .filter(dsl::id.eq(sled_id))
            .select(Sled::as_select())
            .limit(1)
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
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
        match self
            .sled_set_policy_impl(
                opctx,
                authz_sled,
                SledPolicy::InService { provision_policy: policy },
                ValidateTransition::Yes,
            )
            .await
        {
            Ok(old_policy) => Ok(old_policy
                .provision_policy()
                .expect("only valid policy was in-servie")),
            Err(TransitionError::InvalidTransition(sled)) => {
                Err(external::Error::conflict(format!(
                    "the sled has policy \"{}\" and state \"{:?}\",
                    and its provision policy cannot be changed",
                    sled.policy(),
                    sled.state(),
                )))
            }
            Err(TransitionError::External(e)) => Err(e),
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
        self.sled_set_policy_impl(
            opctx,
            authz_sled,
            SledPolicy::Expunged,
            ValidateTransition::Yes,
        )
        .await
        .map_err(|error| match error {
            TransitionError::InvalidTransition(sled) => {
                external::Error::conflict(format!(
                    "the sled has policy \"{}\" and state \"{:?}\",
                        and it cannot be set to expunged",
                    sled.policy(),
                    sled.state(),
                ))
            }
            TransitionError::External(e) => e,
        })
    }

    async fn sled_set_policy_impl(
        &self,
        opctx: &OpContext,
        authz_sled: &authz::Sled,
        new_policy: SledPolicy,
        check: ValidateTransition,
    ) -> Result<SledPolicy, TransitionError> {
        use db::schema::sled::dsl;

        opctx.authorize(authz::Action::Modify, authz_sled).await?;

        let sled_id = authz_sled.id();
        let query = diesel::update(dsl::sled)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(sled_id));

        let t = Transition::Policy(new_policy);
        let valid_old_policies = t.valid_old_policies();
        let valid_old_states = t.valid_old_states();

        let query = match check {
            ValidateTransition::Yes => query
                .filter(dsl::sled_policy.eq_any(
                    valid_old_policies.into_iter().map(to_db_sled_policy),
                ))
                .filter(
                    dsl::sled_state.eq_any(valid_old_states.iter().copied()),
                )
                .into_boxed(),
            #[cfg(test)]
            ValidateTransition::No => query.into_boxed(),
        };

        let query = query
            .set((
                dsl::sled_policy.eq(to_db_sled_policy(new_policy)),
                dsl::time_modified.eq(Utc::now()),
            ))
            .check_if_exists::<Sled>(sled_id);

        let result = query
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        match (check, result.status) {
            (ValidateTransition::Yes, UpdateStatus::Updated) => {
                Ok(result.found.policy())
            }
            (ValidateTransition::Yes, UpdateStatus::NotUpdatedButExists) => {
                // Two reasons this can happen:
                // 1. An idempotent update: this is treated as a success.
                // 2. Invalid state transition: a failure.
                //
                // To differentiate between the two, check that the new policy
                // is the same as the old policy, and that the old state is
                // valid.
                if result.found.policy() == new_policy
                    && valid_old_states.contains(&result.found.state())
                {
                    Ok(result.found.policy())
                } else {
                    Err(TransitionError::InvalidTransition(result.found))
                }
            }
            #[cfg(test)]
            (ValidateTransition::No, _) => Ok(result.found.policy()),
        }
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
        self.sled_set_state_impl(
            opctx,
            authz_sled,
            SledState::Decommissioned,
            ValidateTransition::Yes,
        )
        .await
        .map_err(|error| match error {
            TransitionError::InvalidTransition(sled) => {
                external::Error::conflict(format!(
                    "the sled has policy \"{}\" and state \"{:?}\",
                    and it cannot be set to decommissioned",
                    sled.policy(),
                    sled.state(),
                ))
            }
            TransitionError::External(e) => e,
        })
    }

    async fn sled_set_state_impl(
        &self,
        opctx: &OpContext,
        authz_sled: &authz::Sled,
        new_state: SledState,
        check: ValidateTransition,
    ) -> Result<SledState, TransitionError> {
        use db::schema::sled::dsl;

        opctx.authorize(authz::Action::Modify, authz_sled).await?;

        let sled_id = authz_sled.id();
        let query = diesel::update(dsl::sled)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(sled_id));

        let t = Transition::State(new_state);
        let valid_old_policies = t.valid_old_policies();
        let valid_old_states = t.valid_old_states();

        let query = match check {
            ValidateTransition::Yes => query
                .filter(dsl::sled_policy.eq_any(
                    valid_old_policies.iter().copied().map(to_db_sled_policy),
                ))
                .filter(dsl::sled_state.eq_any(valid_old_states))
                .into_boxed(),
            #[cfg(test)]
            ValidateTransition::No => query.into_boxed(),
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

        match (check, result.status) {
            (ValidateTransition::Yes, UpdateStatus::Updated) => {
                Ok(result.found.state())
            }
            (ValidateTransition::Yes, UpdateStatus::NotUpdatedButExists) => {
                // Two reasons this can happen:
                // 1. An idempotent update: this is treated as a success.
                // 2. Invalid state transition: a failure.
                //
                // To differentiate between the two, check that the new state
                // is the same as the old state, and the found policy is valid.
                if result.found.state() == new_state
                    && valid_old_policies.contains(&result.found.policy())
                {
                    Ok(result.found.state())
                } else {
                    Err(TransitionError::InvalidTransition(result.found))
                }
            }
            #[cfg(test)]
            (ValidateTransition::No, _) => Ok(result.found.state()),
        }
    }
}

/// The result of [`DataStore::sled_upsert`].
#[derive(Clone, Debug)]
#[must_use]
pub enum SledUpsertOutput {
    /// The sled was updated.
    Updated(Sled),
    /// The sled was not updated because it is decommissioned.
    Decommissioned,
}

impl SledUpsertOutput {
    /// Returns the sled if it was updated, or panics if it was not.
    pub fn unwrap(self) -> Sled {
        match self {
            SledUpsertOutput::Updated(sled) => sled,
            SledUpsertOutput::Decommissioned => {
                panic!("sled was decommissioned, not updated")
            }
        }
    }
}

/// An error that occurred while setting a policy or state.
#[derive(Debug, Error)]
#[must_use]
enum TransitionError {
    /// The state transition check failed.
    ///
    /// The sled is returned.
    #[error("invalid transition: old sled: {0:?}")]
    InvalidTransition(Sled),

    /// Some other kind of error occurred.
    #[error("database error")]
    External(#[from] external::Error),
}

impl TransitionError {
    #[cfg(test)]
    fn ensure_invalid_transition(self) -> anyhow::Result<()> {
        match self {
            TransitionError::InvalidTransition(_) => Ok(()),
            TransitionError::External(e) => Err(anyhow::anyhow!(e)
                .context("expected invalid transition, got other error")),
        }
    }
}

// ---
// State transition validators
// ---

// The functions in this section return the old policies or states that are
// valid for a new policy or state, except idempotent transitions.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Transition {
    Policy(SledPolicy),
    State(SledState),
}

impl Transition {
    /// Returns the list of valid old policies, other than the provided one
    /// (which is always considered valid).
    ///
    /// For a more descriptive listing of valid transitions, see
    /// [`test_sled_transitions`].
    fn valid_old_policies(&self) -> Vec<SledPolicy> {
        use SledPolicy::*;
        use SledProvisionPolicy::*;
        use SledState::*;

        match self {
            Transition::Policy(new_policy) => match new_policy {
                InService { provision_policy: Provisionable } => {
                    vec![InService { provision_policy: NonProvisionable }]
                }
                InService { provision_policy: NonProvisionable } => {
                    vec![InService { provision_policy: Provisionable }]
                }
                Expunged => SledProvisionPolicy::iter()
                    .map(|provision_policy| InService { provision_policy })
                    .collect(),
            },
            Transition::State(state) => {
                match state {
                    Active => {
                        // Any policy is valid for the active state.
                        SledPolicy::iter().collect()
                    }
                    Decommissioned => {
                        SledPolicy::all_decommissionable().to_vec()
                    }
                }
            }
        }
    }

    /// Returns the list of valid old states, other than the provided one
    /// (which is always considered valid).
    ///
    /// For a more descriptive listing of valid transitions, see
    /// [`test_sled_transitions`].
    fn valid_old_states(&self) -> Vec<SledState> {
        use SledState::*;

        match self {
            Transition::Policy(_) => {
                // Policies can only be transitioned in the active state. (In
                // the future, this will include other non-decommissioned
                // states.)
                vec![Active]
            }
            Transition::State(state) => match state {
                Active => vec![],
                Decommissioned => vec![Active],
            },
        }
    }
}

impl IntoEnumIterator for Transition {
    type Iterator = std::vec::IntoIter<Self>;

    fn iter() -> Self::Iterator {
        let v: Vec<_> = SledPolicy::iter()
            .map(Transition::Policy)
            .chain(SledState::iter().map(Transition::State))
            .collect();
        v.into_iter()
    }
}

/// A private enum to see whether state transitions should be checked while
/// setting a new policy and/or state. Intended only for testing around illegal
/// states.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[must_use]
enum ValidateTransition {
    Yes,
    #[cfg(test)]
    No,
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
    use anyhow::{bail, ensure, Context, Result};
    use itertools::Itertools;
    use nexus_test_utils::db::test_setup_database;
    use nexus_types::identity::Asset;
    use omicron_common::api::external;
    use omicron_test_utils::dev;
    use predicates::{prelude::*, BoxPredicate};
    use std::net::{Ipv6Addr, SocketAddrV6};

    fn rack_id() -> Uuid {
        Uuid::parse_str(nexus_test_utils::RACK_UUID).unwrap()
    }

    #[tokio::test]
    async fn upsert_sled_updates_hardware() {
        let logctx = dev::test_setup_log("upsert_sled_updates_hardware");
        let mut db = test_setup_database(&logctx.log).await;
        let (_opctx, datastore) = datastore_test(&logctx, &db).await;

        let mut sled_update = test_new_sled_update();
        let observed_sled =
            datastore.sled_upsert(sled_update.clone()).await.unwrap().unwrap();
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
            .expect("Could not upsert sled during test prep")
            .unwrap();
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

    #[tokio::test]
    async fn upsert_sled_doesnt_update_decommissioned() {
        let logctx =
            dev::test_setup_log("upsert_sled_doesnt_update_decommissioned");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let mut sled_update = test_new_sled_update();
        let observed_sled =
            datastore.sled_upsert(sled_update.clone()).await.unwrap().unwrap();
        assert_eq!(
            observed_sled.usable_hardware_threads,
            sled_update.usable_hardware_threads
        );
        assert_eq!(
            observed_sled.usable_physical_ram,
            sled_update.usable_physical_ram
        );
        assert_eq!(observed_sled.reservoir_size, sled_update.reservoir_size);

        // Set the sled to decommissioned (this is not a legal transition, but
        // we don't care about sled policy in sled_upsert, just the state.)
        set_state(
            &opctx,
            &datastore,
            observed_sled.id(),
            SledState::Decommissioned,
            ValidateTransition::No,
            Expected::Ok(SledState::Active),
        )
        .await
        .unwrap();

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

        // Upserting the sled should produce the `Decommisioned` variant.
        let sled = datastore
            .sled_upsert(sled_update.clone())
            .await
            .expect("updating a decommissioned sled should succeed");
        assert!(
            matches!(sled, SledUpsertOutput::Decommissioned),
            "sled should be decommissioned"
        );

        // The sled should not have been updated.
        let observed_sled_2 = datastore
            .sled_fetch_by_id(&opctx, observed_sled.id())
            .await
            .unwrap();
        assert_eq!(
            observed_sled_2.usable_hardware_threads,
            observed_sled.usable_hardware_threads,
            "usable_hardware_threads should not have changed"
        );
        assert_eq!(
            observed_sled_2.usable_physical_ram,
            observed_sled.usable_physical_ram,
            "usable_physical_ram should not have changed"
        );
        assert_eq!(
            observed_sled_2.reservoir_size, observed_sled.reservoir_size,
            "reservoir_size should not have changed"
        );

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
        let non_provisionable_sled = datastore
            .sled_upsert(test_new_sled_update())
            .await
            .unwrap()
            .unwrap();
        set_policy(
            &opctx,
            &datastore,
            non_provisionable_sled.id(),
            SledPolicy::InService {
                provision_policy: SledProvisionPolicy::NonProvisionable,
            },
            ValidateTransition::Yes,
            Expected::Ok(SledPolicy::provisionable()),
        )
        .await
        .unwrap();

        let expunged_sled = datastore
            .sled_upsert(test_new_sled_update())
            .await
            .unwrap()
            .unwrap();
        set_policy(
            &opctx,
            &datastore,
            expunged_sled.id(),
            SledPolicy::Expunged,
            ValidateTransition::Yes,
            Expected::Ok(SledPolicy::provisionable()),
        )
        .await
        .unwrap();

        let decommissioned_sled = datastore
            .sled_upsert(test_new_sled_update())
            .await
            .unwrap()
            .unwrap();
        // Legally, we must set the policy to expunged before setting the state
        // to decommissioned. (In the future, we'll want to test graceful
        // removal as well.)
        set_policy(
            &opctx,
            &datastore,
            decommissioned_sled.id(),
            SledPolicy::Expunged,
            ValidateTransition::Yes,
            Expected::Ok(SledPolicy::provisionable()),
        )
        .await
        .unwrap();
        set_state(
            &opctx,
            &datastore,
            decommissioned_sled.id(),
            SledState::Decommissioned,
            ValidateTransition::Yes,
            Expected::Ok(SledState::Active),
        )
        .await
        .unwrap();

        // This is _not_ a legal state, BUT we test it out to ensure that if
        // the system somehow enters this state anyway, we don't try and
        // provision resources on it.
        let illegal_decommissioned_sled = datastore
            .sled_upsert(test_new_sled_update())
            .await
            .unwrap()
            .unwrap();
        set_state(
            &opctx,
            &datastore,
            illegal_decommissioned_sled.id(),
            SledState::Decommissioned,
            ValidateTransition::No,
            Expected::Ok(SledState::Active),
        )
        .await
        .unwrap();

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
            datastore.sled_upsert(sled_update.clone()).await.unwrap().unwrap();

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

    #[tokio::test]
    async fn test_sled_transitions() {
        // Test valid and invalid state and policy transitions.
        let logctx = dev::test_setup_log("test_sled_transitions");
        let mut db = test_setup_database(&logctx.log).await;

        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // This test generates all possible sets of transitions. Below, we list
        // the before and after predicates for valid transitions.
        //
        // While it's possible to derive the list of valid transitions from the
        // [`Transition::valid_old_policies`] and
        // [`Transition::valid_old_states`] methods, we list them here
        // explicitly since tests are really about writing things down twice.
        let valid_transitions = [
            (
                // In-service and active sleds can be marked as expunged.
                Before::new(
                    predicate::in_iter(SledPolicy::all_in_service()),
                    predicate::eq(SledState::Active),
                ),
                Transition::Policy(SledPolicy::Expunged),
            ),
            (
                // The provision policy of in-service sleds can be changed, or
                // kept the same (1 of 2).
                Before::new(
                    predicate::in_iter(SledPolicy::all_in_service()),
                    predicate::eq(SledState::Active),
                ),
                Transition::Policy(SledPolicy::InService {
                    provision_policy: SledProvisionPolicy::Provisionable,
                }),
            ),
            (
                // (2 of 2)
                Before::new(
                    predicate::in_iter(SledPolicy::all_in_service()),
                    predicate::eq(SledState::Active),
                ),
                Transition::Policy(SledPolicy::InService {
                    provision_policy: SledProvisionPolicy::NonProvisionable,
                }),
            ),
            (
                // Active sleds can be marked as active, regardless of their
                // policy.
                Before::new(
                    predicate::always(),
                    predicate::eq(SledState::Active),
                ),
                Transition::State(SledState::Active),
            ),
            (
                // Expunged sleds can be marked as decommissioned.
                Before::new(
                    predicate::eq(SledPolicy::Expunged),
                    predicate::eq(SledState::Active),
                ),
                Transition::State(SledState::Decommissioned),
            ),
            (
                // Expunged sleds can always be marked as expunged again, as
                // long as they aren't already decommissioned (we do not allow
                // any transitions once a sled is decommissioned).
                Before::new(
                    predicate::eq(SledPolicy::Expunged),
                    predicate::ne(SledState::Decommissioned),
                ),
                Transition::Policy(SledPolicy::Expunged),
            ),
            (
                // Decommissioned sleds can always be marked as decommissioned
                // again, as long as their policy is decommissionable.
                Before::new(
                    predicate::in_iter(SledPolicy::all_decommissionable()),
                    predicate::eq(SledState::Decommissioned),
                ),
                Transition::State(SledState::Decommissioned),
            ),
        ];

        // Generate all possible transitions.
        let all_transitions = SledPolicy::iter()
            .cartesian_product(SledState::iter())
            .cartesian_product(Transition::iter())
            .enumerate();

        // Set up a sled to test against.
        let sled = datastore
            .sled_upsert(test_new_sled_update())
            .await
            .unwrap()
            .unwrap();
        let sled_id = sled.id();

        for (i, ((policy, state), after)) in all_transitions {
            test_sled_state_transitions_once(
                &opctx,
                &datastore,
                sled_id,
                policy,
                state,
                after,
                &valid_transitions,
            )
            .await
            .with_context(|| {
                format!(
                    "failed on transition {i} (policy: {policy}, \
                        state: {state:?}, after: {after:?})",
                )
            })
            .unwrap();
        }

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    async fn test_sled_state_transitions_once(
        opctx: &OpContext,
        datastore: &DataStore,
        sled_id: Uuid,
        before_policy: SledPolicy,
        before_state: SledState,
        after: Transition,
        valid_transitions: &[(Before, Transition)],
    ) -> Result<()> {
        // Is this a valid transition?
        let is_valid = valid_transitions.iter().any(
            |(Before(valid_policy, valid_state), valid_after)| {
                valid_policy.eval(&before_policy)
                    && valid_state.eval(&before_state)
                    && valid_after == &after
            },
        );

        // Set the sled to the initial policy and state, ignoring state
        // transition errors (this is just to set up the initial state).
        set_policy(
            opctx,
            datastore,
            sled_id,
            before_policy,
            ValidateTransition::No,
            Expected::Ignore,
        )
        .await?;

        set_state(
            opctx,
            datastore,
            sled_id,
            before_state,
            ValidateTransition::No,
            Expected::Ignore,
        )
        .await?;

        // Now perform the transition to the new policy or state.
        match after {
            Transition::Policy(new_policy) => {
                let expected = if is_valid {
                    Expected::Ok(before_policy)
                } else {
                    Expected::Invalid
                };

                set_policy(
                    opctx,
                    datastore,
                    sled_id,
                    new_policy,
                    ValidateTransition::Yes,
                    expected,
                )
                .await?;
            }
            Transition::State(new_state) => {
                let expected = if is_valid {
                    Expected::Ok(before_state)
                } else {
                    Expected::Invalid
                };

                set_state(
                    opctx,
                    datastore,
                    sled_id,
                    new_state,
                    ValidateTransition::Yes,
                    expected,
                )
                .await?;
            }
        }

        Ok(())
    }

    // ---
    // Helper methods
    // ---

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

    /// Initial state for state transitions.
    #[derive(Debug)]
    struct Before(BoxPredicate<SledPolicy>, BoxPredicate<SledState>);

    impl Before {
        fn new<
            P: Predicate<SledPolicy> + Send + Sync + 'static,
            Q: Predicate<SledState> + Send + Sync + 'static,
        >(
            policy: P,
            state: Q,
        ) -> Self {
            Before(policy.boxed(), state.boxed())
        }
    }

    async fn set_policy(
        opctx: &OpContext,
        datastore: &DataStore,
        sled_id: Uuid,
        new_policy: SledPolicy,
        check: ValidateTransition,
        expected_old_policy: Expected<SledPolicy>,
    ) -> Result<()> {
        let (authz_sled, _) = LookupPath::new(&opctx, &datastore)
            .sled_id(sled_id)
            .fetch_for(authz::Action::Modify)
            .await
            .unwrap();

        let res = datastore
            .sled_set_policy_impl(opctx, &authz_sled, new_policy, check)
            .await;
        match expected_old_policy {
            Expected::Ok(expected) => {
                let actual = res.context(
                    "failed transition that was expected to be successful",
                )?;
                ensure!(
                    actual == expected,
                    "actual old policy ({actual}) is not \
                     the same as expected ({expected})"
                );
            }
            Expected::Invalid => match res {
                Ok(old_policy) => {
                    bail!(
                        "expected an invalid state transition error, \
                         but transition was accepted with old policy: \
                         {old_policy}"
                    )
                }
                Err(error) => {
                    error.ensure_invalid_transition()?;
                }
            },
            Expected::Ignore => {
                // The return value is ignored.
            }
        }

        Ok(())
    }

    async fn set_state(
        opctx: &OpContext,
        datastore: &DataStore,
        sled_id: Uuid,
        new_state: SledState,
        check: ValidateTransition,
        expected_old_state: Expected<SledState>,
    ) -> Result<()> {
        let (authz_sled, _) = LookupPath::new(&opctx, &datastore)
            .sled_id(sled_id)
            .fetch_for(authz::Action::Modify)
            .await
            .unwrap();

        let res = datastore
            .sled_set_state_impl(&opctx, &authz_sled, new_state, check)
            .await;
        match expected_old_state {
            Expected::Ok(expected) => {
                let actual = res.context(
                    "failed transition that was expected to be successful",
                )?;
                ensure!(
                    actual == expected,
                    "actual old state ({actual:?}) \
                     is not the same as expected ({expected:?})"
                );
            }
            Expected::Invalid => match res {
                Ok(old_state) => {
                    bail!(
                        "expected an invalid state transition error, \
                        but transition was accepted with old state: \
                        {old_state:?}"
                    )
                }
                Err(error) => {
                    error.ensure_invalid_transition()?;
                }
            },
            Expected::Ignore => {
                // The return value is ignored.
            }
        }

        Ok(())
    }

    /// For a policy/state transition, describes the expected value of the old
    /// state.
    enum Expected<T> {
        /// The transition is expected to successful, with the provided old
        /// value.
        Ok(T),

        /// The transition is expected to be invalid.
        Invalid,

        /// The return value is ignored.
        Ignore,
    }
}
