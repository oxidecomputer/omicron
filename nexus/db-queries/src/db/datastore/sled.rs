// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Sled`]s.

use super::DataStore;
use super::SQL_BATCH_SIZE;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::datastore::ValidateTransition;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::model::to_db_sled_policy;
use crate::db::model::Sled;
use crate::db::model::SledResource;
use crate::db::model::SledState;
use crate::db::model::SledUpdate;
use crate::db::pagination::paginated;
use crate::db::pagination::Paginator;
use crate::db::pool::DbConnection;
use crate::db::update_and_check::{UpdateAndCheck, UpdateStatus};
use crate::db::TransactionError;
use crate::transaction_retry::OptionalError;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_model::ApplySledFilterExt;
use nexus_types::deployment::SledFilter;
use nexus_types::external_api::views::SledPolicy;
use nexus_types::external_api::views::SledProvisionPolicy;
use nexus_types::identity::Asset;
use omicron_common::api::external;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::ResourceType;
use omicron_common::bail_unless;
use std::fmt;
use strum::IntoEnumIterator;
use thiserror::Error;
use uuid::Uuid;

impl DataStore {
    /// Stores a new sled in the database.
    ///
    /// Returns the sled, and whether or not it was updated on success.
    ///
    /// Returns an error if `sled_agent_gen` is stale, or the sled is
    /// decommissioned.
    pub async fn sled_upsert(
        &self,
        sled_update: SledUpdate,
    ) -> CreateResult<(Sled, bool)> {
        use db::schema::sled::dsl;
        // required for conditional upsert
        use diesel::query_dsl::methods::FilterDsl;

        let insertable_sled = sled_update.clone().into_insertable();
        let now = insertable_sled.time_modified();

        let sled = diesel::insert_into(dsl::sled)
            .values(insertable_sled)
            .on_conflict(dsl::id)
            .do_update()
            .set((
                dsl::time_modified.eq(now),
                dsl::ip.eq(sled_update.ip),
                dsl::port.eq(sled_update.port),
                dsl::rack_id.eq(sled_update.rack_id),
                dsl::is_scrimlet.eq(sled_update.is_scrimlet()),
                dsl::usable_hardware_threads
                    .eq(sled_update.usable_hardware_threads),
                dsl::usable_physical_ram.eq(sled_update.usable_physical_ram),
                dsl::reservoir_size.eq(sled_update.reservoir_size),
                dsl::sled_agent_gen.eq(sled_update.sled_agent_gen),
            ))
            .filter(dsl::sled_agent_gen.lt(sled_update.sled_agent_gen))
            .filter(dsl::sled_state.ne(SledState::Decommissioned))
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
            })?;

        // We compare only seconds since the epoch, because writing to and
        // reading from the database causes us to lose precision.
        let was_modified = now.timestamp() == sled.time_modified().timestamp();
        Ok((sled, was_modified))
    }

    /// Confirms that a sled exists and is in-service.
    ///
    /// This function may be called from a transaction context.
    pub async fn check_sled_in_service_on_connection(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        sled_id: Uuid,
    ) -> Result<(), TransactionError<Error>> {
        use db::schema::sled::dsl;
        let sled_exists_and_in_service = diesel::select(diesel::dsl::exists(
            dsl::sled
                .filter(dsl::time_deleted.is_null())
                .filter(dsl::id.eq(sled_id))
                .sled_filter(SledFilter::InService),
        ))
        .get_result_async::<bool>(conn)
        .await?;

        bail_unless!(
            sled_exists_and_in_service,
            "Sled {} is not in service",
            sled_id,
        );

        Ok(())
    }

    pub async fn sled_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
        sled_filter: SledFilter,
    ) -> ListResultVec<Sled> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        use db::schema::sled::dsl;
        paginated(dsl::sled, dsl::id, pagparams)
            .select(Sled::as_select())
            .sled_filter(sled_filter)
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// List all sleds, making as many queries as needed to get them all
    ///
    /// This should generally not be used in API handlers or other
    /// latency-sensitive contexts, but it can make sense in saga actions or
    /// background tasks.
    pub async fn sled_list_all_batched(
        &self,
        opctx: &OpContext,
        sled_filter: SledFilter,
    ) -> ListResultVec<Sled> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        opctx.check_complex_operations_allowed()?;

        let mut all_sleds = Vec::new();
        let mut paginator = Paginator::new(SQL_BATCH_SIZE);
        while let Some(p) = paginator.next() {
            let batch = self
                .sled_list(opctx, &p.current_pagparams(), sled_filter)
                .await?;
            paginator =
                p.found_batch(&batch, &|s: &nexus_db_model::Sled| s.id());
            all_sleds.extend(batch);
        }
        Ok(all_sleds)
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
                    let mut sled_targets = sled_dsl::sled
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
                        // Ensure that reservations can be created on the sled.
                        .sled_filter(SledFilter::ReservationCreate)
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

                    // We only actually care about one target here, so this
                    // query should have a `.limit(1)` attached. We fetch all
                    // sled targets to leave additional debugging information in
                    // the logs, for now.
                    let sled_targets = sled_targets
                        .order(random())
                        .get_results_async::<Uuid>(&conn)
                        .await?;
                    info!(
                        opctx.log,
                        "found {} available sled targets", sled_targets.len();
                        "sled_ids" => ?sled_targets,
                    );

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
                .expect("only valid policy was in-service")),
            Err(error) => Err(error.into_external_error()),
        }
    }

    /// Marks a sled as expunged, as directed by the operator.
    ///
    /// This is an irreversible process! It should only be called after
    /// sufficient warning to the operator.
    ///
    /// This is idempotent, and it returns the old policy of the sled.
    ///
    /// Calling this function also implicitly marks the disks attached to a sled
    /// as "expunged".
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
        .map_err(|error| error.into_external_error())
    }

    pub(super) async fn sled_set_policy_impl(
        &self,
        opctx: &OpContext,
        authz_sled: &authz::Sled,
        new_sled_policy: SledPolicy,
        check: ValidateTransition,
    ) -> Result<SledPolicy, TransitionError> {
        opctx.authorize(authz::Action::Modify, authz_sled).await?;

        let sled_id = authz_sled.id();
        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;
        let policy = self
            .transaction_retry_wrapper("sled_set_policy")
            .transaction(&conn, |conn| {
                let err = err.clone();

                async move {
                    let t = SledTransition::Policy(new_sled_policy);
                    let valid_old_policies = t.valid_old_policies();
                    let valid_old_states = t.valid_old_states();

                    use db::schema::sled::dsl;
                    let query = diesel::update(dsl::sled)
                        .filter(dsl::time_deleted.is_null())
                        .filter(dsl::id.eq(sled_id));

                    let query = match check {
                        ValidateTransition::Yes => query
                            .filter(
                                dsl::sled_policy.eq_any(
                                    valid_old_policies
                                        .into_iter()
                                        .map(to_db_sled_policy),
                                ),
                            )
                            .filter(
                                dsl::sled_state
                                    .eq_any(valid_old_states.iter().copied()),
                            )
                            .into_boxed(),
                        #[cfg(test)]
                        ValidateTransition::No => query.into_boxed(),
                    };

                    let query = query
                        .set((
                            dsl::sled_policy
                                .eq(to_db_sled_policy(new_sled_policy)),
                            dsl::time_modified.eq(Utc::now()),
                        ))
                        .check_if_exists::<Sled>(sled_id);

                    let result = query.execute_and_check(&conn).await?;

                    let old_policy = match (check, result.status) {
                        (ValidateTransition::Yes, UpdateStatus::Updated) => {
                            result.found.policy()
                        }
                        (
                            ValidateTransition::Yes,
                            UpdateStatus::NotUpdatedButExists,
                        ) => {
                            // Two reasons this can happen:
                            // 1. An idempotent update: this is treated as a
                            //    success.
                            // 2. Invalid state transition: a failure.
                            //
                            // To differentiate between the two, check that the
                            // new policy is the same as the old policy, and
                            // that the old state is valid.
                            if result.found.policy() == new_sled_policy
                                && valid_old_states
                                    .contains(&result.found.state())
                            {
                                result.found.policy()
                            } else {
                                return Err(err.bail(
                                    TransitionError::InvalidTransition {
                                        current: result.found,
                                        transition: SledTransition::Policy(
                                            new_sled_policy,
                                        ),
                                    },
                                ));
                            }
                        }
                        #[cfg(test)]
                        (ValidateTransition::No, _) => result.found.policy(),
                    };

                    // When a sled is expunged, the associated disks with that
                    // sled should also be implicitly set to expunged.
                    let new_disk_policy = match new_sled_policy {
                        SledPolicy::InService { .. } => None,
                        SledPolicy::Expunged => {
                            Some(nexus_db_model::PhysicalDiskPolicy::Expunged)
                        }
                    };
                    if let Some(new_disk_policy) = new_disk_policy {
                        use db::schema::physical_disk::dsl as physical_disk_dsl;
                        diesel::update(physical_disk_dsl::physical_disk)
                            .filter(physical_disk_dsl::time_deleted.is_null())
                            .filter(physical_disk_dsl::sled_id.eq(sled_id))
                            .set(
                                physical_disk_dsl::disk_policy
                                    .eq(new_disk_policy),
                            )
                            .execute_async(&conn)
                            .await?;
                    }

                    Ok(old_policy)
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    return err;
                }
                TransitionError::from(public_error_from_diesel(
                    e,
                    ErrorHandler::Server,
                ))
            })?;
        Ok(policy)
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
    /// valid to decommission from (i.e. if [`SledPolicy::is_decommissionable`]
    /// returns `false`).
    pub async fn sled_set_state_to_decommissioned(
        &self,
        opctx: &OpContext,
        authz_sled: &authz::Sled,
    ) -> Result<SledState, TransitionError> {
        self.sled_set_state_impl(
            opctx,
            authz_sled,
            SledState::Decommissioned,
            ValidateTransition::Yes,
        )
        .await
    }

    pub(super) async fn sled_set_state_impl(
        &self,
        opctx: &OpContext,
        authz_sled: &authz::Sled,
        new_sled_state: SledState,
        check: ValidateTransition,
    ) -> Result<SledState, TransitionError> {
        use db::schema::sled::dsl;

        opctx.authorize(authz::Action::Modify, authz_sled).await?;

        let sled_id = authz_sled.id();
        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;
        let old_state = self
            .transaction_retry_wrapper("sled_set_state")
            .transaction(&conn, |conn| {
                let err = err.clone();

                async move {
                    let query = diesel::update(dsl::sled)
                        .filter(dsl::time_deleted.is_null())
                        .filter(dsl::id.eq(sled_id));

                    let t = SledTransition::State(new_sled_state);
                    let valid_old_policies = t.valid_old_policies();
                    let valid_old_states = t.valid_old_states();

                    let query = match check {
                        ValidateTransition::Yes => query
                            .filter(
                                dsl::sled_policy.eq_any(
                                    valid_old_policies
                                        .iter()
                                        .copied()
                                        .map(to_db_sled_policy),
                                ),
                            )
                            .filter(dsl::sled_state.eq_any(valid_old_states))
                            .into_boxed(),
                        #[cfg(test)]
                        ValidateTransition::No => query.into_boxed(),
                    };

                    let query = query
                        .set((
                            dsl::sled_state.eq(new_sled_state),
                            dsl::time_modified.eq(Utc::now()),
                        ))
                        .check_if_exists::<Sled>(sled_id);

                    let result = query.execute_and_check(&conn).await?;

                    let old_state = match (check, result.status) {
                        (ValidateTransition::Yes, UpdateStatus::Updated) => {
                            result.found.state()
                        }
                        (
                            ValidateTransition::Yes,
                            UpdateStatus::NotUpdatedButExists,
                        ) => {
                            // Two reasons this can happen:
                            // 1. An idempotent update: this is treated as a
                            //    success.
                            // 2. Invalid state transition: a failure.
                            //
                            // To differentiate between the two, check that the
                            // new state is the same as the old state, and the
                            // found policy is valid.
                            if result.found.state() == new_sled_state
                                && valid_old_policies
                                    .contains(&result.found.policy())
                            {
                                result.found.state()
                            } else {
                                return Err(err.bail(
                                    TransitionError::InvalidTransition {
                                        current: result.found,
                                        transition: SledTransition::State(
                                            new_sled_state,
                                        ),
                                    },
                                ));
                            }
                        }
                        #[cfg(test)]
                        (ValidateTransition::No, _) => result.found.state(),
                    };

                    // When a sled is decommissioned, the associated disks with
                    // that sled should also be implicitly set to
                    // decommissioned.
                    //
                    // We use an explicit `match` to force ourselves to consider
                    // disk state if we add any addition sled states in the
                    // future.
                    let new_disk_state = match new_sled_state {
                        SledState::Active => None,
                        SledState::Decommissioned => Some(
                            nexus_db_model::PhysicalDiskState::Decommissioned,
                        ),
                    };
                    if let Some(new_disk_state) = new_disk_state {
                        use db::schema::physical_disk::dsl as physical_disk_dsl;
                        diesel::update(physical_disk_dsl::physical_disk)
                            .filter(physical_disk_dsl::time_deleted.is_null())
                            .filter(physical_disk_dsl::sled_id.eq(sled_id))
                            .set(
                                physical_disk_dsl::disk_state
                                    .eq(new_disk_state),
                            )
                            .execute_async(&conn)
                            .await?;
                    }

                    Ok(old_state)
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    return err;
                }
                TransitionError::from(public_error_from_diesel(
                    e,
                    ErrorHandler::Server,
                ))
            })?;

        Ok(old_state)
    }
}

// ---
// State transition validators
// ---

// The functions in this section return the old policies or states that are
// valid for a new policy or state, except idempotent transitions.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SledTransition {
    Policy(SledPolicy),
    State(SledState),
}

impl SledTransition {
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
            SledTransition::Policy(new_policy) => match new_policy {
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
            SledTransition::State(state) => {
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
            SledTransition::Policy(_) => {
                // Policies can only be transitioned in the active state. (In
                // the future, this will include other non-decommissioned
                // states.)
                vec![Active]
            }
            SledTransition::State(state) => match state {
                Active => vec![],
                Decommissioned => vec![Active],
            },
        }
    }
}

impl fmt::Display for SledTransition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SledTransition::Policy(policy) => {
                write!(f, "policy \"{}\"", policy)
            }
            SledTransition::State(state) => write!(f, "state \"{}\"", state),
        }
    }
}

impl IntoEnumIterator for SledTransition {
    type Iterator = std::vec::IntoIter<Self>;

    fn iter() -> Self::Iterator {
        let v: Vec<_> = SledPolicy::iter()
            .map(SledTransition::Policy)
            .chain(SledState::iter().map(SledTransition::State))
            .collect();
        v.into_iter()
    }
}

/// An error that occurred while setting a policy or state.
#[derive(Debug, Error)]
#[must_use]
pub enum TransitionError {
    /// The state transition check failed.
    ///
    /// The sled is returned.
    #[error(
        "sled id {} has current policy \"{}\" and state \"{}\" \
        and the transition to {} is not permitted",
        .current.id(),
        .current.policy(),
        .current.state(),
        .transition,
    )]
    InvalidTransition {
        /// The current sled as fetched from the database.
        current: Sled,

        /// The new policy or state that was attempted.
        transition: SledTransition,
    },

    /// Some other kind of error occurred.
    #[error("database error")]
    External(#[from] external::Error),
}

impl TransitionError {
    fn into_external_error(self) -> external::Error {
        match self {
            TransitionError::InvalidTransition { .. } => {
                external::Error::conflict(self.to_string())
            }
            TransitionError::External(e) => e.clone(),
        }
    }

    #[cfg(test)]
    pub(super) fn ensure_invalid_transition(self) -> anyhow::Result<()> {
        match self {
            TransitionError::InvalidTransition { .. } => Ok(()),
            TransitionError::External(e) => Err(anyhow::anyhow!(e)
                .context("expected invalid transition, got other error")),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::datastore::test::{
        sled_baseboard_for_test, sled_system_hardware_for_test,
    };
    use crate::db::datastore::test_utils::{
        datastore_test, sled_set_policy, sled_set_state, Expected,
        IneligibleSleds,
    };
    use crate::db::lookup::LookupPath;
    use crate::db::model::ByteCount;
    use crate::db::model::SqlU32;
    use anyhow::{Context, Result};
    use itertools::Itertools;
    use nexus_db_model::Generation;
    use nexus_db_model::PhysicalDisk;
    use nexus_db_model::PhysicalDiskKind;
    use nexus_db_model::PhysicalDiskPolicy;
    use nexus_db_model::PhysicalDiskState;
    use nexus_test_utils::db::test_setup_database;
    use nexus_types::identity::Asset;
    use omicron_common::api::external;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::SledUuid;
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
        let (observed_sled, _) =
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

        // Bump the generation number so the insert succeeds.
        sled_update.sled_agent_gen.0 = sled_update.sled_agent_gen.0.next();

        // Test that upserting the sled propagates those changes to the DB.
        let (observed_sled, _) = datastore
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

    #[tokio::test]
    async fn upsert_sled_updates_fails_with_stale_sled_agent_gen() {
        let logctx = dev::test_setup_log(
            "upsert_sled_updates_fails_with_stale_sled_agent_gen",
        );
        let mut db = test_setup_database(&logctx.log).await;
        let (_opctx, datastore) = datastore_test(&logctx, &db).await;

        let mut sled_update = test_new_sled_update();
        let (observed_sled, _) =
            datastore.sled_upsert(sled_update.clone()).await.unwrap();

        assert_eq!(observed_sled.reservoir_size, sled_update.reservoir_size);

        // Modify the reservoir size
        const MIB: u64 = 1024 * 1024;

        sled_update.reservoir_size = ByteCount::from(
            external::ByteCount::try_from(
                sled_update.reservoir_size.0.to_bytes() + MIB,
            )
            .unwrap(),
        );

        // Fail the update, since the generation number didn't change.
        assert!(datastore.sled_upsert(sled_update.clone()).await.is_err());

        // Bump the generation number so the next insert succeeds.
        sled_update.sled_agent_gen.0 = sled_update.sled_agent_gen.0.next();

        // Test that upserting the sled propagates those changes to the DB.
        let (observed_sled, _) = datastore
            .sled_upsert(sled_update.clone())
            .await
            .expect("Could not upsert sled during test prep");
        assert_eq!(observed_sled.reservoir_size, sled_update.reservoir_size);

        // Now reset the generation to a lower value and try again.
        // This should fail.
        let current_gen = sled_update.sled_agent_gen;
        sled_update.sled_agent_gen = Generation::new();
        assert!(datastore.sled_upsert(sled_update.clone()).await.is_err());

        // Now bump the generation from the saved `current_gen`
        // Change the reservoir value again. This should succeed.
        sled_update.reservoir_size = ByteCount::from(
            external::ByteCount::try_from(
                sled_update.reservoir_size.0.to_bytes() + MIB,
            )
            .unwrap(),
        );
        sled_update.sled_agent_gen.0 = current_gen.0.next();
        // Test that upserting the sled propagates those changes to the DB.
        let (observed_sled, _) = datastore
            .sled_upsert(sled_update.clone())
            .await
            .expect("Could not upsert sled during test prep");
        assert_eq!(observed_sled.reservoir_size, sled_update.reservoir_size);
        assert_eq!(observed_sled.sled_agent_gen, sled_update.sled_agent_gen);

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
        let (observed_sled, _) =
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

        // Set the sled to decommissioned (this is not a legal transition, but
        // we don't care about sled policy in sled_upsert, just the state.)
        sled_set_state(
            &opctx,
            &datastore,
            SledUuid::from_untyped_uuid(observed_sled.id()),
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

        // Upserting the sled should produce an error, because it is decommissioend.
        assert!(datastore.sled_upsert(sled_update.clone()).await.is_err());

        // The sled should not have been updated.
        let (_, observed_sled_2) = LookupPath::new(&opctx, &datastore)
            .sled_id(observed_sled.id())
            .fetch_for(authz::Action::Modify)
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
        let (non_provisionable_sled, _) =
            datastore.sled_upsert(test_new_sled_update()).await.unwrap();
        let (expunged_sled, _) =
            datastore.sled_upsert(test_new_sled_update()).await.unwrap();
        let (decommissioned_sled, _) =
            datastore.sled_upsert(test_new_sled_update()).await.unwrap();
        let (illegal_decommissioned_sled, _) =
            datastore.sled_upsert(test_new_sled_update()).await.unwrap();

        let ineligible_sleds = IneligibleSleds {
            non_provisionable: SledUuid::from_untyped_uuid(
                non_provisionable_sled.id(),
            ),
            expunged: SledUuid::from_untyped_uuid(expunged_sled.id()),
            decommissioned: SledUuid::from_untyped_uuid(
                decommissioned_sled.id(),
            ),
            illegal_decommissioned: SledUuid::from_untyped_uuid(
                illegal_decommissioned_sled.id(),
            ),
        };
        ineligible_sleds.setup(&opctx, &datastore).await.unwrap();

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
        let (provisionable_sled, _) =
            datastore.sled_upsert(sled_update.clone()).await.unwrap();

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

    async fn lookup_physical_disk(
        datastore: &DataStore,
        id: Uuid,
    ) -> PhysicalDisk {
        use db::schema::physical_disk::dsl;
        dsl::physical_disk
            .filter(dsl::id.eq(id))
            .filter(dsl::time_deleted.is_null())
            .select(PhysicalDisk::as_select())
            .get_result_async(
                &*datastore
                    .pool_connection_for_tests()
                    .await
                    .expect("No connection"),
            )
            .await
            .expect("Failed to lookup physical disk")
    }

    #[tokio::test]
    async fn test_sled_expungement_also_expunges_disks() {
        let logctx =
            dev::test_setup_log("test_sled_expungement_also_expunges_disks");
        let mut db = test_setup_database(&logctx.log).await;

        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // Set up a sled to test against.
        let (sled, _) =
            datastore.sled_upsert(test_new_sled_update()).await.unwrap();
        let sled_id = SledUuid::from_untyped_uuid(sled.id());

        // Add a couple disks to this sled.
        //
        // (Note: This isn't really enough DB fakery to actually provision e.g.
        // Crucible regions, but it creates enough of a control plane object to
        // be associated with the Sled by UUID)
        let disk1 = PhysicalDisk::new(
            Uuid::new_v4(),
            "vendor1".to_string(),
            "serial1".to_string(),
            "model1".to_string(),
            PhysicalDiskKind::U2,
            sled_id.into_untyped_uuid(),
        );
        let disk2 = PhysicalDisk::new(
            Uuid::new_v4(),
            "vendor2".to_string(),
            "serial2".to_string(),
            "model2".to_string(),
            PhysicalDiskKind::U2,
            sled_id.into_untyped_uuid(),
        );

        datastore
            .physical_disk_insert(&opctx, disk1.clone())
            .await
            .expect("Failed to upsert physical disk");
        datastore
            .physical_disk_insert(&opctx, disk2.clone())
            .await
            .expect("Failed to upsert physical disk");

        // Confirm the disks are "in-service".
        //
        // We verify this state because it should be changing below.
        assert_eq!(
            PhysicalDiskPolicy::InService,
            lookup_physical_disk(&datastore, disk1.id()).await.disk_policy
        );
        assert_eq!(
            PhysicalDiskPolicy::InService,
            lookup_physical_disk(&datastore, disk2.id()).await.disk_policy
        );

        // Expunge the sled. As a part of this process, the query should UPDATE
        // the physical_disk table.
        sled_set_policy(
            &opctx,
            &datastore,
            sled_id,
            SledPolicy::Expunged,
            ValidateTransition::Yes,
            Expected::Ok(SledPolicy::provisionable()),
        )
        .await
        .expect("Could not expunge sled");

        // Observe that the disk policy is now expunged
        assert_eq!(
            PhysicalDiskPolicy::Expunged,
            lookup_physical_disk(&datastore, disk1.id()).await.disk_policy
        );
        assert_eq!(
            PhysicalDiskPolicy::Expunged,
            lookup_physical_disk(&datastore, disk2.id()).await.disk_policy
        );

        // We can now decommission the sled, which should also decommission the
        // disks.
        sled_set_state(
            &opctx,
            &datastore,
            sled_id,
            SledState::Decommissioned,
            ValidateTransition::Yes,
            Expected::Ok(SledState::Active),
        )
        .await
        .expect("decommissioned sled");

        // Observe that the disk state is now decommissioned
        assert_eq!(
            PhysicalDiskState::Decommissioned,
            lookup_physical_disk(&datastore, disk1.id()).await.disk_state
        );
        assert_eq!(
            PhysicalDiskState::Decommissioned,
            lookup_physical_disk(&datastore, disk2.id()).await.disk_state
        );

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
                    predicate::in_iter(SledPolicy::all_matching(
                        SledFilter::InService,
                    )),
                    predicate::eq(SledState::Active),
                ),
                SledTransition::Policy(SledPolicy::Expunged),
            ),
            (
                // The provision policy of in-service sleds can be changed, or
                // kept the same (1 of 2).
                Before::new(
                    predicate::in_iter(SledPolicy::all_matching(
                        SledFilter::InService,
                    )),
                    predicate::eq(SledState::Active),
                ),
                SledTransition::Policy(SledPolicy::InService {
                    provision_policy: SledProvisionPolicy::Provisionable,
                }),
            ),
            (
                // (2 of 2)
                Before::new(
                    predicate::in_iter(SledPolicy::all_matching(
                        SledFilter::InService,
                    )),
                    predicate::eq(SledState::Active),
                ),
                SledTransition::Policy(SledPolicy::InService {
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
                SledTransition::State(SledState::Active),
            ),
            (
                // Expunged sleds can be marked as decommissioned.
                Before::new(
                    predicate::eq(SledPolicy::Expunged),
                    predicate::eq(SledState::Active),
                ),
                SledTransition::State(SledState::Decommissioned),
            ),
            (
                // Expunged sleds can always be marked as expunged again, as
                // long as they aren't already decommissioned (we do not allow
                // any transitions once a sled is decommissioned).
                Before::new(
                    predicate::eq(SledPolicy::Expunged),
                    predicate::ne(SledState::Decommissioned),
                ),
                SledTransition::Policy(SledPolicy::Expunged),
            ),
            (
                // Decommissioned sleds can always be marked as decommissioned
                // again, as long as their policy is decommissionable.
                Before::new(
                    predicate::in_iter(SledPolicy::all_decommissionable()),
                    predicate::eq(SledState::Decommissioned),
                ),
                SledTransition::State(SledState::Decommissioned),
            ),
        ];

        // Generate all possible transitions.
        let all_transitions = SledPolicy::iter()
            .cartesian_product(SledState::iter())
            .cartesian_product(SledTransition::iter())
            .enumerate();

        // Set up a sled to test against.
        let (sled, _) =
            datastore.sled_upsert(test_new_sled_update()).await.unwrap();
        let sled_id = sled.id();

        for (i, ((policy, state), after)) in all_transitions {
            test_sled_state_transitions_once(
                &opctx,
                &datastore,
                SledUuid::from_untyped_uuid(sled_id),
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
        sled_id: SledUuid,
        before_policy: SledPolicy,
        before_state: SledState,
        after: SledTransition,
        valid_transitions: &[(Before, SledTransition)],
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
        sled_set_policy(
            opctx,
            datastore,
            sled_id,
            before_policy,
            ValidateTransition::No,
            Expected::Ignore,
        )
        .await?;

        sled_set_state(
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
            SledTransition::Policy(new_policy) => {
                let expected = if is_valid {
                    Expected::Ok(before_policy)
                } else {
                    Expected::Invalid
                };

                sled_set_policy(
                    opctx,
                    datastore,
                    sled_id,
                    new_policy,
                    ValidateTransition::Yes,
                    expected,
                )
                .await?;
            }
            SledTransition::State(new_state) => {
                let expected = if is_valid {
                    Expected::Ok(before_state)
                } else {
                    Expected::Invalid
                };

                sled_set_state(
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
            Generation::new(),
        )
    }

    /// Initial state for state transitions.
    #[derive(Debug)]
    struct Before(BoxPredicate<SledPolicy>, BoxPredicate<SledState>);

    impl Before {
        fn new<P, S>(policy: P, state: S) -> Self
        where
            P: Predicate<SledPolicy> + Send + Sync + 'static,
            S: Predicate<SledState> + Send + Sync + 'static,
        {
            Before(policy.boxed(), state.boxed())
        }
    }

    /// Tests listing large numbers of sleds via the batched interface
    #[tokio::test]
    async fn sled_list_batch() {
        let logctx = dev::test_setup_log("sled_list_batch");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let size = usize::try_from(2 * SQL_BATCH_SIZE.get()).unwrap();
        let mut new_sleds = Vec::with_capacity(size);
        new_sleds.resize_with(size, test_new_sled_update);
        let mut expected_ids: Vec<_> =
            new_sleds.iter().map(|s| s.id()).collect();
        expected_ids.sort();

        // This is essentially the same as `sled_upsert()`.  But since we know
        // none of these exist already, we can just insert them.  And that means
        // we can do them all in one SQL statement.  This is considerably
        // faster.
        let values_to_insert: Vec<_> =
            new_sleds.into_iter().map(|s| s.into_insertable()).collect();
        let ninserted = {
            use db::schema::sled::dsl;
            diesel::insert_into(dsl::sled)
                .values(values_to_insert)
                .execute_async(
                    &*datastore
                        .pool_connection_for_tests()
                        .await
                        .expect("failed to get connection"),
                )
                .await
                .expect("failed to insert sled")
        };
        assert_eq!(ninserted, size);

        let sleds = datastore
            .sled_list_all_batched(&opctx, SledFilter::Commissioned)
            .await
            .expect("failed to list all sleds");

        // We don't need to sort these ids because the sleds are enumerated in
        // id order.
        let found_ids: Vec<_> = sleds.into_iter().map(|s| s.id()).collect();
        assert_eq!(expected_ids, found_ids);
        assert_eq!(found_ids.len(), size);

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
