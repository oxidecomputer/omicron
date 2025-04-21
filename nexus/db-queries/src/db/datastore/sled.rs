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
use crate::db::model::AffinityPolicy;
use crate::db::model::Sled;
use crate::db::model::SledResourceVmm;
use crate::db::model::SledState;
use crate::db::model::SledUpdate;
use crate::db::model::to_db_sled_policy;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use crate::db::queries::sled_reservation::sled_find_targets_query;
use crate::db::queries::sled_reservation::sled_insert_resource_query;
use crate::db::update_and_check::{UpdateAndCheck, UpdateStatus};
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::OptionalError;
use nexus_db_errors::TransactionError;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
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
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::PropolisUuid;
use omicron_uuid_kinds::SledUuid;
use slog::Logger;
use std::collections::HashSet;
use std::fmt;
use strum::IntoEnumIterator;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
enum SledReservationError {
    #[error(
        "Could not find any valid sled on which this instance can be placed"
    )]
    NotFound,
    #[error(
        "This instance belongs to an affinity group that requires it be placed \
         on more than one sled. Instances can only placed on a single sled, so \
         this is impossible to satisfy. Consider stopping other instances in \
         the affinity group."
    )]
    TooManyAffinityConstraints,
    #[error(
        "This instance belongs to an affinity group that requires it to be \
         placed on a sled, but also belongs to an anti-affinity group that \
         prevents it from being placed on that sled. These constraints are \
         contradictory. Consider stopping instances in those \
         affinity/anti-affinity groups, or changing group membership."
    )]
    ConflictingAntiAndAffinityConstraints,
    #[error(
        "This instance must be placed on a specific sled to co-locate it \
         with another instance in its affinity group, but that sled cannot \
         current accept this instance. Consider stopping other instances \
         in this instance's affinity groups, or changing its affinity \
         group membership."
    )]
    RequiredAffinitySledNotValid,
}

impl From<SledReservationError> for external::Error {
    fn from(err: SledReservationError) -> Self {
        let msg = format!("Failed to place instance: {err}");
        match err {
            // "NotFound" can be resolved by adding more capacity
            SledReservationError::NotFound
            // "RequiredAffinitySledNotValid" is *usually* the result of a
            // single sled filling up with several members of an Affinity group.
            //
            // It is also possible briefly when a sled with affinity groups is
            // being expunged with running VMMs, but "insufficient capacity" is
            // the much more common case.
            //
            // (Disambiguating these cases would require additional database
            // queries, hence why this isn't being done right now)
            | SledReservationError::RequiredAffinitySledNotValid => {
                external::Error::insufficient_capacity(&msg, &msg)
            },
            // The following cases are constraint violations due to excessive
            // affinity/anti-affinity groups -- even if additional capacity is
            // added, they won't be fixed. Return a 400 error to signify to the
            // caller that they're responsible for fixing these constraints.
            SledReservationError::TooManyAffinityConstraints
            | SledReservationError::ConflictingAntiAndAffinityConstraints => {
                external::Error::invalid_request(&msg)
            },
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum SledReservationTransactionError {
    #[error(transparent)]
    Connection(#[from] Error),
    #[error(transparent)]
    Diesel(#[from] diesel::result::Error),
    #[error(transparent)]
    Reservation(#[from] SledReservationError),
}

// Chooses a sled for reservation with the supplied constraints.
//
// - "targets": All possible sleds which might be selected.
// - "anti_affinity_sleds": All sleds which are anti-affine to
// our requested reservation.
// - "affinity_sleds": All sleds which are affine to our requested
// reservation.
//
// We use the following logic to calculate a desirable sled, given a possible
// set of "targets", and the information from affinity groups.
//
// # Rules vs Preferences
//
// Due to the flavors of "affinity policy", it's possible to bucket affinity
// choices into two categories: "rules" and "preferences". "rules" are affinity
// dispositions for or against sled placement that must be followed, and
// "preferences" are affinity dispositions that should be followed for sled
// selection, in order of "most preferential" to "least preferential".
//
// As example of a "rule" is "an anti-affinity group exists, containing a
// target sled, with affinity_policy = 'fail'".
//
// An example of a "preference" is "an anti-affinity group exists, containing a
// target sled, but the policy is 'allow'." We don't want to use it as a target,
// but we will if there are no other choices.
//
// We apply rules before preferences to ensure they are always respected.
// Furthermore, the evaluation of preferences is a target-seeking operation,
// which identifies the distinct sets of targets, and searches them in
// decreasing preference order.
//
// # Logic
//
// ## Background: Notation
//
// We use the following symbols for sets below:
// - ∩: Intersection of two sets (A ∩ B is "everything that exists in A and
// also exists in B").
// - ∖: difference of two sets (A ∖ B is "everything that exists in A that does
// not exist in B).
//
// We also use the following notation for brevity:
// - AA,P=Fail: All sleds containing instances that are part of an anti-affinity
//   group with policy = 'fail'.
// - AA,P=Allow: Same as above, but with policy = 'allow'.
// - A,P=Fail: All sleds containing instances that are part of an affinity group
//   with policy = 'fail'.
// - A,P=Allow: Same as above, but with policy = 'allow'.
//
// ## Affinity: Apply Rules
//
// - Targets := All viable sleds for instance placement
// - Banned := AA,P=Fail
// - Required := A,P=Fail
// - if Required.len() > 1: Fail (too many constraints).
// - if Required.len() == 1...
//   - ... if the entry exists in the "Banned" set: Fail
//     (contradicting constraints 'Banned' + 'Required')
//   - ... if the entry does not exist in "Targets": Fail
//     ('Required' constraint not satisfiable)
//   - ... if the entry does not exist in "Banned": Use it.
//
// If we have not yet picked a target, we can filter the set of targets to
// ignore "banned" sleds, and then apply preferences.
//
// - Targets := Targets ∖ Banned
//
// ## Affinity: Apply Preferences
//
// - Preferred := Targets ∩ A,P=Allow
// - Unpreferred := Targets ∩ AA,P=Allow
// - Both := Preferred ∩ Unpreferred
// - Preferred := Preferred ∖ Both
// - Unpreferred := Unpreferred ∖ Both
// - If Preferred isn't empty, pick a target from it.
// - Targets := Targets \ Unpreferred
// - If Targets isn't empty, pick a target from it.
// - If Unpreferred isn't empty, pick a target from it.
// - Fail, no targets are available.
fn pick_sled_reservation_target(
    log: &Logger,
    targets: &HashSet<SledUuid>,
    banned: &HashSet<SledUuid>,
    unpreferred: &HashSet<SledUuid>,
    required: &HashSet<SledUuid>,
    preferred: &HashSet<SledUuid>,
) -> Result<SledUuid, SledReservationError> {
    if !banned.is_empty() {
        info!(
            log,
            "anti-affinity policy prohibits placement on {} sleds", banned.len();
            "banned" => ?banned,
        );
    }
    if !required.is_empty() {
        info!(
            log,
            "affinity policy requires placement on {} sleds", required.len();
            "required" => ?required,
        );
    }

    if required.len() > 1 {
        return Err(SledReservationError::TooManyAffinityConstraints);
    }
    if let Some(required_id) = required.iter().next() {
        // If we have a "required" sled, it must be chosen.
        if banned.contains(&required_id) {
            return Err(
                SledReservationError::ConflictingAntiAndAffinityConstraints,
            );
        }
        if !targets.contains(&required_id) {
            return Err(SledReservationError::RequiredAffinitySledNotValid);
        }
        return Ok(*required_id);
    }

    // We have no "required" sleds, but might have preferences.
    let mut targets: HashSet<_> =
        targets.difference(&banned).cloned().collect();

    // Only consider "preferred" sleds that are viable targets
    let preferred: HashSet<_> =
        targets.intersection(&preferred).cloned().collect();
    // Only consider "unpreferred" sleds that are viable targets
    let mut unpreferred: HashSet<_> =
        targets.intersection(&unpreferred).cloned().collect();

    // If a target is both preferred and unpreferred, it is not considered
    // a part of either set.
    let both = preferred.intersection(&unpreferred).cloned().collect();

    // Grab a preferred target (which isn't also unpreferred) if one exists.
    if let Some(target) = preferred.difference(&both).cloned().next() {
        return Ok(target);
    }
    unpreferred = unpreferred.difference(&both).cloned().collect();
    targets = targets.difference(&unpreferred).cloned().collect();

    // Grab a target which not in the unpreferred set, if one exists.
    if let Some(target) = targets.iter().cloned().next() {
        return Ok(target);
    }

    // Grab a target from the unpreferred set, if one exists.
    if let Some(target) = unpreferred.iter().cloned().next() {
        return Ok(target);
    }
    return Err(SledReservationError::NotFound);
}

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
        use nexus_db_schema::schema::sled::dsl;
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
                dsl::repo_depot_port.eq(sled_update.repo_depot_port),
                dsl::rack_id.eq(sled_update.rack_id),
                dsl::is_scrimlet.eq(sled_update.is_scrimlet()),
                dsl::usable_hardware_threads
                    .eq(sled_update.usable_hardware_threads),
                dsl::usable_physical_ram.eq(sled_update.usable_physical_ram),
                dsl::reservoir_size.eq(sled_update.reservoir_size),
                dsl::cpu_family.eq(sled_update.cpu_family),
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
    pub async fn check_sled_in_service(
        &self,
        opctx: &OpContext,
        sled_id: SledUuid,
    ) -> Result<(), Error> {
        let conn = &*self.pool_connection_authorized(&opctx).await?;
        Self::check_sled_in_service_on_connection(conn, sled_id)
            .await
            .map_err(From::from)
    }

    /// Confirms that a sled exists and is in-service.
    ///
    /// This function may be called from a transaction context.
    pub async fn check_sled_in_service_on_connection(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        sled_id: SledUuid,
    ) -> Result<(), TransactionError<Error>> {
        use nexus_db_schema::schema::sled::dsl;
        let sled_exists_and_in_service = diesel::select(diesel::dsl::exists(
            dsl::sled
                .filter(dsl::time_deleted.is_null())
                .filter(dsl::id.eq(sled_id.into_untyped_uuid()))
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
        use nexus_db_schema::schema::sled::dsl;
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
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
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
        instance_id: InstanceUuid,
        propolis_id: PropolisUuid,
        resources: db::model::Resources,
        constraints: db::model::SledReservationConstraints,
    ) -> CreateResult<db::model::SledResourceVmm> {
        self.sled_reservation_create_inner(
            opctx,
            instance_id,
            propolis_id,
            resources,
            constraints,
        )
        .await
        .map_err(|e| match e {
            SledReservationTransactionError::Connection(e) => e,
            SledReservationTransactionError::Diesel(e) => {
                public_error_from_diesel(e, ErrorHandler::Server)
            }
            SledReservationTransactionError::Reservation(e) => e.into(),
        })
    }

    async fn sled_reservation_create_inner(
        &self,
        opctx: &OpContext,
        instance_id: InstanceUuid,
        propolis_id: PropolisUuid,
        resources: db::model::Resources,
        constraints: db::model::SledReservationConstraints,
    ) -> Result<db::model::SledResourceVmm, SledReservationTransactionError>
    {
        let conn = self.pool_connection_authorized(opctx).await?;

        // Check if resource ID already exists - if so, return it.
        //
        // This check makes this function idempotent. Beyond this point, however
        // we rely on primary key constraints in the database to prevent
        // concurrent reservations for same propolis_id.
        use nexus_db_schema::schema::sled_resource_vmm::dsl as resource_dsl;
        let old_resource = resource_dsl::sled_resource_vmm
            .filter(resource_dsl::id.eq(*propolis_id.as_untyped_uuid()))
            .select(SledResourceVmm::as_select())
            .limit(1)
            .load_async(&*conn)
            .await?;
        if !old_resource.is_empty() {
            return Ok(old_resource[0].clone());
        }

        let must_use_sleds: HashSet<SledUuid> = constraints
            .must_select_from()
            .into_iter()
            .flatten()
            .map(|id| SledUuid::from_untyped_uuid(*id))
            .collect();

        // Query for the set of possible sleds using a CTE.
        //
        // Note that this is not transactional, to reduce contention.
        // However, that lack of transactionality means we need to validate
        // our constraints again when we later try to INSERT the reservation.
        let possible_sleds = sled_find_targets_query(instance_id, &resources, constraints.cpu_families())
            .get_results_async::<(
                // Sled UUID
                Uuid,
                // Would an allocation to this sled fit?
                bool,
                // Affinity policy on this sled
                Option<AffinityPolicy>,
                // Anti-affinity policy on this sled
                Option<AffinityPolicy>,
            )>(&*conn).await?;

        // Translate the database results into a format which we can use to pick
        // a sled using more complex rules.
        //
        // See: `pick_sled_reservation_target(...)`
        let mut sled_targets = HashSet::new();
        let mut banned = HashSet::new();
        let mut unpreferred = HashSet::new();
        let mut required = HashSet::new();
        let mut preferred = HashSet::new();
        for (sled_id, fits, affinity_policy, anti_affinity_policy) in
            possible_sleds
        {
            let sled_id = SledUuid::from_untyped_uuid(sled_id);

            if fits
                && (must_use_sleds.is_empty()
                    || must_use_sleds.contains(&sled_id))
            {
                sled_targets.insert(sled_id);
            }
            if let Some(policy) = affinity_policy {
                match policy {
                    AffinityPolicy::Fail => required.insert(sled_id),
                    AffinityPolicy::Allow => preferred.insert(sled_id),
                };
            }
            if let Some(policy) = anti_affinity_policy {
                match policy {
                    AffinityPolicy::Fail => banned.insert(sled_id),
                    AffinityPolicy::Allow => unpreferred.insert(sled_id),
                };
            }
        }

        // We loop here because our attempts to INSERT may be violated by
        // concurrent operations. We'll respond by looking through a slightly
        // smaller set of possible sleds.
        //
        // In the uncontended case, however, we'll only iterate through this
        // loop once.
        loop {
            // Pick a reservation target, given the constraints we previously
            // saw in the database.
            let sled_target = pick_sled_reservation_target(
                &opctx.log,
                &sled_targets,
                &banned,
                &unpreferred,
                &required,
                &preferred,
            )?;

            // Create a SledResourceVmm record, associate it with the target
            // sled.
            let resource = SledResourceVmm::new(
                propolis_id,
                instance_id,
                sled_target,
                resources.clone(),
            );

            // Try to INSERT the record. If this is still a valid target, we'll
            // use it. If it isn't a valid target, we'll shrink the set of
            // viable sled targets and try again.
            let rows_inserted = sled_insert_resource_query(&resource)
                .execute_async(&*conn)
                .await?;
            if rows_inserted > 0 {
                return Ok(resource);
            }

            sled_targets.remove(&sled_target);
            banned.remove(&sled_target);
            unpreferred.remove(&sled_target);
            preferred.remove(&sled_target);
        }
    }

    pub async fn sled_reservation_delete(
        &self,
        opctx: &OpContext,
        vmm_id: PropolisUuid,
    ) -> DeleteResult {
        use nexus_db_schema::schema::sled_resource_vmm::dsl as resource_dsl;
        diesel::delete(resource_dsl::sled_resource_vmm)
            .filter(resource_dsl::id.eq(vmm_id.into_untyped_uuid()))
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

                    use nexus_db_schema::schema::sled::dsl;
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
                        use nexus_db_schema::schema::physical_disk::dsl as physical_disk_dsl;
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
        use nexus_db_schema::schema::sled::dsl;

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
                        use nexus_db_schema::schema::physical_disk::dsl as physical_disk_dsl;
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
    /// `test_sled_transitions`.
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
    /// `test_sled_transitions`.
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
pub(in crate::db::datastore) mod test {
    use super::*;
    use crate::db::datastore::test_utils::{
        Expected, IneligibleSleds, sled_set_policy, sled_set_state,
    };
    use crate::db::model::ByteCount;
    use crate::db::model::SqlU32;
    use crate::db::model::to_db_typed_uuid;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::pub_test_utils::helpers::SledUpdateBuilder;
    use crate::db::pub_test_utils::helpers::create_affinity_group;
    use crate::db::pub_test_utils::helpers::create_anti_affinity_group;
    use crate::db::pub_test_utils::helpers::create_project;
    use crate::db::pub_test_utils::helpers::small_resource_request;
    use anyhow::{Context, Result};
    use itertools::Itertools;
    use nexus_db_lookup::LookupPath;
    use nexus_db_model::PhysicalDiskKind;
    use nexus_db_model::PhysicalDiskPolicy;
    use nexus_db_model::PhysicalDiskState;
    use nexus_db_model::{Generation, SledCpuFamily};
    use nexus_db_model::{InstanceMinimumCpuPlatform, PhysicalDisk};
    use nexus_types::identity::Asset;
    use nexus_types::identity::Resource;
    use omicron_common::api::external;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::AffinityGroupUuid;
    use omicron_uuid_kinds::AntiAffinityGroupUuid;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::PhysicalDiskUuid;
    use omicron_uuid_kinds::SledUuid;
    use predicates::{BoxPredicate, prelude::*};
    use std::collections::HashMap;

    fn rack_id() -> Uuid {
        Uuid::parse_str(nexus_test_utils::RACK_UUID).unwrap()
    }

    #[tokio::test]
    async fn upsert_sled_updates_hardware() {
        let logctx = dev::test_setup_log("upsert_sled_updates_hardware");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();

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

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn upsert_sled_doesnt_update_decommissioned() {
        let logctx =
            dev::test_setup_log("upsert_sled_doesnt_update_decommissioned");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

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
        let (_, observed_sled_2) = LookupPath::new(&opctx, datastore)
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

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Test that new reservations aren't created on non-provisionable sleds.
    #[tokio::test]
    async fn sled_reservation_create_non_provisionable() {
        let logctx =
            dev::test_setup_log("sled_reservation_create_non_provisionable");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

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
                InstanceUuid::new_v4(),
                PropolisUuid::new_v4(),
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
                    InstanceUuid::new_v4(),
                    PropolisUuid::new_v4(),
                    resources.clone(),
                    constraints,
                )
                .await
                .unwrap();
            assert_eq!(
                resource.sled_id.into_untyped_uuid(),
                provisionable_sled.id(),
                "resource is always allocated to the provisionable sled"
            );

            datastore
                .sled_reservation_delete(&opctx, resource.id.into())
                .await
                .unwrap();
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Utilities to help with Affinity Testing

    // Create a resource request that will entirely fill a sled.
    fn large_resource_request() -> db::model::Resources {
        // NOTE: This is dependent on [`test_new_sled_update`] using the default
        // configuration for [`SledSystemHardware`].
        let sled_resources = SledUpdateBuilder::new().hardware().build();
        let threads = sled_resources.usable_hardware_threads;
        let rss_ram = sled_resources.usable_physical_ram;
        let reservoir_ram = sled_resources.reservoir_size;
        db::model::Resources::new(threads, rss_ram, reservoir_ram)
    }

    // This short-circuits some of the logic and checks we normally have when
    // creating affinity groups, but makes testing easier.
    async fn add_instance_to_anti_affinity_group(
        datastore: &DataStore,
        group_id: AntiAffinityGroupUuid,
        instance_id: InstanceUuid,
    ) {
        use db::model::AntiAffinityGroupInstanceMembership;
        use nexus_db_schema::schema::anti_affinity_group_instance_membership::dsl as membership_dsl;

        diesel::insert_into(
            membership_dsl::anti_affinity_group_instance_membership,
        )
        .values(AntiAffinityGroupInstanceMembership::new(group_id, instance_id))
        .on_conflict((membership_dsl::group_id, membership_dsl::instance_id))
        .do_nothing()
        .execute_async(&*datastore.pool_connection_for_tests().await.unwrap())
        .await
        .unwrap();
    }

    // This short-circuits some of the logic and checks we normally have when
    // creating affinity groups, but makes testing easier.
    async fn add_instance_to_affinity_group(
        datastore: &DataStore,
        group_id: AffinityGroupUuid,
        instance_id: InstanceUuid,
    ) {
        use db::model::AffinityGroupInstanceMembership;
        use nexus_db_schema::schema::affinity_group_instance_membership::dsl as membership_dsl;

        diesel::insert_into(membership_dsl::affinity_group_instance_membership)
            .values(AffinityGroupInstanceMembership::new(group_id, instance_id))
            .on_conflict((
                membership_dsl::group_id,
                membership_dsl::instance_id,
            ))
            .do_nothing()
            .execute_async(
                &*datastore.pool_connection_for_tests().await.unwrap(),
            )
            .await
            .unwrap();
    }

    async fn create_sleds(datastore: &DataStore, count: usize) -> Vec<Sled> {
        let mut sleds = vec![];
        for _ in 0..count {
            let (sled, _) =
                datastore.sled_upsert(test_new_sled_update()).await.unwrap();
            sleds.push(sled);
        }
        sleds
    }

    type GroupName = &'static str;

    #[derive(Copy, Clone, Debug)]
    enum Affinity {
        Positive,
        Negative,
    }

    struct Group {
        affinity: Affinity,
        name: GroupName,
        policy: external::AffinityPolicy,
    }

    impl Group {
        async fn create(
            &self,
            opctx: &OpContext,
            datastore: &DataStore,
            authz_project: &authz::Project,
        ) -> Uuid {
            match self.affinity {
                Affinity::Positive => create_affinity_group(
                    &opctx,
                    &datastore,
                    &authz_project,
                    self.name,
                    self.policy,
                )
                .await
                .id(),
                Affinity::Negative => create_anti_affinity_group(
                    &opctx,
                    &datastore,
                    &authz_project,
                    self.name,
                    self.policy,
                )
                .await
                .id(),
            }
        }
    }

    struct AllGroups {
        id_by_name: HashMap<&'static str, (Affinity, Uuid)>,
    }

    impl AllGroups {
        async fn create(
            opctx: &OpContext,
            datastore: &DataStore,
            authz_project: &authz::Project,
            groups: &[Group],
        ) -> AllGroups {
            let mut id_by_name = HashMap::new();

            for group in groups {
                id_by_name.insert(
                    group.name,
                    (
                        group.affinity,
                        group.create(&opctx, &datastore, &authz_project).await,
                    ),
                );
            }

            Self { id_by_name }
        }
    }

    struct Instance {
        id: InstanceUuid,
        groups: Vec<GroupName>,
        force_onto_sled: Option<SledUuid>,
        resources: db::model::Resources,
        min_cpu_platform: Option<db::model::InstanceMinimumCpuPlatform>,
    }

    struct FindTargetsOutput {
        id: SledUuid,
        fits: bool,
        affinity_policy: Option<AffinityPolicy>,
        anti_affinity_policy: Option<AffinityPolicy>,
    }

    impl Instance {
        fn new() -> Self {
            Self {
                id: InstanceUuid::new_v4(),
                groups: vec![],
                force_onto_sled: None,
                resources: small_resource_request(),
                min_cpu_platform: None,
            }
        }

        // This is the first half of creating a sled reservation.
        // It can be called during tests trying to invoke contention manually.
        async fn find_targets(
            &self,
            datastore: &DataStore,
        ) -> Vec<FindTargetsOutput> {
            assert!(self.force_onto_sled.is_none());

            let families =
                self.min_cpu_platform.map(|p| p.compatible_sled_cpu_families());

            sled_find_targets_query(self.id, &self.resources, families)
                .get_results_async::<(
                    Uuid,
                    bool,
                    Option<AffinityPolicy>,
                    Option<AffinityPolicy>,
                )>(&*datastore.pool_connection_for_tests().await.unwrap())
                .await
                .unwrap()
                .into_iter()
                .map(|(id, fits, affinity_policy, anti_affinity_policy)| {
                    FindTargetsOutput { id: SledUuid::from_untyped_uuid(id), fits, affinity_policy, anti_affinity_policy }
                })
                .collect()
        }

        // This is the second half of creating a sled reservation.
        // It can be called during tests trying to invoke contention manually.
        //
        // Returns "true" if the INSERT succeeded
        async fn insert_resource(
            &self,
            datastore: &DataStore,
            propolis_id: PropolisUuid,
            sled_id: SledUuid,
        ) -> bool {
            assert!(self.force_onto_sled.is_none());

            let resource = SledResourceVmm::new(
                propolis_id,
                self.id,
                sled_id,
                self.resources.clone(),
            );

            sled_insert_resource_query(&resource)
                .execute_async(
                    &*datastore.pool_connection_for_tests().await.unwrap(),
                )
                .await
                .unwrap()
                > 0
        }

        fn use_many_resources(mut self) -> Self {
            self.resources = large_resource_request();
            self
        }

        // Adds this instance to a group. Can be called multiple times.
        fn group(mut self, group: GroupName) -> Self {
            self.groups.push(group);
            self
        }

        // Force this instance to be placed on a specific sled
        fn sled(mut self, sled: Uuid) -> Self {
            self.force_onto_sled = Some(SledUuid::from_untyped_uuid(sled));
            self
        }

        async fn add_to_groups_and_reserve(
            &self,
            opctx: &OpContext,
            datastore: &DataStore,
            all_groups: &AllGroups,
        ) -> Result<db::model::SledResourceVmm, SledReservationTransactionError>
        {
            self.add_to_groups(&datastore, &all_groups).await;
            create_instance_reservation(&datastore, &opctx, self).await
        }

        async fn add_to_groups(
            &self,
            datastore: &DataStore,
            all_groups: &AllGroups,
        ) {
            for our_group_name in &self.groups {
                let (affinity, group_id) = all_groups
                    .id_by_name
                    .get(our_group_name)
                    .expect("Group not found: {our_group_name}");
                match *affinity {
                    Affinity::Positive => {
                        add_instance_to_affinity_group(
                            &datastore,
                            AffinityGroupUuid::from_untyped_uuid(*group_id),
                            self.id,
                        )
                        .await
                    }
                    Affinity::Negative => {
                        add_instance_to_anti_affinity_group(
                            &datastore,
                            AntiAffinityGroupUuid::from_untyped_uuid(*group_id),
                            self.id,
                        )
                        .await
                    }
                }
            }
        }
    }

    async fn create_instance_reservation(
        db: &DataStore,
        opctx: &OpContext,
        instance: &Instance,
    ) -> Result<db::model::SledResourceVmm, SledReservationTransactionError>
    {
        // Pick a specific sled, if requested
        let constraints = db::model::SledReservationConstraintBuilder::new();
        let constraints = if let Some(sled_target) = instance.force_onto_sled {
            constraints.must_select_from(&[sled_target.into_untyped_uuid()])
        } else {
            constraints
        };

        // We're accessing the inner implementation to get a more detailed
        // view of the error code.
        let result = db
            .sled_reservation_create_inner(
                &opctx,
                instance.id,
                PropolisUuid::new_v4(),
                instance.resources.clone(),
                constraints.build(),
            )
            .await?;

        if let Some(sled_target) = instance.force_onto_sled {
            assert_eq!(SledUuid::from(result.sled_id), sled_target);
        }

        Ok(result)
    }

    // Anti-Affinity, Policy = Fail
    // No sleds available => Insufficient Capacity Error
    #[tokio::test]
    async fn anti_affinity_policy_fail_no_capacity() {
        let logctx =
            dev::test_setup_log("anti_affinity_policy_fail_no_capacity");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let (authz_project, _project) =
            create_project(&opctx, &datastore, "project").await;

        const SLED_COUNT: usize = 2;
        let sleds = create_sleds(&datastore, SLED_COUNT).await;

        let groups = [Group {
            affinity: Affinity::Negative,
            name: "anti-affinity",
            policy: external::AffinityPolicy::Fail,
        }];
        let all_groups =
            AllGroups::create(&opctx, &datastore, &authz_project, &groups)
                .await;
        let instances = [
            Instance::new().group("anti-affinity").sled(sleds[0].id()),
            Instance::new().group("anti-affinity").sled(sleds[1].id()),
        ];
        for instance in instances {
            instance
                .add_to_groups_and_reserve(&opctx, &datastore, &all_groups)
                .await
                .expect("Failed to set up instances");
        }

        let test_instance = Instance::new().group("anti-affinity");
        let err = test_instance
            .add_to_groups_and_reserve(&opctx, &datastore, &all_groups)
            .await
            .expect_err("Should have failed to place instance");

        let SledReservationTransactionError::Reservation(
            SledReservationError::NotFound,
        ) = err
        else {
            panic!("Unexpected error: {err:?}");
        };

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Anti-Affinity, Policy = Fail
    // We should reliably pick a sled not occupied by another instance
    #[tokio::test]
    async fn anti_affinity_policy_fail() {
        let logctx = dev::test_setup_log("anti_affinity_policy_fail");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let (authz_project, _project) =
            create_project(&opctx, &datastore, "project").await;

        const SLED_COUNT: usize = 3;
        let sleds = create_sleds(&datastore, SLED_COUNT).await;

        let groups = [Group {
            affinity: Affinity::Negative,
            name: "anti-affinity",
            policy: external::AffinityPolicy::Fail,
        }];
        let all_groups =
            AllGroups::create(&opctx, &datastore, &authz_project, &groups)
                .await;
        let instances = [
            Instance::new().group("anti-affinity").sled(sleds[0].id()),
            Instance::new().group("anti-affinity").sled(sleds[1].id()),
        ];
        for instance in instances {
            instance
                .add_to_groups_and_reserve(&opctx, &datastore, &all_groups)
                .await
                .expect("Failed to set up instances");
        }

        let test_instance = Instance::new().group("anti-affinity");
        let resource = test_instance
            .add_to_groups_and_reserve(&opctx, &datastore, &all_groups)
            .await
            .expect("Should have succeeded allocation");
        assert_eq!(resource.sled_id.into_untyped_uuid(), sleds[2].id());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Anti-Affinity, Policy = Allow
    //
    // Create two sleds, put instances on each belonging to an anti-affinity group.
    // We can continue to add instances belonging to that anti-affinity group, because
    // it has "AffinityPolicy::Allow".
    #[tokio::test]
    async fn anti_affinity_policy_allow() {
        let logctx = dev::test_setup_log("anti_affinity_policy_allow");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let (authz_project, _project) =
            create_project(&opctx, &datastore, "project").await;

        const SLED_COUNT: usize = 2;
        let sleds = create_sleds(&datastore, SLED_COUNT).await;

        let groups = [Group {
            affinity: Affinity::Negative,
            name: "anti-affinity",
            policy: external::AffinityPolicy::Allow,
        }];
        let all_groups =
            AllGroups::create(&opctx, &datastore, &authz_project, &groups)
                .await;
        let instances = [
            Instance::new().group("anti-affinity").sled(sleds[0].id()),
            Instance::new().group("anti-affinity").sled(sleds[1].id()),
        ];
        for instance in instances {
            instance
                .add_to_groups_and_reserve(&opctx, &datastore, &all_groups)
                .await
                .expect("Failed to set up instances");
        }

        let test_instance = Instance::new().group("anti-affinity");
        let resource = test_instance
            .add_to_groups_and_reserve(&opctx, &datastore, &all_groups)
            .await
            .expect("Should have succeeded allocation");
        assert!(
            [sleds[0].id(), sleds[1].id()]
                .contains(resource.sled_id.as_untyped_uuid()),
            "Should have been provisioned to one of the two viable sleds"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Affinity, Policy = Fail
    //
    // Placement of instances with positive affinity will share a sled.
    #[tokio::test]
    async fn affinity_policy_fail() {
        let logctx = dev::test_setup_log("affinity_policy_fail");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let (authz_project, _project) =
            create_project(&opctx, &datastore, "project").await;

        const SLED_COUNT: usize = 2;
        let sleds = create_sleds(&datastore, SLED_COUNT).await;

        let groups = [Group {
            affinity: Affinity::Positive,
            name: "affinity",
            policy: external::AffinityPolicy::Fail,
        }];
        let all_groups =
            AllGroups::create(&opctx, &datastore, &authz_project, &groups)
                .await;
        let instances = [Instance::new().group("affinity").sled(sleds[0].id())];
        for instance in instances {
            instance
                .add_to_groups_and_reserve(&opctx, &datastore, &all_groups)
                .await
                .expect("Failed to set up instances");
        }

        let test_instance = Instance::new().group("affinity");
        let resource = test_instance
            .add_to_groups_and_reserve(&opctx, &datastore, &all_groups)
            .await
            .expect("Should have placed instance");
        assert_eq!(resource.sled_id.into_untyped_uuid(), sleds[0].id());

        let another_test_instance = Instance::new().group("affinity");
        let resource = another_test_instance
            .add_to_groups_and_reserve(&opctx, &datastore, &all_groups)
            .await
            .expect("Should have placed instance (again)");
        assert_eq!(resource.sled_id.into_untyped_uuid(), sleds[0].id());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Affinity, Policy = Fail, with too many constraints.
    #[tokio::test]
    async fn affinity_policy_fail_too_many_constraints() {
        let logctx =
            dev::test_setup_log("affinity_policy_fail_too_many_constraints");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let (authz_project, _project) =
            create_project(&opctx, &datastore, "project").await;

        const SLED_COUNT: usize = 3;
        let sleds = create_sleds(&datastore, SLED_COUNT).await;

        let groups = [
            Group {
                affinity: Affinity::Positive,
                name: "affinity1",
                policy: external::AffinityPolicy::Fail,
            },
            Group {
                affinity: Affinity::Positive,
                name: "affinity2",
                policy: external::AffinityPolicy::Fail,
            },
        ];
        let all_groups =
            AllGroups::create(&opctx, &datastore, &authz_project, &groups)
                .await;

        // We are constrained so that an instance belonging to both groups must be
        // placed on both sled 0 and sled 1. This cannot be satisfied, and it returns
        // an error.
        let instances = [
            Instance::new().group("affinity1").sled(sleds[0].id()),
            Instance::new().group("affinity2").sled(sleds[1].id()),
        ];
        for instance in instances {
            instance
                .add_to_groups_and_reserve(&opctx, &datastore, &all_groups)
                .await
                .expect("Failed to set up instances");
        }

        let test_instance =
            Instance::new().group("affinity1").group("affinity2");
        let err = test_instance
            .add_to_groups_and_reserve(&opctx, &datastore, &all_groups)
            .await
            .expect_err("Should have failed to place instance");

        let SledReservationTransactionError::Reservation(
            SledReservationError::TooManyAffinityConstraints,
        ) = err
        else {
            panic!("Unexpected error: {err:?}");
        };

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Affinity, Policy = Fail forces "no space" early
    //
    // Placement of instances with positive affinity with "policy = Fail"
    // will always be forced to share a sled, even if there are other options.
    #[tokio::test]
    async fn affinity_policy_fail_no_capacity() {
        let logctx = dev::test_setup_log("affinity_policy_fail_no_capacity");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let (authz_project, _project) =
            create_project(&opctx, &datastore, "project").await;

        const SLED_COUNT: usize = 2;
        let sleds = create_sleds(&datastore, SLED_COUNT).await;

        let groups = [Group {
            affinity: Affinity::Positive,
            name: "affinity",
            policy: external::AffinityPolicy::Fail,
        }];
        let all_groups =
            AllGroups::create(&opctx, &datastore, &authz_project, &groups)
                .await;
        let instances = [Instance::new()
            .use_many_resources()
            .group("affinity")
            .sled(sleds[0].id())];
        for instance in instances {
            instance
                .add_to_groups_and_reserve(&opctx, &datastore, &all_groups)
                .await
                .expect("Failed to set up instances");
        }

        let test_instance =
            Instance::new().use_many_resources().group("affinity");
        let err = test_instance
            .add_to_groups_and_reserve(&opctx, &datastore, &all_groups)
            .await
            .expect_err("Should have failed to place instance");
        let SledReservationTransactionError::Reservation(
            SledReservationError::RequiredAffinitySledNotValid,
        ) = err
        else {
            panic!("Unexpected error: {err:?}");
        };

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Affinity, Policy = Allow lets us use other sleds
    //
    // This is similar to "affinity_policy_fail_no_capacity", but by
    // using "Policy = Allow" instead of "Policy = Fail", we are able to pick
    // a different sled for the reservation.
    #[tokio::test]
    async fn affinity_policy_allow_picks_different_sled() {
        let logctx =
            dev::test_setup_log("affinity_policy_allow_picks_different_sled");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let (authz_project, _project) =
            create_project(&opctx, &datastore, "project").await;

        const SLED_COUNT: usize = 2;
        let sleds = create_sleds(&datastore, SLED_COUNT).await;

        let groups = [Group {
            affinity: Affinity::Positive,
            name: "affinity",
            policy: external::AffinityPolicy::Allow,
        }];
        let all_groups =
            AllGroups::create(&opctx, &datastore, &authz_project, &groups)
                .await;
        let instances = [Instance::new()
            .use_many_resources()
            .group("affinity")
            .sled(sleds[0].id())];
        for instance in instances {
            instance
                .add_to_groups_and_reserve(&opctx, &datastore, &all_groups)
                .await
                .expect("Failed to set up instances");
        }

        let test_instance =
            Instance::new().use_many_resources().group("affinity");
        let reservation = test_instance
            .add_to_groups_and_reserve(&opctx, &datastore, &all_groups)
            .await
            .expect("Should have made reservation");
        assert_eq!(reservation.sled_id.into_untyped_uuid(), sleds[1].id());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Anti-Affinity, Policy = Fail + Affinity, Policy = Fail
    //
    // These constraints are contradictory - we're asking the allocator
    // to colocate and NOT colocate our new instance with existing ones, which
    // should not be satisfiable.
    #[tokio::test]
    async fn affinity_and_anti_affinity_policy_fail() {
        let logctx =
            dev::test_setup_log("affinity_and_anti_affinity_policy_fail");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let (authz_project, _project) =
            create_project(&opctx, &datastore, "project").await;

        const SLED_COUNT: usize = 2;
        let sleds = create_sleds(&datastore, SLED_COUNT).await;

        let groups = [
            Group {
                affinity: Affinity::Negative,
                name: "anti-affinity",
                policy: external::AffinityPolicy::Fail,
            },
            Group {
                affinity: Affinity::Positive,
                name: "affinity",
                policy: external::AffinityPolicy::Fail,
            },
        ];
        let all_groups =
            AllGroups::create(&opctx, &datastore, &authz_project, &groups)
                .await;
        let instances = [Instance::new()
            .group("anti-affinity")
            .group("affinity")
            .sled(sleds[0].id())];
        for instance in instances {
            instance
                .add_to_groups_and_reserve(&opctx, &datastore, &all_groups)
                .await
                .expect("Failed to set up instances");
        }

        let test_instance =
            Instance::new().group("anti-affinity").group("affinity");
        let err = test_instance
            .add_to_groups_and_reserve(&opctx, &datastore, &all_groups)
            .await
            .expect_err("Contradictory constraints should not be satisfiable");
        let SledReservationTransactionError::Reservation(
            SledReservationError::ConflictingAntiAndAffinityConstraints,
        ) = err
        else {
            panic!("Unexpected error: {err:?}");
        };

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Anti-Affinity, Policy = Allow + Affinity, Policy = Allow
    //
    // These constraints are contradictory, but since they encode
    // "preferences" rather than "rules", they cancel out.
    #[tokio::test]
    async fn affinity_and_anti_affinity_policy_allow() {
        let logctx =
            dev::test_setup_log("affinity_and_anti_affinity_policy_allow");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let (authz_project, _project) =
            create_project(&opctx, &datastore, "project").await;

        const SLED_COUNT: usize = 2;
        let sleds = create_sleds(&datastore, SLED_COUNT).await;

        let groups = [
            Group {
                affinity: Affinity::Negative,
                name: "anti-affinity",
                policy: external::AffinityPolicy::Allow,
            },
            Group {
                affinity: Affinity::Positive,
                name: "affinity",
                policy: external::AffinityPolicy::Allow,
            },
        ];
        let all_groups =
            AllGroups::create(&opctx, &datastore, &authz_project, &groups)
                .await;
        let instances = [
            Instance::new()
                .group("anti-affinity")
                .group("affinity")
                .sled(sleds[0].id()),
            Instance::new()
                .group("anti-affinity")
                .group("affinity")
                .sled(sleds[1].id()),
        ];
        for instance in instances {
            instance
                .add_to_groups_and_reserve(&opctx, &datastore, &all_groups)
                .await
                .expect("Failed to set up instances");
        }

        let test_instance =
            Instance::new().group("anti-affinity").group("affinity");
        let resource = test_instance
            .add_to_groups_and_reserve(&opctx, &datastore, &all_groups)
            .await
            .expect("Should have succeeded allocation");
        assert!(
            [sleds[0].id(), sleds[1].id()]
                .contains(resource.sled_id.as_untyped_uuid()),
            "Should have been provisioned to one of the two viable sleds"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn anti_affinity_multi_group() {
        let logctx = dev::test_setup_log("anti_affinity_multi_group");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let (authz_project, _project) =
            create_project(&opctx, &datastore, "project").await;

        const SLED_COUNT: usize = 4;
        let sleds = create_sleds(&datastore, SLED_COUNT).await;

        let groups = [
            Group {
                affinity: Affinity::Negative,
                name: "strict-anti-affinity",
                policy: external::AffinityPolicy::Fail,
            },
            Group {
                affinity: Affinity::Negative,
                name: "anti-affinity",
                policy: external::AffinityPolicy::Allow,
            },
            Group {
                affinity: Affinity::Positive,
                name: "affinity",
                policy: external::AffinityPolicy::Allow,
            },
        ];
        let all_groups =
            AllGroups::create(&opctx, &datastore, &authz_project, &groups)
                .await;

        // Sleds 0 and 1 contain the "strict-anti-affinity" group instances,
        // and won't be used.
        //
        // Sled 3 has an "anti-affinity" group instance, and also won't be used.
        //
        // This only leaves sled 2.
        let instances = [
            Instance::new()
                .group("strict-anti-affinity")
                .group("affinity")
                .sled(sleds[0].id()),
            Instance::new().group("strict-anti-affinity").sled(sleds[1].id()),
            Instance::new().group("anti-affinity").sled(sleds[3].id()),
        ];
        for instance in instances {
            instance
                .add_to_groups_and_reserve(&opctx, &datastore, &all_groups)
                .await
                .expect("Failed to set up instances");
        }

        let test_instance = Instance::new()
            .group("strict-anti-affinity")
            .group("anti-affinity")
            .group("affinity");
        let resource = test_instance
            .add_to_groups_and_reserve(&opctx, &datastore, &all_groups)
            .await
            .expect("Should have succeeded allocation");

        assert_eq!(resource.sled_id.into_untyped_uuid(), sleds[2].id());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn affinity_multi_group() {
        let logctx = dev::test_setup_log("affinity_multi_group");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let (authz_project, _project) =
            create_project(&opctx, &datastore, "project").await;

        const SLED_COUNT: usize = 4;
        let sleds = create_sleds(&datastore, SLED_COUNT).await;

        let groups = [
            Group {
                affinity: Affinity::Positive,
                name: "affinity",
                policy: external::AffinityPolicy::Allow,
            },
            Group {
                affinity: Affinity::Negative,
                name: "anti-affinity",
                policy: external::AffinityPolicy::Allow,
            },
        ];
        let all_groups =
            AllGroups::create(&opctx, &datastore, &authz_project, &groups)
                .await;

        // Sled 0 contains an affinity group, but it's large enough to make it
        // non-viable for future allocations.
        //
        // Sled 1 contains an affinity and anti-affinity group, so they cancel out.
        // This gives it "no priority".
        //
        // Sled 2 contains nothing. It's a viable target, neither preferred nor
        // unpreferred.
        //
        // Sled 3 contains an affinity group, which is prioritized.
        let instances = [
            Instance::new()
                .group("affinity")
                .use_many_resources()
                .sled(sleds[0].id()),
            Instance::new()
                .group("affinity")
                .group("anti-affinity")
                .sled(sleds[1].id()),
            Instance::new().group("affinity").sled(sleds[3].id()),
        ];
        for instance in instances {
            instance
                .add_to_groups_and_reserve(&opctx, &datastore, &all_groups)
                .await
                .expect("Failed to set up instances");
        }

        let test_instance =
            Instance::new().group("affinity").group("anti-affinity");
        let resource = test_instance
            .add_to_groups_and_reserve(&opctx, &datastore, &all_groups)
            .await
            .expect("Should have succeeded allocation");

        assert_eq!(resource.sled_id.into_untyped_uuid(), sleds[3].id());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn affinity_ignored_from_other_groups() {
        let logctx = dev::test_setup_log("affinity_ignored_from_other_groups");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let (authz_project, _project) =
            create_project(&opctx, &datastore, "project").await;

        const SLED_COUNT: usize = 3;
        let sleds = create_sleds(&datastore, SLED_COUNT).await;

        let groups = [
            Group {
                affinity: Affinity::Positive,
                name: "affinity1",
                policy: external::AffinityPolicy::Fail,
            },
            Group {
                affinity: Affinity::Positive,
                name: "affinity2",
                policy: external::AffinityPolicy::Fail,
            },
        ];
        let all_groups =
            AllGroups::create(&opctx, &datastore, &authz_project, &groups)
                .await;

        // Only "sleds[1]" has space. We ignore the affinity policy because
        // our new instance won't belong to either group.
        let instances = [
            Instance::new()
                .group("affinity1")
                .use_many_resources()
                .sled(sleds[0].id()),
            Instance::new()
                .group("affinity2")
                .use_many_resources()
                .sled(sleds[2].id()),
        ];
        for instance in instances {
            instance
                .add_to_groups_and_reserve(&opctx, &datastore, &all_groups)
                .await
                .expect("Failed to set up instances");
        }

        let test_instance = Instance::new();
        let resource = test_instance
            .add_to_groups_and_reserve(&opctx, &datastore, &all_groups)
            .await
            .expect("Should have succeeded allocation");

        assert_eq!(resource.sled_id.into_untyped_uuid(), sleds[1].id());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn anti_affinity_ignored_from_other_groups() {
        let logctx =
            dev::test_setup_log("anti_affinity_ignored_from_other_groups");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let (authz_project, _project) =
            create_project(&opctx, &datastore, "project").await;

        const SLED_COUNT: usize = 3;
        let sleds = create_sleds(&datastore, SLED_COUNT).await;

        let groups = [
            Group {
                affinity: Affinity::Negative,
                name: "anti-affinity1",
                policy: external::AffinityPolicy::Fail,
            },
            Group {
                affinity: Affinity::Negative,
                name: "anti-affinity2",
                policy: external::AffinityPolicy::Fail,
            },
        ];
        let all_groups =
            AllGroups::create(&opctx, &datastore, &authz_project, &groups)
                .await;

        // Only "sleds[2]" has space, even though it also contains an anti-affinity group.
        // However, if we don't belong to this group, we won't care.

        let instances = [
            Instance::new().group("anti-affinity1").sled(sleds[0].id()),
            Instance::new().use_many_resources().sled(sleds[1].id()),
            Instance::new().group("anti-affinity2").sled(sleds[2].id()),
        ];
        for instance in instances {
            instance
                .add_to_groups_and_reserve(&opctx, &datastore, &all_groups)
                .await
                .expect("Failed to set up instances");
        }

        let test_instance = Instance::new().group("anti-affinity1");
        let resource = test_instance
            .add_to_groups_and_reserve(&opctx, &datastore, &all_groups)
            .await
            .expect("Should have succeeded allocation");

        assert_eq!(resource.sled_id.into_untyped_uuid(), sleds[2].id());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Test that concurrent provisioning of an affinity group can cause
    // the INSERT of a sled_resource_vmm to fail.
    #[tokio::test]
    async fn sled_reservation_concurrent_affinity_requirement() {
        let logctx = dev::test_setup_log(
            "sled_reservation_concurrent_affinity_requirement",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let (authz_project, _project) =
            create_project(&opctx, &datastore, "project").await;

        const SLED_COUNT: usize = 4;
        let sleds = create_sleds(&datastore, SLED_COUNT).await;

        let test_instance = Instance::new().group("affinity");

        // We manually call the first half of sled reservation: finding targets.
        //
        // All sleds should be available.
        let possible_sleds = test_instance.find_targets(&datastore).await;
        assert_eq!(possible_sleds.len(), SLED_COUNT);
        assert!(possible_sleds.iter().all(|sled| sled.fits));
        assert!(
            possible_sleds.iter().all(|sled| sled.affinity_policy.is_none())
        );
        assert!(
            possible_sleds
                .iter()
                .all(|sled| sled.anti_affinity_policy.is_none())
        );

        // Concurrently create an instance on sleds[0].
        let groups = [Group {
            affinity: Affinity::Positive,
            name: "affinity",
            policy: external::AffinityPolicy::Fail,
        }];
        let all_groups =
            AllGroups::create(&opctx, &datastore, &authz_project, &groups)
                .await;
        let instances = [Instance::new().group("affinity").sled(sleds[0].id())];
        for instance in instances {
            instance
                .add_to_groups_and_reserve(&opctx, &datastore, &all_groups)
                .await
                .expect("Failed to set up instances");
        }

        // Put the instance-under-test in the "affinity" group.
        test_instance.add_to_groups(&datastore, &all_groups).await;

        // Now if we try to find targets again, the result will change.
        let possible_sleds = test_instance.find_targets(&datastore).await;
        assert_eq!(possible_sleds.len(), SLED_COUNT);
        assert!(possible_sleds.iter().all(|sled| sled.fits));
        assert!(
            possible_sleds
                .iter()
                .all(|sled| sled.anti_affinity_policy.is_none())
        );
        let affine_sled = possible_sleds
            .iter()
            .find(|sled| sled.id.into_untyped_uuid() == sleds[0].id())
            .unwrap();
        assert!(matches!(
            affine_sled.affinity_policy.expect("Sled 0 should be affine"),
            AffinityPolicy::Fail
        ));

        // Inserting onto sleds[1..3] should fail -- the affinity requirement
        // should bind us to sleds[0].
        for i in 1..=3 {
            assert!(
                !test_instance
                    .insert_resource(
                        &datastore,
                        PropolisUuid::new_v4(),
                        SledUuid::from_untyped_uuid(sleds[i].id()),
                    )
                    .await,
                "Shouldn't have been able to insert into sled {i}"
            )
        }

        // Inserting into sleds[0] should succeed
        assert!(
            test_instance
                .insert_resource(
                    &datastore,
                    PropolisUuid::new_v4(),
                    SledUuid::from_untyped_uuid(sleds[0].id()),
                )
                .await
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Test that concurrent provisioning of an anti-affinity group can cause
    // the INSERT of a sled_resource_vmm to fail.
    #[tokio::test]
    async fn sled_reservation_concurrent_anti_affinity_requirement() {
        let logctx = dev::test_setup_log(
            "sled_reservation_concurrent_anti_affinity_requirement",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let (authz_project, _project) =
            create_project(&opctx, &datastore, "project").await;

        const SLED_COUNT: usize = 4;
        let sleds = create_sleds(&datastore, SLED_COUNT).await;

        let test_instance = Instance::new().group("anti-affinity");

        // We manually call the first half of sled reservation: finding targets.
        //
        // All sleds should be available.
        let possible_sleds = test_instance.find_targets(&datastore).await;
        assert_eq!(possible_sleds.len(), SLED_COUNT);
        assert!(possible_sleds.iter().all(|sled| sled.fits));
        assert!(
            possible_sleds.iter().all(|sled| sled.affinity_policy.is_none())
        );
        assert!(
            possible_sleds
                .iter()
                .all(|sled| sled.anti_affinity_policy.is_none())
        );

        // Concurrently create an instance on sleds[0].
        let groups = [Group {
            affinity: Affinity::Negative,
            name: "anti-affinity",
            policy: external::AffinityPolicy::Fail,
        }];
        let all_groups =
            AllGroups::create(&opctx, &datastore, &authz_project, &groups)
                .await;
        let instances =
            [Instance::new().group("anti-affinity").sled(sleds[0].id())];
        for instance in instances {
            instance
                .add_to_groups_and_reserve(&opctx, &datastore, &all_groups)
                .await
                .expect("Failed to set up instances");
        }

        // Put the instance-under-test in the "anti-affinity" group.
        test_instance.add_to_groups(&datastore, &all_groups).await;

        // Now if we try to find targets again, the result will change.
        let possible_sleds = test_instance.find_targets(&datastore).await;
        assert_eq!(possible_sleds.len(), SLED_COUNT);
        assert!(possible_sleds.iter().all(|sled| sled.fits));
        assert!(
            possible_sleds.iter().all(|sled| sled.affinity_policy.is_none())
        );
        let anti_affine_sled = possible_sleds
            .iter()
            .find(|sled| sled.id.into_untyped_uuid() == sleds[0].id())
            .unwrap();
        assert!(matches!(
            anti_affine_sled
                .anti_affinity_policy
                .expect("Sled 0 should be anti-affine"),
            AffinityPolicy::Fail
        ));

        // Inserting onto sleds[0] should fail -- the anti-affinity requirement
        // should prevent us from inserting there.
        assert!(
            !test_instance
                .insert_resource(
                    &datastore,
                    PropolisUuid::new_v4(),
                    SledUuid::from_untyped_uuid(sleds[0].id()),
                )
                .await,
            "Shouldn't have been able to insert into sleds[0]"
        );

        // Inserting into sleds[1] should succeed
        assert!(
            test_instance
                .insert_resource(
                    &datastore,
                    PropolisUuid::new_v4(),
                    SledUuid::from_untyped_uuid(sleds[1].id()),
                )
                .await
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn sled_reservation_concurrent_space_requirement() {
        let logctx = dev::test_setup_log(
            "sled_reservation_concurrent_space_requirement",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let (authz_project, _project) =
            create_project(&opctx, &datastore, "project").await;

        const SLED_COUNT: usize = 4;
        let sleds = create_sleds(&datastore, SLED_COUNT).await;

        let test_instance = Instance::new().use_many_resources();

        // We manually call the first half of sled reservation: finding targets.
        //
        // All sleds should be available.
        let possible_sleds = test_instance.find_targets(&datastore).await;
        assert_eq!(possible_sleds.len(), SLED_COUNT);
        assert!(possible_sleds.iter().all(|sled| sled.fits));
        assert!(
            possible_sleds.iter().all(|sled| sled.affinity_policy.is_none())
        );
        assert!(
            possible_sleds
                .iter()
                .all(|sled| sled.anti_affinity_policy.is_none())
        );

        // Concurrently create large instances on sleds 0, 2, 3.
        let groups = [];
        let all_groups =
            AllGroups::create(&opctx, &datastore, &authz_project, &groups)
                .await;
        let instances = [
            Instance::new().use_many_resources().sled(sleds[0].id()),
            Instance::new().use_many_resources().sled(sleds[2].id()),
            Instance::new().use_many_resources().sled(sleds[3].id()),
        ];
        for instance in instances {
            instance
                .add_to_groups_and_reserve(&opctx, &datastore, &all_groups)
                .await
                .expect("Failed to set up instances");
        }

        // Now if we try to find targets again, the result will change.
        let possible_sleds = test_instance.find_targets(&datastore).await;
        assert_eq!(possible_sleds.len(), 1);
        assert!(possible_sleds[0].affinity_policy.is_none());
        assert!(possible_sleds[0].anti_affinity_policy.is_none());
        assert!(possible_sleds[0].fits);
        assert_eq!(possible_sleds[0].id.into_untyped_uuid(), sleds[1].id());

        // Inserting onto sleds[0, 2, 3] should fail - there shouldn't
        // be enough space on these sleds.
        for i in [0, 2, 3] {
            assert!(
                !test_instance
                    .insert_resource(
                        &datastore,
                        PropolisUuid::new_v4(),
                        SledUuid::from_untyped_uuid(sleds[i].id()),
                    )
                    .await,
                "Shouldn't have been able to insert into sleds[i]"
            );
        }

        // Inserting into sleds[1] should succeed
        assert!(
            test_instance
                .insert_resource(
                    &datastore,
                    PropolisUuid::new_v4(),
                    SledUuid::from_untyped_uuid(sleds[1].id()),
                )
                .await
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn sled_reservation_cpu_constraints() {
        let logctx = dev::test_setup_log("sled_reservation_cpu_constraints");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let (_authz_project, _project) =
            create_project(&opctx, &datastore, "project").await;

        let mut sleds = vec![];
        for family in [SledCpuFamily::AmdMilan, SledCpuFamily::AmdTurin] {
            for _ in 0..2 {
                let mut builder = SledUpdateBuilder::new();
                builder.rack_id(rack_id());
                builder.hardware().cpu_family(family);
                let (sled, _) =
                    datastore.sled_upsert(builder.build()).await.unwrap();
                sleds.push(sled);
            }
        }

        let mut test_instance = Instance::new();
        for platform in [None, Some(InstanceMinimumCpuPlatform::AmdMilan)] {
            test_instance.min_cpu_platform = platform;
            let possible_sleds = test_instance.find_targets(&datastore).await;
            assert_eq!(possible_sleds.len(), 4);
        }

        test_instance.min_cpu_platform =
            Some(InstanceMinimumCpuPlatform::AmdTurin);
        let possible_sleds = test_instance.find_targets(&datastore).await;
        assert_eq!(possible_sleds.len(), 2);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    async fn lookup_physical_disk(
        datastore: &DataStore,
        id: PhysicalDiskUuid,
    ) -> PhysicalDisk {
        use nexus_db_schema::schema::physical_disk::dsl;
        dsl::physical_disk
            .filter(dsl::id.eq(to_db_typed_uuid(id)))
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
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

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
            PhysicalDiskUuid::new_v4(),
            "vendor1".to_string(),
            "serial1".to_string(),
            "model1".to_string(),
            PhysicalDiskKind::U2,
            sled_id.into_untyped_uuid(),
        );
        let disk2 = PhysicalDisk::new(
            PhysicalDiskUuid::new_v4(),
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

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_sled_transitions() {
        // Test valid and invalid state and policy transitions.
        let logctx = dev::test_setup_log("test_sled_transitions");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

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

        db.terminate().await;
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

    pub(crate) fn test_new_sled_update() -> SledUpdate {
        SledUpdateBuilder::new().rack_id(rack_id()).build()
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
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

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
            use nexus_db_schema::schema::sled::dsl;
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

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
