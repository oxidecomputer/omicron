// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Sled`]s.

use super::DataStore;
use super::SQL_BATCH_SIZE;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::datastore::LocalStorageDisk;
use crate::db::datastore::ValidateTransition;
use crate::db::datastore::zpool::ZpoolGetForSledReservationResult;
use crate::db::model::AffinityPolicy;
use crate::db::model::Sled;
use crate::db::model::SledResourceVmm;
use crate::db::model::SledState;
use crate::db::model::SledUpdate;
use crate::db::model::to_db_sled_policy;
use crate::db::model::to_db_typed_uuid;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use crate::db::queries::disk::MAX_DISKS_PER_INSTANCE;
use crate::db::queries::sled_reservation::LocalStorageAllocation;
use crate::db::queries::sled_reservation::LocalStorageAllocationRequired;
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
use nonempty::NonEmpty;
use omicron_common::api::external;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::bail_unless;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::PropolisUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use slog::Logger;
use std::collections::HashSet;
use std::collections::VecDeque;
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
                .filter(dsl::id.eq(to_db_typed_uuid(sled_id)))
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
    // TODO-correctness We currently _do_ use this method in an API handler
    // (specifically, the "update status" endpoint). This is okay in a
    // single-rack world because we'll never need more than one page to fetch
    // at most 32 sleds, but we should fix this before we add support for
    // multirack (at which point this should have an
    // `opctx.check_complex_operations_allowed()?` guard).
    pub async fn sled_list_all_batched(
        &self,
        opctx: &OpContext,
        sled_filter: SledFilter,
    ) -> ListResultVec<Sled> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        let mut all_sleds = Vec::new();
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            let batch = self
                .sled_list(opctx, &p.current_pagparams(), sled_filter)
                .await?;
            paginator = p.found_batch(&batch, &|s: &nexus_db_model::Sled| {
                s.id().into_untyped_uuid()
            });
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
        let log = opctx.log.new(o!(
            "query" => "sled_reservation",
            "instance_id" => instance_id.to_string(),
            "propolis_id" => propolis_id.to_string(),
        ));

        info!(&log, "sled reservation starting");

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
            info!(&log, "sled reservation already occurred, returning");
            return Ok(old_resource[0].clone());
        }

        // Get a list of local storage disks attached to this instance
        let local_storage_disks: Vec<LocalStorageDisk> = self
            .instance_list_disks_on_conn(
                &conn,
                instance_id.into_untyped_uuid(),
                &PaginatedBy::Name(DataPageParams {
                    marker: None,
                    direction: dropshot::PaginationOrder::Ascending,
                    limit: std::num::NonZeroU32::new(MAX_DISKS_PER_INSTANCE)
                        .unwrap(),
                }),
            )
            .await?
            .into_iter()
            .filter_map(|disk| match disk {
                db::datastore::Disk::LocalStorage(disk) => Some(disk),
                db::datastore::Disk::Crucible(_) => None,
            })
            .collect();

        // Gather constraints for the instance placement, starting with the
        // arguments to this function.
        let maybe_must_use_sleds: Option<HashSet<SledUuid>> =
            if let Some(must_select_from) = constraints.must_select_from() {
                let set = must_select_from.into_iter().cloned().collect();
                info!(&log, "reservation constrained to sleds {set:?}");
                Some(set)
            } else {
                None
            };

        // If any local storage disks have been allocated already, then this
        // constraints VMM placement and where other unallocated local storage
        // must be.
        let maybe_must_use_sleds = if !local_storage_disks.is_empty() {
            // Any local storage disk that was allocated already will have a
            // sled_id.
            let local_storage_allocation_sleds: HashSet<SledUuid> =
                local_storage_disks
                    .iter()
                    .filter_map(|disk| {
                        disk.local_storage_dataset_allocation
                            .as_ref()
                            .map(|allocation| allocation.sled_id())
                    })
                    .collect();

            if local_storage_allocation_sleds.is_empty() {
                // None of this instance's local storage disks have been
                // allocated yet.
                info!(&log, "no existing local storage allocations");
                maybe_must_use_sleds
            } else {
                info!(
                    &log,
                    "existing local storage allocations on sleds
                        {local_storage_allocation_sleds:?}"
                );

                if local_storage_allocation_sleds.len() != 1 {
                    // It's an error for multiple sleds to host local storage
                    // disks for a single VMM, so return a conflict error here.
                    //
                    // This case can happen if a local storage disk was
                    // allocated on a sled, and then is detached from the
                    // instance whose VMM was on that sled, and then is attached
                    // to another instance that has some local storage disks
                    // that already have some allocations on another sled.
                    //
                    // TODO by the time this query has run that detach + attach
                    // has already occurred. Nexus should disallow attaching
                    // local storage disks to an instance that already has local
                    // storage disks if the allocations are on different sleds.
                    //
                    // TODO for clients to prevent such a scenario they would
                    // need to be aware of which sled a local storage disk's
                    // allocation is on, which means that information has to be
                    // exposed somehow in the Disk view.
                    return Err(SledReservationTransactionError::Connection(
                        Error::conflict(&format!(
                            "local storage disks for instance {instance_id} \
                            allocated on multiple sleds \
                            {local_storage_allocation_sleds:?}"
                        )),
                    ));
                }

                // If there's already a list of sleds to use, that must be
                // filtered further. Otherwise return the one sled that local
                // storage disks have already been allocated on.
                match maybe_must_use_sleds {
                    Some(must_use_sleds) => Some(
                        must_use_sleds
                            .into_iter()
                            .filter(|sled| {
                                local_storage_allocation_sleds.contains(&sled)
                            })
                            .collect(),
                    ),

                    None => Some(local_storage_allocation_sleds),
                }
            }
        } else {
            // `local_storage_disks` is empty, so that does not have an impact
            // on the existing hash set
            info!(&log, "no attached local storage disks");
            maybe_must_use_sleds
        };

        // If filtering has removed all the sleds from the constraint, then we
        // cannot satisfy this allocation.
        if let Some(must_use_sleds) = &maybe_must_use_sleds {
            if must_use_sleds.is_empty() {
                error!(&log, "no sleds available after filtering");

                // Nothing will satisfy this allocation, return an error here.
                return Err(SledReservationTransactionError::Reservation(
                    SledReservationError::NotFound,
                ));
            }
        }

        // Query for the set of possible sleds using a CTE.
        //
        // Note that this is not transactional, to reduce contention.
        // However, that lack of transactionality means we need to validate
        // our constraints again when we later try to INSERT the reservation.
        let possible_sleds = sled_find_targets_query(
            instance_id,
            &resources,
            constraints.cpu_families(),
        )
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
            // this is required because [`sled_find_targets_query`] returns a
            // query where the first element in the tuple is
            // ['`sql_types::Uuid`], and typed uuids can't be returned in
            // queries like this.
            let sled_id = SledUuid::from_untyped_uuid(sled_id);

            if fits {
                // If there is a Some list of sleds to select from, only add
                // this target if it is in that list. A None list means that any
                // sled could be a target.
                match &maybe_must_use_sleds {
                    Some(must_use_sleds) => {
                        if must_use_sleds.contains(&sled_id) {
                            sled_targets.insert(sled_id);
                        }
                    }

                    None => {
                        sled_targets.insert(sled_id);
                    }
                }
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

        info!(&log, "sled targets: {sled_targets:?}");

        let local_storage_allocation_required: Vec<&LocalStorageDisk> =
            local_storage_disks
                .iter()
                .filter(|disk| disk.local_storage_dataset_allocation.is_none())
                .collect();

        info!(
            &log,
            "local_storage_allocation_required: \
                {local_storage_allocation_required:?}"
        );

        // We loop here because our attempts to INSERT may be violated by
        // concurrent operations. We'll respond by looking through a slightly
        // smaller set of possible sleds.
        //
        // In the uncontended case, however, we'll only iterate through this
        // loop once.
        'outer: loop {
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

            info!(&log, "trying sled target {sled_target}");

            // Create a SledResourceVmm record, associate it with the target
            // sled.
            let resource = SledResourceVmm::new(
                propolis_id,
                instance_id,
                sled_target,
                resources.clone(),
            );

            if local_storage_allocation_required.is_empty() {
                info!(
                    &log,
                    "calling insert (no local storage allocation requred)"
                );

                // If no local storage allocation is required, then simply try
                // allocating a VMM to this sled.
                //
                // Try to INSERT the record. If this is still a valid target,
                // we'll use it. If it isn't a valid target, we'll shrink the
                // set of viable sled targets and try again.
                let rows_inserted = sled_insert_resource_query(
                    &resource,
                    &LocalStorageAllocationRequired::No,
                )
                .execute_async(&*conn)
                .await?;

                if rows_inserted > 0 {
                    info!(&log, "reservation succeeded!");
                    return Ok(resource);
                }
                info!(&log, "reservation failed");
            } else {
                // If local storage allocation is required, match the requests
                // with all the zpools of this sled that have available space.
                // This routine finds all possible configurations that would
                // satisfy the requests for local storage and tries them all.

                // First, each request for local storage can possibly be
                // satisfied by a number of zpools on the sled. Find this list
                // of candidate zpools for each required local storage
                // allocation.

                #[derive(Clone, Debug, Hash, PartialEq, Eq)]
                struct CandidateDataset {
                    rendezvous_local_storage_dataset_id: DatasetUuid,
                    pool_id: ZpoolUuid,
                    sled_id: SledUuid,
                }

                #[derive(Clone, Debug)]
                struct PossibleAllocationsForRequest<'a> {
                    request: &'a LocalStorageDisk,
                    candidate_datasets: HashSet<CandidateDataset>,
                }

                let zpools_for_sled = self
                    .zpool_get_for_sled_reservation(&opctx, sled_target)
                    .await?;

                // If there's an existing local storage allocation on a zpool,
                // remove that from the list of candidates. Local storage for
                // the same Instance should not share any zpools.
                let local_storage_zpools_used: HashSet<ZpoolUuid> =
                    local_storage_disks
                        .iter()
                        .filter_map(|disk| {
                            disk.local_storage_dataset_allocation.as_ref().map(
                                |allocation| {
                                    ZpoolUuid::from_untyped_uuid(
                                        allocation
                                            .pool_id()
                                            .into_untyped_uuid(),
                                    )
                                },
                            )
                        })
                        .collect();

                let zpools_for_sled: Vec<_> = zpools_for_sled
                    .into_iter()
                    .filter(|zpool_get_result| {
                        !local_storage_zpools_used
                            .contains(&zpool_get_result.pool.id())
                    })
                    .collect();

                info!(&log, "filtered zpools for sled: {zpools_for_sled:?}");

                if local_storage_allocation_required.len()
                    > zpools_for_sled.len()
                {
                    // Not enough zpools to satisfy the number of allocations
                    // required. Find another sled!
                    sled_targets.remove(&sled_target);
                    banned.remove(&sled_target);
                    unpreferred.remove(&sled_target);
                    preferred.remove(&sled_target);

                    continue;
                }

                let mut allocations_to_perform =
                    Vec::with_capacity(local_storage_allocation_required.len());

                for request in &local_storage_allocation_required {
                    // Find all the zpools that could satisfy this local storage
                    // request. These will be filtered later.
                    let candidate_datasets: HashSet<CandidateDataset> =
                        zpools_for_sled
                            .iter()
                            .filter(|zpool_get_result| {
                                let ZpoolGetForSledReservationResult {
                                    pool,
                                    last_inv_total_size,
                                    rendezvous_local_storage_dataset_id: _,
                                    crucible_dataset_usage,
                                    local_storage_usage,
                                } = zpool_get_result;

                                // The total request size for the local storage
                                // dataset allocation is the disk size plus the
                                // required overhead.
                                let request_size: i64 =
                                    request.size().to_bytes() as i64
                                        + request
                                            .required_dataset_overhead()
                                            .to_bytes()
                                            as i64;

                                let new_size_used: i64 = crucible_dataset_usage
                                    + local_storage_usage
                                    + request_size;

                                let control_plane_storage_buffer: i64 =
                                    pool.control_plane_storage_buffer().into();
                                let adjusted_total: i64 = last_inv_total_size
                                    - control_plane_storage_buffer;

                                // Any zpool that has space for this local
                                // storage dataset allocation is considered a
                                // candidate.
                                new_size_used < adjusted_total
                            })
                            .map(|zpool_get_result| CandidateDataset {
                                rendezvous_local_storage_dataset_id:
                                    zpool_get_result
                                        .rendezvous_local_storage_dataset_id,
                                pool_id: zpool_get_result.pool.id(),
                                sled_id: zpool_get_result.pool.sled_id(),
                            })
                            .collect();

                    if candidate_datasets.is_empty() {
                        // if there's no local storage datasets on this sled for
                        // this request's size, then try another sled.
                        sled_targets.remove(&sled_target);
                        banned.remove(&sled_target);
                        unpreferred.remove(&sled_target);
                        preferred.remove(&sled_target);

                        continue 'outer;
                    }

                    allocations_to_perform.push(
                        PossibleAllocationsForRequest {
                            request,
                            candidate_datasets,
                        },
                    );
                }

                // From the list of allocations to perform, and all the
                // candidate local storage datasets that could fit those
                // allocations, find a list of all valid request -> zpool
                // mappings.

                struct PossibleAllocations {
                    allocations: Vec<LocalStorageAllocation>,
                    candidates_left: HashSet<CandidateDataset>,
                    request_index: usize,
                }

                struct ValidatedAllocations {
                    allocations: NonEmpty<LocalStorageAllocation>,
                }

                let mut validated_allocations: Vec<ValidatedAllocations> =
                    vec![];
                let mut queue = VecDeque::new();

                // Start from no allocations made yet, a list of allocations to
                // perform (stored in `requests`), and all of the available
                // zpools on the sled.

                queue.push_back(PossibleAllocations {
                    allocations: vec![],
                    candidates_left: zpools_for_sled
                        .iter()
                        .map(|zpool_get_result| CandidateDataset {
                            rendezvous_local_storage_dataset_id:
                                zpool_get_result
                                    .rendezvous_local_storage_dataset_id,
                            pool_id: zpool_get_result.pool.id(),
                            sled_id: zpool_get_result.pool.sled_id(),
                        })
                        .collect(),
                    request_index: 0,
                });

                // Find each valid allocation by, for each request:
                //
                // - selecting a local storage dataset from the list of local
                //   storage datasets that have not been used yet that could
                //   fulfill that request
                //
                // - removing that selected local storage dataset from the list
                //   of candidates
                //
                // - considering a set of allocations as "valid" if all requests
                //   have been matched with a local storage dataset.

                while let Some(possible_allocation) = queue.pop_front() {
                    let PossibleAllocations {
                        allocations,
                        candidates_left,
                        request_index,
                    } = possible_allocation;

                    // If we have an allocation for each possible allocation,
                    // this one is valid. Add it to the list!
                    if request_index == allocations_to_perform.len() {
                        let allocations_len = allocations.len();

                        match NonEmpty::from_vec(allocations) {
                            Some(allocations) => {
                                validated_allocations
                                    .push(ValidatedAllocations { allocations });
                            }

                            None => {
                                // There should be `request_index` entries in
                                // the `allocations` vec, this is weird!
                                error!(
                                    &opctx.log,
                                    "expected {request_index} in the \
                                    allocations vec, saw {allocations_len}",
                                );
                            }
                        }

                        continue;
                    }

                    // Try to allocate the Nth possible allocation
                    let request = &allocations_to_perform[request_index];

                    // Create a possible config based on the what datasets are
                    // left, and the candidate datasets for this request.
                    for candidate_dataset in &request.candidate_datasets {
                        if candidates_left.contains(candidate_dataset) {
                            // This request could be satisfied by this dataset.
                            // Select it and search further.

                            let mut set_allocations = allocations.clone();
                            set_allocations.push(LocalStorageAllocation {
                                disk_id: request.request.id(),

                                local_storage_dataset_allocation_id:
                                    DatasetUuid::new_v4(),

                                required_dataset_size: {
                                    let request_size: i64 =
                                        request.request.size().to_bytes()
                                            as i64
                                            + request
                                                .request
                                                .required_dataset_overhead()
                                                .to_bytes()
                                                as i64;
                                    request_size
                                },

                                local_storage_dataset_id: candidate_dataset
                                    .rendezvous_local_storage_dataset_id,

                                pool_id: candidate_dataset.pool_id,

                                sled_id: candidate_dataset.sled_id,
                            });

                            // Note by removing a candidate dataset from the
                            // list in this way, this step mandates that a
                            // single local storage dataset is not used for
                            // multiple local storage allocations for a given
                            // instance.

                            let mut set_candidates_left =
                                candidates_left.clone();
                            set_candidates_left.remove(candidate_dataset);

                            queue.push_back(PossibleAllocations {
                                allocations: set_allocations,
                                candidates_left: set_candidates_left,
                                request_index: request_index + 1,
                            });
                        }

                        // Else there are no candidate datasets left for this
                        // request, and therefore the list of requests cannot be
                        // fulfilled by the current mapping of requests to
                        // datasets
                    }
                }

                // Loop here over each possible set of local storage allocations
                // required, and attempt the sled insert resource query with
                // that particular allocation.
                //
                // If the `validated_allocations` list is empty, control will
                // pass to the end of the loop marked with 'outer, which will
                // then try the next possible sled target. In the case where
                // there were existing local storage allocations there will
                // _not_ be any more sleds to try and the user will see a
                // capacity error.

                for valid_allocation in validated_allocations {
                    let ValidatedAllocations { allocations } = valid_allocation;

                    info!(
                        &log,
                        "calling insert with local storage allocations";
                        "allocations" => ?allocations,
                    );

                    // Try to INSERT the record plus the new local storage
                    // allocations. If this is still a valid target and the new
                    // local storage allocations still fit, we'll use it. If it
                    // isn't a valid target, we'll shrink the set of viable sled
                    // targets and try again.
                    let rows_inserted = sled_insert_resource_query(
                        &resource,
                        &LocalStorageAllocationRequired::Yes { allocations },
                    )
                    .execute_async(&*conn)
                    .await?;

                    if rows_inserted > 0 {
                        info!(&log, "reservation succeeded!");
                        return Ok(resource);
                    }
                    info!(&log, "reservation failed");
                }
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
            .filter(resource_dsl::id.eq(to_db_typed_uuid(vmm_id)))
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

        let sled_id = to_db_typed_uuid(authz_sled.id());
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

        let sled_id = to_db_typed_uuid(authz_sled.id());
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
    use crate::db;
    use crate::db::datastore::test_utils::{
        Expected, IneligibleSleds, sled_set_policy, sled_set_state,
    };
    use crate::db::model::ByteCount;
    use crate::db::model::SqlU32;
    use crate::db::model::Zpool;
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
    use nexus_db_model::{InstanceCpuPlatform, PhysicalDisk};
    use nexus_types::external_api::params;
    use nexus_types::identity::Asset;
    use nexus_types::identity::Resource;
    use omicron_common::api::external;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::AffinityGroupUuid;
    use omicron_uuid_kinds::AntiAffinityGroupUuid;
    use omicron_uuid_kinds::BlueprintUuid;
    use omicron_uuid_kinds::CollectionUuid;
    use omicron_uuid_kinds::ExternalZpoolUuid;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::PhysicalDiskUuid;
    use omicron_uuid_kinds::SledUuid;
    use predicates::{BoxPredicate, prelude::*};
    use std::collections::HashMap;
    use std::net::SocketAddrV6;

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
            non_provisionable: non_provisionable_sled.id(),
            expunged: expunged_sled.id(),
            decommissioned: decommissioned_sled.id(),
            illegal_decommissioned: illegal_decommissioned_sled.id(),
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
                resource.sled_id(),
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
        cpu_platform: Option<db::model::InstanceCpuPlatform>,
    }

    struct FindTargetsOutput {
        id: SledUuid,
        fits: bool,
        affinity_policy: Option<AffinityPolicy>,
        anti_affinity_policy: Option<AffinityPolicy>,
    }

    impl Instance {
        fn new() -> Self {
            Self::new_with_id(InstanceUuid::new_v4())
        }

        fn new_with_id(id: InstanceUuid) -> Self {
            Self {
                id,
                groups: vec![],
                force_onto_sled: None,
                resources: small_resource_request(),
                cpu_platform: None,
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
                self.cpu_platform.map(|p| p.compatible_sled_cpu_families());

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
                    FindTargetsOutput {
                        id: SledUuid::from_untyped_uuid(id),
                        fits,
                        affinity_policy,
                        anti_affinity_policy,
                    }
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

            sled_insert_resource_query(
                &resource,
                &LocalStorageAllocationRequired::No,
            )
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
        fn sled(mut self, sled: SledUuid) -> Self {
            self.force_onto_sled = Some(sled);
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
            constraints.must_select_from(&[sled_target])
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
        assert_eq!(resource.sled_id(), sleds[2].id());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Anti-Affinity, Policy = Allow
    //
    // Create two sleds, put instances on each belonging to an anti-affinity
    // group.  We can continue to add instances belonging to that anti-affinity
    // group, because it has "AffinityPolicy::Allow".
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
            [sleds[0].id(), sleds[1].id()].contains(&resource.sled_id()),
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
        assert_eq!(resource.sled_id(), sleds[0].id());

        let another_test_instance = Instance::new().group("affinity");
        let resource = another_test_instance
            .add_to_groups_and_reserve(&opctx, &datastore, &all_groups)
            .await
            .expect("Should have placed instance (again)");
        assert_eq!(resource.sled_id(), sleds[0].id());

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

        // We are constrained so that an instance belonging to both groups must
        // be placed on both sled 0 and sled 1. This cannot be satisfied, and it
        // returns an error.
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
        assert_eq!(reservation.sled_id(), sleds[1].id());

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
            [sleds[0].id(), sleds[1].id()].contains(&resource.sled_id()),
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

        assert_eq!(resource.sled_id(), sleds[2].id());

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
        // Sled 1 contains an affinity and anti-affinity group, so they cancel
        // out.  This gives it "no priority".
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

        assert_eq!(resource.sled_id(), sleds[3].id());

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

        assert_eq!(resource.sled_id(), sleds[1].id());

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

        // Only "sleds[2]" has space, even though it also contains an
        // anti-affinity group.  However, if we don't belong to this group, we
        // won't care.

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

        assert_eq!(resource.sled_id(), sleds[2].id());

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
            .find(|sled| sled.id == sleds[0].id())
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
                        sleds[i].id(),
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
                    sleds[0].id(),
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
            .find(|sled| sled.id == sleds[0].id())
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
                    sleds[0].id(),
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
                    sleds[1].id(),
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
        assert_eq!(possible_sleds[0].id, sleds[1].id());

        // Inserting onto sleds[0, 2, 3] should fail - there shouldn't
        // be enough space on these sleds.
        for i in [0, 2, 3] {
            assert!(
                !test_instance
                    .insert_resource(
                        &datastore,
                        PropolisUuid::new_v4(),
                        sleds[i].id(),
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
                    sleds[1].id(),
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
        for platform in [None, Some(InstanceCpuPlatform::AmdMilan)] {
            test_instance.cpu_platform = platform;
            let possible_sleds = test_instance.find_targets(&datastore).await;
            assert_eq!(possible_sleds.len(), 4);
        }

        test_instance.cpu_platform = Some(InstanceCpuPlatform::AmdTurin);
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
        let sled_id = sled.id();

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
            sled_id,
        );
        let disk2 = PhysicalDisk::new(
            PhysicalDiskUuid::new_v4(),
            "vendor2".to_string(),
            "serial2".to_string(),
            "model2".to_string(),
            PhysicalDiskKind::U2,
            sled_id,
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

    // local storage allocation tests

    struct LocalStorageTest {
        sleds: Vec<LocalStorageTestSled>,
        affinity_groups: Vec<LocalStorageAffinityGroup>,
        anti_affinity_groups: Vec<LocalStorageAntiAffinityGroup>,
        instances: Vec<LocalStorageTestInstance>,
    }

    struct LocalStorageTestSled {
        sled_id: SledUuid,
        sled_serial: String,
        u2s: Vec<LocalStorageTestSledU2>,
    }

    struct LocalStorageTestSledU2 {
        physical_disk_id: PhysicalDiskUuid,
        physical_disk_serial: String,

        zpool_id: ZpoolUuid,
        control_plane_storage_buffer: external::ByteCount,

        inventory_total_size: external::ByteCount,

        crucible_dataset_id: DatasetUuid,
        crucible_dataset_addr: SocketAddrV6,

        local_storage_dataset_id: DatasetUuid,
    }

    struct LocalStorageAffinityGroup {
        id: AffinityGroupUuid,
        name: String,
        policy: external::AffinityPolicy,
        failure_domain: external::FailureDomain,
    }

    struct LocalStorageAntiAffinityGroup {
        id: AntiAffinityGroupUuid,
        name: String,
        policy: external::AffinityPolicy,
        failure_domain: external::FailureDomain,
    }

    struct LocalStorageTestInstance {
        id: InstanceUuid,
        name: String,
        affinity: Option<(Affinity, usize)>,
        disks: Vec<LocalStorageTestInstanceDisk>,
    }

    struct LocalStorageTestInstanceDisk {
        id: Uuid,
        name: external::Name,
        size: external::ByteCount,
    }

    async fn setup_local_storage_allocation_test(
        opctx: &OpContext,
        datastore: &DataStore,
        config: &LocalStorageTest,
    ) {
        for sled_config in &config.sleds {
            let sled = SledUpdate::new(
                sled_config.sled_id,
                "[::1]:0".parse().unwrap(),
                0,
                db::model::SledBaseboard {
                    serial_number: sled_config.sled_serial.clone(),
                    part_number: "test-pn".to_string(),
                    revision: 0,
                },
                db::model::SledSystemHardware {
                    is_scrimlet: false,
                    usable_hardware_threads: 128,
                    usable_physical_ram: (64 << 30).try_into().unwrap(),
                    reservoir_size: (16 << 30).try_into().unwrap(),
                    cpu_family: SledCpuFamily::AmdMilan,
                },
                Uuid::new_v4(),
                Generation::new(),
            );

            datastore.sled_upsert(sled).await.expect("failed to upsert sled");

            for u2 in &sled_config.u2s {
                let physical_disk = db::model::PhysicalDisk::new(
                    u2.physical_disk_id,
                    String::from("vendor"),
                    u2.physical_disk_serial.clone(),
                    String::from("model"),
                    db::model::PhysicalDiskKind::U2,
                    sled_config.sled_id,
                );

                datastore
                    .physical_disk_insert(opctx, physical_disk)
                    .await
                    .unwrap();

                let zpool = Zpool::new(
                    u2.zpool_id,
                    sled_config.sled_id,
                    u2.physical_disk_id,
                    u2.control_plane_storage_buffer.into(),
                );

                datastore
                    .zpool_insert(opctx, zpool)
                    .await
                    .expect("failed to upsert zpool");

                add_inventory_row_for_zpool(
                    datastore,
                    u2.zpool_id,
                    sled_config.sled_id,
                    u2.inventory_total_size.into(),
                )
                .await;

                add_crucible_dataset(
                    datastore,
                    u2.crucible_dataset_id,
                    u2.zpool_id,
                    u2.crucible_dataset_addr,
                )
                .await;

                add_local_storage_dataset(
                    opctx,
                    datastore,
                    u2.local_storage_dataset_id,
                    u2.zpool_id,
                )
                .await;
            }
        }

        let (authz_project, _project) =
            create_project(opctx, datastore, "project").await;

        for group in &config.affinity_groups {
            datastore
                .affinity_group_create(
                    &opctx,
                    &authz_project,
                    db::model::AffinityGroup {
                        identity: db::model::AffinityGroupIdentity::new(
                            group.id.into_untyped_uuid(),
                            external::IdentityMetadataCreateParams {
                                name: group.name.parse().unwrap(),
                                description: String::from("desc"),
                            },
                        ),
                        project_id: authz_project.id(),
                        policy: group.policy.into(),
                        failure_domain: group.failure_domain.into(),
                    },
                )
                .await
                .unwrap();
        }

        for group in &config.anti_affinity_groups {
            datastore
                .anti_affinity_group_create(
                    &opctx,
                    &authz_project,
                    db::model::AntiAffinityGroup {
                        identity: db::model::AntiAffinityGroupIdentity::new(
                            group.id.into_untyped_uuid(),
                            external::IdentityMetadataCreateParams {
                                name: group.name.parse().unwrap(),
                                description: String::from("desc"),
                            },
                        ),
                        project_id: authz_project.id(),
                        policy: group.policy.into(),
                        failure_domain: group.failure_domain.into(),
                    },
                )
                .await
                .unwrap();
        }

        for instance in &config.instances {
            let authz_instance = create_test_instance(
                datastore,
                opctx,
                &authz_project,
                instance.id,
                &instance.name.as_str(),
            )
            .await;

            if let Some((affinity, index)) = &instance.affinity {
                match affinity {
                    Affinity::Positive => {
                        let group = &config.affinity_groups[*index];
                        add_instance_to_affinity_group(
                            &datastore,
                            group.id,
                            instance.id,
                        )
                        .await;
                    }

                    Affinity::Negative => {
                        let group = &config.anti_affinity_groups[*index];
                        add_instance_to_anti_affinity_group(
                            &datastore,
                            group.id,
                            instance.id,
                        )
                        .await;
                    }
                }
            }

            for disk in &instance.disks {
                let params = params::DiskCreate {
                    identity: external::IdentityMetadataCreateParams {
                        name: disk.name.clone(),
                        description: String::from("desc"),
                    },

                    disk_backend: params::DiskBackend::Local {},

                    size: disk.size,
                };

                datastore
                    .project_create_disk(
                        &opctx,
                        &authz_project,
                        db::datastore::Disk::LocalStorage(
                            db::datastore::LocalStorageDisk::new(
                                db::model::Disk::new(
                                    disk.id,
                                    authz_project.id(),
                                    &params,
                                    db::model::BlockSize::AdvancedFormat,
                                    db::model::DiskRuntimeState::new(),
                                    db::model::DiskType::LocalStorage,
                                ),
                                db::model::DiskTypeLocalStorage::new(
                                    disk.id, disk.size,
                                )
                                .unwrap(),
                            ),
                        ),
                    )
                    .await
                    .unwrap();

                let (.., authz_disk) = LookupPath::new(&opctx, datastore)
                    .disk_id(disk.id)
                    .lookup_for(authz::Action::Read)
                    .await
                    .unwrap();

                datastore
                    .instance_attach_disk(
                        &opctx,
                        &authz_instance,
                        &authz_disk,
                        MAX_DISKS_PER_INSTANCE,
                    )
                    .await
                    .unwrap();
            }
        }
    }

    async fn add_inventory_row_for_zpool(
        datastore: &DataStore,
        zpool_id: ZpoolUuid,
        sled_id: SledUuid,
        total_size: ByteCount,
    ) {
        use nexus_db_schema::schema::inv_zpool::dsl;

        let inv_collection_id = CollectionUuid::new_v4();
        let time_collected = Utc::now();
        let inv_pool = nexus_db_model::InvZpool {
            inv_collection_id: inv_collection_id.into(),
            time_collected,
            id: zpool_id.into(),
            sled_id: to_db_typed_uuid(sled_id),
            total_size,
        };

        diesel::insert_into(dsl::inv_zpool)
            .values(inv_pool)
            .execute_async(
                &*datastore.pool_connection_for_tests().await.unwrap(),
            )
            .await
            .unwrap();
    }

    async fn add_crucible_dataset(
        datastore: &DataStore,
        dataset_id: DatasetUuid,
        pool_id: ZpoolUuid,
        addr: SocketAddrV6,
    ) -> DatasetUuid {
        let dataset =
            db::model::CrucibleDataset::new(dataset_id, pool_id, addr);

        datastore.crucible_dataset_upsert(dataset).await.unwrap();

        dataset_id
    }

    async fn set_crucible_dataset_size_used(
        datastore: &DataStore,
        crucible_dataset_id: DatasetUuid,
        size_used: ByteCount,
    ) {
        use nexus_db_schema::schema::crucible_dataset::dsl;

        let conn = datastore.pool_connection_for_tests().await.unwrap();

        diesel::update(dsl::crucible_dataset)
            .filter(dsl::id.eq(to_db_typed_uuid(crucible_dataset_id)))
            .set(dsl::size_used.eq(size_used))
            .execute_async(&*conn)
            .await
            .unwrap();
    }

    async fn add_local_storage_dataset(
        opctx: &OpContext,
        datastore: &DataStore,
        dataset_id: DatasetUuid,
        pool_id: ZpoolUuid,
    ) -> DatasetUuid {
        let dataset = db::model::RendezvousLocalStorageDataset::new(
            dataset_id,
            pool_id,
            BlueprintUuid::new_v4(),
        );

        datastore
            .local_storage_dataset_insert_if_not_exists(opctx, dataset)
            .await
            .unwrap();

        dataset_id
    }

    async fn set_local_storage_dataset_no_provision(
        datastore: &DataStore,
        local_storage_dataset_id: DatasetUuid,
    ) {
        use nexus_db_schema::schema::rendezvous_local_storage_dataset::dsl;

        let conn = datastore.pool_connection_for_tests().await.unwrap();

        diesel::update(dsl::rendezvous_local_storage_dataset)
            .filter(dsl::id.eq(to_db_typed_uuid(local_storage_dataset_id)))
            .set(dsl::no_provision.eq(true))
            .execute_async(&*conn)
            .await
            .unwrap();
    }

    async fn set_local_storage_allocation(
        datastore: &DataStore,
        disk_id: Uuid,
        local_storage_dataset_id: DatasetUuid,
        pool_id: ExternalZpoolUuid,
        sled_id: SledUuid,
        dataset_size: ByteCount,
    ) {
        let allocation_id = DatasetUuid::new_v4();

        let allocation =
            db::model::LocalStorageDatasetAllocation::new_for_tests_only(
                allocation_id,
                Utc::now(),
                local_storage_dataset_id,
                pool_id,
                sled_id,
                dataset_size,
            );

        let conn = datastore.pool_connection_for_tests().await.unwrap();

        {
            use nexus_db_schema::schema::local_storage_dataset_allocation::dsl;
            diesel::insert_into(dsl::local_storage_dataset_allocation)
                .values(allocation)
                .execute_async(&*conn)
                .await
                .unwrap();
        }

        {
            use nexus_db_schema::schema::disk_type_local_storage::dsl;

            let rows_updated = diesel::update(dsl::disk_type_local_storage)
                .filter(dsl::disk_id.eq(disk_id))
                .set(
                    dsl::local_storage_dataset_allocation_id
                        .eq(to_db_typed_uuid(allocation_id)),
                )
                .execute_async(&*conn)
                .await
                .unwrap();

            assert_eq!(rows_updated, 1);
        }
    }

    async fn create_test_instance(
        datastore: &DataStore,
        opctx: &OpContext,
        authz_project: &authz::Project,
        instance_id: InstanceUuid,
        name: &str,
    ) -> authz::Instance {
        datastore
            .project_create_instance(
                &opctx,
                &authz_project,
                db::model::Instance::new(
                    instance_id,
                    authz_project.id(),
                    &params::InstanceCreate {
                        identity: external::IdentityMetadataCreateParams {
                            name: name.parse().unwrap(),
                            description: "It's an instance".into(),
                        },
                        ncpus: 2i64.try_into().unwrap(),
                        memory: external::ByteCount::from_gibibytes_u32(16),
                        hostname: "myhostname".try_into().unwrap(),
                        user_data: Vec::new(),
                        network_interfaces:
                            params::InstanceNetworkInterfaceAttachment::None,
                        external_ips: Vec::new(),
                        disks: Vec::new(),
                        boot_disk: None,
                        cpu_platform: None,
                        ssh_public_keys: None,
                        start: false,
                        auto_restart_policy: Default::default(),
                        anti_affinity_groups: Vec::new(),
                        multicast_groups: Vec::new(),
                    },
                ),
            )
            .await
            .expect("instance must be created successfully");

        let (.., authz_instance) = LookupPath::new(&opctx, datastore)
            .instance_id(instance_id.into_untyped_uuid())
            .lookup_for(authz::Action::Modify)
            .await
            .expect("instance must exist");

        authz_instance
    }

    #[tokio::test]
    async fn local_storage_allocation() {
        let logctx = dev::test_setup_log("local_storage_allocation");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let config = LocalStorageTest {
            // One sled, with one U2
            sleds: vec![LocalStorageTestSled {
                sled_id: SledUuid::new_v4(),
                sled_serial: String::from("sled_0"),
                u2s: vec![LocalStorageTestSledU2 {
                    physical_disk_id: PhysicalDiskUuid::new_v4(),
                    physical_disk_serial: String::from("phys0"),

                    zpool_id: ZpoolUuid::new_v4(),
                    control_plane_storage_buffer:
                        external::ByteCount::from_gibibytes_u32(250),

                    inventory_total_size:
                        external::ByteCount::from_gibibytes_u32(1024),

                    crucible_dataset_id: DatasetUuid::new_v4(),
                    crucible_dataset_addr: "[fd00:1122:3344:101::1]:12345"
                        .parse()
                        .unwrap(),

                    local_storage_dataset_id: DatasetUuid::new_v4(),
                }],
            }],
            affinity_groups: vec![],
            anti_affinity_groups: vec![],
            // Configure an instance with one local storage disk
            instances: vec![LocalStorageTestInstance {
                id: InstanceUuid::new_v4(),
                name: "local".to_string(),
                affinity: None,
                disks: vec![LocalStorageTestInstanceDisk {
                    id: Uuid::new_v4(),
                    name: external::Name::try_from("local".to_string())
                        .unwrap(),
                    size: external::ByteCount::from_gibibytes_u32(512),
                }],
            }],
        };

        setup_local_storage_allocation_test(&opctx, datastore, &config).await;
        let instance = Instance::new_with_id(config.instances[0].id);

        // the output of the find targets query does not currently take required
        // local storage allocations into account
        assert_eq!(instance.find_targets(datastore).await.len(), 1);

        // make sure that insertion works
        let vmm = datastore
            .sled_reservation_create(
                opctx,
                instance.id,
                PropolisUuid::new_v4(),
                db::model::Resources::new(
                    1,
                    ByteCount::try_from(1024).unwrap(),
                    ByteCount::try_from(1024).unwrap(),
                ),
                db::model::SledReservationConstraints::none(),
            )
            .await
            .unwrap();

        assert_eq!(vmm.sled_id, config.sleds[0].sled_id.into());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Ensure that an existing allocation restricts where a VMM can be
    #[tokio::test]
    async fn local_storage_allocation_existing_allocation() {
        let logctx =
            dev::test_setup_log("local_storage_allocation_existing_allocation");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let config = LocalStorageTest {
            // Two sleds, each with one U2
            sleds: vec![
                LocalStorageTestSled {
                    sled_id: SledUuid::new_v4(),
                    sled_serial: String::from("sled_0"),
                    u2s: vec![LocalStorageTestSledU2 {
                        physical_disk_id: PhysicalDiskUuid::new_v4(),
                        physical_disk_serial: String::from("phys0"),

                        zpool_id: ZpoolUuid::new_v4(),
                        control_plane_storage_buffer:
                            external::ByteCount::from_gibibytes_u32(250),

                        inventory_total_size:
                            external::ByteCount::from_gibibytes_u32(1024),

                        crucible_dataset_id: DatasetUuid::new_v4(),
                        crucible_dataset_addr: "[fd00:1122:3344:101::1]:12345"
                            .parse()
                            .unwrap(),

                        local_storage_dataset_id: DatasetUuid::new_v4(),
                    }],
                },
                LocalStorageTestSled {
                    sled_id: SledUuid::new_v4(),
                    sled_serial: String::from("sled_1"),
                    u2s: vec![LocalStorageTestSledU2 {
                        physical_disk_id: PhysicalDiskUuid::new_v4(),
                        physical_disk_serial: String::from("phys1"),

                        zpool_id: ZpoolUuid::new_v4(),
                        control_plane_storage_buffer:
                            external::ByteCount::from_gibibytes_u32(250),

                        inventory_total_size:
                            external::ByteCount::from_gibibytes_u32(1024),

                        crucible_dataset_id: DatasetUuid::new_v4(),
                        crucible_dataset_addr: "[fd00:1122:3344:201::1]:12345"
                            .parse()
                            .unwrap(),

                        local_storage_dataset_id: DatasetUuid::new_v4(),
                    }],
                },
            ],
            affinity_groups: vec![],
            anti_affinity_groups: vec![],
            // Configure an instance with one local storage disk
            instances: vec![LocalStorageTestInstance {
                id: InstanceUuid::new_v4(),
                name: "local".to_string(),
                affinity: None,
                disks: vec![LocalStorageTestInstanceDisk {
                    id: Uuid::new_v4(),
                    name: external::Name::try_from("local".to_string())
                        .unwrap(),
                    size: external::ByteCount::from_gibibytes_u32(512),
                }],
            }],
        };

        setup_local_storage_allocation_test(&opctx, datastore, &config).await;
        let instance = Instance::new_with_id(config.instances[0].id);

        // Add an allocation for this disk to the first sled's zpool
        set_local_storage_allocation(
            datastore,
            config.instances[0].disks[0].id,
            config.sleds[0].u2s[0].local_storage_dataset_id,
            ExternalZpoolUuid::from_untyped_uuid(
                config.sleds[0].u2s[0].zpool_id.into_untyped_uuid(),
            ),
            config.sleds[0].sled_id,
            external::ByteCount::from_gibibytes_u32(512 + 10).into(),
        )
        .await;

        // the output of the find targets query does not currently take required
        // local storage allocations into account
        assert_eq!(instance.find_targets(datastore).await.len(), 2);

        // insertion should succeed but never be on the second sled
        let vmm = datastore
            .sled_reservation_create(
                opctx,
                instance.id,
                PropolisUuid::new_v4(),
                db::model::Resources::new(
                    1,
                    ByteCount::try_from(1024).unwrap(),
                    ByteCount::try_from(1024).unwrap(),
                ),
                db::model::SledReservationConstraints::none(),
            )
            .await
            .unwrap();

        assert_eq!(vmm.sled_id, config.sleds[0].sled_id.into());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Ensure that `no_provision` on the local storage dataset is respected
    #[tokio::test]
    async fn local_storage_allocation_fail_no_provision() {
        let logctx =
            dev::test_setup_log("local_storage_allocation_fail_no_provision");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let config = LocalStorageTest {
            // One sled, with one U2
            sleds: vec![LocalStorageTestSled {
                sled_id: SledUuid::new_v4(),
                sled_serial: String::from("sled_0"),
                u2s: vec![LocalStorageTestSledU2 {
                    physical_disk_id: PhysicalDiskUuid::new_v4(),
                    physical_disk_serial: String::from("phys0"),

                    zpool_id: ZpoolUuid::new_v4(),
                    control_plane_storage_buffer:
                        external::ByteCount::from_gibibytes_u32(250),

                    inventory_total_size:
                        external::ByteCount::from_gibibytes_u32(1024),

                    crucible_dataset_id: DatasetUuid::new_v4(),
                    crucible_dataset_addr: "[fd00:1122:3344:101::1]:12345"
                        .parse()
                        .unwrap(),

                    local_storage_dataset_id: DatasetUuid::new_v4(),
                }],
            }],
            affinity_groups: vec![],
            anti_affinity_groups: vec![],
            // Configure an instance with one local storage disk
            instances: vec![LocalStorageTestInstance {
                id: InstanceUuid::new_v4(),
                name: "local".to_string(),
                affinity: None,
                disks: vec![LocalStorageTestInstanceDisk {
                    id: Uuid::new_v4(),
                    name: external::Name::try_from("local".to_string())
                        .unwrap(),
                    size: external::ByteCount::from_gibibytes_u32(512),
                }],
            }],
        };

        setup_local_storage_allocation_test(&opctx, datastore, &config).await;
        let instance = Instance::new_with_id(config.instances[0].id);

        set_local_storage_dataset_no_provision(
            datastore,
            config.sleds[0].u2s[0].local_storage_dataset_id,
        )
        .await;

        // the output of the find targets query does not currently take required
        // local storage allocations into account
        assert_eq!(instance.find_targets(datastore).await.len(), 1);

        // make sure that insertion fails
        datastore
            .sled_reservation_create(
                opctx,
                instance.id,
                PropolisUuid::new_v4(),
                db::model::Resources::new(
                    1,
                    ByteCount::try_from(1024).unwrap(),
                    ByteCount::try_from(1024).unwrap(),
                ),
                db::model::SledReservationConstraints::none(),
            )
            .await
            .unwrap_err();

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Ensure that local storage allocation does not run the zpool out of space
    #[tokio::test]
    async fn local_storage_allocation_fail_no_space() {
        let logctx =
            dev::test_setup_log("local_storage_allocation_fail_no_space");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let config = LocalStorageTest {
            // One sled, with one U2
            sleds: vec![LocalStorageTestSled {
                sled_id: SledUuid::new_v4(),
                sled_serial: String::from("sled_0"),
                u2s: vec![LocalStorageTestSledU2 {
                    physical_disk_id: PhysicalDiskUuid::new_v4(),
                    physical_disk_serial: String::from("phys0"),

                    zpool_id: ZpoolUuid::new_v4(),
                    control_plane_storage_buffer:
                        external::ByteCount::from_gibibytes_u32(250),

                    inventory_total_size:
                        external::ByteCount::from_gibibytes_u32(1024),

                    crucible_dataset_id: DatasetUuid::new_v4(),
                    crucible_dataset_addr: "[fd00:1122:3344:101::1]:12345"
                        .parse()
                        .unwrap(),

                    local_storage_dataset_id: DatasetUuid::new_v4(),
                }],
            }],
            affinity_groups: vec![],
            anti_affinity_groups: vec![],
            // Configure an instance with one local storage disk
            instances: vec![LocalStorageTestInstance {
                id: InstanceUuid::new_v4(),
                name: "local".to_string(),
                affinity: None,
                disks: vec![LocalStorageTestInstanceDisk {
                    id: Uuid::new_v4(),
                    name: external::Name::try_from("local".to_string())
                        .unwrap(),
                    size: external::ByteCount::from_gibibytes_u32(512),
                }],
            }],
        };

        setup_local_storage_allocation_test(&opctx, datastore, &config).await;
        let instance = Instance::new_with_id(config.instances[0].id);

        // The zpool size is 1 TiB, and the control plane buffer is 250 GiB. If
        // we set a crucible dataset size_used of 300 GiB, then ensure a 512 GiB
        // allocation won't work.

        set_crucible_dataset_size_used(
            datastore,
            config.sleds[0].u2s[0].crucible_dataset_id,
            external::ByteCount::from_gibibytes_u32(300).into(),
        )
        .await;

        // the output of the find targets query does not currently take required
        // local storage allocations into account
        assert_eq!(instance.find_targets(datastore).await.len(), 1);

        // make sure that insertion fails
        datastore
            .sled_reservation_create(
                opctx,
                instance.id,
                PropolisUuid::new_v4(),
                db::model::Resources::new(
                    1,
                    ByteCount::try_from(1024).unwrap(),
                    ByteCount::try_from(1024).unwrap(),
                ),
                db::model::SledReservationConstraints::none(),
            )
            .await
            .unwrap_err();

        // if someone frees up some crucible disks, then insertion should work
        set_crucible_dataset_size_used(
            datastore,
            config.sleds[0].u2s[0].crucible_dataset_id,
            external::ByteCount::from_gibibytes_u32(200).into(),
        )
        .await;

        let vmm = datastore
            .sled_reservation_create(
                opctx,
                instance.id,
                PropolisUuid::new_v4(),
                db::model::Resources::new(
                    1,
                    ByteCount::try_from(1024).unwrap(),
                    ByteCount::try_from(1024).unwrap(),
                ),
                db::model::SledReservationConstraints::none(),
            )
            .await
            .unwrap();

        assert_eq!(vmm.sled_id, config.sleds[0].sled_id.into());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Ensure that local storage allocations do not share a U2
    #[tokio::test]
    async fn local_storage_allocation_fail_no_share_u2() {
        let logctx =
            dev::test_setup_log("local_storage_allocation_fail_no_share_u2");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let config = LocalStorageTest {
            // One sled, with one U2
            sleds: vec![LocalStorageTestSled {
                sled_id: SledUuid::new_v4(),
                sled_serial: String::from("sled_0"),
                u2s: vec![LocalStorageTestSledU2 {
                    physical_disk_id: PhysicalDiskUuid::new_v4(),
                    physical_disk_serial: String::from("phys0"),

                    zpool_id: ZpoolUuid::new_v4(),
                    control_plane_storage_buffer:
                        external::ByteCount::from_gibibytes_u32(250),

                    inventory_total_size:
                        external::ByteCount::from_gibibytes_u32(1024),

                    crucible_dataset_id: DatasetUuid::new_v4(),
                    crucible_dataset_addr: "[fd00:1122:3344:101::1]:12345"
                        .parse()
                        .unwrap(),

                    local_storage_dataset_id: DatasetUuid::new_v4(),
                }],
            }],
            affinity_groups: vec![],
            anti_affinity_groups: vec![],
            // Configure an instance with two local storage disks
            instances: vec![LocalStorageTestInstance {
                id: InstanceUuid::new_v4(),
                name: "local".to_string(),
                affinity: None,
                disks: vec![
                    LocalStorageTestInstanceDisk {
                        id: Uuid::new_v4(),
                        name: external::Name::try_from("local1".to_string())
                            .unwrap(),
                        size: external::ByteCount::from_gibibytes_u32(64),
                    },
                    LocalStorageTestInstanceDisk {
                        id: Uuid::new_v4(),
                        name: external::Name::try_from("local2".to_string())
                            .unwrap(),
                        size: external::ByteCount::from_gibibytes_u32(64),
                    },
                ],
            }],
        };

        setup_local_storage_allocation_test(&opctx, datastore, &config).await;
        let instance = Instance::new_with_id(config.instances[0].id);

        // the output of the find targets query does not currently take required
        // local storage allocations into account
        assert_eq!(instance.find_targets(datastore).await.len(), 1);

        // make sure that insertion fails
        datastore
            .sled_reservation_create(
                opctx,
                instance.id,
                PropolisUuid::new_v4(),
                db::model::Resources::new(
                    1,
                    ByteCount::try_from(1024).unwrap(),
                    ByteCount::try_from(1024).unwrap(),
                ),
                db::model::SledReservationConstraints::none(),
            )
            .await
            .unwrap_err();

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Ensure that multiple local storage allocations can use multiple U2s
    #[tokio::test]
    async fn local_storage_allocation_multiple_u2() {
        let logctx =
            dev::test_setup_log("local_storage_allocation_multiple_u2");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let config = LocalStorageTest {
            // One sled, with two U2s
            sleds: vec![LocalStorageTestSled {
                sled_id: SledUuid::new_v4(),
                sled_serial: String::from("sled_0"),
                u2s: vec![
                    LocalStorageTestSledU2 {
                        physical_disk_id: PhysicalDiskUuid::new_v4(),
                        physical_disk_serial: String::from("phys0"),

                        zpool_id: ZpoolUuid::new_v4(),
                        control_plane_storage_buffer:
                            external::ByteCount::from_gibibytes_u32(250),

                        inventory_total_size:
                            external::ByteCount::from_gibibytes_u32(1024),

                        crucible_dataset_id: DatasetUuid::new_v4(),
                        crucible_dataset_addr: "[fd00:1122:3344:101::1]:12345"
                            .parse()
                            .unwrap(),

                        local_storage_dataset_id: DatasetUuid::new_v4(),
                    },
                    LocalStorageTestSledU2 {
                        physical_disk_id: PhysicalDiskUuid::new_v4(),
                        physical_disk_serial: String::from("phys1"),

                        zpool_id: ZpoolUuid::new_v4(),
                        control_plane_storage_buffer:
                            external::ByteCount::from_gibibytes_u32(250),

                        inventory_total_size:
                            external::ByteCount::from_gibibytes_u32(1024),

                        crucible_dataset_id: DatasetUuid::new_v4(),
                        crucible_dataset_addr: "[fd00:1122:3344:201::1]:12345"
                            .parse()
                            .unwrap(),

                        local_storage_dataset_id: DatasetUuid::new_v4(),
                    },
                ],
            }],
            affinity_groups: vec![],
            anti_affinity_groups: vec![],
            // Configure an instance with two local storage disks
            instances: vec![LocalStorageTestInstance {
                id: InstanceUuid::new_v4(),
                name: "local".to_string(),
                affinity: None,
                disks: vec![
                    LocalStorageTestInstanceDisk {
                        id: Uuid::new_v4(),
                        name: external::Name::try_from("local1".to_string())
                            .unwrap(),
                        size: external::ByteCount::from_gibibytes_u32(64),
                    },
                    LocalStorageTestInstanceDisk {
                        id: Uuid::new_v4(),
                        name: external::Name::try_from("local2".to_string())
                            .unwrap(),
                        size: external::ByteCount::from_gibibytes_u32(64),
                    },
                ],
            }],
        };

        setup_local_storage_allocation_test(&opctx, datastore, &config).await;
        let instance = Instance::new_with_id(config.instances[0].id);

        // the output of the find targets query does not currently take required
        // local storage allocations into account
        assert_eq!(instance.find_targets(datastore).await.len(), 1);

        // make sure that insertion succeeds
        let vmm = datastore
            .sled_reservation_create(
                opctx,
                instance.id,
                PropolisUuid::new_v4(),
                db::model::Resources::new(
                    1,
                    ByteCount::try_from(1024).unwrap(),
                    ByteCount::try_from(1024).unwrap(),
                ),
                db::model::SledReservationConstraints::none(),
            )
            .await
            .unwrap();

        assert_eq!(vmm.sled_id, config.sleds[0].sled_id.into());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Ensure that multiple local storage allocations will not partially
    /// succeed
    #[tokio::test]
    async fn local_storage_allocation_fail_no_partial_success() {
        let logctx = dev::test_setup_log(
            "local_storage_allocation_fail_no_partial_success",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let config = LocalStorageTest {
            // One sled, with two U2s
            sleds: vec![LocalStorageTestSled {
                sled_id: SledUuid::new_v4(),
                sled_serial: String::from("sled_0"),
                u2s: vec![
                    LocalStorageTestSledU2 {
                        physical_disk_id: PhysicalDiskUuid::new_v4(),
                        physical_disk_serial: String::from("phys0"),

                        zpool_id: ZpoolUuid::new_v4(),
                        control_plane_storage_buffer:
                            external::ByteCount::from_gibibytes_u32(250),

                        inventory_total_size:
                            external::ByteCount::from_gibibytes_u32(1024),

                        crucible_dataset_id: DatasetUuid::new_v4(),
                        crucible_dataset_addr: "[fd00:1122:3344:101::1]:12345"
                            .parse()
                            .unwrap(),

                        local_storage_dataset_id: DatasetUuid::new_v4(),
                    },
                    LocalStorageTestSledU2 {
                        physical_disk_id: PhysicalDiskUuid::new_v4(),
                        physical_disk_serial: String::from("phys1"),

                        zpool_id: ZpoolUuid::new_v4(),
                        control_plane_storage_buffer:
                            external::ByteCount::from_gibibytes_u32(250),

                        inventory_total_size:
                            external::ByteCount::from_gibibytes_u32(1024),

                        crucible_dataset_id: DatasetUuid::new_v4(),
                        crucible_dataset_addr: "[fd00:1122:3344:201::1]:12345"
                            .parse()
                            .unwrap(),

                        local_storage_dataset_id: DatasetUuid::new_v4(),
                    },
                ],
            }],
            affinity_groups: vec![],
            anti_affinity_groups: vec![],
            // Configure an instance with two local storage disks
            instances: vec![LocalStorageTestInstance {
                id: InstanceUuid::new_v4(),
                name: "local".to_string(),
                affinity: None,
                disks: vec![
                    LocalStorageTestInstanceDisk {
                        id: Uuid::new_v4(),
                        name: external::Name::try_from("local1".to_string())
                            .unwrap(),
                        size: external::ByteCount::from_gibibytes_u32(512),
                    },
                    LocalStorageTestInstanceDisk {
                        id: Uuid::new_v4(),
                        name: external::Name::try_from("local2".to_string())
                            .unwrap(),
                        size: external::ByteCount::from_gibibytes_u32(512),
                    },
                ],
            }],
        };

        setup_local_storage_allocation_test(&opctx, datastore, &config).await;
        let instance = Instance::new_with_id(config.instances[0].id);

        // The zpool size is 1 TiB, and the control plane buffer is 250 GiB. If
        // we set the first U2's crucible dataset size_used of 300 GiB, then
        // we ensure one of the 512 GiB allocations won't work.

        set_crucible_dataset_size_used(
            datastore,
            config.sleds[0].u2s[0].crucible_dataset_id,
            external::ByteCount::from_gibibytes_u32(300).into(),
        )
        .await;

        // the output of the find targets query does not currently take required
        // local storage allocations into account
        assert_eq!(instance.find_targets(datastore).await.len(), 1);

        // make sure that insertion fails
        datastore
            .sled_reservation_create(
                opctx,
                instance.id,
                PropolisUuid::new_v4(),
                db::model::Resources::new(
                    1,
                    ByteCount::try_from(1024).unwrap(),
                    ByteCount::try_from(1024).unwrap(),
                ),
                db::model::SledReservationConstraints::none(),
            )
            .await
            .unwrap_err();

        // Assert no partial allocations occurred
        {
            let (.., authz_instance) = LookupPath::new(&opctx, datastore)
                .instance_id(instance.id.into_untyped_uuid())
                .lookup_for(authz::Action::Modify)
                .await
                .expect("instance must exist");

            let disks = datastore
                .instance_list_disks(
                    &opctx,
                    &authz_instance,
                    &PaginatedBy::Name(DataPageParams {
                        marker: None,
                        direction: dropshot::PaginationOrder::Ascending,
                        limit: std::num::NonZeroU32::new(
                            MAX_DISKS_PER_INSTANCE,
                        )
                        .unwrap(),
                    }),
                )
                .await
                .unwrap();

            for disk in disks {
                let db::datastore::Disk::LocalStorage(disk) = disk else {
                    panic!("wrong disk type");
                };

                assert!(disk.local_storage_dataset_allocation.is_none());
            }
        }

        // if someone frees up some crucible disks, then insertion should work
        set_crucible_dataset_size_used(
            datastore,
            config.sleds[0].u2s[0].crucible_dataset_id,
            external::ByteCount::from_gibibytes_u32(200).into(),
        )
        .await;

        let vmm = datastore
            .sled_reservation_create(
                opctx,
                instance.id,
                PropolisUuid::new_v4(),
                db::model::Resources::new(
                    1,
                    ByteCount::try_from(1024).unwrap(),
                    ByteCount::try_from(1024).unwrap(),
                ),
                db::model::SledReservationConstraints::none(),
            )
            .await
            .unwrap();

        assert_eq!(vmm.sled_id, config.sleds[0].sled_id.into());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Ensure that multiple local storage allocations work with one of them
    /// existing already
    #[tokio::test]
    async fn local_storage_allocation_multiple_with_one_already() {
        let logctx = dev::test_setup_log(
            "local_storage_allocation_multiple_with_one_already",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let config = LocalStorageTest {
            // One sled, with two U2s
            sleds: vec![LocalStorageTestSled {
                sled_id: SledUuid::new_v4(),
                sled_serial: String::from("sled_0"),
                u2s: vec![
                    LocalStorageTestSledU2 {
                        physical_disk_id: PhysicalDiskUuid::new_v4(),
                        physical_disk_serial: String::from("phys0"),

                        zpool_id: ZpoolUuid::new_v4(),
                        control_plane_storage_buffer:
                            external::ByteCount::from_gibibytes_u32(250),

                        inventory_total_size:
                            external::ByteCount::from_gibibytes_u32(1024),

                        crucible_dataset_id: DatasetUuid::new_v4(),
                        crucible_dataset_addr: "[fd00:1122:3344:101::1]:12345"
                            .parse()
                            .unwrap(),

                        local_storage_dataset_id: DatasetUuid::new_v4(),
                    },
                    LocalStorageTestSledU2 {
                        physical_disk_id: PhysicalDiskUuid::new_v4(),
                        physical_disk_serial: String::from("phys1"),

                        zpool_id: ZpoolUuid::new_v4(),
                        control_plane_storage_buffer:
                            external::ByteCount::from_gibibytes_u32(250),

                        inventory_total_size:
                            external::ByteCount::from_gibibytes_u32(1024),

                        crucible_dataset_id: DatasetUuid::new_v4(),
                        crucible_dataset_addr: "[fd00:1122:3344:201::1]:12345"
                            .parse()
                            .unwrap(),

                        local_storage_dataset_id: DatasetUuid::new_v4(),
                    },
                ],
            }],
            affinity_groups: vec![],
            anti_affinity_groups: vec![],
            // Configure an instance with two local storage disks
            instances: vec![LocalStorageTestInstance {
                id: InstanceUuid::new_v4(),
                name: "local".to_string(),
                affinity: None,
                disks: vec![
                    LocalStorageTestInstanceDisk {
                        id: Uuid::new_v4(),
                        name: external::Name::try_from("local1".to_string())
                            .unwrap(),
                        size: external::ByteCount::from_gibibytes_u32(512),
                    },
                    LocalStorageTestInstanceDisk {
                        id: Uuid::new_v4(),
                        name: external::Name::try_from("local2".to_string())
                            .unwrap(),
                        size: external::ByteCount::from_gibibytes_u32(512),
                    },
                ],
            }],
        };

        setup_local_storage_allocation_test(&opctx, datastore, &config).await;
        let instance = Instance::new_with_id(config.instances[0].id);

        // One of the local storage have been allocated already.

        set_local_storage_allocation(
            datastore,
            config.instances[0].disks[0].id,
            config.sleds[0].u2s[0].local_storage_dataset_id,
            ExternalZpoolUuid::from_untyped_uuid(
                config.sleds[0].u2s[0].zpool_id.into_untyped_uuid(),
            ),
            config.sleds[0].sled_id,
            external::ByteCount::from_gibibytes_u32(512 + 10).into(),
        )
        .await;

        // the output of the find targets query does not currently take required
        // local storage allocations into account
        assert_eq!(instance.find_targets(datastore).await.len(), 1);

        let vmm = datastore
            .sled_reservation_create(
                opctx,
                instance.id,
                PropolisUuid::new_v4(),
                db::model::Resources::new(
                    1,
                    ByteCount::try_from(1024).unwrap(),
                    ByteCount::try_from(1024).unwrap(),
                ),
                db::model::SledReservationConstraints::none(),
            )
            .await
            .unwrap();

        assert_eq!(vmm.sled_id, config.sleds[0].sled_id.into());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Ensure that multiple instances will share slices of a single U2 for
    /// local storage allocations
    #[tokio::test]
    async fn local_storage_allocation_multiple_instances_share_u2() {
        let logctx = dev::test_setup_log(
            "local_storage_allocation_multiple_instances_share_u2",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let config = LocalStorageTest {
            // One sled, with two U2s
            sleds: vec![LocalStorageTestSled {
                sled_id: SledUuid::new_v4(),
                sled_serial: String::from("sled_0"),
                u2s: vec![
                    LocalStorageTestSledU2 {
                        physical_disk_id: PhysicalDiskUuid::new_v4(),
                        physical_disk_serial: String::from("phys0"),

                        zpool_id: ZpoolUuid::new_v4(),
                        control_plane_storage_buffer:
                            external::ByteCount::from_gibibytes_u32(250),

                        inventory_total_size:
                            external::ByteCount::from_gibibytes_u32(1024),

                        crucible_dataset_id: DatasetUuid::new_v4(),
                        crucible_dataset_addr: "[fd00:1122:3344:101::1]:12345"
                            .parse()
                            .unwrap(),

                        local_storage_dataset_id: DatasetUuid::new_v4(),
                    },
                    LocalStorageTestSledU2 {
                        physical_disk_id: PhysicalDiskUuid::new_v4(),
                        physical_disk_serial: String::from("phys1"),

                        zpool_id: ZpoolUuid::new_v4(),
                        control_plane_storage_buffer:
                            external::ByteCount::from_gibibytes_u32(250),

                        inventory_total_size:
                            external::ByteCount::from_gibibytes_u32(1024),

                        crucible_dataset_id: DatasetUuid::new_v4(),
                        crucible_dataset_addr: "[fd00:1122:3344:201::1]:12345"
                            .parse()
                            .unwrap(),

                        local_storage_dataset_id: DatasetUuid::new_v4(),
                    },
                ],
            }],
            affinity_groups: vec![],
            anti_affinity_groups: vec![],
            // Configure two instances, each with two local storage disks
            instances: vec![
                LocalStorageTestInstance {
                    id: InstanceUuid::new_v4(),
                    name: "local".to_string(),
                    affinity: None,
                    disks: vec![
                        LocalStorageTestInstanceDisk {
                            id: Uuid::new_v4(),
                            name: external::Name::try_from(
                                "local1".to_string(),
                            )
                            .unwrap(),
                            size: external::ByteCount::from_gibibytes_u32(128),
                        },
                        LocalStorageTestInstanceDisk {
                            id: Uuid::new_v4(),
                            name: external::Name::try_from(
                                "local2".to_string(),
                            )
                            .unwrap(),
                            size: external::ByteCount::from_gibibytes_u32(128),
                        },
                    ],
                },
                LocalStorageTestInstance {
                    id: InstanceUuid::new_v4(),
                    name: "local2".to_string(),
                    affinity: None,
                    disks: vec![
                        LocalStorageTestInstanceDisk {
                            id: Uuid::new_v4(),
                            name: external::Name::try_from(
                                "local21".to_string(),
                            )
                            .unwrap(),
                            size: external::ByteCount::from_gibibytes_u32(128),
                        },
                        LocalStorageTestInstanceDisk {
                            id: Uuid::new_v4(),
                            name: external::Name::try_from(
                                "local22".to_string(),
                            )
                            .unwrap(),
                            size: external::ByteCount::from_gibibytes_u32(128),
                        },
                    ],
                },
            ],
        };

        setup_local_storage_allocation_test(&opctx, datastore, &config).await;

        for instance in &config.instances {
            let instance = Instance::new_with_id(instance.id);

            // the output of the find targets query does not currently take
            // required local storage allocations into account
            assert_eq!(instance.find_targets(datastore).await.len(), 1);

            let vmm = datastore
                .sled_reservation_create(
                    opctx,
                    instance.id,
                    PropolisUuid::new_v4(),
                    db::model::Resources::new(
                        32,
                        ByteCount::try_from(1024).unwrap(),
                        ByteCount::try_from(1024).unwrap(),
                    ),
                    db::model::SledReservationConstraints::none(),
                )
                .await
                .unwrap();

            assert_eq!(vmm.sled_id, config.sleds[0].sled_id.into());
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Ensure that multiple local storage allocations won't work if one of them
    /// exists already but the second would result in the zpool running out of
    /// space
    #[tokio::test]
    async fn local_storage_allocation_fail_multiple_with_existing_no_space() {
        let logctx = dev::test_setup_log(
            "local_storage_allocation_fail_multiple_with_existing_no_space",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let config = LocalStorageTest {
            // One sled, with two U2s
            sleds: vec![LocalStorageTestSled {
                sled_id: SledUuid::new_v4(),
                sled_serial: String::from("sled_0"),
                u2s: vec![
                    LocalStorageTestSledU2 {
                        physical_disk_id: PhysicalDiskUuid::new_v4(),
                        physical_disk_serial: String::from("phys0"),

                        zpool_id: ZpoolUuid::new_v4(),
                        control_plane_storage_buffer:
                            external::ByteCount::from_gibibytes_u32(250),

                        inventory_total_size:
                            external::ByteCount::from_gibibytes_u32(1024),

                        crucible_dataset_id: DatasetUuid::new_v4(),
                        crucible_dataset_addr: "[fd00:1122:3344:101::1]:12345"
                            .parse()
                            .unwrap(),

                        local_storage_dataset_id: DatasetUuid::new_v4(),
                    },
                    LocalStorageTestSledU2 {
                        physical_disk_id: PhysicalDiskUuid::new_v4(),
                        physical_disk_serial: String::from("phys1"),

                        zpool_id: ZpoolUuid::new_v4(),
                        control_plane_storage_buffer:
                            external::ByteCount::from_gibibytes_u32(250),

                        inventory_total_size:
                            external::ByteCount::from_gibibytes_u32(1024),

                        crucible_dataset_id: DatasetUuid::new_v4(),
                        crucible_dataset_addr: "[fd00:1122:3344:201::1]:12345"
                            .parse()
                            .unwrap(),

                        local_storage_dataset_id: DatasetUuid::new_v4(),
                    },
                ],
            }],
            affinity_groups: vec![],
            anti_affinity_groups: vec![],
            // Configure an instance with two local storage disks
            instances: vec![LocalStorageTestInstance {
                id: InstanceUuid::new_v4(),
                name: "local".to_string(),
                affinity: None,
                disks: vec![
                    LocalStorageTestInstanceDisk {
                        id: Uuid::new_v4(),
                        name: external::Name::try_from("local1".to_string())
                            .unwrap(),
                        size: external::ByteCount::from_gibibytes_u32(512),
                    },
                    LocalStorageTestInstanceDisk {
                        id: Uuid::new_v4(),
                        name: external::Name::try_from("local2".to_string())
                            .unwrap(),
                        size: external::ByteCount::from_gibibytes_u32(512),
                    },
                ],
            }],
        };

        setup_local_storage_allocation_test(&opctx, datastore, &config).await;
        let instance = Instance::new_with_id(config.instances[0].id);

        // One of the local storage have been allocated already to the first U2

        set_local_storage_allocation(
            datastore,
            config.instances[0].disks[0].id,
            config.sleds[0].u2s[0].local_storage_dataset_id,
            ExternalZpoolUuid::from_untyped_uuid(
                config.sleds[0].u2s[0].zpool_id.into_untyped_uuid(),
            ),
            config.sleds[0].sled_id,
            external::ByteCount::from_gibibytes_u32(512 + 10).into(),
        )
        .await;

        // The zpool size is 1 TiB, and the control plane buffer is 250 GiB. If
        // we set the second U2's crucible dataset size_used of 300 GiB, then
        // we ensure the second of the 512 GiB allocations won't work.

        set_crucible_dataset_size_used(
            datastore,
            config.sleds[0].u2s[1].crucible_dataset_id,
            external::ByteCount::from_gibibytes_u32(300).into(),
        )
        .await;

        // the output of the find targets query does not currently take required
        // local storage allocations into account
        assert_eq!(instance.find_targets(datastore).await.len(), 1);

        datastore
            .sled_reservation_create(
                opctx,
                instance.id,
                PropolisUuid::new_v4(),
                db::model::Resources::new(
                    1,
                    ByteCount::try_from(1024).unwrap(),
                    ByteCount::try_from(1024).unwrap(),
                ),
                db::model::SledReservationConstraints::none(),
            )
            .await
            .unwrap_err();

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Ensure that a full rack can have one VMM take all the U2s on each sled
    #[tokio::test]
    async fn local_storage_allocation_full_rack() {
        let logctx = dev::test_setup_log("local_storage_allocation_full_rack");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let mut config = LocalStorageTest {
            sleds: vec![],
            affinity_groups: vec![],
            anti_affinity_groups: vec![],
            instances: vec![],
        };

        for i in 0..32 {
            let mut u2s = vec![];

            for n in 0..MAX_DISKS_PER_INSTANCE {
                u2s.push(LocalStorageTestSledU2 {
                    physical_disk_id: PhysicalDiskUuid::new_v4(),
                    physical_disk_serial: format!("phys_{i}_{n}"),

                    zpool_id: ZpoolUuid::new_v4(),
                    control_plane_storage_buffer:
                        external::ByteCount::from_gibibytes_u32(250),

                    inventory_total_size:
                        external::ByteCount::from_gibibytes_u32(1024),

                    crucible_dataset_id: DatasetUuid::new_v4(),
                    crucible_dataset_addr: format!(
                        "[fd00:1122:3344:{i}0{n}::1]:12345"
                    )
                    .parse()
                    .unwrap(),

                    local_storage_dataset_id: DatasetUuid::new_v4(),
                });
            }

            config.sleds.push(LocalStorageTestSled {
                sled_id: SledUuid::new_v4(),
                sled_serial: format!("sled_{i}"),
                u2s,
            });

            let mut disks = vec![];

            for n in 0..MAX_DISKS_PER_INSTANCE {
                disks.push(LocalStorageTestInstanceDisk {
                    id: Uuid::new_v4(),
                    name: external::Name::try_from(format!("local-{i}-{n}"))
                        .unwrap(),
                    size: external::ByteCount::from_gibibytes_u32(512),
                });
            }

            config.instances.push(LocalStorageTestInstance {
                id: InstanceUuid::new_v4(),
                name: format!("inst{i}"),
                affinity: None,
                disks,
            });
        }

        setup_local_storage_allocation_test(&opctx, datastore, &config).await;

        let mut vmms = vec![];

        for (i, config_instance) in config.instances.iter().enumerate() {
            let instance = Instance::new_with_id(config_instance.id);

            // the output of the find targets query does not currently take
            // required local storage allocations into account, but each VMM
            // will occupy all the available threads on a sled.
            assert_eq!(instance.find_targets(datastore).await.len(), 32 - i);

            let vmm = datastore
                .sled_reservation_create(
                    opctx,
                    instance.id,
                    PropolisUuid::new_v4(),
                    db::model::Resources::new(
                        128,
                        ByteCount::try_from(1024).unwrap(),
                        ByteCount::try_from(1024).unwrap(),
                    ),
                    db::model::SledReservationConstraints::none(),
                )
                .await
                .unwrap();

            vmms.push(vmm);
        }

        let sleds: HashSet<_> =
            vmms.into_iter().map(|vmm| vmm.sled_id).collect();

        assert_eq!(sleds.len(), 32);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Ensure that an affinity grouping will cause instances with local storage
    /// to fail allocation even if there's space left.
    #[tokio::test]
    async fn local_storage_allocation_blocked_by_affinity() {
        let logctx =
            dev::test_setup_log("local_storage_allocation_blocked_by_affinity");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let config = LocalStorageTest {
            // Two sleds, with one U2 each
            sleds: vec![
                LocalStorageTestSled {
                    sled_id: SledUuid::new_v4(),
                    sled_serial: String::from("sled_0"),
                    u2s: vec![LocalStorageTestSledU2 {
                        physical_disk_id: PhysicalDiskUuid::new_v4(),
                        physical_disk_serial: String::from("phys0"),

                        zpool_id: ZpoolUuid::new_v4(),
                        control_plane_storage_buffer:
                            external::ByteCount::from_gibibytes_u32(250),

                        inventory_total_size:
                            external::ByteCount::from_gibibytes_u32(1024),

                        crucible_dataset_id: DatasetUuid::new_v4(),
                        crucible_dataset_addr: "[fd00:1122:3344:101::1]:12345"
                            .parse()
                            .unwrap(),

                        local_storage_dataset_id: DatasetUuid::new_v4(),
                    }],
                },
                LocalStorageTestSled {
                    sled_id: SledUuid::new_v4(),
                    sled_serial: String::from("sled_1"),
                    u2s: vec![LocalStorageTestSledU2 {
                        physical_disk_id: PhysicalDiskUuid::new_v4(),
                        physical_disk_serial: String::from("phys1"),

                        zpool_id: ZpoolUuid::new_v4(),
                        control_plane_storage_buffer:
                            external::ByteCount::from_gibibytes_u32(250),

                        inventory_total_size:
                            external::ByteCount::from_gibibytes_u32(1024),

                        crucible_dataset_id: DatasetUuid::new_v4(),
                        crucible_dataset_addr: "[fd00:1122:3344:201::1]:12345"
                            .parse()
                            .unwrap(),

                        local_storage_dataset_id: DatasetUuid::new_v4(),
                    }],
                },
            ],
            affinity_groups: vec![LocalStorageAffinityGroup {
                id: AffinityGroupUuid::new_v4(),
                name: String::from("group-0"),
                policy: external::AffinityPolicy::Fail,
                failure_domain: external::FailureDomain::Sled,
            }],
            anti_affinity_groups: vec![],
            // Configure two instances with one local storage disk each, both in
            // the same affinity group
            instances: vec![
                LocalStorageTestInstance {
                    id: InstanceUuid::new_v4(),
                    name: "local".to_string(),
                    affinity: Some((Affinity::Positive, 0)),
                    disks: vec![LocalStorageTestInstanceDisk {
                        id: Uuid::new_v4(),
                        name: external::Name::try_from("local".to_string())
                            .unwrap(),
                        size: external::ByteCount::from_gibibytes_u32(600),
                    }],
                },
                LocalStorageTestInstance {
                    id: InstanceUuid::new_v4(),
                    name: "local2".to_string(),
                    affinity: Some((Affinity::Positive, 0)),
                    disks: vec![LocalStorageTestInstanceDisk {
                        id: Uuid::new_v4(),
                        name: external::Name::try_from("local2".to_string())
                            .unwrap(),
                        size: external::ByteCount::from_gibibytes_u32(600),
                    }],
                },
            ],
        };

        setup_local_storage_allocation_test(&opctx, datastore, &config).await;

        // The first instance's sled reservation should succeed, there's enough
        // space for the disk

        let instance = Instance::new_with_id(config.instances[0].id);

        // the output of the find targets query does not currently take required
        // local storage allocations into account
        assert_eq!(instance.find_targets(datastore).await.len(), 2);

        // make sure that insertion works
        datastore
            .sled_reservation_create(
                opctx,
                instance.id,
                PropolisUuid::new_v4(),
                db::model::Resources::new(
                    1,
                    ByteCount::try_from(1024).unwrap(),
                    ByteCount::try_from(1024).unwrap(),
                ),
                db::model::SledReservationConstraints::none(),
            )
            .await
            .unwrap();

        // The second instance's sled reservation should not succeed, because
        // the affinity group's policy is set to Fail, and there isn't enough
        // space for the second instance's disk on the one sled.

        let instance = Instance::new_with_id(config.instances[1].id);

        // the output of the find targets query does not currently take required
        // local storage allocations into account
        assert_eq!(instance.find_targets(datastore).await.len(), 2);

        datastore
            .sled_reservation_create(
                opctx,
                instance.id,
                PropolisUuid::new_v4(),
                db::model::Resources::new(
                    1,
                    ByteCount::try_from(1024).unwrap(),
                    ByteCount::try_from(1024).unwrap(),
                ),
                db::model::SledReservationConstraints::none(),
            )
            .await
            .unwrap_err();

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Ensure that an anti-affinity grouping will be honoured for instances
    /// with local storage.
    #[tokio::test]
    async fn local_storage_allocation_blocked_by_anti_affinity() {
        let logctx = dev::test_setup_log(
            "local_storage_allocation_blocked_by_anti_affinity",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let config = LocalStorageTest {
            // One sled, with one U2
            sleds: vec![LocalStorageTestSled {
                sled_id: SledUuid::new_v4(),
                sled_serial: String::from("sled_0"),
                u2s: vec![LocalStorageTestSledU2 {
                    physical_disk_id: PhysicalDiskUuid::new_v4(),
                    physical_disk_serial: String::from("phys0"),

                    zpool_id: ZpoolUuid::new_v4(),
                    control_plane_storage_buffer:
                        external::ByteCount::from_gibibytes_u32(250),

                    inventory_total_size:
                        external::ByteCount::from_gibibytes_u32(1024),

                    crucible_dataset_id: DatasetUuid::new_v4(),
                    crucible_dataset_addr: "[fd00:1122:3344:101::1]:12345"
                        .parse()
                        .unwrap(),

                    local_storage_dataset_id: DatasetUuid::new_v4(),
                }],
            }],
            affinity_groups: vec![],
            anti_affinity_groups: vec![LocalStorageAntiAffinityGroup {
                id: AntiAffinityGroupUuid::new_v4(),
                name: String::from("anti-group-0"),
                policy: external::AffinityPolicy::Fail,
                failure_domain: external::FailureDomain::Sled,
            }],
            // Configure two instances with one local storage disk each, both in
            // the same anti-affinity group
            instances: vec![
                LocalStorageTestInstance {
                    id: InstanceUuid::new_v4(),
                    name: "local".to_string(),
                    affinity: Some((Affinity::Negative, 0)),
                    disks: vec![LocalStorageTestInstanceDisk {
                        id: Uuid::new_v4(),
                        name: external::Name::try_from("local".to_string())
                            .unwrap(),
                        size: external::ByteCount::from_gibibytes_u32(64),
                    }],
                },
                LocalStorageTestInstance {
                    id: InstanceUuid::new_v4(),
                    name: "local2".to_string(),
                    affinity: Some((Affinity::Negative, 0)),
                    disks: vec![LocalStorageTestInstanceDisk {
                        id: Uuid::new_v4(),
                        name: external::Name::try_from("local2".to_string())
                            .unwrap(),
                        size: external::ByteCount::from_gibibytes_u32(64),
                    }],
                },
            ],
        };

        setup_local_storage_allocation_test(&opctx, datastore, &config).await;

        // The first instance's sled reservation should succeed

        let instance = Instance::new_with_id(config.instances[0].id);

        // the output of the find targets query does not currently take required
        // local storage allocations into account
        assert_eq!(instance.find_targets(datastore).await.len(), 1);

        // make sure that insertion works
        let vmm = datastore
            .sled_reservation_create(
                opctx,
                instance.id,
                PropolisUuid::new_v4(),
                db::model::Resources::new(
                    1,
                    ByteCount::try_from(1024).unwrap(),
                    ByteCount::try_from(1024).unwrap(),
                ),
                db::model::SledReservationConstraints::none(),
            )
            .await
            .unwrap();

        assert_eq!(vmm.sled_id, config.sleds[0].sled_id.into());

        // The second instance's sled reservation should not succeed, because
        // the anti-affinity group's policy is set to Fail.

        let instance = Instance::new_with_id(config.instances[1].id);

        // the output of the find targets query does not currently take required
        // local storage allocations into account
        assert_eq!(instance.find_targets(datastore).await.len(), 1);

        datastore
            .sled_reservation_create(
                opctx,
                instance.id,
                PropolisUuid::new_v4(),
                db::model::Resources::new(
                    1,
                    ByteCount::try_from(1024).unwrap(),
                    ByteCount::try_from(1024).unwrap(),
                ),
                db::model::SledReservationConstraints::none(),
            )
            .await
            .unwrap_err();

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Ensure that instances with local storage honour the reservation
    /// constraints.
    #[tokio::test]
    async fn local_storage_allocation_blocked_by_constraint() {
        let logctx = dev::test_setup_log(
            "local_storage_allocation_blocked_by_constraint",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let config = LocalStorageTest {
            // One sled, with one U2, and another with two U2s
            sleds: vec![
                LocalStorageTestSled {
                    sled_id: SledUuid::new_v4(),
                    sled_serial: String::from("sled_0"),
                    u2s: vec![LocalStorageTestSledU2 {
                        physical_disk_id: PhysicalDiskUuid::new_v4(),
                        physical_disk_serial: String::from("phys0"),

                        zpool_id: ZpoolUuid::new_v4(),
                        control_plane_storage_buffer:
                            external::ByteCount::from_gibibytes_u32(250),

                        inventory_total_size:
                            external::ByteCount::from_gibibytes_u32(1024),

                        crucible_dataset_id: DatasetUuid::new_v4(),
                        crucible_dataset_addr: "[fd00:1122:3344:101::1]:12345"
                            .parse()
                            .unwrap(),

                        local_storage_dataset_id: DatasetUuid::new_v4(),
                    }],
                },
                LocalStorageTestSled {
                    sled_id: SledUuid::new_v4(),
                    sled_serial: String::from("sled_1"),
                    u2s: vec![
                        LocalStorageTestSledU2 {
                            physical_disk_id: PhysicalDiskUuid::new_v4(),
                            physical_disk_serial: String::from("phys1"),

                            zpool_id: ZpoolUuid::new_v4(),
                            control_plane_storage_buffer:
                                external::ByteCount::from_gibibytes_u32(250),

                            inventory_total_size:
                                external::ByteCount::from_gibibytes_u32(1024),

                            crucible_dataset_id: DatasetUuid::new_v4(),
                            crucible_dataset_addr:
                                "[fd00:1122:3344:201::1]:12345".parse().unwrap(),

                            local_storage_dataset_id: DatasetUuid::new_v4(),
                        },
                        LocalStorageTestSledU2 {
                            physical_disk_id: PhysicalDiskUuid::new_v4(),
                            physical_disk_serial: String::from("phys2"),

                            zpool_id: ZpoolUuid::new_v4(),
                            control_plane_storage_buffer:
                                external::ByteCount::from_gibibytes_u32(250),

                            inventory_total_size:
                                external::ByteCount::from_gibibytes_u32(1024),

                            crucible_dataset_id: DatasetUuid::new_v4(),
                            crucible_dataset_addr:
                                "[fd00:1122:3344:202::1]:12345".parse().unwrap(),

                            local_storage_dataset_id: DatasetUuid::new_v4(),
                        },
                    ],
                },
            ],
            affinity_groups: vec![],
            anti_affinity_groups: vec![],
            // Configure one instance with two local storage disks
            instances: vec![LocalStorageTestInstance {
                id: InstanceUuid::new_v4(),
                name: "local".to_string(),
                affinity: None,
                disks: vec![
                    LocalStorageTestInstanceDisk {
                        id: Uuid::new_v4(),
                        name: external::Name::try_from("local1".to_string())
                            .unwrap(),
                        size: external::ByteCount::from_gibibytes_u32(512),
                    },
                    LocalStorageTestInstanceDisk {
                        id: Uuid::new_v4(),
                        name: external::Name::try_from("local2".to_string())
                            .unwrap(),
                        size: external::ByteCount::from_gibibytes_u32(512),
                    },
                ],
            }],
        };

        setup_local_storage_allocation_test(&opctx, datastore, &config).await;

        // If there's a constraint that this instance must use sled_0, it cannot
        // succeed, as it needs two local storage allocations (which it could
        // get if it was allowed to use sled_1).

        let instance = Instance::new_with_id(config.instances[0].id);

        // the output of the find targets query does not currently take required
        // local storage allocations into account
        assert_eq!(instance.find_targets(datastore).await.len(), 2);

        // make sure that insertion fails if we are restricted to sled_0
        let constraints = db::model::SledReservationConstraintBuilder::new()
            .must_select_from(&[config.sleds[0].sled_id])
            .build();

        datastore
            .sled_reservation_create(
                opctx,
                instance.id,
                PropolisUuid::new_v4(),
                db::model::Resources::new(
                    1,
                    ByteCount::try_from(1024).unwrap(),
                    ByteCount::try_from(1024).unwrap(),
                ),
                constraints,
            )
            .await
            .unwrap_err();

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Ensure that after sled reservation, local storage disks cannot be
    /// attached to instances, even if they don't yet have a VMM.
    #[tokio::test]
    async fn local_storage_disk_no_attach_after_reservation() {
        let logctx = dev::test_setup_log(
            "local_storage_disk_no_attach_after_reservation",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let config = LocalStorageTest {
            // One sled with two U2s
            sleds: vec![LocalStorageTestSled {
                sled_id: SledUuid::new_v4(),
                sled_serial: String::from("sled_0"),
                u2s: vec![
                    LocalStorageTestSledU2 {
                        physical_disk_id: PhysicalDiskUuid::new_v4(),
                        physical_disk_serial: String::from("phys0"),

                        zpool_id: ZpoolUuid::new_v4(),
                        control_plane_storage_buffer:
                            external::ByteCount::from_gibibytes_u32(250),

                        inventory_total_size:
                            external::ByteCount::from_gibibytes_u32(1024),

                        crucible_dataset_id: DatasetUuid::new_v4(),
                        crucible_dataset_addr: "[fd00:1122:3344:101::1]:12345"
                            .parse()
                            .unwrap(),

                        local_storage_dataset_id: DatasetUuid::new_v4(),
                    },
                    LocalStorageTestSledU2 {
                        physical_disk_id: PhysicalDiskUuid::new_v4(),
                        physical_disk_serial: String::from("phys1"),

                        zpool_id: ZpoolUuid::new_v4(),
                        control_plane_storage_buffer:
                            external::ByteCount::from_gibibytes_u32(250),

                        inventory_total_size:
                            external::ByteCount::from_gibibytes_u32(1024),

                        crucible_dataset_id: DatasetUuid::new_v4(),
                        crucible_dataset_addr: "[fd00:1122:3344:102::1]:12345"
                            .parse()
                            .unwrap(),

                        local_storage_dataset_id: DatasetUuid::new_v4(),
                    },
                ],
            }],
            affinity_groups: vec![],
            anti_affinity_groups: vec![],
            // Configure one instance with two local storage disks. One will be
            // detached before sled reservation.
            instances: vec![LocalStorageTestInstance {
                id: InstanceUuid::new_v4(),
                name: "local".to_string(),
                affinity: None,
                disks: vec![
                    LocalStorageTestInstanceDisk {
                        id: Uuid::new_v4(),
                        name: external::Name::try_from("local1".to_string())
                            .unwrap(),
                        size: external::ByteCount::from_gibibytes_u32(64),
                    },
                    LocalStorageTestInstanceDisk {
                        id: Uuid::new_v4(),
                        name: external::Name::try_from("local2".to_string())
                            .unwrap(),
                        size: external::ByteCount::from_gibibytes_u32(64),
                    },
                ],
            }],
        };

        setup_local_storage_allocation_test(&opctx, datastore, &config).await;

        let instance = Instance::new_with_id(config.instances[0].id);

        // Detach the second disk from the instance
        let (.., authz_instance) = LookupPath::new(&opctx, datastore)
            .instance_id(instance.id.into_untyped_uuid())
            .lookup_for(authz::Action::Modify)
            .await
            .expect("instance must exist");

        let (.., authz_disk) = LookupPath::new(&opctx, datastore)
            .disk_id(config.instances[0].disks[1].id)
            .lookup_for(authz::Action::Read)
            .await
            .expect("disk must exist");

        datastore
            .instance_detach_disk(&opctx, &authz_instance, &authz_disk)
            .await
            .unwrap();

        // the output of the find targets query does not currently take required
        // local storage allocations into account
        assert_eq!(instance.find_targets(datastore).await.len(), 1);

        datastore
            .sled_reservation_create(
                opctx,
                instance.id,
                PropolisUuid::new_v4(),
                db::model::Resources::new(
                    1,
                    ByteCount::try_from(1024).unwrap(),
                    ByteCount::try_from(1024).unwrap(),
                ),
                db::model::SledReservationConstraints::none(),
            )
            .await
            .unwrap();

        // Try to attach the second disk to the instance - it should fail.

        datastore
            .instance_attach_disk(
                &opctx,
                &authz_instance,
                &authz_disk,
                MAX_DISKS_PER_INSTANCE,
            )
            .await
            .unwrap_err();

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
