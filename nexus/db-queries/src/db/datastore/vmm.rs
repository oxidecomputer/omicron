// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] helpers for working with VMM records.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::model::Vmm;
use crate::db::model::VmmRuntimeState;
use crate::db::model::VmmState as DbVmmState;
use crate::db::pagination::paginated;
use crate::db::schema::vmm::dsl;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::update_and_check::UpdateStatus;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::internal::nexus::Migrations;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::PropolisUuid;
use std::net::SocketAddr;
use uuid::Uuid;

/// The result of an [`DataStore::vmm_and_migration_update_runtime`] call,
/// indicating which records were updated.
#[derive(Copy, Clone, Debug)]
pub struct VmmStateUpdateResult {
    /// `true` if the VMM record was updated, `false` otherwise.
    pub vmm_updated: bool,

    /// `true` if a migration record was updated for the migration in, false if
    /// no update was performed or no migration in was provided.
    pub migration_in_updated: bool,

    /// `true` if a migration record was updated for the migration out, false if
    /// no update was performed or no migration out was provided.
    pub migration_out_updated: bool,
}

impl DataStore {
    pub async fn vmm_insert(
        &self,
        opctx: &OpContext,
        vmm: Vmm,
    ) -> CreateResult<Vmm> {
        let vmm = diesel::insert_into(dsl::vmm)
            .values(vmm)
            .on_conflict(dsl::id)
            .do_update()
            .set(dsl::time_state_updated.eq(dsl::time_state_updated))
            .returning(Vmm::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(vmm)
    }

    pub async fn vmm_mark_deleted(
        &self,
        opctx: &OpContext,
        vmm_id: &PropolisUuid,
    ) -> UpdateResult<bool> {
        let valid_states = vec![DbVmmState::Destroyed, DbVmmState::Failed];

        let updated = diesel::update(dsl::vmm)
            .filter(dsl::id.eq(vmm_id.into_untyped_uuid()))
            .filter(dsl::state.eq_any(valid_states))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::time_deleted.eq(Utc::now()))
            .check_if_exists::<Vmm>(vmm_id.into_untyped_uuid())
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map(|r| match r.status {
                UpdateStatus::Updated => true,
                UpdateStatus::NotUpdatedButExists => false,
            })
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Vmm,
                        LookupType::ById(vmm_id.into_untyped_uuid()),
                    ),
                )
            })?;

        Ok(updated)
    }

    pub async fn vmm_fetch(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        vmm_id: &PropolisUuid,
    ) -> LookupResult<Vmm> {
        opctx.authorize(authz::Action::Read, authz_instance).await?;

        let vmm = dsl::vmm
            .filter(dsl::id.eq(vmm_id.into_untyped_uuid()))
            .filter(dsl::instance_id.eq(authz_instance.id()))
            .filter(dsl::time_deleted.is_null())
            .select(Vmm::as_select())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Vmm,
                        LookupType::ById(vmm_id.into_untyped_uuid()),
                    ),
                )
            })?;

        Ok(vmm)
    }

    pub async fn vmm_update_runtime(
        &self,
        vmm_id: &PropolisUuid,
        new_runtime: &VmmRuntimeState,
    ) -> Result<bool, Error> {
        let updated = diesel::update(dsl::vmm)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(vmm_id.into_untyped_uuid()))
            .filter(dsl::state_generation.lt(new_runtime.gen))
            .set(new_runtime.clone())
            .check_if_exists::<Vmm>(vmm_id.into_untyped_uuid())
            .execute_and_check(&*self.pool_connection_unauthorized().await?)
            .await
            .map(|r| match r.status {
                UpdateStatus::Updated => true,
                UpdateStatus::NotUpdatedButExists => false,
            })
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Vmm,
                        LookupType::ById(vmm_id.into_untyped_uuid()),
                    ),
                )
            })?;

        Ok(updated)
    }

    /// Updates a VMM record and associated migration record(s) with a single
    /// database command.
    ///
    /// This is intended to be used to apply updates from sled agent that
    /// may change a VMM's runtime state (e.g. moving an instance from Running
    /// to Stopped) and the state of its current active mgiration in a single
    /// transaction. The caller is responsible for ensuring the VMM and
    /// migration states are consistent with each other before calling this
    /// routine.
    ///
    /// # Arguments
    ///
    /// - `vmm_id`: The ID of the VMM to update.
    /// - `new_runtime`: The new VMM runtime state to try to write.
    /// - `migrations`: The (optional) migration-in and migration-out states to
    ///   try to write.
    ///
    /// # Return value
    ///
    /// - `Ok(`[`VmmStateUpdateResult`]`)` if the query was issued
    ///   successfully. The returned [`VmmStateUpdateResult`] indicates which
    ///   database record(s) were updated. Note that an update can fail because
    ///   it was inapplicable (i.e. the database has state with a newer
    ///   generation already) or because the relevant record was not found.
    /// - `Err` if another error occurred while accessing the database.
    pub async fn vmm_and_migration_update_runtime(
        &self,
        vmm_id: PropolisUuid,
        new_runtime: &VmmRuntimeState,
        migrations: Migrations<'_>,
    ) -> Result<VmmStateUpdateResult, Error> {
        let query = crate::db::queries::vmm::VmmAndMigrationUpdate::new(
            vmm_id,
            new_runtime.clone(),
            migrations,
        );

        // The VmmAndMigrationUpdate query handles and indicates failure to find
        // either the VMM or the migration, so a query failure here indicates
        // some kind of internal error and not a failed lookup.
        let result = query
            .execute_and_check(&*self.pool_connection_unauthorized().await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(VmmStateUpdateResult {
            vmm_updated: match result.vmm_status {
                Some(UpdateStatus::Updated) => true,
                Some(UpdateStatus::NotUpdatedButExists) => false,
                None => false,
            },
            migration_in_updated: result.migration_in_status.was_updated(),
            migration_out_updated: result.migration_out_status.was_updated(),
        })
    }

    /// Forcibly overwrites the Propolis IP/Port in the supplied VMM's record with
    /// the supplied Propolis IP.
    ///
    /// This is used in tests to overwrite the IP for a VMM that is backed by a
    /// mock Propolis server that serves on localhost but has its Propolis IP
    /// allocated by the instance start procedure. (Unfortunately, this can't be
    /// marked #[cfg(test)] because the integration tests require this
    /// functionality.)
    pub async fn vmm_overwrite_addr_for_test(
        &self,
        opctx: &OpContext,
        vmm_id: &PropolisUuid,
        new_addr: SocketAddr,
    ) -> UpdateResult<Vmm> {
        let new_ip = ipnetwork::IpNetwork::from(new_addr.ip());
        let new_port = new_addr.port();
        let vmm = diesel::update(dsl::vmm)
            .filter(dsl::id.eq(vmm_id.into_untyped_uuid()))
            .set((
                dsl::propolis_ip.eq(new_ip),
                dsl::propolis_port.eq(i32::from(new_port)),
            ))
            .returning(Vmm::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(vmm)
    }

    /// Lists VMMs which have been abandoned by their instances after a
    /// migration and are in need of cleanup.
    ///
    /// A VMM is considered "abandoned" if (and only if):
    ///
    /// - It is in the `Destroyed` state.
    /// - It is not currently running an instance, and it is also not the
    ///   migration target of any instance (i.e. it is not pointed to by
    ///   any instance record's `active_propolis_id` and `target_propolis_id`
    ///   fields).
    /// - It has not been deleted yet.
    pub async fn vmm_list_abandoned(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Vmm> {
        use crate::db::schema::instance::dsl as instance_dsl;
        let destroyed = DbVmmState::Destroyed;
        paginated(dsl::vmm, dsl::id, pagparams)
            // In order to be considered "abandoned", a VMM must be:
            // - in the `Destroyed` state
            .filter(dsl::state.eq(destroyed))
            // - not deleted yet
            .filter(dsl::time_deleted.is_null())
            // - not pointed to by any instance's `active_propolis_id` or
            //   `target_propolis_id`.
            //
            .left_join(
                // Left join with the `instance` table on the VMM's instance ID, so
                // that we can check if the instance pointed to by this VMM (if
                // any exists) has this VMM pointed to by its
                // `active_propolis_id` or `target_propolis_id` fields.
                instance_dsl::instance
                    .on(instance_dsl::id.eq(dsl::instance_id)),
            )
            .filter(
                dsl::id
                    .nullable()
                    .ne(instance_dsl::active_propolis_id)
                    // In SQL, *all* comparisons with NULL are `false`, even `!=
                    // NULL`, so we have to explicitly check for nulls here, or
                    // else VMMs whose instances have no `active_propolis_id`
                    // will not be considered abandoned (incorrectly).
                    .or(instance_dsl::active_propolis_id.is_null()),
            )
            .filter(
                dsl::id
                    .nullable()
                    .ne(instance_dsl::target_propolis_id)
                    // As above, we must add this clause because SQL nulls have
                    // the most irritating behavior possible.
                    .or(instance_dsl::target_propolis_id.is_null()),
            )
            .select(Vmm::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}
