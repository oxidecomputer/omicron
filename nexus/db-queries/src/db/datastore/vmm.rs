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
use crate::db::schema::vmm::dsl;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::update_and_check::UpdateStatus;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use uuid::Uuid;

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
        vmm_id: &Uuid,
    ) -> UpdateResult<bool> {
        use crate::db::model::InstanceState as DbInstanceState;
        use omicron_common::api::external::InstanceState as ApiInstanceState;

        let valid_states = vec![
            DbInstanceState::new(ApiInstanceState::Destroyed),
            DbInstanceState::new(ApiInstanceState::Failed),
        ];

        let updated = diesel::update(dsl::vmm)
            .filter(dsl::id.eq(*vmm_id))
            .filter(dsl::state.eq_any(valid_states))
            .set(dsl::time_deleted.eq(Utc::now()))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Vmm,
                        LookupType::ById(*vmm_id),
                    ),
                )
            })?;

        Ok(updated != 0)
    }

    pub async fn vmm_fetch(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        vmm_id: &Uuid,
    ) -> LookupResult<Vmm> {
        opctx.authorize(authz::Action::Read, authz_instance).await?;

        let vmm = dsl::vmm
            .filter(dsl::id.eq(*vmm_id))
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
                        LookupType::ById(*vmm_id),
                    ),
                )
            })?;

        Ok(vmm)
    }

    pub async fn vmm_update_runtime(
        &self,
        vmm_id: &Uuid,
        new_runtime: &VmmRuntimeState,
    ) -> Result<bool, Error> {
        let updated = diesel::update(dsl::vmm)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(*vmm_id))
            .filter(dsl::state_generation.lt(new_runtime.gen))
            .set(new_runtime.clone())
            .check_if_exists::<Vmm>(*vmm_id)
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
                        LookupType::ById(*vmm_id),
                    ),
                )
            })?;

        Ok(updated)
    }

    /// Forcibly overwrites the Propolis IP in the supplied VMM's record with
    /// the supplied Propolis IP.
    ///
    /// This is used in tests to overwrite the IP for a VMM that is backed by a
    /// mock Propolis server that serves on localhost but has its Propolis IP
    /// allocated by the instance start procedure. (Unfortunately, this can't be
    /// marked #[cfg(test)] because the integration tests require this
    /// functionality.)
    pub async fn vmm_overwrite_ip_for_test(
        &self,
        opctx: &OpContext,
        vmm_id: &Uuid,
        new_ip: ipnetwork::IpNetwork,
    ) -> UpdateResult<Vmm> {
        let vmm = diesel::update(dsl::vmm)
            .filter(dsl::id.eq(*vmm_id))
            .set(dsl::propolis_ip.eq(new_ip))
            .returning(Vmm::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(vmm)
    }
}
