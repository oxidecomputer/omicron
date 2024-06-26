// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`db::saga_types::Saga`]s.

use super::DataStore;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::model::Generation;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::update_and_check::UpdateStatus;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;

impl DataStore {
    pub async fn saga_create(
        &self,
        saga: &db::saga_types::Saga,
    ) -> Result<(), Error> {
        use db::schema::saga::dsl;

        diesel::insert_into(dsl::saga)
            .values(saga.clone())
            .execute_async(&*self.pool_connection_unauthorized().await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        Ok(())
    }

    pub async fn saga_create_event(
        &self,
        event: &db::saga_types::SagaNodeEvent,
    ) -> Result<(), Error> {
        use db::schema::saga_node_event::dsl;

        // TODO-robustness This INSERT ought to be conditional on this SEC still
        // owning this saga.
        diesel::insert_into(dsl::saga_node_event)
            .values(event.clone())
            .execute_async(&*self.pool_connection_unauthorized().await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(ResourceType::SagaDbg, "Saga Event"),
                )
            })?;
        Ok(())
    }

    pub async fn saga_update_state(
        &self,
        saga_id: steno::SagaId,
        new_state: steno::SagaCachedState,
        current_sec: db::saga_types::SecId,
        current_adopt_generation: Generation,
    ) -> Result<(), Error> {
        use db::schema::saga::dsl;

        let saga_id: db::saga_types::SagaId = saga_id.into();
        let result = diesel::update(dsl::saga)
            .filter(dsl::id.eq(saga_id))
            .filter(dsl::current_sec.eq(current_sec))
            .filter(dsl::adopt_generation.eq(current_adopt_generation))
            .set(dsl::saga_state.eq(db::saga_types::SagaCachedState(new_state)))
            .check_if_exists::<db::saga_types::Saga>(saga_id)
            .execute_and_check(&*self.pool_connection_unauthorized().await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::SagaDbg,
                        LookupType::ById(saga_id.0.into()),
                    ),
                )
            })?;

        match result.status {
            UpdateStatus::Updated => Ok(()),
            UpdateStatus::NotUpdatedButExists => Err(Error::invalid_request(
                format!(
                    "failed to update saga {:?} with state {:?}: preconditions not met: \
                    expected current_sec = {:?}, adopt_generation = {:?}, \
                    but found current_sec = {:?}, adopt_generation = {:?}, state = {:?}",
                    saga_id,
                    new_state,
                    current_sec,
                    current_adopt_generation,
                    result.found.current_sec,
                    result.found.adopt_generation,
                    result.found.saga_state,
                )
            )),
        }
    }
}
