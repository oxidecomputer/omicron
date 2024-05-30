// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`db::saga_types::Saga`]s.

use super::DataStore;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::model::Generation;
use crate::db::pagination::paginated;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::update_and_check::UpdateStatus;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use uuid::Uuid;

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
        //
        // Note that while conditional update is important to keep the DB clean,
        // we still have to ensure that only the current SEC is actually running
        // the saga in the first place. Doing a check here is not good enough.
        // In the short term, we only mitigate harm of dueling SECs via
        // removal of sleds before expungement, and then only migrating
        // when the Nexus zone executing the SEC is expunged. In the long term
        // we can force expungement by removing the sled from trust quorum and rebooting.
        //
        // This will help prevent operator error.
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

    /// Transfer ownership of a saga from one SEC to another.
    pub async fn saga_update_sec(
        &self,
        saga_id: steno::SagaId,
        old_sec: db::saga_types::SecId,
        new_sec: db::saga_types::SecId,
        old_gen: Generation,
    ) -> Result<(), Error> {
        let new_gen: Generation = old_gen.next().into();
        let saga_id: db::saga_types::SagaId = saga_id.into();
        use db::schema::saga::dsl;
        let result = diesel::update(dsl::saga)
            .filter(dsl::id.eq(saga_id))
            .filter(dsl::current_sec.eq(old_sec))
            .filter(dsl::adopt_generation.eq(old_gen))
            .set((
                dsl::current_sec.eq(new_sec),
                dsl::adopt_generation.eq(new_gen),
            ))
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
            UpdateStatus::NotUpdatedButExists => {
                Err(Error::invalid_request(format!(
                    "failed to update sec {:?}: preconditions not met: \
                    expected current_sec = {:?}, adopt_generation = {:?}, \
                    but found current_sec = {:?}, adopt_generation = {:?}",
                    saga_id,
                    old_sec,
                    old_gen,
                    result.found.current_sec,
                    result.found.adopt_generation,
                )))
            }
        }
    }

    pub async fn saga_update_state(
        &self,
        saga_id: steno::SagaId,
        new_state: steno::SagaCachedState,
        current_sec: db::saga_types::SecId,
    ) -> Result<(), Error> {
        use db::schema::saga::dsl;

        let saga_id: db::saga_types::SagaId = saga_id.into();
        let result = diesel::update(dsl::saga)
            .filter(dsl::id.eq(saga_id))
            .filter(dsl::current_sec.eq(current_sec))
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
                    expected current_sec = {:?}, \
                    but found current_sec = {:?}, state = {:?}",
                    saga_id,
                    new_state,
                    current_sec,
                    result.found.current_sec,
                    result.found.saga_state,
                )
            )),
        }
    }

    pub async fn saga_list_unfinished_by_id(
        &self,
        sec_id: &db::SecId,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::saga_types::Saga> {
        use db::schema::saga::dsl;
        paginated(dsl::saga, dsl::id, &pagparams)
            .filter(dsl::saga_state.ne(db::saga_types::SagaCachedState(
                steno::SagaCachedState::Done,
            )))
            .filter(dsl::current_sec.eq(*sec_id))
            .load_async(&*self.pool_connection_unauthorized().await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::SagaDbg,
                        LookupType::ById(sec_id.0),
                    ),
                )
            })
    }

    pub async fn saga_node_event_list_by_id(
        &self,
        id: db::saga_types::SagaId,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<steno::SagaNodeEvent> {
        use db::schema::saga_node_event::dsl;
        paginated(dsl::saga_node_event, dsl::saga_id, &pagparams)
            .filter(dsl::saga_id.eq(id))
            .load_async::<db::saga_types::SagaNodeEvent>(
                &*self.pool_connection_unauthorized().await?,
            )
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::SagaDbg,
                        LookupType::ById(id.0 .0),
                    ),
                )
            })?
            .into_iter()
            .map(|db_event| steno::SagaNodeEvent::try_from(db_event))
            .collect::<Result<_, Error>>()
    }
}
