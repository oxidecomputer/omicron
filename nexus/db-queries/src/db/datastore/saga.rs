// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`db::saga_types::Saga`]s.

use super::DataStore;
use super::SQL_BATCH_SIZE;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::model::Generation;
use crate::db::pagination::paginated;
use crate::db::pagination::paginated_multicolumn;
use crate::db::pagination::Paginator;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::update_and_check::UpdateStatus;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use nexus_auth::context::OpContext;
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

    /// Returns a list of unfinished sagas assigned to SEC `sec_id`, making as
    /// many queries as needed (in batches) to get them all
    pub async fn saga_list_recovery_candidates_batched(
        &self,
        opctx: &OpContext,
        sec_id: db::saga_types::SecId,
    ) -> Result<Vec<db::saga_types::Saga>, Error> {
        let mut sagas = vec![];
        let mut paginator = Paginator::new(SQL_BATCH_SIZE);
        let conn = self.pool_connection_authorized(opctx).await?;
        while let Some(p) = paginator.next() {
            use db::schema::saga::dsl;

            let mut batch =
                paginated(dsl::saga, dsl::id, &p.current_pagparams())
                    .filter(dsl::saga_state.ne(
                        db::saga_types::SagaCachedState(
                            steno::SagaCachedState::Done,
                        ),
                    ))
                    .filter(dsl::current_sec.eq(sec_id))
                    .select(db::saga_types::Saga::as_select())
                    .load_async(&*conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel(e, ErrorHandler::Server)
                    })?;

            paginator = p.found_batch(&batch, &|row| row.id);
            sagas.append(&mut batch);
        }
        Ok(sagas)
    }

    /// Returns a list of all saga log entries for the given saga, making as
    /// many queries as needed (in batches) to get them all
    pub async fn saga_fetch_log_batched(
        &self,
        opctx: &OpContext,
        saga: &db::saga_types::Saga,
    ) -> Result<Vec<steno::SagaNodeEvent>, Error> {
        let mut events = vec![];
        let mut paginator = Paginator::new(SQL_BATCH_SIZE);
        let conn = self.pool_connection_authorized(opctx).await?;
        while let Some(p) = paginator.next() {
            use db::schema::saga_node_event::dsl;
            let batch = paginated_multicolumn(
                dsl::saga_node_event,
                (dsl::node_id, dsl::event_type),
                &p.current_pagparams(),
            )
            .filter(dsl::saga_id.eq(saga.id))
            .select(db::saga_types::SagaNodeEvent::as_select())
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

            paginator = p.found_batch(&batch, &|row| {
                (row.node_id, row.event_type.clone())
            });

            let mut batch = batch
                .into_iter()
                .map(|event| steno::SagaNodeEvent::try_from(event))
                .collect::<Result<Vec<_>, Error>>()?;

            events.append(&mut batch);
        }

        Ok(events)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::datastore::test_utils::datastore_test;
    use nexus_test_utils::db::test_setup_database;
    use omicron_test_utils::dev;
    use rand::seq::SliceRandom;
    use uuid::Uuid;

    // Tests pagination in listing sagas that are candidates for recovery
    #[tokio::test]
    async fn test_list_candidate_sagas() {
        // Test setup
        let logctx = dev::test_setup_log("test_list_candidate_sagas");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;
        let sec_id = db::SecId(uuid::Uuid::new_v4());

        // Create a couple batches of sagas.
        let new_running_db_saga = || {
            let params = steno::SagaCreateParams {
                id: steno::SagaId(Uuid::new_v4()),
                name: steno::SagaName::new("test saga"),
                dag: serde_json::value::Value::Null,
                state: steno::SagaCachedState::Running,
            };

            db::model::saga_types::Saga::new(sec_id, params)
        };
        let mut inserted_sagas = (0..SQL_BATCH_SIZE.get() * 2)
            .map(|_| new_running_db_saga())
            .collect::<Vec<_>>();

        // Shuffle these sagas into a random order to check that the pagination
        // order is working as intended on the read path, which we'll do later
        // in this test.
        inserted_sagas.shuffle(&mut rand::thread_rng());

        // Insert the batches of unfinished sagas into the database
        let conn = datastore
            .pool_connection_unauthorized()
            .await
            .expect("Failed to access db connection");
        diesel::insert_into(db::schema::saga::dsl::saga)
            .values(inserted_sagas.clone())
            .execute_async(&*conn)
            .await
            .expect("Failed to insert test setup data");

        // List them, expect to see them all in order by ID.
        let mut observed_sagas = datastore
            .saga_list_recovery_candidates_batched(&opctx, sec_id)
            .await
            .expect("Failed to list unfinished sagas");
        inserted_sagas.sort_by_key(|a| a.id);

        // Timestamps can change slightly when we insert them.
        //
        // Sanitize them to make input/output equality checks easier.
        let sanitize_timestamps = |sagas: &mut Vec<db::saga_types::Saga>| {
            for saga in sagas {
                saga.time_created = chrono::DateTime::UNIX_EPOCH;
                saga.adopt_time = chrono::DateTime::UNIX_EPOCH;
            }
        };
        sanitize_timestamps(&mut observed_sagas);
        sanitize_timestamps(&mut inserted_sagas);

        assert_eq!(
            inserted_sagas, observed_sagas,
            "Observed sagas did not match inserted sagas"
        );

        // Test cleanup
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    // Tests pagination in loading a saga log
    #[tokio::test]
    async fn test_list_unfinished_nodes() {
        // Test setup
        let logctx = dev::test_setup_log("test_list_unfinished_nodes");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;
        let sec_id = db::SecId(uuid::Uuid::new_v4());
        let saga_id = steno::SagaId(Uuid::new_v4());

        // Create a couple batches of saga events
        let new_db_saga_nodes =
            |node_id: u32, event_type: steno::SagaNodeEventType| {
                let event = steno::SagaNodeEvent {
                    saga_id,
                    node_id: steno::SagaNodeId::from(node_id),
                    event_type,
                };

                db::model::saga_types::SagaNodeEvent::new(event, sec_id)
            };
        let mut inserted_nodes = (0..SQL_BATCH_SIZE.get() * 2)
            .flat_map(|i| {
                // This isn't an exhaustive list of event types, but gives us a
                // few options to pick from. Since this is a pagination key,
                // it's important to include a variety here.
                use steno::SagaNodeEventType::*;
                [
                    new_db_saga_nodes(i, Started),
                    new_db_saga_nodes(i, UndoStarted),
                    new_db_saga_nodes(i, UndoFinished),
                ]
            })
            .collect::<Vec<_>>();

        // Shuffle these nodes into a random order to check that the pagination
        // order is working as intended on the read path, which we'll do later
        // in this test.
        inserted_nodes.shuffle(&mut rand::thread_rng());

        // Insert them into the database
        let conn = datastore
            .pool_connection_unauthorized()
            .await
            .expect("Failed to access db connection");
        diesel::insert_into(db::schema::saga_node_event::dsl::saga_node_event)
            .values(inserted_nodes.clone())
            .execute_async(&*conn)
            .await
            .expect("Failed to insert test setup data");

        // List them, expect to see them all in order by ID.
        //
        // Note that we need to make up a saga to see this, but the
        // part of it that actually matters is the ID.
        let params = steno::SagaCreateParams {
            id: saga_id,
            name: steno::SagaName::new("test saga"),
            dag: serde_json::value::Value::Null,
            state: steno::SagaCachedState::Running,
        };
        let saga = db::model::saga_types::Saga::new(sec_id, params);
        let observed_nodes = datastore
            .saga_fetch_log_batched(&opctx, &saga)
            .await
            .expect("Failed to list nodes of unfinished saga");
        inserted_nodes.sort_by_key(|a| (a.node_id, a.event_type.clone()));

        let inserted_nodes = inserted_nodes
            .into_iter()
            .map(|node| steno::SagaNodeEvent::try_from(node))
            .collect::<Result<Vec<_>, _>>()
            .expect("Couldn't convert DB nodes to steno nodes");

        // The steno::SagaNodeEvent type doesn't implement PartialEq, so we need
        // to do this a little manually.
        assert_eq!(inserted_nodes.len(), observed_nodes.len());
        for (inserted, observed) in
            inserted_nodes.iter().zip(observed_nodes.iter())
        {
            assert_eq!(inserted.saga_id, observed.saga_id);
            assert_eq!(inserted.node_id, observed.node_id);
            assert_eq!(
                inserted.event_type.label(),
                observed.event_type.label()
            );
        }

        // Test cleanup
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    // Tests the special case of listing an empty saga log
    #[tokio::test]
    async fn test_list_no_unfinished_nodes() {
        // Test setup
        let logctx = dev::test_setup_log("test_list_no_unfinished_nodes");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;
        let sec_id = db::SecId(uuid::Uuid::new_v4());
        let saga_id = steno::SagaId(Uuid::new_v4());

        let params = steno::SagaCreateParams {
            id: saga_id,
            name: steno::SagaName::new("test saga"),
            dag: serde_json::value::Value::Null,
            state: steno::SagaCachedState::Running,
        };
        let saga = db::model::saga_types::Saga::new(sec_id, params);

        // Test that this returns "no nodes" rather than throwing some "not
        // found" error.
        let observed_nodes = datastore
            .saga_fetch_log_batched(&opctx, &saga)
            .await
            .expect("Failed to list nodes of unfinished saga");
        assert_eq!(observed_nodes.len(), 0);

        // Test cleanup
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
