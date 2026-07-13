// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`db::saga_types::Saga`]s.

use super::DataStore;
use super::SQL_BATCH_SIZE;
use crate::db;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use crate::db::pagination::paginated_multicolumn;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::update_and_check::UpdateStatus;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::DateTime;
use chrono::Utc;
use diesel::prelude::*;
use nexus_auth::authz;
use nexus_auth::context::OpContext;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_model::SagaState;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use std::ops::Add;

impl DataStore {
    pub async fn saga_create(
        &self,
        saga: &db::saga_types::Saga,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::saga::dsl;

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
        use nexus_db_schema::schema::saga_node_event::dsl;

        // TODO-robustness This INSERT ought to be conditional on this SEC still
        // owning this saga.
        diesel::insert_into(dsl::saga_node_event)
            .values(event.clone())
            // (saga_id, node_id, event_type) is the primary key, and this is
            // expected to be idempotent.
            //
            // Consider the situation where a saga event gets recorded and
            // committed, but there's a network reset which makes the client
            // (us) believe that the event wasn't recorded. If we retry the
            // event, we want to not fail with a conflict.
            .on_conflict((dsl::saga_id, dsl::node_id, dsl::event_type))
            .do_nothing()
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

    /// Update the state of a saga in the database.
    ///
    /// This function is meant to be called in a loop, so that in the event of
    /// network flakiness, the operation is retried until successful.
    ///
    /// ## About conflicts
    ///
    /// Currently, if the value of `saga_state` in the database is the same as
    /// the value we're trying to set it to, the update will be a no-op. That
    /// is okay, because at any time only one SEC will update the saga. (For
    /// now, we're implementing saga adoption only in cases where the original
    /// SEC/Nexus has been expunged.)
    ///
    /// It's conceivable that multiple SECs do try to udpate the same saga
    /// concurrently.  That would be a bug.  This is noticed and prevented by
    /// making this query conditional on current_sec and failing with a conflict
    /// if the current SEC has changed.
    pub async fn saga_update_state(
        &self,
        saga_id: steno::SagaId,
        new_state: SagaState,
        current_sec: db::saga_types::SecId,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::saga::dsl;

        let saga_id: db::saga_types::SagaId = saga_id.into();
        let result = diesel::update(dsl::saga)
            .filter(dsl::id.eq(saga_id))
            .filter(dsl::current_sec.eq(current_sec))
            .set(dsl::saga_state.eq(new_state))
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
                    "failed to update saga {:?} with state {:?}:\
                     preconditions not met: \
                     expected current_sec = {:?}, \
                     but found current_sec = {:?}, state = {:?}",
                    saga_id,
                    new_state,
                    current_sec,
                    result.found.current_sec,
                    result.found.saga_state,
                )))
            }
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
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        let conn = self.pool_connection_authorized(opctx).await?;
        while let Some(p) = paginator.next() {
            use nexus_db_schema::schema::saga::dsl;

            let mut batch =
                paginated(dsl::saga, dsl::id, &p.current_pagparams())
                    .filter(
                        dsl::saga_state
                            .eq_any(SagaState::RECOVERY_CANDIDATE_STATES),
                    )
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

    /// Returns a list of sagas that were created before `time_limit` and are
    /// in a running or unwinding state (limit of 500).
    pub async fn saga_list_running_or_unwinding_older_than(
        &self,
        opctx: &OpContext,
        time_limit: DateTime<Utc>,
    ) -> Result<Vec<db::saga_types::Saga>, Error> {
        use nexus_db_schema::schema::saga::dsl;
        let conn = self.pool_connection_authorized(opctx).await?;

        dsl::saga
            .filter(
                dsl::saga_state
                    .eq_any(vec![SagaState::Running, SagaState::Unwinding]),
            )
            .filter(dsl::time_created.lt(time_limit))
            .limit(500)
            .select(db::saga_types::Saga::as_select())
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Returns all unfinished sagas: running, unwinding, or abandoned. Makes
    /// as many queries as needed (in batches) to get them all.
    pub async fn saga_list_unfinished_batched(
        &self,
        opctx: &OpContext,
    ) -> Result<Vec<db::saga_types::Saga>, Error> {
        const UNFINISHED_STATES: &[SagaState] =
            &[SagaState::Running, SagaState::Unwinding, SagaState::Abandoned];
        let mut sagas = vec![];
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        let conn = self.pool_connection_authorized(opctx).await?;
        while let Some(p) = paginator.next() {
            use nexus_db_schema::schema::saga::dsl;

            let mut batch =
                paginated(dsl::saga, dsl::id, &p.current_pagparams())
                    .filter(dsl::saga_state.eq_any(UNFINISHED_STATES))
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

    /// For each of the given sagas, returns the timestamp of its most recent
    /// node event (`MAX(event_time)`), i.e. the last durably-recorded forward
    /// or undo step. Sagas with no node events are absent from the result.
    ///
    /// This is the saga diagnosis engine's progress signal: `now - max` is how
    /// long a saga has gone without recording progress. The query seeks by the
    /// `saga_node_event` primary-key prefix (`saga_id`), so it does not scan
    /// the whole table.
    pub async fn saga_latest_node_event_times(
        &self,
        opctx: &OpContext,
        saga_ids: &[db::saga_types::SagaId],
    ) -> Result<Vec<(db::saga_types::SagaId, Option<DateTime<Utc>>)>, Error>
    {
        use nexus_db_schema::schema::saga_node_event::dsl;
        let conn = self.pool_connection_authorized(opctx).await?;
        let mut results = Vec::with_capacity(saga_ids.len());
        // Chunk the IDs so a large non-terminal saga count doesn't produce one
        // giant statement.
        for chunk in saga_ids.chunks(SQL_BATCH_SIZE.get() as usize) {
            let batch: Vec<(db::saga_types::SagaId, Option<DateTime<Utc>>)> =
                dsl::saga_node_event
                    .filter(dsl::saga_id.eq_any(chunk.to_vec()))
                    .group_by(dsl::saga_id)
                    .select((dsl::saga_id, diesel::dsl::max(dsl::event_time)))
                    .load_async(&*conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel(e, ErrorHandler::Server)
                    })?;
            results.extend(batch);
        }
        Ok(results)
    }

    /// Returns a list of all saga log entries for the given saga, making as
    /// many queries as needed (in batches) to get them all
    pub async fn saga_fetch_log_batched(
        &self,
        opctx: &OpContext,
        saga_id: db::saga_types::SagaId,
    ) -> Result<Vec<steno::SagaNodeEvent>, Error> {
        let mut events = vec![];
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        let conn = self.pool_connection_authorized(opctx).await?;
        while let Some(p) = paginator.next() {
            use nexus_db_schema::schema::saga_node_event::dsl;
            let batch = paginated_multicolumn(
                dsl::saga_node_event,
                (dsl::node_id, dsl::event_type),
                &p.current_pagparams(),
            )
            .filter(dsl::saga_id.eq(saga_id))
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

    /// Updates all sagas that are currently assigned to any of the SEC ids in
    /// `sec_ids`, assigning them to `new_sec_id` instead.
    ///
    /// Generally, an SEC id corresponds to a Nexus id.  This change causes the
    /// Nexus instance `new_sec_id` to discover these sagas and resume executing
    /// them the next time it performs saga recovery (which is normally on
    /// startup and periodically).  Generally, `new_sec_id` is the _current_
    /// Nexus instance and the caller should activate the saga recovery
    /// background task after calling this function to immediately resume the
    /// newly-assigned sagas.
    ///
    /// **Warning:** This operation is only safe if the other SECs `sec_ids` are
    /// not currently running.  If those SECs are still running, then two (or
    /// more) SECs may wind up running the same saga concurrently.  This would
    /// likely violate implicit assumptions made by various saga actions,
    /// leading to hard-to-debug errors and state corruption.
    pub async fn sagas_reassign_sec(
        &self,
        opctx: &OpContext,
        sec_ids: &[db::saga_types::SecId],
        new_sec_id: db::saga_types::SecId,
    ) -> Result<usize, Error> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        let now = chrono::Utc::now();
        let conn = self.pool_connection_authorized(opctx).await?;

        // It would be more robust to do this in batches.  However, Diesel does
        // not appear to support the UPDATE ... LIMIT syntax using the normal
        // builder.  In practice, it's extremely unlikely we'd have so many
        // in-progress sagas that this would be a problem.
        use nexus_db_schema::schema::saga::dsl;
        diesel::update(
            dsl::saga
                .filter(dsl::current_sec.is_not_null())
                .filter(
                    dsl::current_sec.eq_any(
                        sec_ids.into_iter().cloned().collect::<Vec<_>>(),
                    ),
                )
                .filter(dsl::saga_state.ne(db::saga_types::SagaState::Done)),
        )
        .set((
            dsl::current_sec.eq(Some(new_sec_id)),
            dsl::adopt_generation.eq(dsl::adopt_generation.add(1)),
            dsl::adopt_time.eq(now),
        ))
        .execute_async(&*conn)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::pub_test_utils::TestDatabase;
    use async_bb8_diesel::AsyncConnection;
    use async_bb8_diesel::AsyncSimpleConnection;
    use chrono::TimeDelta;
    use db::queries::ALLOW_FULL_TABLE_SCAN_SQL;
    use nexus_db_model::SagaState;
    use nexus_db_model::{SagaNodeEvent, SecId};
    use omicron_common::api::external::Generation;
    use omicron_test_utils::dev;
    use rand::seq::SliceRandom;
    use std::collections::BTreeSet;
    use uuid::Uuid;

    // Tests that the logic for producing candidates for saga recovery only
    // includes sagas in the correct states and that the recovered sagas are
    // properly paginated.
    #[tokio::test]
    async fn test_list_candidate_sagas() {
        // Test setup
        let logctx = dev::test_setup_log("test_list_candidate_sagas");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let sec_id = db::SecId(uuid::Uuid::new_v4());
        let mut inserted_sagas = (0..SQL_BATCH_SIZE.get() * 2)
            .map(|_| SagaTestContext::new(sec_id).new_running_db_saga())
            .collect::<Vec<_>>();

        // Add a saga in the Abandoned state. This shouldn't be returned in the
        // list of recovery candidates.
        inserted_sagas
            .push(SagaTestContext::new(sec_id).new_abandoned_db_saga());

        // Shuffle these sagas into a random order to check that the pagination
        // order is working as intended on the read path, which we'll do later
        // in this test.
        inserted_sagas.shuffle(&mut rand::rng());

        // Insert the batches of unfinished sagas into the database
        let conn = datastore
            .pool_connection_unauthorized()
            .await
            .expect("Failed to access db connection");
        diesel::insert_into(nexus_db_schema::schema::saga::dsl::saga)
            .values(inserted_sagas.clone())
            .execute_async(&*conn)
            .await
            .expect("Failed to insert test setup data");

        // List them, expect to see them all in order by ID.
        let mut observed_sagas = datastore
            .saga_list_recovery_candidates_batched(&opctx, sec_id)
            .await
            .expect("Failed to list unfinished sagas");

        // The abandoned saga shouldn't show up in the output list.
        assert!(
            !observed_sagas
                .iter()
                .any(|s| s.saga_state == SagaState::Abandoned)
        );

        // Remove the abandoned saga from the inserted set so that it can be
        // compared to the observed set.
        inserted_sagas.retain(|s| s.saga_state != SagaState::Abandoned);

        // The observed list is sorted by ID, so sort the inserted list that way
        // too so that the lists can be tested for equality.
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
        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Tests that `saga_list_unfinished_batched` includes running, unwinding,
    // and abandoned sagas (unlike recovery candidate listing, which skips
    // abandoned ones), excludes done sagas, and paginates correctly.
    #[tokio::test]
    async fn test_list_unfinished_batched() {
        // Test setup
        let logctx = dev::test_setup_log("test_list_unfinished_batched");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let sec_id = db::SecId(uuid::Uuid::new_v4());

        // More than one batch of running sagas, so pagination is exercised,
        // plus a few sagas in each of the other states.
        let mut inserted_sagas = (0..SQL_BATCH_SIZE.get() * 2)
            .map(|_| SagaTestContext::new(sec_id).new_running_db_saga())
            .collect::<Vec<_>>();
        for _ in 0..3 {
            inserted_sagas
                .push(SagaTestContext::new(sec_id).new_unwinding_db_saga());
            inserted_sagas
                .push(SagaTestContext::new(sec_id).new_abandoned_db_saga());
            inserted_sagas
                .push(SagaTestContext::new(sec_id).new_done_db_saga());
        }

        // Shuffle these sagas into a random order to check that the
        // pagination order is working as intended on the read path.
        inserted_sagas.shuffle(&mut rand::rng());

        let conn = datastore
            .pool_connection_unauthorized()
            .await
            .expect("Failed to access db connection");
        diesel::insert_into(nexus_db_schema::schema::saga::dsl::saga)
            .values(inserted_sagas.clone())
            .execute_async(&*conn)
            .await
            .expect("Failed to insert test setup data");

        let mut observed_sagas = datastore
            .saga_list_unfinished_batched(&opctx)
            .await
            .expect("Failed to list unfinished sagas");

        // Every state but Done should be present in the result.
        inserted_sagas.retain(|s| s.saga_state != SagaState::Done);
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
            "Observed sagas did not match inserted unfinished sagas"
        );

        // Test cleanup
        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Tests that `saga_latest_node_event_times` returns MAX(event_time) per
    // saga, omits sagas with no events, and stitches chunks together when
    // given more IDs than one chunk holds.
    #[tokio::test]
    async fn test_latest_node_event_times() {
        // Test setup
        let logctx = dev::test_setup_log("test_latest_node_event_times");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let sec_id = SecId(Uuid::new_v4());

        // Explicit whole-second timestamps: CockroachDB stores microseconds,
        // so higher-precision values would not round-trip.
        let t = |secs: i64| {
            chrono::DateTime::from_timestamp(secs, 0).expect("valid timestamp")
        };

        // More sagas than one chunk holds, so the results must be stitched
        // together from more than one query. Each saga gets one event.
        let num_sagas = i64::from(SQL_BATCH_SIZE.get()) + 5;
        let mut event_batch = Vec::new();
        let mut expected = Vec::new();
        for i in 0..num_sagas {
            let cx = SagaTestContext::new(sec_id);
            let mut event =
                cx.new_db_event(0, steno::SagaNodeEventType::Started);
            event.event_time = t(1_000 + i);
            event_batch.push(event);
            expected.push((
                db::saga_types::SagaId::from(cx.saga_id),
                Some(t(1_000 + i)),
            ));
        }

        // One saga with several events: only the latest time is returned.
        let multi_cx = SagaTestContext::new(sec_id);
        for (i, secs) in [10_000, 30_000, 20_000].into_iter().enumerate() {
            let mut event = multi_cx
                .new_db_event(i as u32, steno::SagaNodeEventType::Started);
            event.event_time = t(secs);
            event_batch.push(event);
        }
        expected.push((
            db::saga_types::SagaId::from(multi_cx.saga_id),
            Some(t(30_000)),
        ));

        // One saga with no events at all: absent from the result.
        let no_events_cx = SagaTestContext::new(sec_id);

        let conn = datastore
            .pool_connection_unauthorized()
            .await
            .expect("Failed to access db connection");
        diesel::insert_into(
            nexus_db_schema::schema::saga_node_event::dsl::saga_node_event,
        )
        .values(event_batch)
        .execute_async(&*conn)
        .await
        .expect("Failed to insert test setup data");

        let queried_ids: Vec<_> = expected
            .iter()
            .map(|(id, _)| *id)
            .chain([db::saga_types::SagaId::from(no_events_cx.saga_id)])
            .collect();
        let mut observed = datastore
            .saga_latest_node_event_times(&opctx, &queried_ids)
            .await
            .expect("Failed to load latest node event times");

        observed.sort();
        expected.sort();
        assert_eq!(
            observed, expected,
            "expected MAX(event_time) per saga with events; the saga with \
             no events should be absent"
        );

        // Test cleanup
        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Tests pagination in loading a saga log
    #[tokio::test]
    async fn test_list_unfinished_nodes() {
        // Test setup
        let logctx = dev::test_setup_log("test_list_unfinished_nodes");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let node_cx = SagaTestContext::new(SecId(Uuid::new_v4()));

        // Create a couple batches of saga events
        let mut inserted_nodes = (0..SQL_BATCH_SIZE.get() * 2)
            .flat_map(|i| {
                // This isn't an exhaustive list of event types, but gives us a
                // few options to pick from. Since this is a pagination key,
                // it's important to include a variety here.
                use steno::SagaNodeEventType::*;
                [
                    node_cx.new_db_event(i, Started),
                    node_cx.new_db_event(i, UndoStarted),
                    node_cx.new_db_event(i, UndoFinished),
                ]
            })
            .collect::<Vec<_>>();

        // Shuffle these nodes into a random order to check that the pagination
        // order is working as intended on the read path, which we'll do later
        // in this test.
        inserted_nodes.shuffle(&mut rand::rng());

        // Insert them into the database
        let conn = datastore
            .pool_connection_unauthorized()
            .await
            .expect("Failed to access db connection");
        diesel::insert_into(
            nexus_db_schema::schema::saga_node_event::dsl::saga_node_event,
        )
        .values(inserted_nodes.clone())
        .execute_async(&*conn)
        .await
        .expect("Failed to insert test setup data");

        // List them, expect to see them all in order by ID.
        let observed_nodes = datastore
            .saga_fetch_log_batched(
                &opctx,
                nexus_db_model::saga_types::SagaId::from(node_cx.saga_id),
            )
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
        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Tests the special case of listing an empty saga log
    #[tokio::test]
    async fn test_list_no_unfinished_nodes() {
        // Test setup
        let logctx = dev::test_setup_log("test_list_no_unfinished_nodes");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let saga_id = steno::SagaId(Uuid::new_v4());

        // Test that this returns "no nodes" rather than throwing some "not
        // found" error.
        let observed_nodes = datastore
            .saga_fetch_log_batched(
                &opctx,
                nexus_db_model::saga_types::SagaId::from(saga_id),
            )
            .await
            .expect("Failed to list nodes of unfinished saga");
        assert_eq!(observed_nodes.len(), 0);

        // Test cleanup
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_create_event_idempotent() {
        // Test setup
        let logctx = dev::test_setup_log("test_create_event_idempotent");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let node_cx = SagaTestContext::new(SecId(Uuid::new_v4()));

        // Generate a bunch of events.
        let inserted_nodes = (0..2)
            .flat_map(|i| {
                use steno::SagaNodeEventType::*;
                [
                    node_cx.new_db_event(i, Started),
                    node_cx.new_db_event(i, UndoStarted),
                    node_cx.new_db_event(i, UndoFinished),
                ]
            })
            .collect::<Vec<_>>();

        // Insert the events into the database.
        for node in &inserted_nodes {
            datastore
                .saga_create_event(node)
                .await
                .expect("inserting first node events");
        }

        // Insert the events again into the database and ensure that we don't
        // get a conflict.
        for node in &inserted_nodes {
            datastore
                .saga_create_event(node)
                .await
                .expect("inserting duplicate node events");
        }

        // Test cleanup
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_update_state_idempotent() {
        // Test setup
        let logctx = dev::test_setup_log("test_update_state_idempotent");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let node_cx = SagaTestContext::new(SecId(Uuid::new_v4()));

        // Create a saga in the running state.
        let params = node_cx.new_running_db_saga();
        datastore
            .saga_create(&params)
            .await
            .expect("creating saga in Running state");

        // Attempt to update its state to Running, which is a no-op -- this
        // should be idempotent, so expect success.
        datastore
            .saga_update_state(
                node_cx.saga_id,
                SagaState::Running,
                node_cx.sec_id,
            )
            .await
            .expect("updating state to Running again");

        // Update the state to Done.
        datastore
            .saga_update_state(node_cx.saga_id, SagaState::Done, node_cx.sec_id)
            .await
            .expect("updating state to Done");

        // Attempt to update its state to Done again, which is a no-op -- this
        // should be idempotent, so expect success.
        datastore
            .saga_update_state(node_cx.saga_id, SagaState::Done, node_cx.sec_id)
            .await
            .expect("updating state to Done again");

        // Test cleanup
        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Helpers to create sagas.
    struct SagaTestContext {
        saga_id: steno::SagaId,
        sec_id: SecId,
    }

    impl SagaTestContext {
        fn new(sec_id: SecId) -> Self {
            Self { saga_id: steno::SagaId(Uuid::new_v4()), sec_id }
        }

        fn new_running_db_saga(&self) -> db::model::saga_types::Saga {
            let params = steno::SagaCreateParams {
                id: self.saga_id,
                name: steno::SagaName::new("test saga"),
                dag: serde_json::value::Value::Null,
                state: steno::SagaCachedState::Running,
            };

            db::model::saga_types::Saga::new(self.sec_id, params)
        }

        fn new_abandoned_db_saga(&self) -> db::model::saga_types::Saga {
            let params = steno::SagaCreateParams {
                id: self.saga_id,
                name: steno::SagaName::new("test saga"),
                dag: serde_json::value::Value::Null,
                state: steno::SagaCachedState::Running,
            };

            let mut saga =
                db::model::saga_types::Saga::new(self.sec_id, params);
            saga.saga_state = SagaState::Abandoned;
            saga
        }

        fn new_unwinding_db_saga(&self) -> db::model::saga_types::Saga {
            let params = steno::SagaCreateParams {
                id: self.saga_id,
                name: steno::SagaName::new("test saga"),
                dag: serde_json::value::Value::Null,
                state: steno::SagaCachedState::Unwinding,
            };

            db::model::saga_types::Saga::new(self.sec_id, params)
        }

        fn new_done_db_saga(&self) -> db::model::saga_types::Saga {
            let params = steno::SagaCreateParams {
                id: self.saga_id,
                name: steno::SagaName::new("test saga"),
                dag: serde_json::value::Value::Null,
                state: steno::SagaCachedState::Done,
            };

            db::model::saga_types::Saga::new(self.sec_id, params)
        }

        fn new_db_event(
            &self,
            node_id: u32,
            event_type: steno::SagaNodeEventType,
        ) -> SagaNodeEvent {
            let event = steno::SagaNodeEvent {
                saga_id: self.saga_id,
                node_id: steno::SagaNodeId::from(node_id),
                event_type,
            };

            SagaNodeEvent::new(event, self.sec_id)
        }
    }

    #[tokio::test]
    async fn test_saga_reassignment() {
        // Test setup
        let logctx = dev::test_setup_log("test_saga_reassignment");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Populate the database with a few different sagas:
        //
        // - assigned to SEC A: done, running, and unwinding
        // - assigned to SEC B: done, running, and unwinding
        // - assigned to SEC C: done, running, and unwinding
        // - assigned to SEC D: done, running, and unwinding
        //
        // Then we'll reassign SECs B's and C's sagas to SEC A and check exactly
        // which sagas were changed by this.  This exercises:
        // - that we don't touch A's sagas (the one we're assigning *to*)
        // - that we do touch both B's and C's sagas (the ones we're assigning
        //   *from*)
        // - that we don't touch D's sagas (some other SEC)
        // - that we don't touch any "done" sagas
        // - that we do touch both running and unwinding sagas
        let mut sagas_to_insert = Vec::new();
        let sec_a = SecId(Uuid::new_v4());
        let sec_b = SecId(Uuid::new_v4());
        let sec_c = SecId(Uuid::new_v4());
        let sec_d = SecId(Uuid::new_v4());

        for sec_id in [sec_a, sec_b, sec_c, sec_d] {
            for state in [
                steno::SagaCachedState::Running,
                steno::SagaCachedState::Unwinding,
                steno::SagaCachedState::Done,
            ] {
                let params = steno::SagaCreateParams {
                    id: steno::SagaId(Uuid::new_v4()),
                    name: steno::SagaName::new("tewst saga"),
                    dag: serde_json::value::Value::Null,
                    state,
                };

                sagas_to_insert
                    .push(db::model::saga_types::Saga::new(sec_id, params));
            }
        }
        println!("sagas to insert: {:?}", sagas_to_insert);

        // These two sets are complements, but we write out the conditions to
        // double-check that we've got it right.
        let sagas_affected: BTreeSet<_> = sagas_to_insert
            .iter()
            .filter_map(|saga| {
                ((saga.creator == sec_b || saga.creator == sec_c)
                    && (saga.saga_state == SagaState::Running
                        || saga.saga_state == SagaState::Unwinding))
                    .then(|| saga.id)
            })
            .collect();
        let sagas_unaffected: BTreeSet<_> = sagas_to_insert
            .iter()
            .filter_map(|saga| {
                (saga.creator == sec_a
                    || saga.creator == sec_d
                    || saga.saga_state == SagaState::Done)
                    .then(|| saga.id)
            })
            .collect();
        println!("sagas affected: {:?}", sagas_affected);
        println!("sagas UNaffected: {:?}", sagas_unaffected);
        assert_eq!(sagas_affected.intersection(&sagas_unaffected).count(), 0);
        assert_eq!(
            sagas_affected.len() + sagas_unaffected.len(),
            sagas_to_insert.len()
        );

        // Insert the sagas.
        let count = {
            use nexus_db_schema::schema::saga::dsl;
            let conn = datastore.pool_connection_for_tests().await.unwrap();
            diesel::insert_into(dsl::saga)
                .values(sagas_to_insert)
                .execute_async(&*conn)
                .await
                .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
                .expect("successful insertion")
        };
        assert_eq!(count, sagas_affected.len() + sagas_unaffected.len());

        // Reassign uncompleted sagas from SECs B and C to SEC A.
        let nreassigned = datastore
            .sagas_reassign_sec(&opctx, &[sec_b, sec_c], sec_a)
            .await
            .expect("failed to re-assign sagas");

        // Fetch all the sagas and check their states.
        #[allow(clippy::disallowed_methods)]
        let all_sagas: Vec<_> = datastore
            .pool_connection_for_tests()
            .await
            .unwrap()
            .transaction_async(|conn| async move {
                use nexus_db_schema::schema::saga::dsl;
                conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;
                dsl::saga
                    .select(nexus_db_model::Saga::as_select())
                    .load_async(&conn)
                    .await
            })
            .await
            .unwrap();

        for saga in all_sagas {
            println!("checking saga: {:?}", saga);
            let current_sec = saga.current_sec.unwrap();
            if sagas_affected.contains(&saga.id) {
                assert!(saga.creator == sec_b || saga.creator == sec_c);
                assert_eq!(current_sec, sec_a);
                assert_eq!(*saga.adopt_generation, Generation::from(2));
                assert!(
                    saga.saga_state == SagaState::Running
                        || saga.saga_state == SagaState::Unwinding
                );
            } else if sagas_unaffected.contains(&saga.id) {
                assert_eq!(current_sec, saga.creator);
                assert_eq!(*saga.adopt_generation, Generation::from(1));
                // Its SEC and state could be anything since we've deliberately
                // included sagas with various states and SECs that should not
                // be affected by the reassignment.
            } else {
                println!(
                    "ignoring saga that was not created by this test: {:?}",
                    saga
                );
            }
        }

        assert_eq!(nreassigned, sagas_affected.len());

        // If we do it again, we should make no changes.
        let nreassigned = datastore
            .sagas_reassign_sec(&opctx, &[sec_b, sec_c], sec_a)
            .await
            .expect("failed to re-assign sagas");
        assert_eq!(nreassigned, 0);

        // Test cleanup
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_list_long_running_or_unwinding_sagas() {
        // Test setup
        let logctx =
            dev::test_setup_log("test_list_long_running_or_unwinding_sagas");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let sec_id = db::SecId(uuid::Uuid::new_v4());
        let sec_id2 = db::SecId(uuid::Uuid::new_v4());

        // Insert one saga in each state, plus an additional running saga.
        let running = SagaTestContext::new(sec_id).new_running_db_saga();
        let running2 = SagaTestContext::new(sec_id2).new_running_db_saga();
        let unwinding = SagaTestContext::new(sec_id).new_unwinding_db_saga();
        let done = SagaTestContext::new(sec_id).new_done_db_saga();
        let abandoned = SagaTestContext::new(sec_id).new_abandoned_db_saga();

        let conn = datastore.pool_connection_for_tests().await.unwrap();
        diesel::insert_into(nexus_db_schema::schema::saga::dsl::saga)
            .values(vec![
                running.clone(),
                running2.clone(),
                unwinding.clone(),
                done.clone(),
                abandoned.clone(),
            ])
            .execute_async(&*conn)
            .await
            .expect("Failed to insert test setup data");

        // Querying with a time limit 10 hours in the past should return no
        // sagas, since all test sagas were just created.
        let observed_sagas = datastore
            .saga_list_running_or_unwinding_older_than(
                &opctx,
                Utc::now() - TimeDelta::hours(10),
            )
            .await
            .expect("Failed to list sagas by states");
        assert!(
            observed_sagas.is_empty(),
            "Should return no sagas with a large threshold, got: {:?}",
            observed_sagas,
        );

        // Pushing the time limit into the future (10 seconds from now) avoids
        // flakiness. All sagas in the Running or Unwinding states should be
        // returned.
        let observed_sagas = datastore
            .saga_list_running_or_unwinding_older_than(
                &opctx,
                Utc::now() + TimeDelta::seconds(10),
            )
            .await
            .expect("Failed to list running/unwinding sagas");

        let mut expected_sagas =
            vec![running.clone(), running2.clone(), unwinding.clone()];
        expected_sagas.sort_by_key(|s| s.id);

        assert_eq!(
            observed_sagas, expected_sagas,
            "Should return the Running and Unwinding sagas"
        );

        // Test cleanup
        db.terminate().await;
        logctx.cleanup_successful();
    }
}
