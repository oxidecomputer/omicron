// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manage Nexus quiesce state

use assert_matches::assert_matches;
use chrono::Utc;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::internal_api::views::QuiesceState;
use nexus_types::internal_api::views::QuiesceStatus;
use nexus_types::quiesce::SagaQuiesceHandle;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::UpdateResult;
use slog::Logger;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::watch;

impl super::Nexus {
    pub async fn quiesce_start(&self, opctx: &OpContext) -> UpdateResult<()> {
        opctx.authorize(authz::Action::Modify, &authz::QUIESCE_STATE).await?;
        self.quiesce.set_quiescing(true);
        Ok(())
    }

    pub async fn quiesce_state(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<QuiesceStatus> {
        opctx.authorize(authz::Action::Read, &authz::QUIESCE_STATE).await?;
        let state = self.quiesce.state();
        let sagas_pending = self.quiesce.sagas().sagas_pending();
        let db_claims = self.datastore().claims_held();
        Ok(QuiesceStatus { state, sagas_pending, db_claims })
    }
}

/// Describes the configuration and state around quiescing Nexus
#[derive(Clone)]
pub struct NexusQuiesceHandle {
    log: Logger,
    datastore: Arc<DataStore>,
    sagas: SagaQuiesceHandle,
    state: watch::Sender<QuiesceState>,
}

impl NexusQuiesceHandle {
    pub fn new(log: &Logger, datastore: Arc<DataStore>) -> NexusQuiesceHandle {
        let my_log = log.new(o!("component" => "NexusQuiesceHandle"));
        let saga_quiesce_log = log.new(o!("component" => "SagaQuiesceHandle"));
        let sagas = SagaQuiesceHandle::new(saga_quiesce_log);
        let (state, _) = watch::channel(QuiesceState::Undetermined);
        NexusQuiesceHandle { log: my_log, datastore, sagas, state }
    }

    pub fn sagas(&self) -> SagaQuiesceHandle {
        self.sagas.clone()
    }

    pub fn state(&self) -> QuiesceState {
        self.state.borrow().clone()
    }

    pub fn set_quiescing(&self, quiescing: bool) {
        let new_state = if quiescing {
            let time_requested = Utc::now();
            let time_draining_sagas = Instant::now();
            QuiesceState::DrainingSagas { time_requested, time_draining_sagas }
        } else {
            QuiesceState::Running
        };

        let changed = self.state.send_if_modified(|q| {
            match q {
                QuiesceState::Undetermined => {
                    info!(&self.log, "initial state"; "state" => ?new_state);
                    *q = new_state;
                    true
                }
                QuiesceState::Running if quiescing => {
                    info!(&self.log, "quiesce starting");
                    *q = new_state;
                    true
                }
                _ => {
                    // All other cases are either impossible or no-ops.
                    false
                }
            }
        });

        // Immediately (synchronously) update the saga quiesce status.  It's
        // okay to do this even if there wasn't a change.
        self.sagas.set_quiescing(quiescing);

        if changed && quiescing {
            // Asynchronously complete the rest of the quiesce process.
            tokio::spawn(do_quiesce(self.clone()));
        }
    }
}

async fn do_quiesce(quiesce: NexusQuiesceHandle) {
    let saga_quiesce = quiesce.sagas.clone();
    let datastore = quiesce.datastore.clone();

    // NOTE: This sequence will change as we implement RFD 588.
    // We will need to use the datastore to report our saga drain status and
    // also to see when other Nexus instances have finished draining their
    // sagas.  For now, this implementation begins quiescing its database as
    // soon as its sagas are locally drained.
    assert_matches!(
        *quiesce.state.borrow(),
        QuiesceState::DrainingSagas { .. }
    );

    // TODO per RFD 588, this is where we will enter a loop, pausing either on
    // timeout or when our local quiesce state changes.  At each pause: if we
    // need to update our db_metadata_nexus record, do so.  Then load the
    // current blueprint and check the records for all nexus instances.
    //
    // For now, we skip the cross-Nexus coordination and simply wait for our own
    // Nexus to finish what it's doing.
    saga_quiesce.wait_for_drained().await;

    quiesce.state.send_modify(|q| {
        let QuiesceState::DrainingSagas {
            time_requested,
            time_draining_sagas,
        } = *q
        else {
            panic!("wrong state in do_quiesce(): {:?}", q);
        };
        let time_draining_db = Instant::now();
        *q = QuiesceState::DrainingDb {
            time_requested,
            time_draining_sagas,
            duration_draining_sagas: time_draining_db - time_draining_sagas,
            time_draining_db,
        };
    });

    datastore.quiesce();
    datastore.wait_for_quiesced().await;
    quiesce.state.send_modify(|q| {
        let QuiesceState::DrainingDb {
            time_requested,
            time_draining_sagas,
            duration_draining_sagas,
            time_draining_db,
        } = *q
        else {
            panic!("wrong state in do_quiesce(): {:?}", q);
        };
        let time_recording_quiesce = Instant::now();
        *q = QuiesceState::RecordingQuiesce {
            time_requested,
            time_draining_sagas,
            duration_draining_sagas,
            duration_draining_db: time_recording_quiesce - time_draining_db,
            time_recording_quiesce,
        };
    });

    // TODO per RFD 588, this is where we will enter a loop trying to update our
    // database record for the last time.

    quiesce.state.send_modify(|q| {
        let QuiesceState::RecordingQuiesce {
            time_requested,
            time_draining_sagas,
            duration_draining_sagas,
            duration_draining_db,
            time_recording_quiesce,
        } = *q
        else {
            panic!("wrong state in do_quiesce(): {:?}", q);
        };

        let finished = Instant::now();
        *q = QuiesceState::Quiesced {
            time_requested,
            time_quiesced: Utc::now(),
            duration_draining_sagas,
            duration_draining_db,
            duration_recording_quiesce: finished - time_recording_quiesce,
            duration_total: finished - time_draining_sagas,
        };
    });
}

#[cfg(test)]
mod test {
    use assert_matches::assert_matches;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use chrono::DateTime;
    use chrono::Utc;
    use diesel::ExpressionMethods;
    use diesel::QueryDsl;
    use http::StatusCode;
    use nexus_client::types::QuiesceState;
    use nexus_client::types::QuiesceStatus;
    use nexus_test_interface::NexusServer;
    use nexus_test_utils_macros::nexus_test;
    use omicron_test_utils::dev::poll::CondCheckError;
    use omicron_test_utils::dev::poll::wait_for_condition;
    use slog::Logger;
    use std::time::Duration;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;
    type NexusClientError = nexus_client::Error<nexus_client::types::Error>;

    async fn wait_quiesce(
        log: &Logger,
        client: &nexus_client::Client,
        timeout: Duration,
    ) -> QuiesceStatus {
        wait_for_condition(
            || async {
                let rv = client
                    .quiesce_get()
                    .await
                    .map_err(|e| CondCheckError::Failed(e))?
                    .into_inner();
                debug!(log, "found quiesce state"; "state" => ?rv);
                if matches!(rv.state, QuiesceState::Quiesced { .. }) {
                    Ok(rv)
                } else {
                    Err(CondCheckError::<
                        nexus_client::Error<nexus_client::types::Error>,
                    >::NotYet)
                }
            },
            &Duration::from_millis(50),
            &timeout,
        )
        .await
        .expect("timed out waiting for quiesce to finish")
    }

    fn verify_quiesced(
        before: DateTime<Utc>,
        after: DateTime<Utc>,
        status: QuiesceStatus,
    ) {
        let QuiesceState::Quiesced {
            time_requested,
            time_quiesced,
            duration_draining_sagas,
            duration_draining_db,
            duration_recording_quiesce,
            duration_total,
        } = status.state
        else {
            panic!("not quiesced");
        };
        let duration_total = Duration::from(duration_total);
        let duration_draining_sagas = Duration::from(duration_draining_sagas);
        let duration_draining_db = Duration::from(duration_draining_db);
        let duration_recording_quiesce =
            Duration::from(duration_recording_quiesce);
        assert!(time_requested >= before);
        assert!(time_requested <= after);
        assert!(time_quiesced >= before);
        assert!(time_quiesced <= after);
        assert!(time_quiesced >= time_requested);
        assert!(duration_total >= duration_draining_sagas);
        assert!(duration_total >= duration_draining_db);
        assert!(duration_total >= duration_recording_quiesce);
        assert!(duration_total <= (after - before).to_std().unwrap());
        assert!(status.sagas_pending.is_empty());
        assert!(status.db_claims.is_empty());
    }

    #[nexus_test(server = crate::Server)]
    async fn test_quiesce_easy(cptestctx: &ControlPlaneTestContext) {
        let log = &cptestctx.logctx.log;
        let nexus_internal_url = format!(
            "http://{}",
            cptestctx.server.get_http_server_internal_address().await
        );
        let nexus_client =
            nexus_client::Client::new(&nexus_internal_url, log.clone());

        // If we quiesce Nexus while it's not doing anything, that should
        // complete quickly.
        let before = Utc::now();
        let _ = nexus_client
            .quiesce_start()
            .await
            .expect("failed to start quiesce");
        let rv =
            wait_quiesce(log, &nexus_client, Duration::from_secs(30)).await;
        let after = Utc::now();
        verify_quiesced(before, after, rv);
    }

    #[nexus_test(server = crate::Server)]
    async fn test_quiesce_full(cptestctx: &ControlPlaneTestContext) {
        let log = &cptestctx.logctx.log;
        let nexus_internal_url = format!(
            "http://{}",
            cptestctx.server.get_http_server_internal_address().await
        );
        let nexus_client =
            nexus_client::Client::new(&nexus_internal_url, log.clone());

        // Kick off a demo saga that will block quiescing.
        let demo_saga = nexus_client
            .saga_demo_create()
            .await
            .expect("create demo saga")
            .into_inner();

        // Grab a database connection.  We'll use this for two purposes: first,
        // this will block database quiescing, allowing us to inspect that
        // intermediate state.  Second, we can use later to check the saga's
        // state.  (We wouldn't be able to get a connection later to do this
        // because quiescing will move forward to the point where we can't get
        // new database connections.)
        let apictx = &cptestctx.server.server_context();
        let nexus = &apictx.nexus;
        let datastore = nexus.datastore();
        let conn = datastore
            .pool_connection_for_tests()
            .await
            .expect("obtaining connection");

        // Start quiescing.
        let before = Utc::now();
        let _ = nexus_client
            .quiesce_start()
            .await
            .expect("failed to start quiesce");
        let quiesce_status = nexus_client
            .quiesce_get()
            .await
            .expect("fetching quiesce state")
            .into_inner();
        debug!(log, "found quiesce status"; "status" => ?quiesce_status);
        assert_matches!(
            quiesce_status.state,
            QuiesceState::DrainingSagas { .. }
        );
        assert!(quiesce_status.sagas_pending.contains_key(&demo_saga.saga_id));
        // We should see at least one held database claim from the one we took
        // above.
        assert!(!quiesce_status.db_claims.is_empty());

        // Creating a demo saga now should fail with a 503.
        let error = nexus_client
            .saga_demo_create()
            .await
            .expect_err("cannot create saga while quiescing");
        assert_eq!(
            error.status().expect("status code"),
            StatusCode::SERVICE_UNAVAILABLE
        );

        // Doing something else that uses the database should work.
        let _unused = nexus_client
            .sled_list_uninitialized()
            .await
            .expect("list uninitialized sleds");

        // Now, complete the demo saga.
        nexus_client
            .saga_demo_complete(&demo_saga.demo_saga_id)
            .await
            .expect("mark demo saga completed");

        // Check that the saga did complete successfully (e.g., that we didn't
        // restrict database activity before successfully recording the final
        // saga state).
        wait_for_condition(
            || async {
                use nexus_db_schema::schema::saga::dsl;
                let saga_state: nexus_db_model::SagaState = dsl::saga
                    .filter(dsl::id.eq(demo_saga.saga_id))
                    .select(dsl::saga_state)
                    .first_async(&*conn)
                    .await
                    .expect("loading saga state");
                if saga_state == nexus_db_model::SagaState::Done {
                    Ok(())
                } else {
                    Err(CondCheckError::<()>::NotYet)
                }
            },
            &Duration::from_millis(50),
            &Duration::from_secs(20),
        )
        .await
        .expect("demo saga should finish");

        // Quiescing should move on to waiting for database claims to be
        // released.
        wait_for_condition(
            || async {
                let rv = nexus_client
                    .quiesce_get()
                    .await
                    .map_err(|e| CondCheckError::Failed(e))?
                    .into_inner();
                debug!(log, "found quiesce state"; "state" => ?rv);
                if !matches!(rv.state, QuiesceState::DrainingDb { .. }) {
                    return Err(CondCheckError::<NexusClientError>::NotYet);
                }
                assert!(rv.sagas_pending.is_empty());
                // The database claim we took is still held.
                assert!(!rv.db_claims.is_empty());
                Ok(())
            },
            &Duration::from_millis(50),
            &Duration::from_secs(30),
        )
        .await
        .expect("updated quiesce state (blocked on saga)");

        // Now, doing something that accesses the database ought to fail with a
        // 503 because we cannot create new database connections.
        let error = nexus_client
            .sled_list_uninitialized()
            .await
            .expect_err("cannot use database while quiescing");
        assert_eq!(
            error.status().expect("status code"),
            StatusCode::SERVICE_UNAVAILABLE
        );

        // Release our connection.  Nexus should finish quiescing.
        drop(conn);
        let rv =
            wait_quiesce(log, &nexus_client, Duration::from_secs(30)).await;
        let after = Utc::now();
        verify_quiesced(before, after, rv);

        // Just to be sure, make sure we can neither create new saga nor access
        // the database.
        let error = nexus_client
            .saga_demo_create()
            .await
            .expect_err("cannot create saga while quiescing");
        assert_eq!(
            error.status().expect("status code"),
            StatusCode::SERVICE_UNAVAILABLE
        );
        let error = nexus_client
            .sled_list_uninitialized()
            .await
            .expect_err("cannot use database while quiescing");
        assert_eq!(
            error.status().expect("status code"),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
}
