// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manage Nexus quiesce state

use super::saga::SagaExecutor;
use assert_matches::assert_matches;
use chrono::Utc;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::internal_api::views::QuiesceState;
use nexus_types::internal_api::views::QuiesceStatus;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::UpdateResult;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::watch;

impl super::Nexus {
    pub async fn quiesce_start(&self, opctx: &OpContext) -> UpdateResult<()> {
        opctx.authorize(authz::Action::Modify, &authz::QUIESCE_STATE).await?;
        let started = self.quiesce.send_if_modified(|q| {
            if let QuiesceState::Running = q {
                let time_requested = Utc::now();
                let time_waiting_for_sagas = Instant::now();
                *q = QuiesceState::WaitingForSagas {
                    time_requested,
                    time_waiting_for_sagas,
                };
                true
            } else {
                false
            }
        });
        if started {
            tokio::spawn(do_quiesce(
                self.quiesce.clone(),
                self.sagas.clone(),
                self.datastore().clone(),
            ));
        }
        Ok(())
    }

    pub async fn quiesce_state(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<QuiesceStatus> {
        opctx.authorize(authz::Action::Read, &authz::QUIESCE_STATE).await?;
        let state = self.quiesce.borrow().clone();
        let sagas_running = self.sagas.sagas_running();
        let db_claims = self.datastore().claims_held();
        Ok(QuiesceStatus { state, sagas_running, db_claims })
    }
}

async fn do_quiesce(
    quiesce: watch::Sender<QuiesceState>,
    saga_exec: Arc<SagaExecutor>,
    datastore: Arc<DataStore>,
) {
    assert_matches!(*quiesce.borrow(), QuiesceState::WaitingForSagas { .. });
    saga_exec.quiesce();
    saga_exec.wait_for_quiesced().await;
    quiesce.send_modify(|q| {
        let QuiesceState::WaitingForSagas {
            time_requested,
            time_waiting_for_sagas,
        } = *q
        else {
            panic!("wrong state in do_quiesce(): {:?}", q);
        };
        *q = QuiesceState::WaitingForDb {
            time_requested,
            time_waiting_for_sagas,
            duration_waiting_for_sagas: time_waiting_for_sagas.elapsed(),
            time_waiting_for_db: Instant::now(),
        };
    });

    datastore.quiesce();
    datastore.wait_for_quiesced().await;
    quiesce.send_modify(|q| {
        let QuiesceState::WaitingForDb {
            time_requested,
            time_waiting_for_sagas,
            duration_waiting_for_sagas,
            time_waiting_for_db,
        } = *q
        else {
            panic!("wrong state in do_quiesce(): {:?}", q);
        };
        let finished = Instant::now();
        *q = QuiesceState::Quiesced {
            time_requested,
            duration_waiting_for_sagas,
            duration_waiting_for_db: finished - time_waiting_for_db,
            duration_total: finished - time_waiting_for_sagas,
            time_quiesced: Utc::now(),
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
            duration_waiting_for_sagas,
            duration_waiting_for_db,
            duration_total,
        } = status.state
        else {
            panic!("not quiesced");
        };
        let duration_total = Duration::from(duration_total);
        let duration_waiting_for_sagas =
            Duration::from(duration_waiting_for_sagas);
        let duration_waiting_for_db = Duration::from(duration_waiting_for_db);
        assert!(time_requested >= before);
        assert!(time_requested <= after);
        assert!(time_quiesced >= before);
        assert!(time_quiesced <= after);
        assert!(time_quiesced >= time_requested);
        assert!(duration_total >= duration_waiting_for_sagas);
        assert!(duration_total >= duration_waiting_for_db);
        assert!(duration_total <= (after - before).to_std().unwrap());
        assert!(status.sagas_running.is_empty());
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
            QuiesceState::WaitingForSagas { .. }
        );
        assert!(quiesce_status.sagas_running.contains_key(&demo_saga.saga_id));
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
                if !matches!(rv.state, QuiesceState::WaitingForDb { .. }) {
                    return Err(CondCheckError::<NexusClientError>::NotYet);
                }
                assert!(rv.sagas_running.is_empty());
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
