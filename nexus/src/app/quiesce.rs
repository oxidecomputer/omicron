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
use nexus_types::internal_api::views::PendingSagaInfo;
use nexus_types::internal_api::views::QuiesceState;
use nexus_types::internal_api::views::QuiesceStatus;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::UpdateResult;
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;
use tokio::sync::watch;

#[derive(Debug, Error)]
#[error(
    "saga creation and reassignment are disallowed (Nexus quiescing/quiesced)"
)]
pub struct NoSagasAllowedError;
impl From<NoSagasAllowedError> for Error {
    fn from(value: NoSagasAllowedError) -> Self {
        Error::unavail(&value.to_string())
    }
}

/// Describes both the configuration (whether sagas are allowed to be created)
/// and the state (how many sagas are pending) for the purpose of quiescing
/// Nexus.
// Both configuration and state must be combined (under the same watch channel)
// to avoid races in detecting quiesce.  We want the quiescer to be able to say
// that if `sagas_allowed` is `Disallowed` and there are no sagas running, then
// sagas are quiesced.  But there's no way to guarantee that if these are stored
// in separate channels.
//
// This is used by three consumers:
//
// 1. The SagaExecutor uses this in deciding whether to allow creating new
//    sagas and for keeping track of running sagas
//
// 2. The saga recovery background task uses this to track recovered sagas
//
// 3. The blueprint execution background task uses this to decide whether to
//    attempt to re-assign ourselves sagas from expunged Nexus instances.
//
// All of this ensures that once we begin quiescing, we can reliably tell when
// there are no more sagas running *and* that there will never be any more
// sagas running within this process.
#[derive(Debug, Clone)]
struct SagaQuiesce {
    new_sagas_allowed: SagasAllowed,
    sagas_pending: IdOrdMap<PendingSagaInfo>,
    first_recovery_complete: bool,
    reassignment_generation: Generation,
    reassignment_pending: bool,

    recovered_reassignment_generation: Generation,
    recovery_pending: Option<Generation>,
}

impl SagaQuiesce {
    pub fn new() -> SagaQuiesce {
        SagaQuiesce {
            new_sagas_allowed: SagasAllowed::Allowed,
            sagas_pending: IdOrdMap::new(),
            first_recovery_complete: false,
            reassignment_generation: Generation::new(),
            recovered_reassignment_generation: Generation::new(),
        }
    }

    /// Disallow new sagas from being started or re-assigned to this Nexus
    ///
    /// This is currently a one-way trip.  Sagas cannot be un-quiesced.
    pub fn quiesce(&mut self) {
        // Log this before changing the config to make sure this message
        // appears before messages from code paths that saw this change.
        info!(&self.log, "starting saga quiesce");
        self.sagas_allowed = SagasAllowed::Disallowed;
    }

    /// Returns information about running sagas (involves a clone)
    pub fn sagas_pending(&self) -> &IdOrdMap<PendingSagaInfo> {
        &self.sagas_pending
    }

    /// Returns whether sagas are fully and permanently quiesced
    pub fn is_fully_quiesced(&self) -> bool {
        // No new sagas may be created
        self.new_sagas_allowed == SagasAllowed::Disallowed
            // and there are none currently running
            && self.sagas_pending.is_empty()
            // and there are none from a previous lifetime that still need to be
            // recovered
            && self.first_recovery_complete
            // and there are none that blueprint execution may have re-assigned
            // to us that have not been recovered
            && self.reassignment_generation
                <= self.recovered_reassignment_generation
    }

    /// Record that we're beginning an operation that might assign sagas to us.
    ///
    /// Only one of these may be outstanding at a time.  The caller must call
    /// `reassignment_finish()` before starting another one of these.
    pub fn reassignment_begin(&mut self) -> Result<(), NoSagasAllowedError> {
        if self.new_sagas_allowed != SagasAllowed::Allowed {
            return Err(NoSagasAllowedError);
        }

        assert!(!self.reassignment_pending);
        self.reassignment_pending = true;
    }

    /// Record that we've finished an operation that might assign new sagas to
    /// ourselves.
    pub fn reassignment_finish(&mut self, maybe_reassigned: bool) {
        assert!(self.reassignment_pending);
        self.reassignment_pending = false;

        if maybe_reassigned {
            // XXX-dap double-check that this is the right time to do this,
            // particularly in the very first generation
            self.reassignment_generation = self.reassignment_generation.next();
        }
    }

    /// Record that we've begun recovering sagas.
    ///
    /// Only one of these may be outstanding at a time.  The caller must call
    /// `saga_recovery_finish()` before starting another one of these.
    pub fn saga_recovery_start(&mut self) {
        assert!(self.recovery_pending.is_none());
        self.recovery_pending = Some(self.reassignment_generation);
    }

    /// Record that we've finished recovering sagas.
    pub fn saga_recovery_done(&mut self) {
        let Some(generation) = self.recovery_pending.take() else {
            panic!("cannot finish saga recovery when it was not running");
        };

        self.recovered_reassignment_generation = generation;
        self.first_recovery_complete = true;
    }

    /// Report that a saga has started running
    ///
    /// This fails if sagas are quiesced.
    ///
    /// Callers must also call `saga_completion_future()` to make sure it's
    /// recorded when this saga finishes.
    pub fn saga_created(
        &mut self,
        saga_id: steno::SagaId,
        saga_name: &steno::SagaName,
    ) -> Result<(), NoSagasAllowedError> {
        if self.new_sagas_allowed != SagasAllowed::Allowed {
            return Err(NoSagasAllowedError);
        }

        self.sagas_pending
            .insert_unique(PendingSagaInfo {
                saga_id,
                saga_name: saga_name.clone(),
                time_pending: Utc::now(),
                recovered: false,
                completion_task: None,
            })
            .expect("created saga should have unique id");
        Ok(())
    }

    /// Report that the given saga is being recovered
    ///
    /// This is analogous to `saga_created()`, but for sagas that were recovered
    /// rather than having been created within the lifetime of this Nexus
    /// process.
    ///
    /// Callers must also call `saga_completion_future()` to make sure it's
    /// recorded when this saga finishes.
    ///
    /// Callers may invoke this function more than once for a saga as long as
    /// the saga has not yet finished.  (This only has one caller, the saga
    /// recovery background task, which aready takes care to avoid recovering
    /// sagas that might possibly have finished already.)
    ///
    /// Unlike `saga_created()`, this cannot fail as a result of sagas being
    /// quiesced.  That's because a saga that *needs* to be recovered is a
    /// blocker for quiesce, whether it's running or not.  So we need to
    /// actually run and finish it.  We do still want to prevent ourselves from
    /// taking on sagas needing recovery -- that's why we fail
    /// `reassignment_start()` when saga creation is disallowed.
    // XXX-dap is that right?  do we really want to block re-assignment of
    // sagas?
    pub fn saga_recovered(
        &mut self,
        saga_id: steno::SagaId,
        saga_name: &steno::SagaName,
    ) {
        // It's okay to call this more than once, so we ignore the possible
        // error from `insert_unique()`.
        let _ = self.sagas_pending.insert_unique(PendingSagaInfo {
            saga_id,
            saga_name: saga_name.clone(),
            time_pending: Utc::now(),
            recovered: true,
            completion_task: None,
        });
    }

    /// Provide a `Future` that finishes when the saga has finished running.
    ///
    /// Returns an equivalent Future.
    ///
    /// This must be called exactly once for each saga created or recovered.
    ///
    /// Internally, the returned future wraps the provided future, but allows
    /// this structure to update its state to reflect that the saga is no longer
    /// running.
    pub fn saga_completion_future(
        &mut self,
        saga_id: steno::SagaId,
        fut: BoxFuture<'static, Result<(), Error>>,
    ) -> BoxFuture<'static, Result<(), Error>> {
        let pending = self
            .sagas_pending
            .get(&saga_id)
            .expect("saga should already be running");
        assert!(
            pending.completion_task.is_none(),
            "saga_completion_future() invoked twice for the same saga id"
        );
        todo!(); // XXX-dap
    }

    /// Record that a saga will never finish because creating or recovering it
    /// failed
    pub fn saga_start_fail(&mut self, saga_id: steno::SagaId) {
        let pending = self
            .sagas_pending
            .remove(&saga_id)
            .expect("saga should have been pending");
        assert!(
            pending.completion_task.is_none(),
            "saga_start_fail() cannot be invoked on saga with completion future"
        );
    }
}

/// Policy determining whether new sagas are allowed to be started
///
/// This is used by Nexus quiesce to disallow creation of new sagas when we're
/// trying to quiesce Nexus.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum SagasAllowed {
    /// New sagas may be started (normal condition)
    Allowed,
    /// New sagas may not be started (happens during quiesce)
    Disallowed,
}

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
