// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manage Nexus quiesce state

use assert_matches::assert_matches;
use chrono::Utc;
use nexus_db_model::DbMetadataNexus;
use nexus_db_model::DbMetadataNexusState;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintTarget;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::internal_api::views::QuiesceState;
use nexus_types::internal_api::views::QuiesceStatus;
use nexus_types::quiesce::SagaQuiesceHandle;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::UpdateResult;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use slog::Logger;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
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
        let sagas = self.quiesce.sagas().status();
        let db_claims = self.datastore().claims_held();
        Ok(QuiesceStatus { state, sagas, db_claims })
    }
}

/// Describes the configuration and state around quiescing Nexus
#[derive(Clone)]
pub struct NexusQuiesceHandle {
    log: Logger,
    datastore: Arc<DataStore>,
    my_nexus_id: OmicronZoneUuid,
    sagas: SagaQuiesceHandle,
    latest_blueprint:
        watch::Receiver<Option<Arc<(BlueprintTarget, Blueprint)>>>,
    state: watch::Sender<QuiesceState>,
}

impl NexusQuiesceHandle {
    pub fn new(
        log: &Logger,
        datastore: Arc<DataStore>,
        my_nexus_id: OmicronZoneUuid,
        latest_blueprint: watch::Receiver<
            Option<Arc<(BlueprintTarget, Blueprint)>>,
        >,
    ) -> NexusQuiesceHandle {
        let my_log = log.new(o!("component" => "NexusQuiesceHandle"));
        let saga_quiesce_log = log.new(o!("component" => "SagaQuiesceHandle"));
        let sagas = SagaQuiesceHandle::new(saga_quiesce_log);
        let (state, _) = watch::channel(QuiesceState::Undetermined);
        NexusQuiesceHandle {
            log: my_log,
            datastore,
            my_nexus_id,
            sagas,
            latest_blueprint,
            state,
        }
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
                QuiesceState::Running => {
                    if quiescing {
                        info!(&self.log, "quiesce starting");
                        *q = new_state;
                        true
                    } else {
                        // We're not quiescing and not being asked to quiesce.
                        // Nothing to do.
                        false
                    }
                }
                QuiesceState::DrainingSagas { .. }
                | QuiesceState::DrainingDb { .. }
                | QuiesceState::RecordingQuiesce { .. }
                | QuiesceState::Quiesced { .. } => {
                    // Once we start quiescing, we never go back.
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
    let my_nexus_id = quiesce.my_nexus_id;

    assert_matches!(
        *quiesce.state.borrow(),
        QuiesceState::DrainingSagas { .. }
    );

    // We've recorded that we should be quiescing.  This will prevent new sagas
    // from starting.  However, we can still wind up running new sagas if a
    // blueprint execution re-assigns us saga from an expunged Nexus.
    //
    // So here's the plan: we keep track of the blueprint id as of which we have
    // fully drained.  That means that we've processed all Nexus expungements up
    // to that blueprint, _and_ we've recovered all sagas assigned to us, _and
    // finished running them all.  When this blueprint id changes (because we've
    // processed re-assignments for a new blueprint), we'll do two things:
    //
    // (1) update our `db_metadata_nexus` record to reflect the change
    //
    // (2) check the `db_metadata_nexus` records of the other active Nexus
    //     instances to see if they've drained as of the same blueprint.  If so,
    //     then all of the active Nexus instances have finished all sagas in the
    //     system and we can proceed to quiesce the database.
    //
    // If the other Nexus instances aren't drained up through the same
    // blueprint, we'll check again shortly.
    //
    // One obvious case that's not handled here is that it's possible that other
    // Nexus instances are drained as of a *subsequent* blueprint than the one
    // we're looking for.  That's no problem.  We'll check again with the latest
    // blueprint on the next lap.  For this to converge, we have to assume
    // blueprints aren't changing faster than we're checking.  That's a pretty
    // safe assumption here.  They shouldn't be changing at all at this point
    // unless reacting to something unrelated to the upgrade (e.g., a sled
    // expungement), and we are checking awfully frequently, too.
    const POLL_TIMEOUT: Duration = Duration::from_secs(1);
    let mut last_recorded_blueprint_id: Option<BlueprintUuid> = None;
    loop {
        // Grab the most recently loaded blueprint.  As usual, we clone to avoid
        // locking the watch channel for the lifetime of this value.
        let Some(current_blueprint): Option<Blueprint> = quiesce
            .latest_blueprint
            .borrow()
            .as_deref()
            .map(|(_target, blueprint)| blueprint)
            .cloned()
        else {
            // XXX-dap log
            // XXX-dap need to sleep here, but we won't.  Could we use
            // wait_until() it's non-None?
            continue;
        };

        // Determine our own Nexus generation number.
        //
        // This doesn't change across iterations of the loop.  But we need a
        // blueprint to figure it out, and we don't have one until we're in this
        // loop.
        let Some(my_generation) = current_blueprint
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            .find_map(|(_sled_id, zone)| {
                if let BlueprintZoneType::Nexus(nexus) = &zone.zone_type {
                    (zone.id == my_nexus_id).then_some(nexus.nexus_generation)
                } else {
                    None
                }
            })
        else {
            // XXX-dap log, and also need to sleep here
            continue;
        };

        // Wait up to a second for sagas to become drained as of this blueprint.
        let Ok(_) = tokio::time::timeout(
            POLL_TIMEOUT,
            saga_quiesce.wait_for_drained_blueprint(current_blueprint.id),
        )
        .await
        else {
            // We're still not drained as of this blueprint.  But the
            // current target blueprint could have changed.  So take another
            // lap.
            continue;
        };

        // We're drained up through this blueprint.  First, update our record to
        // reflect that, if we haven't already.
        match last_recorded_blueprint_id {
            Some(blueprint_id) if blueprint_id == current_blueprint.id => (),
            _ => {
                let try_update = datastore
                    .database_nexus_access_update_blueprint(
                        my_nexus_id,
                        Some(current_blueprint.id),
                    )
                    .await;
                match try_update {
                    Err(error) => {
                        // Take another lap.
                        // XXX-dap log error
                        continue;
                    }
                    Ok(0) => (),
                    Ok(count) => {
                        // XXX-dap log
                    }
                }

                last_recorded_blueprint_id = Some(current_blueprint.id);
            }
        };

        // Now, see if everybody else is drained up to the same point, or if any
        // of them is already quiesced.  (That means *they* already saw that we
        // were all drained up to the same point, which is good enough for us to
        // proceed.)
        let other_nexus_ids: BTreeSet<OmicronZoneUuid> = current_blueprint
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            .filter_map(|(_sled_id, zone)| {
                if let BlueprintZoneType::Nexus(nexus) = &zone.zone_type {
                    (nexus.nexus_generation == my_generation).then_some(zone.id)
                } else {
                    None
                }
            })
            .collect();
        assert!(!other_nexus_ids.is_empty());
        let Ok(other_records): Result<Vec<DbMetadataNexus>, _> =
            datastore.database_nexus_access_all(&other_nexus_ids).await
        else {
            // XXX-dap log error
            continue;
        };

        if other_records
            .iter()
            .any(|r| r.state() == DbMetadataNexusState::Quiesced)
        {
            // XXX-dap log
            break;
        }

        if other_records.len() < other_nexus_ids.len() {
            // XXX-dap log
            continue;
        }

        if other_records.iter().all(|r| {
            r.last_drained_blueprint_id() == last_recorded_blueprint_id
        }) {
            // XXX-dap log
            break;
        }

        // XXX-dap log take another lap
    }

    // We're ready to hand off.
    //
    // XXX-dap write down that this fixes oxidecomputer/omicron#8859,
    // oxidecomputer/omicron#8857, and oxidecomputer/omicron#8796
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

    // XXX-dap write that this fixes omicron#8971.
    loop {
        match datastore.database_nexus_access_update_quiesced(my_nexus_id).await
        {
            Ok(count) => {
                // XXX-dap log and proceed
                break;
            }
            Err(error) => {
                // XXX-dap log, sleep and try again
                tokio::time::sleep(POLL_TIMEOUT).await;
            }
        }
    }

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

    // We're done!  Thank you for service, Nexus `$nexus_id`.
    //
    // At this point, although most of the rest of Nexus is still running (e.g.,
    // background tasks) and it can even receive HTTP requests, everything that
    // tries to use the database will fail.  That's just about everything.
    //
    // This process will hang around until the new Nexus instances expunge this
    // one.  This is useful, since debugging tools and tests can still query
    // this Nexus for its quiesce state.
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
        assert!(status.sagas.sagas_pending.is_empty());
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
        assert!(
            quiesce_status.sagas.sagas_pending.contains_key(&demo_saga.saga_id)
        );
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
                assert!(rv.sagas.sagas_pending.is_empty());
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
