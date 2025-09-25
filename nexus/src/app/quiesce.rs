// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manage Nexus quiesce state

use anyhow::{Context, anyhow, bail};
use assert_matches::assert_matches;
use chrono::Utc;
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
use slog::{error, info};
use slog_error_chain::InlineErrorChain;
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
    datastore: Arc<DataStore>,
    my_nexus_id: OmicronZoneUuid,
    sagas: SagaQuiesceHandle,
    quiesce_opctx: Arc<OpContext>,
    latest_blueprint:
        watch::Receiver<Option<Arc<(BlueprintTarget, Blueprint)>>>,
    state: watch::Sender<QuiesceState>,
}

impl NexusQuiesceHandle {
    pub fn new(
        datastore: Arc<DataStore>,
        my_nexus_id: OmicronZoneUuid,
        latest_blueprint: watch::Receiver<
            Option<Arc<(BlueprintTarget, Blueprint)>>,
        >,
        quiesce_opctx: OpContext,
    ) -> NexusQuiesceHandle {
        let saga_quiesce_log =
            quiesce_opctx.log.new(o!("component" => "SagaQuiesceHandle"));
        let sagas = SagaQuiesceHandle::new(saga_quiesce_log);
        let (state, _) = watch::channel(QuiesceState::Undetermined);
        NexusQuiesceHandle {
            datastore,
            my_nexus_id,
            sagas,
            quiesce_opctx: Arc::new(quiesce_opctx),
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

        let log = &self.quiesce_opctx.log;
        let changed = self.state.send_if_modified(|q| {
            match q {
                QuiesceState::Undetermined => {
                    info!(log, "initial state"; "state" => ?new_state);
                    *q = new_state;
                    true
                }
                QuiesceState::Running => {
                    if quiescing {
                        info!(log, "quiesce starting");
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

async fn do_quiesce(mut quiesce: NexusQuiesceHandle) {
    let datastore = quiesce.datastore.clone();
    let my_nexus_id = quiesce.my_nexus_id;

    /// minimal timeout used just to avoid tight loops when encountering
    /// transient errors
    const PAUSE_TIMEOUT: Duration = Duration::from_secs(5);

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
    // to that blueprint, _and_ we've recovered all sagas assigned to us, _and_
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
    let mut last_recorded_blueprint_id: Option<BlueprintUuid> = None;
    loop {
        match check_all_sagas_drained(
            &mut quiesce,
            &mut last_recorded_blueprint_id,
            PAUSE_TIMEOUT,
        )
        .await
        {
            Err(error) => {
                // Log the error, sleep a bit to avoid spinning rapidly when
                // conditions haven't changed, then take another lap.
                let log = &quiesce.quiesce_opctx.log;
                warn!(log, "not yet quiesced"; InlineErrorChain::new(&*error));
                tokio::time::sleep(PAUSE_TIMEOUT).await;
                continue;
            }
            Ok(_) => {
                // We're done with this stage.
                let log = &quiesce.quiesce_opctx.log;
                info!(log, "sagas quiesced -- moving on");
                break;
            }
        }
    }

    // We're ready to hand off.
    let quiesce_opctx = &quiesce.quiesce_opctx;
    let log = &quiesce_opctx.log;
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

    loop {
        match datastore.database_nexus_access_update_quiesced(my_nexus_id).await
        {
            Ok(count) => {
                info!(
                    log,
                    "updated Nexus record to 'quiesced'";
                    "count" => count,
                );
                break;
            }
            Err(error) => {
                warn!(
                    log,
                    "failed to update Nexus record to 'quiesced'";
                    InlineErrorChain::new(&error)
                );
                tokio::time::sleep(PAUSE_TIMEOUT).await;
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

/// Determines whether the fleet of currently-active Nexus instances have
/// all become drained as of the same blueprint.  If some, returns `Ok(())`.
/// Otherwise, returns an error describing why we're not drained.
///
/// Invoked by the caller in a loop until this function returns that we're
/// quiesced.
async fn check_all_sagas_drained(
    quiesce: &mut NexusQuiesceHandle,
    last_recorded_blueprint_id: &mut Option<BlueprintUuid>,
    pause_timeout: Duration,
) -> Result<(), anyhow::Error> {
    // See the comment in our caller for an explanation of the big picture here.
    // This is factored into a function so that we can more easily bail out with
    // an error and handle those uniformly (in the caller).

    let saga_quiesce = quiesce.sagas.clone();
    let datastore = quiesce.datastore.clone();
    let my_nexus_id = quiesce.my_nexus_id;
    let quiesce_opctx = &quiesce.quiesce_opctx;
    let log = &quiesce_opctx.log;
    debug!(log, "try_saga_quiesce(): enter");

    // Grab the most recently loaded blueprint.
    let current_blueprint = quiesce
        .latest_blueprint
        // Wait for a blueprint to be loaded, if necessary.
        .wait_for(|value| value.is_some())
        .await
        // This should be impossible
        .context("latest_blueprint rx channel closed")?
        .as_deref()
        // unwrap(): wait_for() returns a value for which the closure
        // returns true, and we checked that this is `Some`
        .unwrap()
        // extract just the blueprint part
        .1
        // As usual, we clone to avoid locking the watch channel for the
        // lifetime of this value.
        .clone();
    debug!(
        log,
        "try_saga_quiesce(): blueprint";
        "blueprint_id" => %current_blueprint.id
    );

    // Determine our own Nexus generation number.
    //
    // This doesn't ever change once we've determined it once.  But we don't
    // know what the value is until we see our first blueprint.
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
        // This case should generally be impossible.  We *are* a working Nexus
        // zone, so how do we not exist in the blueprint?  Anyway, there's
        // really not much we can do here.  Sleep a little bit just to avoid
        // spinning rapidly, then try again in hopes that this was some
        // transient thing.
        //
        // (Panicking would not be better.  That would likely put us into a
        // restart loop until SMF gives up.  Then nothing would clear that
        // condition.)
        let error = anyhow!(
            "could not find self ({my_nexus_id}) in blueprint {}",
            current_blueprint.id
        );
        error!(
            log,
            "try_saga_quiesce(): impossible condition";
            InlineErrorChain::new(&*error)
        );
        return Err(error);
    };

    // Wait a bounded amount of time for sagas to become drained as of this
    // blueprint.
    let Ok(_) = tokio::time::timeout(
        pause_timeout,
        saga_quiesce.wait_for_drained_blueprint(current_blueprint.id),
    )
    .await
    else {
        // We're still not drained as of this blueprint (or we're drained as of
        // a newer one).  It's possible that the current target blueprint could
        // have changed, so bail out and let the caller invoke us again.
        bail!("not locally drained as of blueprint {}", current_blueprint.id);
    };

    // We're drained up through this blueprint.  First, update our record to
    // reflect that, if we haven't already.
    match last_recorded_blueprint_id {
        Some(blueprint_id) if *blueprint_id == current_blueprint.id => (),
        _ => {
            info!(
                log,
                "locally drained as of blueprint";
                "blueprint_id" => %current_blueprint.id
            );
            let try_update = datastore
                .database_nexus_access_update_blueprint(
                    quiesce_opctx,
                    my_nexus_id,
                    Some(current_blueprint.id),
                )
                .await;
            match try_update {
                Err(error) => {
                    return Err(anyhow!(error).context(
                        "updating our db_metadata_nexus record's drained \
                         blueprint",
                    ));
                }
                Ok(0) => (),
                Ok(count) => {
                    info!(
                        log,
                        "updated our db_metadata_nexus record blueprint id";
                        "nexus_id" => %my_nexus_id,
                        "blueprint_id" => %current_blueprint.id,
                        "count" => count,
                    );
                }
            }

            *last_recorded_blueprint_id = Some(current_blueprint.id);
        }
    };

    // Now, see if everybody else is drained up to the same point, or if any
    // of them is already quiesced.  That would mean *they* already saw that we
    // were all drained up to the same point, which is good enough for us to
    // proceed, too.
    let our_gen_nexus_ids: BTreeSet<OmicronZoneUuid> = current_blueprint
        .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
        .filter_map(|(_sled_id, zone)| {
            if let BlueprintZoneType::Nexus(nexus) = &zone.zone_type {
                (nexus.nexus_generation == my_generation).then_some(zone.id)
            } else {
                None
            }
        })
        .collect();
    assert!(!our_gen_nexus_ids.is_empty());
    let other_records = datastore
        .database_nexus_access_all(&quiesce_opctx, &our_gen_nexus_ids)
        .await
        .context(
            "loading db_metadata_nexus records for other Nexus instances",
        )?;

    if let Some(other) = other_records
        .iter()
        .find(|r| r.state() == DbMetadataNexusState::Quiesced)
    {
        info!(
            log,
            "found other Nexus instance with 'quiesced' record";
            "other_nexus" => %other.nexus_id(),
        );
        return Ok(());
    }

    if other_records.len() < our_gen_nexus_ids.len() {
        // This shouldn't ever happen.  But if it does, we don't want to
        // proceed because we don't know if the missing Nexus zone really is
        // drained.
        bail!(
            "found too few other Nexus records (expected {:?}, found {:?})",
            our_gen_nexus_ids,
            other_records,
        );
    }

    match other_records
        .iter()
        .find(|r| r.last_drained_blueprint_id() != *last_recorded_blueprint_id)
    {
        Some(undrained) => Err(anyhow!(
            "at least one Nexus instance is not drained as of blueprint \
                 {:?}: Nexus {}",
            last_recorded_blueprint_id,
            undrained.nexus_id()
        )),
        None => {
            info!(
                log,
                "all Nexus instances are drained as of blueprint {:?}",
                last_recorded_blueprint_id
            );
            Ok(())
        }
    }
}

#[cfg(test)]
mod test {
    use crate::app::quiesce::NexusQuiesceHandle;
    use crate::app::sagas::test_helpers::test_opctx;
    use assert_matches::assert_matches;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use chrono::DateTime;
    use chrono::Utc;
    use diesel::ExpressionMethods;
    use diesel::QueryDsl;
    use http::StatusCode;
    use nexus_db_model::DbMetadataNexusState;
    use nexus_lockstep_client::types::QuiesceState;
    use nexus_lockstep_client::types::QuiesceStatus;
    use nexus_test_interface::NexusServer;
    use nexus_test_utils::db::TestDatabase;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::deployment::BlueprintTarget;
    use nexus_types::deployment::BlueprintTargetSet;
    use nexus_types::deployment::BlueprintZoneDisposition;
    use nexus_types::quiesce::SagaReassignmentDone;
    use omicron_test_utils::dev::poll::CondCheckError;
    use omicron_test_utils::dev::poll::wait_for_condition;
    use omicron_test_utils::dev::test_setup_log;
    use slog::Logger;
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::watch;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;
    type NexusClientError =
        nexus_lockstep_client::Error<nexus_lockstep_client::types::Error>;

    async fn wait_quiesce(
        log: &Logger,
        client: &nexus_lockstep_client::Client,
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
                    Err(CondCheckError::<NexusClientError>::NotYet)
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
        before_instant: Instant,
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
        assert!(duration_total <= before_instant.elapsed());
        assert!(status.sagas.sagas_pending.is_empty());
        assert!(status.db_claims.is_empty());
    }

    async fn enable_blueprint_execution(cptestctx: &ControlPlaneTestContext) {
        let opctx = test_opctx(&cptestctx);
        let nexus = &cptestctx.server.server_context().nexus;
        nexus
            .blueprint_target_set_enabled(
                &opctx,
                BlueprintTargetSet {
                    target_id: nexus
                        .blueprint_target_view(&opctx)
                        .await
                        .expect("current blueprint target")
                        .target_id,
                    enabled: true,
                },
            )
            .await
            .expect("enable blueprint execution");
    }

    /// Exercise trivial case of app-level quiesce in an environment with just
    /// one Nexus
    #[nexus_test(server = crate::Server)]
    async fn test_quiesce_easy(cptestctx: &ControlPlaneTestContext) {
        let log = &cptestctx.logctx.log;
        let nexus_lockstep_url = format!(
            "http://{}",
            cptestctx.server.get_http_server_lockstep_address().await
        );
        let nexus_client = nexus_lockstep_client::Client::new(
            &nexus_lockstep_url,
            log.clone(),
        );

        // We need to enable blueprint execution in order to complete a saga
        // assignment pass, which is required for quiescing to work.
        enable_blueprint_execution(&cptestctx).await;

        // If we quiesce the only Nexus while it's not doing anything, that
        // should complete quickly.
        let before = Utc::now();
        let before_instant = Instant::now();
        let _ = nexus_client
            .quiesce_start()
            .await
            .expect("failed to start quiesce");
        let rv =
            wait_quiesce(log, &nexus_client, Duration::from_secs(30)).await;
        let after = Utc::now();
        verify_quiesced(before, before_instant, after, rv);
    }

    /// Exercise non-trivial app-level quiesce in an environment with just one
    /// Nexus
    #[nexus_test(server = crate::Server)]
    async fn test_quiesce_full(cptestctx: &ControlPlaneTestContext) {
        let log = &cptestctx.logctx.log;
        let nexus_lockstep_url = format!(
            "http://{}",
            cptestctx.server.get_http_server_lockstep_address().await
        );
        let nexus_client = nexus_lockstep_client::Client::new(
            &nexus_lockstep_url,
            log.clone(),
        );

        // We need to enable blueprint execution in order to complete a saga
        // assignment pass, which is required for quiescing to work.
        enable_blueprint_execution(&cptestctx).await;

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
        let before_instant = Instant::now();
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
        verify_quiesced(before, before_instant, after, rv);

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

    /// Test Nexus quiesce with multiple different Nexus instances
    ///
    /// Unlike the tests above, this is not an "app-level" test.  There's not a
    /// full Nexus here.  We're testing with just a `NexusQuiesceHandle` and the
    /// few things that it requires (e.g., the datastore).
    #[tokio::test]
    async fn test_quiesce_multi() {
        use nexus_types::internal_api::views::QuiesceState;

        let logctx = test_setup_log("test_quiesce_multi");
        let log = &logctx.log;
        let testdb = TestDatabase::new_with_datastore(log).await;
        let opctx = testdb.opctx();
        let datastore = testdb.datastore();

        // Set up.  We need:
        //
        // - a datastore
        // - a blueprint that includes several Nexus instances
        // - a watch Receiver to provide this blueprint to the
        //   NexusQuiesceHandle
        // - the datastore needs to be initialized with the Nexus access records
        //   for the Nexus instances in the blueprint

        let (_collection, _input, blueprint) =
            nexus_reconfigurator_planning::example::example(
                log,
                "test_quiesce_multi",
            );
        let nexus_ids = blueprint
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            .filter_map(|(_sled_id, z)| z.zone_type.is_nexus().then_some(z.id))
            .collect::<Vec<_>>();
        // The example system creates three sleds.  Each sled gets a Nexus zone.
        assert_eq!(
            nexus_ids.len(),
            3,
            "Example system configuration has been changed.  \
             Please update this test."
        );

        let mut nexus_id_iter = nexus_ids.iter();
        let nexus_id1 = *nexus_id_iter.next().expect("at least one Nexus id");
        let nexus_id2 = *nexus_id_iter.next().expect("at least two Nexus ids");
        let nexus_id3 =
            *nexus_id_iter.next().expect("at least three Nexus ids");

        // Fake up enough of what the blueprint loader produces.
        let bp_target = BlueprintTarget {
            target_id: blueprint.id,
            enabled: false,
            time_made_target: Utc::now(),
        };
        let blueprint_id = blueprint.id;
        let (_, blueprint_rx) =
            watch::channel(Some(Arc::new((bp_target, blueprint))));

        // Insert active records for the Nexus instances.
        let conn =
            datastore.pool_connection_for_tests().await.expect("db conn");
        datastore
            .initialize_nexus_access_from_blueprint_on_connection(
                &conn,
                nexus_ids.into_iter().collect(),
            )
            .await
            .expect("initialize Nexus database access records");
        drop(conn);

        // Initialize our quiesce handles.
        //
        // Each needs its own DataStore, backed by its own Pool, in order to
        // behave like three different Nexus instances.
        let nexus_qq1 = NexusQuiesceHandle::new(
            testdb.extra_datastore(log).await,
            nexus_id1,
            blueprint_rx.clone(),
            opctx.child(BTreeMap::new()),
        );
        let nexus_qq2 = NexusQuiesceHandle::new(
            testdb.extra_datastore(log).await,
            nexus_id2,
            blueprint_rx.clone(),
            opctx.child(BTreeMap::new()),
        );
        let nexus_qq3 = NexusQuiesceHandle::new(
            testdb.extra_datastore(log).await,
            nexus_id3,
            blueprint_rx.clone(),
            opctx.child(BTreeMap::new()),
        );

        let handles = [&nexus_qq1, &nexus_qq2, &nexus_qq3];

        // Verify that at start, each handle's quiesce state is undetermined.
        // This is tested more exhaustively elsewhere (in the SagaQuiesceHandle
        // tests).  We're just checking our assumption.
        for qq in &handles {
            let state = qq.state();
            assert_matches!(state, QuiesceState::Undetermined);
        }

        // Mark each handle as not-quiescing.
        for qq in &handles {
            qq.set_quiescing(false);
            let state = qq.state();
            assert_matches!(state, QuiesceState::Running);
        }

        // In order to quiesce, each handle will need to have completed a
        // re-assignment pass for our blueprint and also one saga recovery pass.
        // Do both of those now.
        for qq in &handles {
            let sagas = qq.sagas();
            sagas
                .reassign_sagas(async || {
                    (
                        (),
                        SagaReassignmentDone::ReassignedAllAsOf(
                            blueprint_id,
                            false,
                        ),
                    )
                })
                .await;
            sagas.recover(async |_| ((), true)).await;
        }

        // Before we actually start quiescing, create a saga in one of these
        // handles.
        let saga_ref = nexus_qq1
            .sagas()
            .saga_create(
                steno::SagaId(Uuid::new_v4()),
                &steno::SagaName::new("test-saga"),
            )
            .expect("create saga while not quiesced");

        // Now, start quiescing them all.
        for qq in &handles {
            qq.set_quiescing(true);
            let state = qq.state();
            assert_matches!(state, QuiesceState::DrainingSagas { .. });
        }

        // Importantly, *none* of these handles should quiesce while there's a
        // saga running in any of them.  It's hard to verify a negative.  We'll
        // wait a little while and make sure the state hasn't changed.
        let _ = tokio::time::sleep(Duration::from_secs(10)).await;
        for qq in &handles {
            assert_matches!(qq.state(), QuiesceState::DrainingSagas { .. });
        }
        let records = datastore
            .database_nexus_access_all(
                &opctx,
                &BTreeSet::from([nexus_id1, nexus_id2, nexus_id3]),
            )
            .await
            .expect("reading access records");
        assert_eq!(records.len(), 3);
        assert!(
            records.iter().all(|r| r.state() == DbMetadataNexusState::Active)
        );

        // Now finish that saga.  All three handles should quiesce.
        drop(saga_ref);
        wait_for_condition(
            || async {
                if !handles
                    .iter()
                    .all(|q| matches!(q.state(), QuiesceState::Quiesced { .. }))
                {
                    return Err(CondCheckError::<()>::NotYet);
                }

                Ok(())
            },
            &Duration::from_millis(250),
            &Duration::from_secs(15),
        )
        .await
        .expect("did not quiesce within timeout");

        // Each "Nexus" record should say that it's quiesced.
        //
        // Note that we're using a different datastore (backed by the same
        // CockroachDB instance) than the quiesce handles are.  That's important
        // since their datastores are all quiesced!
        let records = datastore
            .database_nexus_access_all(
                &opctx,
                &BTreeSet::from([nexus_id1, nexus_id2, nexus_id3]),
            )
            .await
            .expect("reading access records");
        assert_eq!(records.len(), 3);
        assert!(
            records.iter().all(|r| r.state() == DbMetadataNexusState::Quiesced)
        );

        testdb.terminate().await;
        logctx.cleanup_successful();
    }
}
