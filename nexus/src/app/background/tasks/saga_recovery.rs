// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Saga recovery
//!
//! ## Review of distributed sagas
//!
//! Nexus uses distributed sagas via [`steno`] to manage multi-step operations
//! that have their own unwinding or cleanup steps.  While executing sagas,
//! critical state is durably stored in the **saga log** such that after a
//! crash, the saga can be resumed while maintaining certain guarantees:
//!
//! - During normal execution, each **action** will be executed at least once.
//! - If an action B depends on action A, then once B has started, A will not
//!   run again.
//! - Once any action has failed, the saga is **unwound**, meaning that the undo
//!   actions are executed for any action that has successfully completed.
//! - The saga will not come to rest until one of these three things has
//!   happened:
//!   1. All actions complete successfully.  This is the normal case of saga
//!      completion.
//!   2. Any number of actions complete successfully, at least one action
//!      failed, and the undo actions complete successfully for any actions that
//!      *did* run.  This is the normal case of clean saga failure where
//!      intuitively the state of the world is unwound to match whatever it was
//!      before the saga ran.
//!   3. Any number of actions complete successfully, at least one action
//!      failed, and at least one undo action also failed.  This is a nebulous
//!      "stuck" state where the world may be partially changed by the saga.
//!
//! There's more to all this (see the Steno docs), but the important thing here
//! is that the persistent state is critical for ensuring these properties
//! across a Nexus crash.  The process of resuming in-progress sagas after a
//! crash is called **saga recovery**.  Fortunately, Steno handles the details
//! of those constraints.  All we have to do is provide Steno with the
//! persistent state of any sagas that it needs to resume.
//!
//!
//! ## Saga recovery and persistent state
//!
//! Everything needed to recover a saga is stored in:
//!
//! 1. a **saga** record, which is mostly immutable
//! 2. the **saga log**, an append-only description of exactly what happened
//!    during execution
//!
//! Responsibility for persisting this state is divided across Steno and Nexus:
//!
//! 1. Steno tells its consumer (Nexus) precisely what information needs to be
//!    stored and when.  It does this by invoking methods on the `SecStore`
//!    trait at key points in the saga's execution.  Steno does not care how
//!    this information is stored or where it is stored.
//!
//! 2. Nexus serializes the given state and stores it into CockroachDB using
//!    the `saga` and `saga_node_event` tables.
//!
//! After a crash, Nexus is then responsible for:
//!
//! 1. Identifying what sagas were in progress before the crash,
//! 2. Loading all the information about them from the database (namely, the
//!    `saga` record and the full saga log in the form of records from the
//!    `saga_node_event` table), and
//! 3. Providing all this information to Steno so that it can resume running the
//!    saga.
//!
//!
//! ## Saga recovery: not just at startup
//!
//! So far, this is fairly straightforward.  What makes it tricky is that there
//! are situations where we want to carry out saga recovery after Nexus has
//! already started and potentially recovered other sagas and started its own
//! sagas.  Specifically, when a Nexus instance gets **expunged** (removed
//! permanently), it may have outstanding sagas that need to be re-assigned to
//! another Nexus instance, which then needs to resume them.  To do this, we run
//! saga recovery in a Nexus background task so that it runs both periodically
//! and on-demand when activated.  (This approach is also useful for other
//! reasons, like retrying recovery for sagas whose recovery failed due to
//! transient errors.)
//!
//! Why does this make things tricky?  When Nexus goes to identify what sagas
//! it needs to recover, it lists sagas that are (1) assigned to it (as opposed
//! to a different Nexus) and (2) not yet finished.  But that could include
//! sagas in one of three groups:
//!
//! 1. Sagas from a previous Nexus [Unix process] lifetime that have not yet
//!    been recovered in this lifetime.  These **should** be recovered.
//! 2. Sagas from a previous Nexus [Unix process] lifetime that have already
//!    been recovered in this lifetime.  These **should not** be recovered.
//! 3. Sagas that were created in this Nexus lifetime.  These **should not** be
//!    recovered.
//!
//! There are a bunch of ways to attack this problem.  We do it by keeping track
//! in-memory of the set of sagas that might be running in the current process
//! and then ignoring those when we do recovery.  Even this is easier said than
//! done!  It's easy enough to insert new sagas into the set whenever a saga is
//! successfully recovered as well as any time a saga is created for the first
//! time (though that requires a structure that's modifiable from multiple
//! different contexts).  But to avoid this set growing unbounded, we should
//! remove entries when a saga finishes running.  When exactly can we do that?
//! We have to be careful of the intrinsic race between when the recovery
//! process queries the database to list candidate sagas for recovery (i.e.,
//! unfinished sagas assigned to this Nexus) and when it checks the set of sagas
//! that should be ignored.  Suppose a saga is running, the recovery process
//! finds it, then the saga finishes, it gets removed from the set, and then the
//! recovery process checks the set.  We'll think it wasn't running and start it
//! again -- very bad.  We can't remove anything from the set until we know that
//! the saga recovery task _doesn't_ have a stale list of candidate sagas to be
//! recovered.
//!
//! This constraint suggests the solution: the set will be owned and managed
//! entirely by the task that's doing saga recovery.  We'll use a channel to
//! trigger inserts when sagas are created elsewhere in Nexus.  What about
//! deletes?  The recovery process can actually figure out on its own when a
//! saga can be removed: if a saga was _not_ in the list of candidates to be
//! recovered, then that means it's finished, and that means it can be deleted
//! from the set.  Care must be taken to process things in the right order, but
//! in the end it's pretty simple: the recovery process first fetches the
//! candidate list of sagas, then processes all insertions that have come in
//! over the channel and adds them to the set, then compares the set against the
//! candidate list.  Sagas in the set and not in the list can be removed.  Sagas
//! in the list and not in the set must be recovered.  Sagas in both the set and
//! the list may or may not still be running, but they were running at some
//! point during this recovery pass and they can be safely ignored until the
//! next pass.

use crate::app::background::BackgroundTask;
use crate::app::sagas::ActionRegistry;
use crate::saga_interface::SagaContext;
use crate::Nexus;
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::DataStore;
use serde::Serialize;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Arc;
use steno::SagaId;
use uuid::Uuid;

/// Maximum number of recent failures to keep track of for debugging
const N_FAILED_SAGA_HISTORY: usize = 16;

/// Background task that recovers sagas assigned to this Nexus
///
/// Normally, this task only does anything of note once, when Nexus starts up.
/// However, it runs periodically and can be activated explicitly for the rare
/// case when a saga has been re-assigned to this Nexus (e.g., because some
/// other Nexus has been expunged) and to handle retries for sagas whose
/// previous recovery failed.
pub struct SagaRecovery {
    datastore: Arc<DataStore>,
    /// Unique identifier for this Saga Execution Coordinator
    ///
    /// This always matches the Nexus id.
    sec_id: db::SecId,
    /// OpContext used for saga recovery
    saga_recovery_opctx: OpContext,
    nexus: Arc<Nexus>,
    sec: Arc<steno::SecClient>,
    registry: Arc<ActionRegistry>,

    sagas_recovered: BTreeMap<SagaId, DateTime<Utc>>,
    recent_failures: VecDeque<RecoveryFailure>,
    last_pass: LastPass,
}

// XXX-dap TODO-doc
// XXX-dap omdb
#[derive(Clone, Serialize)]
pub struct SagaRecoveryTaskStatus {
    all_recovered: BTreeMap<SagaId, DateTime<Utc>>,
    recent_failures: VecDeque<RecoveryFailure>,
    last_pass: LastPass,
}

// XXX-dap TODO-doc
#[derive(Clone, Serialize)]
pub struct RecoveryFailure {
    time: DateTime<Utc>,
    saga_id: SagaId,
    message: String,
}

// XXX-dap TODO-doc
#[derive(Clone, Serialize)]
pub enum LastPass {
    NeverStarted,
    Failed { message: String },
    Recovered { nrecovered: usize, nfailed: usize, nskipped: usize },
}

impl SagaRecovery {
    pub fn new(
        datastore: Arc<DataStore>,
        sec_id: Uuid,
        saga_recovery_opctx: OpContext,
        nexus: Arc<Nexus>,
        sec: Arc<steno::SecClient>,
        registry: Arc<ActionRegistry>,
    ) -> SagaRecovery {
        SagaRecovery {
            datastore,
            sec_id: db::SecId(sec_id),
            saga_recovery_opctx,
            nexus,
            sec,
            registry,
            sagas_recovered: BTreeMap::new(),
            recent_failures: VecDeque::with_capacity(N_FAILED_SAGA_HISTORY),
            last_pass: LastPass::NeverStarted,
        }
    }
}

impl BackgroundTask for SagaRecovery {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            // XXX-dap TODO-doc all of this, especially why it's critical that
            // we keep track of already-recovered sagas and why we do it the way
            // we do.
            // XXX-dap TODO-coverage figure out how to test all this
            // XXX-dap review logging around all of this

            let recovered = db::recover(
                &self.saga_recovery_opctx,
                self.sec_id,
                &|saga_id| self.sagas_recovered.contains_key(&saga_id),
                &|saga_logger| {
                    // The extra `Arc` is a little ridiculous.  The problem is
                    // that Steno expects (in `sec_client.saga_resume()`) that
                    // the user-defined context will be wrapped in an `Arc`.
                    // But we already use `Arc<SagaContext>` for our type.
                    // Hence we need two Arcs.
                    Arc::new(Arc::new(SagaContext::new(
                        self.nexus.clone(),
                        saga_logger.clone(),
                    )))
                },
                &self.datastore,
                &self.sec,
                self.registry.clone(),
            )
            .await;

            let last_pass = match recovered {
                Err(error) => {
                    warn!(opctx.log, "saga recovery pass failed"; &error);
                    LastPass::Failed {
                        message: InlineErrorChain::new(&error).to_string(),
                    }
                }

                Ok(ok) => {
                    let nrecovered = ok.iter_recovered().count();
                    let nfailed = ok.iter_failed().count();
                    let nskipped = ok.iter_skipped().count();

                    info!(opctx.log, "saga recovery pass completed";
                        "nrecovered" => nrecovered,
                        "nfailed" => nfailed,
                        "nskipped" => nskipped,
                    );

                    let now = Utc::now();
                    for saga_id in ok.iter_recovered() {
                        self.sagas_recovered.insert(saga_id, now);
                    }

                    for (saga_id, error) in ok.iter_failed() {
                        if self.recent_failures.len() == N_FAILED_SAGA_HISTORY {
                            let _ = self.recent_failures.pop_front();
                        }
                        self.recent_failures.push_back(RecoveryFailure {
                            time: now,
                            saga_id,
                            message: InlineErrorChain::new(error).to_string(),
                        });
                    }

                    LastPass::Recovered { nrecovered, nfailed, nskipped }
                }
            };

            self.last_pass = last_pass;

            serde_json::to_value(SagaRecoveryTaskStatus {
                all_recovered: self.sagas_recovered.clone(),
                recent_failures: self.recent_failures.clone(),
                last_pass: self.last_pass.clone(),
            })
            .unwrap()
        }
        .boxed()
    }
}

// XXX-dap TODO-coverage
