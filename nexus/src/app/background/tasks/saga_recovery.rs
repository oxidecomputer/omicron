// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for recovering sagas assigned to this Nexus

use crate::app::background::BackgroundTask;
use crate::app::sagas::ActionRegistry;
use crate::saga_interface::SagaContext;
use crate::Nexus;
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use serde::Serialize;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::sync::Arc;
use steno::SagaId;
use uuid::Uuid;

/// Background task that recovers sagas assigned to this Nexus
///
/// Normally, this task only does anything of note once, when Nexus starts up.
/// However, it runs periodically and can be activated explicitly for the rare
/// case when a saga has been re-assigned to this Nexus (e.g., because some
/// other Nexus has been expunged).
pub struct SagaRecovery {
    datastore: Arc<DataStore>,
    /// Unique identifier for this Saga Execution Coordinator
    ///
    /// This always matches the Nexus id.
    sec_id: nexus_db_queries::db::SecId,
    /// OpContext used for saga recovery
    saga_recovery_opctx: OpContext,
    nexus: Arc<Nexus>,
    sec: Arc<steno::SecClient>,
    registry: Arc<ActionRegistry>,

    sagas_recovered: BTreeMap<SagaId, DateTime<Utc>>,
    recent_failures: Vec<RecoveryFailure>,
    last_pass: LastPass,
}

// XXX-dap TODO-doc
// XXX-dap omdb
#[derive(Clone, Serialize)]
pub struct SagaRecoveryTaskStatus {
    all_recovered: BTreeMap<SagaId, DateTime<Utc>>,
    recent_failures: Vec<RecoveryFailure>,
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
            sec_id: nexus_db_queries::db::SecId(sec_id),
            saga_recovery_opctx,
            nexus,
            sec,
            registry,
            sagas_recovered: BTreeMap::new(),
            recent_failures: Vec::new(),
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

            let recovered = nexus_db_queries::db::recover(
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
                        self.recent_failures.push(RecoveryFailure {
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
