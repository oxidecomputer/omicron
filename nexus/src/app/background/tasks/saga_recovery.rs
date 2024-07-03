// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for recovering sagas assigned to this Nexus

use crate::app::background::BackgroundTask;
use crate::app::sagas::ActionRegistry;
use crate::saga_interface::SagaContext;
use crate::Nexus;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use serde_json::json;
use std::sync::Arc;
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
        }
    }
}

impl BackgroundTask for SagaRecovery {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            // XXX-dap We need to modify `recover_saga()` to handle the case
            // that we've already recovered this saga or we're already running
            // it.  This should include cases where:
            // - we recovered in a previous run
            // - we already started it (and maybe finished and are trying to
            //   write out saga log entries reflecting that)
            //   - including the case where it actually finishes in between when
            //     we listed it and when we tried to recover it -- this is
            //     important since the SEC might not remember it any more!
            // - we want tests for all of this, which we might be able to
            //   simulate by manually messing with the same SecClient
            // XXX-dap it'd be nice if this function returned:
            // - a list of sagas that it successfully recovered
            // - a list of sagas whose recovery failed (for a reason other than
            //   "we were already working on it")
            // Then this background task's status could report:
            // - the N most recently-recovered sagas
            // - up to M sagas we failed to recover, and why
            // - how many we recovered in the last go-around
            // XXX-dap review logging around all of this

            nexus_db_queries::db::recover(
                &self.saga_recovery_opctx,
                self.sec_id,
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

            // XXX-dap
            serde_json::Value::Null
        }
        .boxed()
    }
}

// XXX-dap TODO-coverage
