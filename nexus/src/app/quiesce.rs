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
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
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
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
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
