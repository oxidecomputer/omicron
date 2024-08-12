// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Demo saga, used for testing and interactive debugging
//!
//! The "Demo" saga exists so that developers and automated tests can create a
//! saga that will not complete until they take some action to complete it.  The
//! saga just waits until it gets the message that it should finish.  Users
//! create demo sagas and complete them using requests to the internal API.
//!
//! The implementation is entirely in-memory, which means you have to send the
//! completion message to the Nexus that's running the saga.  However, it does
//! work across Nexus restarts, so this can be used to exercise the saga
//! recovery path.
//!
//! It's tempting to build this only for development and not official releases,
//! but that'd be more work, there's little downside to always including it, and
//! it's conceivable that it'd be useful for production systems, too.

use super::NexusActionContext;
use super::{ActionRegistry, NexusSaga, SagaInitError};
use crate::app::sagas::declare_saga_actions;
use anyhow::ensure;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::DemoSagaUuid;
use serde::Deserialize;
use serde::Serialize;
use slog::info;
use std::collections::BTreeMap;
use steno::ActionError;
use tokio::sync::oneshot;

/// Set of demo sagas that have been marked completed
///
/// Nexus maintains one of these at the top level.  Individual demo sagas wait
/// until their id shows up here, then remove it and proceed.
pub struct CompletingDemoSagas {
    ids: BTreeMap<DemoSagaUuid, oneshot::Sender<()>>,
}

impl CompletingDemoSagas {
    pub fn new() -> CompletingDemoSagas {
        CompletingDemoSagas { ids: BTreeMap::new() }
    }

    pub fn complete(&mut self, id: DemoSagaUuid) -> Result<(), Error> {
        self.ids
            .remove(&id)
            .ok_or_else(|| {
                Error::non_resourcetype_not_found(format!(
                    "demo saga with id {:?}",
                    id
                ))
            })?
            .send(())
            .map_err(|_| {
                Error::internal_error(
                    "saga stopped listening (Nexus shutting down?)",
                )
            })
    }

    pub fn subscribe(
        &mut self,
        id: DemoSagaUuid,
    ) -> Result<oneshot::Receiver<()>, anyhow::Error> {
        let (tx, rx) = oneshot::channel();
        ensure!(
            self.ids.insert(id, tx).is_none(),
            "multiple subscriptions for the same demo saga"
        );
        Ok(rx)
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub id: DemoSagaUuid,
}

declare_saga_actions! {
    demo;
    DEMO_WAIT -> "demo_wait" {
        + demo_wait
    }
}

#[derive(Debug)]
pub(crate) struct SagaDemo;
impl NexusSaga for SagaDemo {
    const NAME: &'static str = "demo";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        demo_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, SagaInitError> {
        builder.append(demo_wait_action());
        Ok(builder.build()?)
    }
}

async fn demo_wait(sagactx: NexusActionContext) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let demo_id = sagactx.saga_params::<Params>()?.id;
    let log = osagactx.log();
    info!(log, "demo saga: begin wait"; "id" => %demo_id);
    let rx = {
        let mut demo_sagas = osagactx
            .nexus()
            .demo_sagas()
            .map_err(ActionError::action_failed)?;
        demo_sagas.subscribe(demo_id).map_err(|e| {
            ActionError::action_failed(Error::internal_error(&format!(
                "demo saga subscribe failed: {:#}",
                e
            )))
        })?
    };
    match rx.await {
        Ok(_) => {
            info!(log, "demo saga: completing"; "id" => %demo_id);
        }
        Err(_) => {
            info!(log, "demo saga: waiting failed (Nexus shutting down?)";
                "id" => %demo_id);
        }
    }
    Ok(())
}
