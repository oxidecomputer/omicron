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
use anyhow::Context;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::DemoSagaUuid;
use serde::Deserialize;
use serde::Serialize;
use slog::info;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use steno::ActionError;
use tokio::sync::Semaphore;

/// Rendezvous point for demo sagas
///
/// This is where:
///
/// - demo sagas wait for a completion message
/// - completion messages are recorded for demo sagas that haven't started
///   waiting yet
///
/// Nexus maintains one of these structures at the top level.
pub struct CompletingDemoSagas {
    sagas: BTreeMap<DemoSagaUuid, Arc<Semaphore>>,
}

impl CompletingDemoSagas {
    pub fn new() -> CompletingDemoSagas {
        CompletingDemoSagas { sagas: BTreeMap::new() }
    }

    pub fn preregister(&mut self, id: DemoSagaUuid) {
        assert!(self.sagas.insert(id, Arc::new(Semaphore::new(0))).is_none());
    }

    pub fn subscribe(
        &mut self,
        id: DemoSagaUuid,
    ) -> impl Future<Output = Result<(), anyhow::Error>> + use<> {
        let sem =
            self.sagas.entry(id).or_insert_with(|| Arc::new(Semaphore::new(0)));
        let sem_clone = sem.clone();
        async move {
            sem_clone
                .acquire()
                .await
                // We don't need the Semaphore permit once we've acquired it.
                .map(|_| ())
                .context("acquiring demo saga semaphore")
        }
    }

    pub fn complete(&mut self, id: DemoSagaUuid) -> Result<(), Error> {
        let sem = self.sagas.get_mut(&id).ok_or_else(|| {
            Error::non_resourcetype_not_found(format!(
                "demo saga with demo saga id {:?}",
                id
            ))
        })?;
        sem.add_permits(1);
        Ok(())
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
        demo_sagas.subscribe(demo_id)
    };
    match rx.await {
        Ok(_) => {
            info!(log, "demo saga: completing"; "id" => %demo_id);
            Ok(())
        }
        Err(error) => {
            warn!(log, "demo saga: waiting failed (Nexus shutting down?)";
                "id" => %demo_id,
                "error" => #?error,
            );
            Err(ActionError::action_failed(Error::internal_error(&format!(
                "demo saga wait failed: {:#}",
                error
            ))))
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use assert_matches::assert_matches;

    #[tokio::test]
    async fn test_demo_saga_rendezvous() {
        let mut hub = CompletingDemoSagas::new();

        // The most straightforward sequence is:
        // - create (preregister) demo saga
        // - demo saga starts and waits for completion (subscribe)
        // - complete demo saga
        let demo_saga_id = DemoSagaUuid::new_v4();
        println!("demo saga: {demo_saga_id}");
        hub.preregister(demo_saga_id);
        println!("demo saga: {demo_saga_id} preregistered");
        let subscribe = hub.subscribe(demo_saga_id);
        println!("demo saga: {demo_saga_id} subscribed");
        assert!(hub.complete(demo_saga_id).is_ok());
        println!("demo saga: {demo_saga_id} marked completed");
        subscribe.await.unwrap();
        println!("demo saga: {demo_saga_id} done");

        // It's also possible that the completion request arrives before the
        // saga started waiting.  In that case, the sequence is:
        //
        // - create (preregister) demo saga
        // - complete demo saga
        // - demo saga starts and waits for completion (subscribe)
        //
        // This should work, too, with no errors.
        let demo_saga_id = DemoSagaUuid::new_v4();
        println!("demo saga: {demo_saga_id}");
        hub.preregister(demo_saga_id);
        println!("demo saga: {demo_saga_id} preregistered");
        assert!(hub.complete(demo_saga_id).is_ok());
        println!("demo saga: {demo_saga_id} marked completed");
        let subscribe = hub.subscribe(demo_saga_id);
        println!("demo saga: {demo_saga_id} subscribed");
        subscribe.await.unwrap();
        println!("demo saga: {demo_saga_id} done");

        // It's also possible to have no preregistration at all.  This happens
        // if the demo saga was recovered.  That's fine, too, but then it will
        // only work if the completion arrives after the saga starts waiting.
        let demo_saga_id = DemoSagaUuid::new_v4();
        println!("demo saga: {demo_saga_id}");
        let subscribe = hub.subscribe(demo_saga_id);
        println!("demo saga: {demo_saga_id} subscribed");
        assert!(hub.complete(demo_saga_id).is_ok());
        println!("demo saga: {demo_saga_id} marked completed");
        subscribe.await.unwrap();
        println!("demo saga: {demo_saga_id} done");

        // If there's no preregistration and we get a completion request, then
        // that request should fail.
        let demo_saga_id = DemoSagaUuid::new_v4();
        println!("demo saga: {demo_saga_id}");
        let error = hub.complete(demo_saga_id).unwrap_err();
        assert_matches!(error, Error::NotFound { .. });
        println!("demo saga: {demo_saga_id} complete error: {:#}", error);
    }
}
