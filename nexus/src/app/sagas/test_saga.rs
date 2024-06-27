// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Saga only used for testing
#![cfg(test)]

use super::{NexusActionContext, NexusSaga, SagaInitError};
use crate::app::saga::create_saga_dag;
use crate::app::sagas::declare_saga_actions;
use nexus_test_utils_macros::nexus_test;
use omicron_common::api::external::Error;
use serde::Deserialize;
use serde::Serialize;
use std::fmt::Debug;
use steno::ActionError;

// test saga: input parameters

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Params {}

// test saga: actions

declare_saga_actions! {
    test_saga;
    TEST_ACTION1 -> "n1" {
        + ts_test_action
        - ts_test_undo
    }
    TEST_ACTION2 -> "n2" {
        + ts_test_action
        - ts_test_undo
    }
}

// test saga: definition

#[derive(Debug)]
pub(crate) struct SagaTest;
impl NexusSaga for SagaTest {
    const NAME: &'static str = "test-saga";
    type Params = Params;

    fn register_actions(registry: &mut super::ActionRegistry) {
        test_saga_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, SagaInitError> {
        builder.append(test_action1_action());
        builder.append(test_action2_action());
        Ok(builder.build()?)
    }
}

async fn ts_test_action(
    _sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    Ok(())
}

async fn ts_test_undo(
    _sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    Ok(())
}

// tests

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<crate::Server>;

#[nexus_test(server = crate::Server)]
async fn test_saga_stuck(cptestctx: &ControlPlaneTestContext) {
    let nexus = &cptestctx.server.server_context().nexus;
    let params = Params {};
    let dag = create_saga_dag::<SagaTest>(params).unwrap();
    let runnable_saga = nexus.sagas.saga_prepare(dag.clone()).await.unwrap();
    let saga_id = runnable_saga.id();

    // Inject an error into the second node's action and the first node's undo
    // action.  This should cause the saga to become stuck.
    let n1 = dag.get_index("n1").unwrap();
    let n2 = dag.get_index("n2").unwrap();
    nexus.sec().saga_inject_error(saga_id, n2).await.unwrap();
    nexus.sec().saga_inject_error_undo(saga_id, n1).await.unwrap();
    let result = runnable_saga
        .start()
        .await
        .expect("expected saga to start")
        .wait_until_stopped()
        .await
        .into_omicron_result()
        .expect_err("expected saga to finish stuck");

    match result {
        Error::InternalError { internal_message } => {
            assert_eq!(
                internal_message,
                "UNDO ACTION failed (node \"n1\", error undo action failed \
                permanently: {\n  \"message\": \"undo action attempt 1: \
                error injected\"\n}) after: saga ACTION error at node \
                \"n2\": error injected"
            );
        }
        error => panic!("unexpected error from saga: {:?}", error),
    }
}
