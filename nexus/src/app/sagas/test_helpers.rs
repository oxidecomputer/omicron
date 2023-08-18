// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helper functions that provide common scaffolding for testing sagas.
//!
//! These functions aim to help verify two important properties that all sagas
//! should have:
//!
//! - All actions and undo actions must be idempotent.
//! - Sagas must unwind properly from a failure in any node in the saga DAG.
//!
//! It is easy to accidentally write a test that tries to verify one of these
//! properties but produces false negative results. See omicron#3265 and
//! omicron#3894 for examples. The helpers in this module provide common
//! scaffolding that can help prevent these mistakes. They also avoid
//! duplicating boilerplate between saga tests.

use super::NexusSaga;
use crate::{app::saga::create_saga_dag, Nexus};
use futures::future::BoxFuture;
use slog::{info, Logger};
use std::sync::Arc;

/// Tests that a saga `S` functions properly when any of its nodes fails and
/// causes the saga to unwind by iterating over all saga nodes, creating a new
/// saga DAG for each node, injecting an error at the chosen node, and verifying
/// both that the saga failed and that the node at which the failure was
/// injected was the one that actually caused the saga to fail. This last check
/// ensures that all possible unwindings are executed.
///
/// # Arguments
///
/// - `nexus`: A reference to the Nexus that should execute the saga.
/// - `initial_params`: The parameters to use to construct an initial instance
///   of saga `S` so that the scaffold can figure out how many nodes are in the
///   DAG.
/// - `generate_params`: A callback called at the beginning of each loop
///   iteration that returns a future that yields the saga parameters to use for
///   that loop iteration.
/// - `after_saga`: A callback called after saga execution in each loop
///   iteration. The caller may use this to check additional post-execution
///   invariants and to prepare the test for the next loop iteration.
/// - `log`: A logger to which the scaffold should log messages.
///
/// # Panics
///
/// This function asserts that each saga it executes (a) starts successfully,
/// (b) fails, and (c) fails at the specific node at which the function injected
/// a failure.
pub async fn action_failure_can_unwind<'a, S, G, P>(
    nexus: &Arc<Nexus>,
    initial_params: S::Params,
    generate_params: G,
    after_saga: P,
    log: &Logger,
) where
    S: NexusSaga,
    G: Fn() -> BoxFuture<'a, S::Params>,
    P: Fn() -> BoxFuture<'a, ()>,
{
    let dag = create_saga_dag::<S>(initial_params).unwrap();
    let num_nodes = dag.get_nodes().count();
    for failure_index in 0..num_nodes {
        let params = generate_params().await;
        let dag = create_saga_dag::<S>(params).unwrap();
        let node = dag.get_nodes().nth(failure_index).unwrap();
        info!(
            log,
            "Creating new saga that will fail at index {:?}", node.index();
            "node_name" => node.name().as_ref(),
            "label" => node.label()
        );

        let runnable_saga =
            nexus.create_runnable_saga(dag.clone()).await.unwrap();

        nexus
            .sec()
            .saga_inject_error(runnable_saga.id(), node.index())
            .await
            .unwrap();

        let saga_error = nexus
            .run_saga_raw_result(runnable_saga)
            .await
            .expect("saga should have started successfully")
            .kind
            .expect_err("saga execution should have failed");

        assert_eq!(saga_error.error_node_name, *node.name());

        after_saga().await;
    }
}
