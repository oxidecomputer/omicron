// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helper functions for writing saga undo tests and working with instances in
//! saga tests.

use super::NexusSaga;
use crate::{
    app::{saga::create_saga_dag, test_interfaces::TestInterfaces as _},
    Nexus,
};
use futures::future::BoxFuture;
use nexus_db_queries::context::OpContext;
use nexus_types::identity::Resource;
use omicron_common::api::external::NameOrId;
use sled_agent_client::TestInterfaces as _;
use slog::{info, Logger};
use std::{num::NonZeroU32, sync::Arc};
use uuid::Uuid;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<crate::Server>;

pub fn test_opctx(cptestctx: &ControlPlaneTestContext) -> OpContext {
    OpContext::for_tests(
        cptestctx.logctx.log.new(o!()),
        cptestctx.server.apictx().nexus.datastore().clone(),
    )
}

pub async fn instance_start(cptestctx: &ControlPlaneTestContext, id: &Uuid) {
    let nexus = &cptestctx.server.apictx().nexus;
    let opctx = test_opctx(&cptestctx);
    let instance_selector =
        nexus_types::external_api::params::InstanceSelector {
            project: None,
            instance: NameOrId::from(*id),
        };

    let instance_lookup =
        nexus.instance_lookup(&opctx, instance_selector).unwrap();
    nexus
        .instance_start(&opctx, &instance_lookup)
        .await
        .expect("Failed to start instance");
}

pub async fn instance_stop(cptestctx: &ControlPlaneTestContext, id: &Uuid) {
    let nexus = &cptestctx.server.apictx().nexus;
    let opctx = test_opctx(&cptestctx);
    let instance_selector =
        nexus_types::external_api::params::InstanceSelector {
            project: None,
            instance: NameOrId::from(*id),
        };

    let instance_lookup =
        nexus.instance_lookup(&opctx, instance_selector).unwrap();
    nexus
        .instance_stop(&opctx, &instance_lookup)
        .await
        .expect("Failed to stop instance");
}

pub async fn instance_stop_by_name(
    cptestctx: &ControlPlaneTestContext,
    name: &str,
    project_name: &str,
) {
    let nexus = &cptestctx.server.apictx().nexus;
    let opctx = test_opctx(&cptestctx);
    let instance_selector =
        nexus_types::external_api::params::InstanceSelector {
            project: Some(project_name.to_string().try_into().unwrap()),
            instance: name.to_string().try_into().unwrap(),
        };

    let instance_lookup =
        nexus.instance_lookup(&opctx, instance_selector).unwrap();
    nexus
        .instance_stop(&opctx, &instance_lookup)
        .await
        .expect("Failed to stop instance");
}

pub async fn instance_delete_by_name(
    cptestctx: &ControlPlaneTestContext,
    name: &str,
    project_name: &str,
) {
    let nexus = &cptestctx.server.apictx().nexus;
    let opctx = test_opctx(&cptestctx);
    let instance_selector =
        nexus_types::external_api::params::InstanceSelector {
            project: Some(project_name.to_string().try_into().unwrap()),
            instance: name.to_string().try_into().unwrap(),
        };

    let instance_lookup =
        nexus.instance_lookup(&opctx, instance_selector).unwrap();
    nexus
        .project_destroy_instance(&opctx, &instance_lookup)
        .await
        .expect("Failed to destroy instance");
}

pub async fn instance_simulate(
    cptestctx: &ControlPlaneTestContext,
    instance_id: &Uuid,
) {
    info!(&cptestctx.logctx.log, "Poking simulated instance";
          "instance_id" => %instance_id);
    let nexus = &cptestctx.server.apictx().nexus;
    let sa = nexus.instance_sled_by_id(instance_id).await.unwrap();
    sa.instance_finish_transition(*instance_id).await;
}

pub async fn instance_simulate_by_name(
    cptestctx: &ControlPlaneTestContext,
    name: &str,
    project_name: &str,
) {
    info!(&cptestctx.logctx.log, "Poking simulated instance";
          "instance_name" => %name,
          "project_name" => %project_name);

    let nexus = &cptestctx.server.apictx().nexus;
    let opctx = test_opctx(&cptestctx);
    let instance_selector =
        nexus_types::external_api::params::InstanceSelector {
            project: Some(project_name.to_string().try_into().unwrap()),
            instance: name.to_string().try_into().unwrap(),
        };

    let instance_lookup =
        nexus.instance_lookup(&opctx, instance_selector).unwrap();
    let (.., instance) = instance_lookup.fetch().await.unwrap();
    let sa = nexus.instance_sled_by_id(&instance.id()).await.unwrap();
    sa.instance_finish_transition(instance.id()).await;
}

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

/// Tests that saga `S` functions properly when any of its nodes fails and the
/// prior node's undo step is repeated during unwind. Like
/// `action_failure_can_unwind`, this routine creates a new DAG with new
/// parameters for each node and verifies that the saga failed at the expected
/// point.
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
pub async fn action_failure_can_unwind_idempotently<'a, S, G, P>(
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

    let node_indices = Vec::from_iter(0..num_nodes);
    for indices in node_indices.windows(2) {
        let params = generate_params().await;
        let dag = create_saga_dag::<S>(params).unwrap();
        let undo_node = dag.get_nodes().nth(indices[0]).unwrap();
        let error_node = dag.get_nodes().nth(indices[1]).unwrap();
        info!(
            log,
            "Creating new saga that will fail at index {:?}", error_node.index();
            "node_name" => error_node.name().as_ref(),
            "label" => error_node.label(),
        );

        let runnable_saga =
            nexus.create_runnable_saga(dag.clone()).await.unwrap();

        nexus
            .sec()
            .saga_inject_error(runnable_saga.id(), error_node.index())
            .await
            .unwrap();

        nexus
            .sec()
            .saga_inject_repeat(
                runnable_saga.id(),
                undo_node.index(),
                steno::RepeatInjected {
                    action: NonZeroU32::new(1).unwrap(),
                    undo: NonZeroU32::new(2).unwrap(),
                },
            )
            .await
            .unwrap();

        let saga_error = nexus
            .run_saga_raw_result(runnable_saga)
            .await
            .expect("saga should have started successfully")
            .kind
            .expect_err("saga execution should have failed");

        assert_eq!(saga_error.error_node_name, *error_node.name());

        after_saga().await;
    }
}
