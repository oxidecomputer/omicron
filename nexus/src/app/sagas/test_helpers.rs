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
use async_bb8_diesel::{AsyncRunQueryDsl, AsyncSimpleConnection};
use diesel::{
    BoolExpressionMethods, ExpressionMethods, QueryDsl, SelectableHelper,
};
use futures::future::BoxFuture;
use nexus_db_queries::{
    authz,
    context::OpContext,
    db::{datastore::InstanceAndActiveVmm, lookup::LookupPath, DataStore},
};
use nexus_types::identity::Resource;
use omicron_common::api::external::NameOrId;
use sled_agent_client::TestInterfaces as _;
use slog::{info, warn, Logger};
use std::{num::NonZeroU32, sync::Arc};
use steno::SagaDag;
use uuid::Uuid;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<crate::Server>;

pub fn test_opctx(cptestctx: &ControlPlaneTestContext) -> OpContext {
    OpContext::for_tests(
        cptestctx.logctx.log.new(o!()),
        cptestctx.server.server_context().nexus.datastore().clone(),
    )
}

pub(crate) async fn instance_start(
    cptestctx: &ControlPlaneTestContext,
    id: &Uuid,
) {
    let nexus = &cptestctx.server.server_context().nexus;
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

pub(crate) async fn instance_stop(
    cptestctx: &ControlPlaneTestContext,
    id: &Uuid,
) {
    let nexus = &cptestctx.server.server_context().nexus;
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

pub(crate) async fn instance_stop_by_name(
    cptestctx: &ControlPlaneTestContext,
    name: &str,
    project_name: &str,
) {
    let nexus = &cptestctx.server.server_context().nexus;
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

pub(crate) async fn instance_delete_by_name(
    cptestctx: &ControlPlaneTestContext,
    name: &str,
    project_name: &str,
) {
    let nexus = &cptestctx.server.server_context().nexus;
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

pub(crate) async fn instance_simulate(
    cptestctx: &ControlPlaneTestContext,
    instance_id: &Uuid,
) {
    info!(&cptestctx.logctx.log, "Poking simulated instance";
          "instance_id" => %instance_id);
    let nexus = &cptestctx.server.server_context().nexus;
    let sa = nexus
        .instance_sled_by_id(instance_id)
        .await
        .unwrap()
        .expect("instance must be on a sled to simulate a state change");

    sa.instance_finish_transition(*instance_id).await;
}

pub(crate) async fn instance_simulate_by_name(
    cptestctx: &ControlPlaneTestContext,
    name: &str,
    project_name: &str,
) {
    info!(&cptestctx.logctx.log, "Poking simulated instance";
          "instance_name" => %name,
          "project_name" => %project_name);

    let nexus = &cptestctx.server.server_context().nexus;
    let opctx = test_opctx(&cptestctx);
    let instance_selector =
        nexus_types::external_api::params::InstanceSelector {
            project: Some(project_name.to_string().try_into().unwrap()),
            instance: name.to_string().try_into().unwrap(),
        };

    let instance_lookup =
        nexus.instance_lookup(&opctx, instance_selector).unwrap();
    let (.., instance) = instance_lookup.fetch().await.unwrap();
    let sa = nexus
        .instance_sled_by_id(&instance.id())
        .await
        .unwrap()
        .expect("instance must be on a sled to simulate a state change");
    sa.instance_finish_transition(instance.id()).await;
}

pub async fn instance_fetch(
    cptestctx: &ControlPlaneTestContext,
    instance_id: Uuid,
) -> InstanceAndActiveVmm {
    let datastore = cptestctx.server.server_context().nexus.datastore().clone();
    let opctx = test_opctx(&cptestctx);
    let (.., authz_instance) = LookupPath::new(&opctx, &datastore)
        .instance_id(instance_id)
        .lookup_for(authz::Action::Read)
        .await
        .expect("test instance should be present in datastore");

    let db_state = datastore
        .instance_fetch_with_vmm(&opctx, &authz_instance)
        .await
        .expect("test instance's info should be fetchable");

    info!(&cptestctx.logctx.log, "refetched instance info from db";
              "instance_id" => %instance_id,
              "instance_and_vmm" => ?db_state);

    db_state
}

pub async fn no_virtual_provisioning_resource_records_exist(
    cptestctx: &ControlPlaneTestContext,
) -> bool {
    use nexus_db_queries::db::model::VirtualProvisioningResource;
    use nexus_db_queries::db::schema::virtual_provisioning_resource::dsl;

    let datastore = cptestctx.server.server_context().nexus.datastore().clone();
    let conn = datastore.pool_connection_for_tests().await.unwrap();

    datastore
        .transaction_retry_wrapper("no_virtual_provisioning_resource_records_exist")
        .transaction(&conn, |conn| async move {
            conn
                .batch_execute_async(nexus_test_utils::db::ALLOW_FULL_TABLE_SCAN_SQL)
                .await
                .unwrap();

            Ok(
                dsl::virtual_provisioning_resource
                    .filter(dsl::resource_type.eq(nexus_db_queries::db::model::ResourceTypeProvisioned::Instance.to_string()))
                    .select(VirtualProvisioningResource::as_select())
                    .get_results_async::<VirtualProvisioningResource>(&conn)
                    .await
                    .unwrap()
                    .is_empty()
            )
        }).await.unwrap()
}

pub async fn no_virtual_provisioning_collection_records_using_instances(
    cptestctx: &ControlPlaneTestContext,
) -> bool {
    use nexus_db_queries::db::model::VirtualProvisioningCollection;
    use nexus_db_queries::db::schema::virtual_provisioning_collection::dsl;

    let datastore = cptestctx.server.server_context().nexus.datastore().clone();
    let conn = datastore.pool_connection_for_tests().await.unwrap();

    datastore
        .transaction_retry_wrapper(
            "no_virtual_provisioning_collection_records_using_instances",
        )
        .transaction(&conn, |conn| async move {
            conn.batch_execute_async(
                nexus_test_utils::db::ALLOW_FULL_TABLE_SCAN_SQL,
            )
            .await
            .unwrap();
            Ok(dsl::virtual_provisioning_collection
                .filter(
                    dsl::cpus_provisioned.ne(0).or(dsl::ram_provisioned.ne(0)),
                )
                .select(VirtualProvisioningCollection::as_select())
                .get_results_async::<VirtualProvisioningCollection>(&conn)
                .await
                .unwrap()
                .is_empty())
        })
        .await
        .unwrap()
}

/// Tests that the saga described by `dag` succeeds if each of its nodes is
/// repeated.
///
/// # Panics
///
/// Asserts that a saga can be created from the supplied DAG and that it
/// succeeds when it is executed.
pub(crate) async fn actions_succeed_idempotently(
    nexus: &Arc<Nexus>,
    dag: SagaDag,
) {
    let runnable_saga = nexus.create_runnable_saga(dag.clone()).await.unwrap();
    for node in dag.get_nodes() {
        nexus
            .sec()
            .saga_inject_repeat(
                runnable_saga.id(),
                node.index(),
                steno::RepeatInjected {
                    action: NonZeroU32::new(2).unwrap(),
                    undo: NonZeroU32::new(1).unwrap(),
                },
            )
            .await
            .unwrap();
    }

    nexus.run_saga(runnable_saga).await.expect("Saga should have succeeded");
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
/// - `before_saga`: A function that runs before each execution of the saga
///   under test. This function returns the set of parameters to use for the
///   next saga execution. It may also set up other aspects of the test
///   environment needed to test the target saga (e.g. creating a test
///   instance).
/// - `after_saga`: A function that runs after each execution of the saga under
///   test. This function checks any post-saga invariants and cleans up any
///   objects that should be destroyed before the next test iteration.
/// - `log`: A logger to which the scaffold should log messages.
///
/// # Panics
///
/// This function asserts that each saga it executes (a) starts successfully,
/// (b) fails, and (c) fails at the specific node at which the function injected
/// a failure.
pub(crate) async fn action_failure_can_unwind<'a, S, B, A>(
    nexus: &Arc<Nexus>,
    before_saga: B,
    after_saga: A,
    log: &Logger,
) where
    S: NexusSaga,
    B: Fn() -> BoxFuture<'a, S::Params>,
    A: Fn() -> BoxFuture<'a, ()>,
{
    // Construct the failure index by hand (instead of iterating over a range)
    // to avoid having to pre-construct a DAG for a saga of type S, which
    // requires a separate `S::Params`. (Obtaining parameters from `before_saga`
    // for this purpose may not be correct because that function may have side
    // effects.)
    let mut failure_index = 0;
    let mut previous_node_count = None;
    loop {
        let params = before_saga().await;
        let dag = create_saga_dag::<S>(params).unwrap();
        let node_count = dag.get_nodes().count();

        // Verify that the DAG is not empty and that, if this is not the first
        // iteration, the node count has not changed between iterations (it
        // might be a function of the generated parameters).
        assert_ne!(node_count, 0);
        if let Some(prev_count) = previous_node_count {
            assert_eq!(prev_count, node_count);
        } else {
            previous_node_count = Some(node_count);
        }

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

        failure_index += 1;
        if failure_index >= node_count {
            break;
        }
    }

    assert_no_failed_undo_steps(log, nexus.datastore()).await;
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
/// - `before_saga`: A function that runs before each execution of the saga
///   under test. This function returns the set of parameters to use for the
///   next saga execution. It may also set up other aspects of the test
///   environment needed to test the target saga (e.g. creating a test
///   instance).
/// - `after_saga`: A function that runs after each execution of the saga under
///   test. This function checks any post-saga invariants and cleans up any
///   objects that should be destroyed before the next test iteration.
/// - `log`: A logger to which the scaffold should log messages.
///
/// # Panics
///
/// This function asserts that each saga it executes (a) starts successfully,
/// (b) fails, and (c) fails at the specific node at which the function injected
/// a failure.
pub(crate) async fn action_failure_can_unwind_idempotently<'a, S, B, A>(
    nexus: &Arc<Nexus>,
    before_saga: B,
    after_saga: A,
    log: &Logger,
) where
    S: NexusSaga,
    B: Fn() -> BoxFuture<'a, S::Params>,
    A: Fn() -> BoxFuture<'a, ()>,
{
    // Construct the error index by hand (instead of iterating over a range) to
    // avoid having to pre-construct a DAG for a saga of type S, which requires
    // a separate `S::Params`. (Obtaining parameters from `before_saga` for this
    // purpose may not be correct because that function may have side effects.)
    //
    // To test the effects of repeating an undo node, start injecting failures
    // at the second node in the DAG so that there's always at least one
    // preceding node whose undo step will run.
    let mut error_index = 1;
    let mut previous_node_count = None;
    loop {
        let params = before_saga().await;
        let dag = create_saga_dag::<S>(params).unwrap();
        let node_count = dag.get_nodes().count();

        // Verify that the DAG's node count doesn't change between iterations.
        // The DAG must have at least two nodes so that there's always a step
        // preceding the error step.
        if let Some(prev_count) = previous_node_count {
            assert_eq!(prev_count, node_count);
        } else {
            if node_count < 2 {
                warn!(log, "Saga has fewer than 2 nodes; nothing to undo");
                return;
            }

            previous_node_count = Some(node_count);
        }

        let undo_node = dag.get_nodes().nth(error_index - 1).unwrap();
        let error_node = dag.get_nodes().nth(error_index).unwrap();
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

        error_index += 1;
        if error_index >= node_count {
            break;
        }
    }

    assert_no_failed_undo_steps(log, nexus.datastore()).await;
}

/// Asserts that there are no sagas in the supplied `datastore` for which an
/// undo step failed.
pub(crate) async fn assert_no_failed_undo_steps(
    log: &Logger,
    datastore: &DataStore,
) {
    use nexus_db_queries::db::model::saga_types::SagaNodeEvent;

    let conn = datastore.pool_connection_for_tests().await.unwrap();
    let saga_node_events: Vec<SagaNodeEvent> = datastore
        .transaction_retry_wrapper("assert_no_failed_undo_steps")
        .transaction(&conn, |conn| async move {
            use nexus_db_queries::db::schema::saga_node_event::dsl;

            conn.batch_execute_async(
                nexus_test_utils::db::ALLOW_FULL_TABLE_SCAN_SQL,
            )
            .await
            .unwrap();

            Ok(dsl::saga_node_event
                .filter(dsl::event_type.eq(String::from("undo_failed")))
                .select(SagaNodeEvent::as_select())
                .load_async::<SagaNodeEvent>(&conn)
                .await
                .unwrap())
        })
        .await
        .unwrap();

    for saga_node_event in &saga_node_events {
        error!(log, "saga {:?} is stuck!", saga_node_event.saga_id);
    }

    assert!(saga_node_events.is_empty());
}
