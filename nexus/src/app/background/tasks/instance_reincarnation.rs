// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for automatically restarting failed instances.

use crate::app::background::BackgroundTask;
use crate::app::db;
use crate::app::saga::StartSaga;
use crate::app::sagas::instance_start;
use crate::app::sagas::NexusSaga;
use futures::future::BoxFuture;
use nexus_db_queries::authn;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::pagination::Paginator;
use nexus_db_queries::db::DataStore;
use nexus_types::identity::Resource;
use nexus_types::internal_api::background::InstanceReincarnationStatus;
use omicron_common::api::external::Error;
use std::num::NonZeroU32;
use std::sync::Arc;
use steno::SagaId;
use uuid::Uuid;

pub struct InstanceReincarnation {
    datastore: Arc<DataStore>,
    sagas: Arc<dyn StartSaga>,
    /// The maximum number of concurrently executing instance-start sagas.
    concurrency_limit: NonZeroU32,
    disabled: bool,
}

const DEFAULT_MAX_CONCURRENT_REINCARNATIONS: NonZeroU32 =
    match NonZeroU32::new(16) {
        Some(n) => n,
        None => unreachable!(), // 16 > 0
    };

type RunningSaga = (Uuid, SagaId, BoxFuture<'static, Result<(), Error>>);

impl BackgroundTask for InstanceReincarnation {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        Box::pin(async move {
            let mut status = InstanceReincarnationStatus::default();
            match self.actually_activate(opctx, &mut status).await {
                Err(error) => {
                    error!(
                        &opctx.log,
                        "instance reincarnation failed!";
                        "instances_found" => status.instances_found,
                        "instances_reincarnated" => status.instances_reincarnated.len(),
                        "instances_changed_state" => status.changed_state.len(),
                        "error" => %error,
                        "restart_errors" => status.restart_errors.len(),
                    );
                    status.query_error = Some(error.to_string());
                }
                Ok(()) if !status.restart_errors.is_empty() => {
                    warn!(
                        &opctx.log,
                        "instance reincarnation completed with saga errors";
                        "instances_found" => status.instances_found,
                        "instances_reincarnated" => status.instances_reincarnated.len(),
                        "instances_changed_state" => status.changed_state.len(),
                        "restart_errors" => status.restart_errors.len(),
                    );
                }
                Ok(()) => {
                    info!(
                        &opctx.log,
                        "instance reincarnation completed successfully";
                        "instances_found" => status.instances_found,
                        "instances_reincarnated" => status.instances_reincarnated.len(),
                        "instances_changed_state" => status.changed_state.len(),
                    );
                }
            }

            serde_json::json!(status)
        })
    }
}

impl InstanceReincarnation {
    pub(crate) fn new(
        datastore: Arc<DataStore>,
        sagas: Arc<dyn StartSaga>,
        disabled: bool,
    ) -> Self {
        Self {
            datastore,
            sagas,
            concurrency_limit: DEFAULT_MAX_CONCURRENT_REINCARNATIONS,
            disabled,
        }
    }

    async fn actually_activate(
        &mut self,
        opctx: &OpContext,
        status: &mut InstanceReincarnationStatus,
    ) -> anyhow::Result<()> {
        // /!\ BREAK GLASS IN CASE OF EMERGENCY /!\
        anyhow::ensure!(
            !self.disabled,
            "instance reincarnation explicitly disabled by config"
        );

        let mut running_sagas =
            Vec::with_capacity(self.concurrency_limit.get() as usize);
        let serialized_authn = authn::saga::Serialized::for_opctx(opctx);

        let mut paginator = Paginator::new(self.concurrency_limit);
        while let Some(p) = paginator.next() {
            let batch = self
                .datastore
                .find_reincarnatable_instances(opctx, &p.current_pagparams())
                .await?;

            paginator = p.found_batch(&batch, &|instance| instance.id());

            let found = batch.len();
            status.instances_found += found;
            if found == 0 {
                trace!(
                    opctx.log,
                    "no more instances in need of reincarnation";
                    "total_found" => status.instances_found,
                );
                break;
            }

            self.reincarnate_batch(
                &opctx.log,
                status,
                &mut running_sagas,
                &serialized_authn,
                batch,
            )
            .await;
        }

        Ok(())
    }

    async fn reincarnate_batch(
        &mut self,
        log: &slog::Logger,
        status: &mut InstanceReincarnationStatus,
        running_sagas: &mut Vec<RunningSaga>,
        serialized_authn: &authn::saga::Serialized,
        batch: Vec<db::model::Instance>,
    ) {
        let found = batch.len();
        for db_instance in batch {
            let instance_id = db_instance.id();
            info!(
                log,
                "attempting to reincarnate instance...";
                "instance_id" => %instance_id,
                "instance_state" => ?db_instance.runtime().nexus_state,
                "auto_restart_config" => ?db_instance.auto_restart,
                "last_auto_restarted_at" => ?db_instance.runtime().time_last_auto_restarted,
            );

            let running_saga = async {
                let dag = instance_start::SagaInstanceStart::prepare(
                    &instance_start::Params {
                        db_instance,
                        serialized_authn: serialized_authn.clone(),
                        reason: instance_start::Reason::AutoRestart,
                    },
                )?;
                self.sagas.saga_run(dag).await
            }
            .await;
            match running_saga {
                Ok((saga_id, completed)) => {
                    running_sagas.push((instance_id, saga_id, completed));
                }
                Err(error) => {
                    const ERR_MSG: &'static str =
                        "failed to start instance-start saga";
                    error!(
                        log,
                        "{ERR_MSG} for {instance_id}";
                        "instance_id" => %instance_id,
                        "error" => %error,
                    );
                    let _prev_error = status
                        .restart_errors
                        .insert(instance_id, format!("{ERR_MSG}: {error}"));
                    debug_assert_eq!(
                        _prev_error, None,
                        "if a saga for {instance_id} already failed, we \
                         shouldn't see it again in the same activation!",
                    );
                }
            };
        }

        debug!(
            log,
            "found instances in need of reincarnation";
            "instances_found" => found,
            "total_found" => status.instances_found,
            "sagas_started" => running_sagas.len(),
            "total_sagas_started" => running_sagas.len() + status.total_sagas_started(),
        );

        // All sagas started, wait for them to come back before moving on to
        // the next chunk.
        // N.B. that although it's tempting to want to query the database
        // again as soon as one saga completes, so that we're *always*
        // running `concurrency_limit` sagas in parallel, rather than
        // running *up to* that many sagas, we ought not to query the
        // database again until all the sagas we've started have finished.
        // Otherwise, we may see some instances multiple times, because
        // their sagas completing is what changes the instance record's
        // state so that it no longer shows up in the query results.
        for (instance_id, saga_id, saga) in running_sagas.drain(..) {
            match saga.await {
                // Start saga completed successfully
                Ok(_) => {
                    debug!(
                        log,
                        "welcome back to the realm of the living, {instance_id}!";
                        "instance_id" => %instance_id,
                        "start_saga_id" => %saga_id,
                    );
                    status.instances_reincarnated.push(instance_id);
                }
                // The instance's state changed in the meantime, that's fine...
                Err(err @ Error::Conflict { .. }) => {
                    debug!(
                        log,
                        "instance {instance_id} changed state before it could be reincarnated";
                        "instance_id" => %instance_id,
                        "start_saga_id" => %saga_id,
                        "error" => err,
                    );
                    status.changed_state.push(instance_id);
                }
                // Start saga failed
                Err(error) => {
                    const ERR_MSG: &'static str = "instance-start saga";
                    warn!(log,
                        "{ERR_MSG} failed";
                        "instance_id" => %instance_id,
                        "start_saga_id" => %saga_id,
                        "error" => %error,
                    );
                    let _prev_error = status.restart_errors.insert(
                        instance_id,
                        format!("{ERR_MSG} {saga_id} failed: {error}"),
                    );
                    debug_assert_eq!(
                        _prev_error, None,
                        "if a saga for {instance_id} already failed, we \
                         shouldn't see it again in the same activation!",
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::app::sagas::test_helpers;
    use crate::external_api::params;
    use chrono::Utc;
    use nexus_db_model::InstanceRuntimeState;
    use nexus_db_model::InstanceState;
    use nexus_db_queries::authz;
    use nexus_db_queries::db::lookup::LookupPath;
    use nexus_test_utils::resource_helpers::{
        create_default_ip_pool, create_project, object_create,
    };
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::ByteCount;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_common::api::external::InstanceAutoRestartPolicy;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::InstanceUuid;
    use std::collections::HashMap;
    use std::time::Duration;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const PROJECT_NAME: &str = "reincarnation-station";

    async fn setup_test_project(
        cptestctx: &ControlPlaneTestContext,
        opctx: &OpContext,
    ) -> authz::Project {
        create_default_ip_pool(&cptestctx.external_client).await;
        let project =
            create_project(&cptestctx.external_client, PROJECT_NAME).await;

        let datastore = cptestctx.server.server_context().nexus.datastore();
        let (_, authz_project) = LookupPath::new(opctx, datastore)
            .project_id(project.identity.id)
            .lookup_for(authz::Action::CreateChild)
            .await
            .expect("project must exist");
        authz_project
    }

    async fn create_instance(
        cptestctx: &ControlPlaneTestContext,
        opctx: &OpContext,
        name: &str,
        auto_restart: InstanceAutoRestartPolicy,
        state: InstanceState,
    ) -> InstanceUuid {
        let instances_url = format!("/v1/instances?project={}", PROJECT_NAME);
        // Use the first chunk of the UUID as the name, to avoid conflicts.
        // Start with a lower ascii character to satisfy the name constraints.
        let name = name.parse().unwrap();
        let instance =
            object_create::<_, omicron_common::api::external::Instance>(
                &cptestctx.external_client,
                &instances_url,
                &params::InstanceCreate {
                    identity: IdentityMetadataCreateParams {
                        name,
                        description: "It's an instance".into(),
                    },
                    // In this test, we will "leak" sled resources, since we
                    // munge the database records for the instance without
                    // deleting its VMM (as we want to explicitly activate the
                    // reincarnation task, rather than letting the
                    // `instance-update` saga do so). Therefore, make our
                    // resource requests as small as possible.
                    ncpus: 1i64.try_into().unwrap(),
                    memory: ByteCount::from_gibibytes_u32(2),
                    hostname: "myhostname".try_into().unwrap(),
                    user_data: Vec::new(),
                    network_interfaces:
                        params::InstanceNetworkInterfaceAttachment::None,
                    external_ips: Vec::new(),
                    disks: Vec::new(),
                    ssh_public_keys: None,
                    start: state == InstanceState::Vmm,
                    auto_restart_policy: Some(auto_restart),
                },
            )
            .await;

        let id = InstanceUuid::from_untyped_uuid(instance.identity.id);
        if state != InstanceState::Vmm {
            put_instance_in_state(cptestctx, opctx, id, state).await;
        }

        eprintln!(
            "instance {id}: auto_restart_policy={auto_restart:?}; state={state:?}"
        );
        id
    }

    async fn put_instance_in_state(
        cptestctx: &ControlPlaneTestContext,
        opctx: &OpContext,
        id: InstanceUuid,
        state: InstanceState,
    ) {
        info!(
            &cptestctx.logctx.log,
            "putting instance {id} in state {state:?}"
        );

        let datastore = cptestctx.server.server_context().nexus.datastore();
        let (_, _, authz_instance) = LookupPath::new(&opctx, datastore)
            .instance_id(id.into_untyped_uuid())
            .lookup_for(authz::Action::Modify)
            .await
            .expect("instance 2 must exist");
        let prev_state = datastore
            .instance_refetch(&opctx, &authz_instance)
            .await
            .expect("instance 2 must exist")
            .runtime_state;
        let propolis_id = if state == InstanceState::Vmm {
            prev_state.propolis_id
        } else {
            None
        };
        datastore
            .instance_update_runtime(
                &id,
                &InstanceRuntimeState {
                    time_updated: Utc::now(),
                    nexus_state: state,
                    propolis_id,
                    r#gen: nexus_db_model::Generation(prev_state.r#gen.next()),
                    ..prev_state
                },
            )
            .await
            .expect("instance runtime state should update");
    }

    // Boilerplate reducer.
    //
    // This is a macro so that the `dbg!` has the line number of the place where
    // the task was activated, rather than the line where we invoke `dbg!` --- it
    // turns out that a `#[track_caller]` function only affects panic locations,
    // and not `dbg!`. Ah well.
    macro_rules! assert_activation_ok {
        ($result:expr) => {{
            let activation =
                serde_json::from_value::<InstanceReincarnationStatus>($result)
                    .expect("JSON must be correctly shaped");
            let status = dbg!(activation);
            assert_eq!(status.query_error, None);
            assert_eq!(status.restart_errors, HashMap::new());
            status
        }};
    }

    #[nexus_test(server = crate::Server)]
    async fn test_reincarnates_failed_instances(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        setup_test_project(&cptestctx, &opctx).await;

        let mut task = InstanceReincarnation::new(
            datastore.clone(),
            nexus.sagas.clone(),
            false,
        );

        // Noop test
        let status = assert_activation_ok!(task.activate(&opctx).await);
        assert_eq!(status.instances_found, 0);
        assert_eq!(status.instances_reincarnated, Vec::new());
        assert_eq!(status.changed_state, Vec::new());

        // Create an instance in the `Failed` state that's eligible to be
        // restarted.
        let instance_id = create_instance(
            &cptestctx,
            &opctx,
            "my-cool-instance",
            InstanceAutoRestartPolicy::BestEffort,
            InstanceState::Failed,
        )
        .await;

        // Activate the task again, and check that our instance had an
        // instance-start saga started.
        let status = assert_activation_ok!(task.activate(&opctx).await);
        assert_eq!(status.instances_found, 1);
        assert_eq!(
            status.instances_reincarnated,
            vec![instance_id.into_untyped_uuid()]
        );
        assert_eq!(status.changed_state, Vec::new());

        test_helpers::instance_wait_for_state(
            &cptestctx,
            instance_id,
            InstanceState::Vmm,
        )
        .await;
    }

    #[nexus_test(server = crate::Server)]
    async fn test_only_reincarnates_eligible_instances(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        setup_test_project(&cptestctx, &opctx).await;

        let mut task = InstanceReincarnation::new(
            datastore.clone(),
            nexus.sagas.clone(),
            false,
        );

        // Create instances in the `Failed` state that are eligible to be
        // restarted.
        let mut will_reincarnate = std::collections::BTreeSet::new();
        for i in 0..3 {
            let id = create_instance(
                &cptestctx,
                &opctx,
                &format!("sotapanna-{i}"),
                InstanceAutoRestartPolicy::BestEffort,
                InstanceState::Failed,
            )
            .await;
            will_reincarnate.insert(id.into_untyped_uuid());
        }

        // Create some instances that will not reicnarnate.
        let mut will_not_reincarnate = std::collections::BTreeSet::new();
        // Some instances which are `Failed` but don't have policies permitting
        // them to be reincarnated.
        for i in 0..3 {
            let id = create_instance(
                &cptestctx,
                &opctx,
                &format!("arahant-{i}"),
                InstanceAutoRestartPolicy::Never,
                InstanceState::Failed,
            )
            .await;
            will_not_reincarnate.insert(id.into_untyped_uuid());
        }

        // Some instances with policies permitting them to be reincarnated, but
        // which are not `Failed`.
        for (i, &state) in
            [InstanceState::Vmm, InstanceState::NoVmm, InstanceState::Destroyed]
                .iter()
                .enumerate()
        {
            let id = create_instance(
                &cptestctx,
                &opctx,
                &format!("anagami-{i}"),
                InstanceAutoRestartPolicy::BestEffort,
                state,
            )
            .await;
            will_not_reincarnate.insert(id.into_untyped_uuid());
        }

        // Activate the task again, and check that our instance had an
        // instance-start saga started.
        let status = assert_activation_ok!(task.activate(&opctx).await);
        assert_eq!(status.instances_found, will_reincarnate.len());
        assert_eq!(status.instances_reincarnated.len(), will_reincarnate.len());
        assert_eq!(status.changed_state, Vec::new());
        assert_eq!(status.query_error, None);
        assert_eq!(status.restart_errors, HashMap::new());

        for id in &status.instances_reincarnated {
            eprintln!("instance {id} reincarnated");
            assert!(
                !will_not_reincarnate.contains(id),
                "expected {id} not to reincarnate! reincarnated: {:?}",
                status.instances_reincarnated
            );
        }

        for id in will_reincarnate {
            assert!(
                status.instances_reincarnated.contains(&id),
                "expected {id} to have reincarnated! reincarnated: {:?}",
                status.instances_reincarnated
            );

            test_helpers::instance_wait_for_state(
                &cptestctx,
                InstanceUuid::from_untyped_uuid(id),
                InstanceState::Vmm,
            )
            .await;
        }
    }

    #[nexus_test(server = crate::Server)]
    async fn test_cooldown_on_subsequent_reincarnations(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        setup_test_project(&cptestctx, &opctx).await;

        let mut task = InstanceReincarnation::new(
            datastore.clone(),
            nexus.sagas.clone(),
            false,
        );

        let instance1_id = create_instance(
            &cptestctx,
            &opctx,
            "victor",
            InstanceAutoRestartPolicy::BestEffort,
            InstanceState::Failed,
        )
        .await;
        // Use the test-only API to set the cooldown period for instance 1 to ten
        // seconds, so that we don't have to make the test run for an hour to wait
        // out the default cooldown.
        const COOLDOWN_SECS: u64 = 10;
        datastore
            .instance_set_auto_restart_cooldown(
                &opctx,
                &instance1_id,
                chrono::TimeDelta::seconds(COOLDOWN_SECS as i64),
            )
            .await
            .expect("we must be able to set the cooldown period");

        let instance2_id = create_instance(
            &cptestctx,
            &opctx,
            "frankenstein",
            InstanceAutoRestartPolicy::BestEffort,
            InstanceState::Vmm,
        )
        .await;

        // On the first activation, instance 1 should be restarted.
        let status = assert_activation_ok!(task.activate(&opctx).await);
        assert_eq!(status.instances_found, 1);
        assert_eq!(
            status.instances_reincarnated,
            &[instance1_id.into_untyped_uuid()]
        );
        assert_eq!(status.changed_state, Vec::new());

        // Now, let's do some state changes:
        // Pretend instance 1 restarted, and then failed again.
        test_helpers::instance_wait_for_state(
            &cptestctx,
            instance1_id,
            InstanceState::Vmm,
        )
        .await;
        put_instance_in_state(
            &cptestctx,
            &opctx,
            instance1_id,
            InstanceState::Failed,
        )
        .await;

        // Move instance 2 to failed.
        put_instance_in_state(
            &cptestctx,
            &opctx,
            instance2_id,
            InstanceState::Failed,
        )
        .await;

        // Activate the background task again. Now, only instance 2 should be
        // restarted.
        let status = assert_activation_ok!(task.activate(&opctx).await);
        assert_eq!(status.instances_found, 1);
        assert_eq!(
            status.instances_reincarnated,
            &[instance2_id.into_untyped_uuid()]
        );
        assert_eq!(status.changed_state, Vec::new());

        // Instance 2 should be started
        test_helpers::instance_wait_for_state(
            &cptestctx,
            instance2_id,
            InstanceState::Vmm,
        )
        .await;

        // Wait out the cooldown period, and give it another shot. Now, instance
        // 1 should be restarted again.
        tokio::time::sleep(Duration::from_secs(COOLDOWN_SECS + 1)).await;

        let status = assert_activation_ok!(task.activate(&opctx).await);
        assert_eq!(status.instances_found, 1);
        assert_eq!(
            status.instances_reincarnated,
            &[instance1_id.into_untyped_uuid()]
        );
        assert_eq!(status.changed_state, Vec::new());

        // Instance 1 should be started.
        test_helpers::instance_wait_for_state(
            &cptestctx,
            instance1_id,
            InstanceState::Vmm,
        )
        .await;
    }
}
