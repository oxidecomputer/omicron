// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for automatically restarting failed instances.

use crate::app::background::BackgroundTask;
use crate::app::saga::StartSaga;
use crate::app::sagas::NexusSaga;
use crate::app::sagas::instance_start;
use futures::future::BoxFuture;
use nexus_db_queries::authn;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::pagination::Paginator;
use nexus_types::identity::Resource;
use nexus_types::internal_api::background::InstanceReincarnationStatus;
use nexus_types::internal_api::background::ReincarnatableInstance;
use nexus_types::internal_api::background::ReincarnationReason;
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
            // /!\ BREAK GLASS IN CASE OF EMERGENCY /!\
            if self.disabled {
                status.disabled = true;
                debug!(
                    &opctx.log,
                    "instance reincarnation disabled, doing nothing",
                );
                return serde_json::json!(status);
            }

            let mut running_sagas =
                Vec::with_capacity(self.concurrency_limit.get() as usize);

            if let Err(error) = self
                .reincarnate_all(
                    &opctx,
                    ReincarnationReason::Failed,
                    &mut status,
                    &mut running_sagas,
                )
                .await
            {
                error!(
                    opctx.log,
                    "failed to find all Failed instances in need of \
                     reincarnation";
                    "error" => %error,
                );
                status
                    .errors
                    .push(format!("finding Failed instances: {error}"));
            }

            if let Err(error) = self
                .reincarnate_all(
                    &opctx,
                    ReincarnationReason::SagaUnwound,
                    &mut status,
                    &mut running_sagas,
                )
                .await
            {
                error!(
                    opctx.log,
                    "failed to find all instances with unwound start sagas \
                     in need of reincarnation";
                    "error" => %error,
                );
                status.errors.push(format!(
                    "finding instances with unwound start sagas: {error}"
                ));
            }

            if status.total_errors() > 0 {
                warn!(
                    &opctx.log,
                    "instance reincarnation completed with errors";
                    "instances_found" => status.total_instances_found(),
                    "instances_reincarnated" => status.instances_reincarnated.len(),
                    "instances_changed_state" => status.changed_state.len(),
                    "query_errors" => status.errors.len(),
                    "restart_errors" => status.restart_errors.len(),
                );
            } else {
                info!(
                    &opctx.log,
                    "instance reincarnation completed successfully";
                    "instances_found" => status.total_instances_found(),
                    "instances_reincarnated" => status.instances_reincarnated.len(),
                    "instances_changed_state" => status.changed_state.len(),
                );
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

    async fn reincarnate_all(
        &mut self,
        opctx: &OpContext,
        reason: ReincarnationReason,
        status: &mut InstanceReincarnationStatus,
        running_sagas: &mut Vec<RunningSaga>,
    ) -> anyhow::Result<()> {
        let serialized_authn = authn::saga::Serialized::for_opctx(opctx);

        let mut paginator = Paginator::new(
            self.concurrency_limit,
            dropshot::PaginationOrder::Ascending,
        );
        let instances_found = status.instances_found.entry(reason).or_insert(0);
        let mut sagas_started = 0;
        while let Some(p) = paginator.next() {
            let batch = self
                .datastore
                .find_reincarnatable_instances(
                    &opctx,
                    reason,
                    &p.current_pagparams(),
                )
                .await?;
            paginator = p.found_batch(&batch, &|instance| instance.id());

            let found = batch.len();
            *instances_found += found;
            if found == 0 {
                trace!(
                    opctx.log,
                    "no more instances in need of reincarnation";
                    "total_found" => *instances_found,
                    "reincarnation_reason" => %reason,
                );
                break;
            }
            for db_instance in batch {
                let instance_id = db_instance.id();
                info!(
                    opctx.log,
                    "attempting to reincarnate instance...";
                    "instance_id" => %instance_id,
                    "reincarnation_reason" => %reason,
                    "instance_state" => ?db_instance.runtime().nexus_state,
                    "auto_restart_config" => ?db_instance.auto_restart,
                    "last_auto_restarted_at" => ?db_instance
                        .runtime()
                        .time_last_auto_restarted,
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
                            opctx.log,
                            "{ERR_MSG} for instance {instance_id}";
                            "instance_id" => %instance_id,
                            "reincarnation_reason" => %reason,
                            "error" => %error,
                        );
                        status.restart_errors.push((
                            ReincarnatableInstance { instance_id, reason },
                            format!(
                                "{ERR_MSG} for {reason:?} instance: {error}"
                            ),
                        ));
                    }
                };
            }

            sagas_started += running_sagas.len();
            debug!(
                opctx.log,
                "found {reason:?} instances in need of reincarnation";
                "reincarnation_reason" => %reason,
                "instances_found" => found,
                "total_found" => *instances_found,
                "sagas_started" => running_sagas.len(),
                "total_sagas_started" => sagas_started,
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
                            opctx.log,
                            "welcome back to the realm of the living, \
                             {instance_id}!";
                            "instance_id" => %instance_id,
                            "start_saga_id" => %saga_id,
                            "reincarnation_reason" => %reason,
                        );
                        status.instances_reincarnated.push(
                            ReincarnatableInstance { instance_id, reason },
                        );
                    }
                    // The instance's state changed in the meantime, that's fine...
                    Err(err @ Error::Conflict { .. }) => {
                        debug!(
                            opctx.log,
                            "{reason:?} instance {instance_id} changed state \
                             before it could be reincarnated";
                            "instance_id" => %instance_id,
                            "reincarnation_reason" => %reason,
                            "start_saga_id" => %saga_id,
                            "error" => err,
                        );
                        status.changed_state.push(ReincarnatableInstance {
                            instance_id,
                            reason,
                        });
                    }
                    // Start saga failed
                    Err(error) => {
                        const ERR_MSG: &'static str = "instance-start saga";
                        warn!(
                            opctx.log,
                            "{ERR_MSG} for instance {instance_id} failed";
                            "instance_id" => %instance_id,
                            "reincarnation_reason" => %reason,
                            "start_saga_id" => %saga_id,
                            "error" => %error,
                        );
                        status.restart_errors.push((
                            ReincarnatableInstance { instance_id, reason },
                            format!(
                                "{ERR_MSG} {saga_id} for instance \
                                 failed: {error}",
                            ),
                        ));
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::app::sagas::test_helpers;
    use crate::external_api::params;
    use chrono::Utc;
    use nexus_db_lookup::LookupPath;
    use nexus_db_model::Generation;
    use nexus_db_model::InstanceIntendedState;
    use nexus_db_model::InstanceRuntimeState;
    use nexus_db_model::InstanceState;
    use nexus_db_model::Vmm;
    use nexus_db_model::VmmRuntimeState;
    use nexus_db_model::VmmState;
    use nexus_db_queries::authz;
    use nexus_test_utils::resource_helpers::{
        create_default_ip_pool, create_project, object_create,
    };
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::api::external::ByteCount;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_common::api::external::InstanceAutoRestartPolicy;
    use omicron_uuid_kinds::GenericUuid;
    use omicron_uuid_kinds::InstanceUuid;
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
        auto_restart: impl Into<Option<InstanceAutoRestartPolicy>>,
        state: InstanceState,
        intent: InstanceIntendedState,
    ) -> authz::Instance {
        let auto_restart_policy = auto_restart.into();
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
                    boot_disk: None,
                    ssh_public_keys: None,
                    start: state == InstanceState::Vmm,
                    auto_restart_policy,
                    anti_affinity_groups: Vec::new(),
                },
            )
            .await;

        let id = InstanceUuid::from_untyped_uuid(instance.identity.id);
        let datastore = cptestctx.server.server_context().nexus.datastore();
        let (_, _, authz_instance) = LookupPath::new(&opctx, datastore)
            .instance_id(id.into_untyped_uuid())
            .lookup_for(authz::Action::Modify)
            .await
            .expect("instance must exist");
        if state != InstanceState::Vmm && state != InstanceState::NoVmm {
            put_instance_in_state(cptestctx, opctx, id, state).await;
        };
        datastore
            .instance_set_intended_state(opctx, &authz_instance, intent)
            .await
            .unwrap();

        eprintln!(
            "instance {id}: auto_restart_policy={auto_restart_policy:?}; \
             state={state:?}; intent={intent}"
        );
        authz_instance
    }

    async fn attach_saga_unwound_vmm(
        cptestctx: &ControlPlaneTestContext,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
    ) {
        let datastore = cptestctx.server.server_context().nexus.datastore();
        let instance_id = InstanceUuid::from_untyped_uuid(authz_instance.id());
        let vmm = datastore
            .vmm_insert(
                &opctx,
                Vmm {
                    id: Uuid::new_v4(),
                    time_created: Utc::now(),
                    time_deleted: None,
                    instance_id: authz_instance.id(),
                    sled_id: Uuid::new_v4(),
                    propolis_ip: "10.1.9.42".parse().unwrap(),
                    propolis_port: 420.into(),
                    runtime: VmmRuntimeState {
                        time_state_updated: Utc::now(),
                        r#gen: Generation::new(),
                        state: VmmState::SagaUnwound,
                    },
                },
            )
            .await
            .expect("SagaUnwound VMM should be inserted");
        let vmm_id = vmm.id;
        let prev_state = datastore
            .instance_refetch(&opctx, &authz_instance)
            .await
            .expect("instance must exist")
            .runtime_state;
        let updated = datastore
            .instance_update_runtime(
                &instance_id,
                &InstanceRuntimeState {
                    time_updated: Utc::now(),
                    r#gen: Generation(prev_state.r#gen.next()),
                    nexus_state: InstanceState::Vmm,
                    propolis_id: Some(vmm_id),
                    ..prev_state
                },
            )
            .await
            .expect("instance update should succeed");
        assert!(updated, "instance {instance_id} was not updated");
        eprintln!(
            "instance {instance_id}: attached SagaUnwound active VMM {vmm_id}"
        );
    }

    async fn put_instance_in_state(
        cptestctx: &ControlPlaneTestContext,
        opctx: &OpContext,
        id: InstanceUuid,
        state: InstanceState,
    ) -> authz::Instance {
        info!(
            &cptestctx.logctx.log,
            "putting instance {id} in state {state:?}"
        );

        let datastore = cptestctx.server.server_context().nexus.datastore();
        let (_, _, authz_instance) = LookupPath::new(&opctx, datastore)
            .instance_id(id.into_untyped_uuid())
            .lookup_for(authz::Action::Modify)
            .await
            .expect("instance must exist");
        let prev_state = datastore
            .instance_refetch(&opctx, &authz_instance)
            .await
            .expect("instance must exist")
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
                    r#gen: Generation(prev_state.r#gen.next()),
                    ..prev_state
                },
            )
            .await
            .expect("instance runtime state should update");
        authz_instance
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
            assert_eq!(status.errors, Vec::<String>::new());
            assert_eq!(status.restart_errors, Vec::new());
            status
        }};
    }

    fn failed(instance_id: Uuid) -> ReincarnatableInstance {
        ReincarnatableInstance {
            instance_id,
            reason: ReincarnationReason::Failed,
        }
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
        assert_eq!(status.total_instances_found(), 0);
        assert_eq!(status.instances_reincarnated, Vec::new());
        assert_eq!(status.changed_state, Vec::new());

        // Create an instance in the `Failed` state that's eligible to be
        // restarted.
        let instance = create_instance(
            &cptestctx,
            &opctx,
            "my-cool-instance",
            InstanceAutoRestartPolicy::BestEffort,
            InstanceState::Failed,
            InstanceIntendedState::Running,
        )
        .await;

        // Activate the task again, and check that our instance had an
        // instance-start saga started.
        let status = assert_activation_ok!(task.activate(&opctx).await);
        assert_eq!(status.total_instances_found(), 1);
        assert_eq!(status.instances_reincarnated, vec![failed(instance.id())]);
        assert_eq!(status.changed_state, Vec::new());

        test_helpers::instance_wait_for_state(
            &cptestctx,
            InstanceUuid::from_untyped_uuid(instance.id()),
            InstanceState::Vmm,
        )
        .await;
    }

    #[nexus_test(server = crate::Server)]
    async fn test_default_policy_is_reincarnatable(
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

        // Create an instance in the `Failed` state that's eligible to be
        // restarted.
        let instance = create_instance(
            &cptestctx,
            &opctx,
            "my-cool-instance",
            None,
            InstanceState::Failed,
            InstanceIntendedState::Running,
        )
        .await;

        // Activate the task again, and check that our instance had an
        // instance-start saga started.
        let status = assert_activation_ok!(task.activate(&opctx).await);
        assert_eq!(status.total_instances_found(), 1);
        assert_eq!(status.instances_reincarnated, vec![failed(instance.id())]);
        assert_eq!(status.changed_state, Vec::new());

        test_helpers::instance_wait_for_state(
            &cptestctx,
            InstanceUuid::from_untyped_uuid(instance.id()),
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
        let num_failed = 3;
        for i in 0..num_failed {
            let instance = create_instance(
                &cptestctx,
                &opctx,
                &format!("sotapanna-{i}"),
                InstanceAutoRestartPolicy::BestEffort,
                InstanceState::Failed,
                InstanceIntendedState::Running,
            )
            .await;
            will_reincarnate.insert(failed(instance.id()));
        }
        // Create instances with SagaUnwound active VMMs that are eligible to be
        // reincarnated.
        let num_saga_unwound = 2;
        for i in 0..num_saga_unwound {
            // Make the instance record.
            let instance = create_instance(
                &cptestctx,
                &opctx,
                &format!("sadakagami-{i}"),
                InstanceAutoRestartPolicy::BestEffort,
                InstanceState::NoVmm,
                InstanceIntendedState::Running,
            )
            .await;
            // Now, give the instance an active VMM which is SagaUnwound.
            attach_saga_unwound_vmm(&cptestctx, &opctx, &instance).await;
            will_reincarnate.insert(ReincarnatableInstance {
                instance_id: instance.id(),
                reason: ReincarnationReason::SagaUnwound,
            });
        }

        // Create some instances that will not reincarnate.
        let mut will_not_reincarnate = std::collections::BTreeSet::new();
        // Some instances which are `Failed` but don't have policies permitting
        // them to be reincarnated.
        for i in 0..3 {
            let instance = create_instance(
                &cptestctx,
                &opctx,
                &format!("arahant-{i}"),
                InstanceAutoRestartPolicy::Never,
                InstanceState::Failed,
                InstanceIntendedState::Running,
            )
            .await;

            will_not_reincarnate.insert(instance.id());
        }

        // Some instances which have `SagaUnwound VMMs` but don't have policies
        // permitting them to be reincarnated.
        for i in 3..5 {
            let instance = create_instance(
                &cptestctx,
                &opctx,
                &format!("arahant-{i}"),
                InstanceAutoRestartPolicy::Never,
                InstanceState::NoVmm,
                InstanceIntendedState::Running,
            )
            .await;

            attach_saga_unwound_vmm(&cptestctx, &opctx, &instance).await;
            will_not_reincarnate.insert(instance.id());
        }

        // Some instances with policies permitting them to be reincarnated, but
        // which are not `Failed`.
        for (i, &state) in
            [InstanceState::Vmm, InstanceState::NoVmm, InstanceState::Destroyed]
                .iter()
                .enumerate()
        {
            let instance = create_instance(
                &cptestctx,
                &opctx,
                &format!("anagami-{i}"),
                InstanceAutoRestartPolicy::BestEffort,
                state,
                InstanceIntendedState::Running,
            )
            .await;
            will_not_reincarnate.insert(instance.id());
        }

        // Some `Failed` instances with policies permitting them to be
        // reincarnated, but whose intended state is not `Running`.
        let failed_stopped = create_instance(
            &cptestctx,
            &opctx,
            "anagami-4",
            InstanceAutoRestartPolicy::BestEffort,
            InstanceState::Failed,
            InstanceIntendedState::Stopped,
        )
        .await;
        will_not_reincarnate.insert(failed_stopped.id());
        let unwound_stopped = create_instance(
            &cptestctx,
            &opctx,
            "anagami-5",
            InstanceAutoRestartPolicy::BestEffort,
            InstanceState::NoVmm,
            InstanceIntendedState::Stopped,
        )
        .await;
        attach_saga_unwound_vmm(&cptestctx, &opctx, &unwound_stopped).await;
        will_not_reincarnate.insert(unwound_stopped.id());

        // Activate the task again, and check that our instance had an
        // instance-start saga started.
        let status = assert_activation_ok!(task.activate(&opctx).await);
        assert_eq!(status.total_instances_found(), will_reincarnate.len());
        assert_eq!(
            status.instances_found.get(&ReincarnationReason::Failed),
            Some(&num_failed)
        );
        assert_eq!(
            status.instances_found.get(&ReincarnationReason::SagaUnwound),
            Some(&num_saga_unwound)
        );
        assert_eq!(status.instances_reincarnated.len(), will_reincarnate.len());
        assert_eq!(status.changed_state, Vec::new());
        assert_eq!(status.errors, Vec::<String>::new());
        assert_eq!(
            status.restart_errors,
            Vec::<(ReincarnatableInstance, String)>::new()
        );

        for instance in &status.instances_reincarnated {
            eprintln!("instance {instance} reincarnated");
            assert!(
                !will_not_reincarnate.contains(&instance.instance_id),
                "expected {instance} not to reincarnate! reincarnated: {:?}",
                status.instances_reincarnated
            );
        }

        for instance in will_reincarnate {
            assert!(
                status.instances_reincarnated.contains(&instance),
                "expected {instance} to have reincarnated! reincarnated: {:?}",
                status.instances_reincarnated
            );

            test_helpers::instance_wait_for_state(
                &cptestctx,
                InstanceUuid::from_untyped_uuid(instance.instance_id),
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

        let instance1 = create_instance(
            &cptestctx,
            &opctx,
            "victor",
            InstanceAutoRestartPolicy::BestEffort,
            InstanceState::Failed,
            InstanceIntendedState::Running,
        )
        .await;
        let instance1_id = InstanceUuid::from_untyped_uuid(instance1.id());

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

        let instance2 = create_instance(
            &cptestctx,
            &opctx,
            "frankenstein",
            InstanceAutoRestartPolicy::BestEffort,
            InstanceState::Vmm,
            InstanceIntendedState::Running,
        )
        .await;
        let instance2_id = InstanceUuid::from_untyped_uuid(instance2.id());

        // On the first activation, instance 1 should be restarted.
        let status = assert_activation_ok!(task.activate(&opctx).await);
        assert_eq!(status.total_instances_found(), 1);
        assert_eq!(
            status.instances_reincarnated,
            &[failed(instance1_id.into_untyped_uuid())]
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
        assert_eq!(status.total_instances_found(), 1);
        assert_eq!(
            status.instances_reincarnated,
            &[failed(instance2_id.into_untyped_uuid())]
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
        assert_eq!(status.total_instances_found(), 1);
        assert_eq!(
            status.instances_reincarnated,
            &[failed(instance1_id.into_untyped_uuid())]
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
