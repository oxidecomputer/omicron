// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Virtual Machine Instances

use super::MAX_DISKS_PER_INSTANCE;
use super::MAX_EXTERNAL_IPS_PER_INSTANCE;
use super::MAX_MEMORY_BYTES_PER_INSTANCE;
use super::MAX_NICS_PER_INSTANCE;
use super::MAX_VCPU_PER_INSTANCE;
use super::MIN_MEMORY_BYTES_PER_INSTANCE;
use crate::app::sagas;
use crate::cidata::InstanceCiData;
use crate::external_api::params;
use cancel_safe_futures::prelude::*;
use futures::future::Fuse;
use futures::{FutureExt, SinkExt, StreamExt};
use nexus_db_model::IpKind;
use nexus_db_queries::authn;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::datastore::InstanceAndActiveVmm;
use nexus_db_queries::db::identity::Resource;
use nexus_db_queries::db::lookup;
use nexus_db_queries::db::lookup::LookupPath;
use omicron_common::address::PROPOLIS_PORT;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::InstanceState;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::Vni;
use omicron_common::api::internal::nexus;
use propolis_client::support::tungstenite::protocol::frame::coding::CloseCode;
use propolis_client::support::tungstenite::protocol::CloseFrame;
use propolis_client::support::tungstenite::Message as WebSocketMessage;
use propolis_client::support::InstanceSerialConsoleHelper;
use propolis_client::support::WSClientOffset;
use propolis_client::support::WebSocketStream;
use sled_agent_client::types::InstanceMigrationSourceParams;
use sled_agent_client::types::InstanceMigrationTargetParams;
use sled_agent_client::types::InstanceProperties;
use sled_agent_client::types::InstancePutMigrationIdsBody;
use sled_agent_client::types::InstancePutStateBody;
use sled_agent_client::types::SourceNatConfig;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use uuid::Uuid;

const MAX_KEYS_PER_INSTANCE: u32 = 8;

/// The kinds of state changes that can be requested of an instance's current
/// VMM (i.e. the VMM pointed to be the instance's `propolis_id` field).
pub(crate) enum InstanceStateChangeRequest {
    Run,
    Reboot,
    Stop,
    Migrate(InstanceMigrationTargetParams),
}

impl From<InstanceStateChangeRequest>
    for sled_agent_client::types::InstanceStateRequested
{
    fn from(value: InstanceStateChangeRequest) -> Self {
        match value {
            InstanceStateChangeRequest::Run => Self::Running,
            InstanceStateChangeRequest::Reboot => Self::Reboot,
            InstanceStateChangeRequest::Stop => Self::Stopped,
            InstanceStateChangeRequest::Migrate(params) => {
                Self::MigrationTarget(params)
            }
        }
    }
}

/// The actions that can be taken in response to an
/// [`InstanceStateChangeRequest`].
enum InstanceStateChangeRequestAction {
    /// The instance is already in the correct state, so no action is needed.
    AlreadyDone,

    /// Request the appropriate state change from the sled with the specified
    /// UUID.
    SendToSled(Uuid),
}

impl super::Nexus {
    pub fn instance_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        instance_selector: params::InstanceSelector,
    ) -> LookupResult<lookup::Instance<'a>> {
        match instance_selector {
            params::InstanceSelector {
                instance: NameOrId::Id(id),
                project: None
            } => {
                let instance =
                    LookupPath::new(opctx, &self.db_datastore).instance_id(id);
                Ok(instance)
            }
            params::InstanceSelector {
                instance: NameOrId::Name(name),
                project: Some(project)
            } => {
                let instance = self
                    .project_lookup(opctx, params::ProjectSelector { project })?
                    .instance_name_owned(name.into());
                Ok(instance)
            }
            params::InstanceSelector {
                instance: NameOrId::Id(_),
                ..
            } => {
                Err(Error::invalid_request(
                    "when providing instance as an ID project should not be specified",
                ))
            }
            _ => {
                Err(Error::invalid_request(
                    "instance should either be UUID or project should be specified",
                ))
            }
        }
    }

    pub(crate) async fn project_create_instance(
        self: &Arc<Self>,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        params: &params::InstanceCreate,
    ) -> CreateResult<InstanceAndActiveVmm> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::CreateChild).await?;

        // Validate parameters
        if params.disks.len() > MAX_DISKS_PER_INSTANCE as usize {
            return Err(Error::invalid_request(&format!(
                "cannot attach more than {} disks to instance",
                MAX_DISKS_PER_INSTANCE
            )));
        }
        for disk in &params.disks {
            if let params::InstanceDiskAttachment::Create(create) = disk {
                self.validate_disk_create_params(opctx, &authz_project, create)
                    .await?;
            }
        }
        if params.ncpus.0 > MAX_VCPU_PER_INSTANCE {
            return Err(Error::invalid_request(&format!(
                "cannot have more than {} vCPUs per instance",
                MAX_VCPU_PER_INSTANCE
            )));
        }
        if params.external_ips.len() > MAX_EXTERNAL_IPS_PER_INSTANCE {
            return Err(Error::invalid_request(&format!(
                "An instance may not have more than {} external IP addresses",
                MAX_EXTERNAL_IPS_PER_INSTANCE,
            )));
        }
        if let params::InstanceNetworkInterfaceAttachment::Create(ref ifaces) =
            params.network_interfaces
        {
            if ifaces.len() > MAX_NICS_PER_INSTANCE {
                return Err(Error::invalid_request(&format!(
                    "An instance may not have more than {} network interfaces",
                    MAX_NICS_PER_INSTANCE,
                )));
            }
            // Check that all VPC names are the same.
            //
            // This isn't strictly necessary, as the queries to create these
            // interfaces would fail in the saga, but it's easier to handle here.
            if ifaces
                .iter()
                .map(|iface| &iface.vpc_name)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != 1
            {
                return Err(Error::invalid_request(
                    "All interfaces must be in the same VPC",
                ));
            }
        }

        // Reject instances where the memory is not at least
        // MIN_MEMORY_BYTES_PER_INSTANCE
        if params.memory.to_bytes() < MIN_MEMORY_BYTES_PER_INSTANCE as u64 {
            return Err(Error::InvalidValue {
                label: String::from("size"),
                message: format!(
                    "memory must be at least {}",
                    ByteCount::from(MIN_MEMORY_BYTES_PER_INSTANCE)
                ),
            });
        }

        // Reject instances where the memory is not divisible by
        // MIN_MEMORY_BYTES_PER_INSTANCE
        if (params.memory.to_bytes() % MIN_MEMORY_BYTES_PER_INSTANCE as u64)
            != 0
        {
            return Err(Error::InvalidValue {
                label: String::from("size"),
                message: format!(
                    "memory must be divisible by {}",
                    ByteCount::from(MIN_MEMORY_BYTES_PER_INSTANCE)
                ),
            });
        }

        // Reject instances where the memory is greater than the limit
        if params.memory.to_bytes() > MAX_MEMORY_BYTES_PER_INSTANCE {
            return Err(Error::InvalidValue {
                label: String::from("size"),
                message: format!(
                    "memory must be less than or equal to {}",
                    ByteCount::try_from(MAX_MEMORY_BYTES_PER_INSTANCE).unwrap()
                ),
            });
        }

        let saga_params = sagas::instance_create::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            project_id: authz_project.id(),
            create_params: params.clone(),
            boundary_switches: self
                .boundary_switches(&self.opctx_alloc)
                .await?,
        };

        let saga_outputs = self
            .execute_saga::<sagas::instance_create::SagaInstanceCreate>(
                saga_params,
            )
            .await?;

        let instance_id = saga_outputs
            .lookup_node_output::<Uuid>("instance_id")
            .map_err(|e| Error::internal_error(&format!("{:#}", &e)))
            .internal_context("looking up output from instance create saga")?;

        // If the caller asked to start the instance, kick off that saga.
        // There's a window in which the instance is stopped and can be deleted,
        // so this is not guaranteed to succeed, and its result should not
        // affect the result of the attempt to create the instance.
        if params.start {
            let lookup = LookupPath::new(opctx, &self.db_datastore)
                .instance_id(instance_id);

            let start_result = self.instance_start(opctx, &lookup).await;
            if let Err(e) = start_result {
                info!(self.log, "failed to start newly-created instance";
                      "instance_id" => %instance_id,
                      "error" => ?e);
            }
        }

        // TODO: This operation should return the instance as it was created.
        // Refetching the instance state here won't return that version of the
        // instance if its state changed between the time the saga finished and
        // the time this lookup was performed.
        //
        // Because the create saga has to synthesize an instance record (and
        // possibly a VMM record), and these are serializable, it should be
        // possible to yank the outputs out of the appropriate saga steps and
        // return them here.

        let (.., authz_instance) = LookupPath::new(opctx, &self.db_datastore)
            .instance_id(instance_id)
            .lookup_for(authz::Action::Read)
            .await?;

        self.db_datastore.instance_fetch_with_vmm(opctx, &authz_instance).await
    }

    pub(crate) async fn instance_list(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<InstanceAndActiveVmm> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::ListChildren).await?;
        self.db_datastore.instance_list(opctx, &authz_project, pagparams).await
    }

    // This operation may only occur on stopped instances, which implies that
    // the attached disks do not have any running "upstairs" process running
    // within the sled.
    pub(crate) async fn project_destroy_instance(
        self: &Arc<Self>,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
    ) -> DeleteResult {
        // TODO-robustness We need to figure out what to do with Destroyed
        // instances?  Presumably we need to clean them up at some point, but
        // not right away so that callers can see that they've been destroyed.
        let (.., authz_instance, instance) =
            instance_lookup.fetch_for(authz::Action::Delete).await?;

        // TODO: #3593 Correctness
        // When the set of boundary switches changes, there is no cleanup /
        // reconciliation logic performed to ensure that the NAT entries are
        // propogated to new boundary switches / removed from former boundary
        // switches, meaning we could end up with stale or missing entries.
        let boundary_switches =
            self.boundary_switches(&self.opctx_alloc).await?;

        let saga_params = sagas::instance_delete::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            authz_instance,
            instance,
            boundary_switches,
        };
        self.execute_saga::<sagas::instance_delete::SagaInstanceDelete>(
            saga_params,
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn project_instance_migrate(
        self: &Arc<Self>,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
        params: params::InstanceMigrate,
    ) -> UpdateResult<InstanceAndActiveVmm> {
        let (.., authz_instance) =
            instance_lookup.lookup_for(authz::Action::Modify).await?;

        let state = self
            .db_datastore
            .instance_fetch_with_vmm(opctx, &authz_instance)
            .await?;
        let (instance, vmm) = (state.instance(), state.vmm());

        if vmm.is_none()
            || vmm.as_ref().unwrap().runtime.state.0 != InstanceState::Running
        {
            return Err(Error::invalid_request(
                "instance must be running before it can migrate",
            ));
        }

        let vmm = vmm.as_ref().unwrap();
        if vmm.sled_id == params.dst_sled_id {
            return Err(Error::invalid_request(
                "instance is already running on destination sled",
            ));
        }

        if instance.runtime().migration_id.is_some() {
            return Err(Error::unavail("instance is already migrating"));
        }

        // Kick off the migration saga
        let saga_params = sagas::instance_migrate::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            instance: instance.clone(),
            src_vmm: vmm.clone(),
            migrate_params: params,
        };
        self.execute_saga::<sagas::instance_migrate::SagaInstanceMigrate>(
            saga_params,
        )
        .await?;

        // TODO correctness TODO robustness TODO design
        // Should we lookup the instance again here?
        // See comment in project_create_instance.
        self.db_datastore.instance_fetch_with_vmm(opctx, &authz_instance).await
    }

    /// Attempts to set the migration IDs for the supplied instance via the
    /// sled specified in `db_instance`.
    ///
    /// The caller is assumed to have fetched the current instance record from
    /// the DB and verified that the record has no migration IDs.
    ///
    /// Returns `Ok` and the updated instance record if this call successfully
    /// updated the instance with the sled agent and that update was
    /// successfully reflected into CRDB. Returns `Err` with an appropriate
    /// error otherwise.
    ///
    /// # Panics
    ///
    /// Asserts that `db_instance` has no migration ID or destination Propolis
    /// ID set.
    pub(crate) async fn instance_set_migration_ids(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
        sled_id: Uuid,
        prev_instance_runtime: &db::model::InstanceRuntimeState,
        migration_params: InstanceMigrationSourceParams,
    ) -> UpdateResult<db::model::Instance> {
        assert!(prev_instance_runtime.migration_id.is_none());
        assert!(prev_instance_runtime.dst_propolis_id.is_none());

        let (.., authz_instance) = LookupPath::new(opctx, &self.db_datastore)
            .instance_id(instance_id)
            .lookup_for(authz::Action::Modify)
            .await?;

        let sa = self.sled_client(&sled_id).await?;
        let instance_put_result = sa
            .instance_put_migration_ids(
                &instance_id,
                &InstancePutMigrationIdsBody {
                    old_runtime: prev_instance_runtime.clone().into(),
                    migration_params: Some(migration_params),
                },
            )
            .await
            .map(|res| Some(res.into_inner()));

        // Write the updated instance runtime state back to CRDB. If this
        // outright fails, this operation fails. If the operation nominally
        // succeeds but nothing was updated, this action is outdated and the
        // caller should not proceed with migration.
        let (updated, _) = self
            .handle_instance_put_result(
                &instance_id,
                prev_instance_runtime,
                instance_put_result.map(|state| state.map(Into::into)),
            )
            .await?;

        if updated {
            Ok(self
                .db_datastore
                .instance_refetch(opctx, &authz_instance)
                .await?)
        } else {
            Err(Error::conflict(
                "instance is already migrating, or underwent an operation that \
                 prevented this migration from proceeding"
            ))
        }
    }

    /// Attempts to clear the migration IDs for the supplied instance via the
    /// sled specified in `db_instance`.
    ///
    /// The supplied instance record must contain valid migration IDs.
    ///
    /// Returns `Ok` if sled agent accepted the request to clear migration IDs
    /// and the resulting attempt to write instance runtime state back to CRDB
    /// succeeded. This routine returns `Ok` even if the update was not actually
    /// applied (due to a separate generation number change).
    ///
    /// # Panics
    ///
    /// Asserts that `db_instance` has a migration ID and destination Propolis
    /// ID set.
    pub(crate) async fn instance_clear_migration_ids(
        &self,
        instance_id: Uuid,
        sled_id: Uuid,
        prev_instance_runtime: &db::model::InstanceRuntimeState,
    ) -> Result<(), Error> {
        assert!(prev_instance_runtime.migration_id.is_some());
        assert!(prev_instance_runtime.dst_propolis_id.is_some());

        let sa = self.sled_client(&sled_id).await?;
        let instance_put_result = sa
            .instance_put_migration_ids(
                &instance_id,
                &InstancePutMigrationIdsBody {
                    old_runtime: prev_instance_runtime.clone().into(),
                    migration_params: None,
                },
            )
            .await
            .map(|res| Some(res.into_inner()));

        self.handle_instance_put_result(
            &instance_id,
            prev_instance_runtime,
            instance_put_result.map(|state| state.map(Into::into)),
        )
        .await?;

        Ok(())
    }

    /// Reboot the specified instance.
    pub(crate) async fn instance_reboot(
        &self,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
    ) -> UpdateResult<InstanceAndActiveVmm> {
        let (.., authz_instance) =
            instance_lookup.lookup_for(authz::Action::Modify).await?;

        let state = self
            .db_datastore
            .instance_fetch_with_vmm(opctx, &authz_instance)
            .await?;

        self.instance_request_state(
            opctx,
            &authz_instance,
            state.instance(),
            state.vmm(),
            InstanceStateChangeRequest::Reboot,
        )
        .await?;
        self.db_datastore.instance_fetch_with_vmm(opctx, &authz_instance).await
    }

    /// Attempts to start an instance if it is currently stopped.
    pub(crate) async fn instance_start(
        self: &Arc<Self>,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
    ) -> UpdateResult<InstanceAndActiveVmm> {
        let (.., authz_instance) =
            instance_lookup.lookup_for(authz::Action::Modify).await?;

        let state = self
            .db_datastore
            .instance_fetch_with_vmm(opctx, &authz_instance)
            .await?;
        let (instance, vmm) = (state.instance(), state.vmm());

        if let Some(vmm) = vmm {
            match vmm.runtime.state.0 {
                InstanceState::Starting
                | InstanceState::Running
                | InstanceState::Rebooting => {
                    debug!(self.log, "asked to start an active instance";
                           "instance_id" => %authz_instance.id());

                    return Ok(state);
                }
                InstanceState::Stopped => {
                    let propolis_id = instance
                        .runtime()
                        .propolis_id
                        .expect("needed a VMM ID to fetch a VMM record");
                    error!(self.log,
                           "instance is stopped but still has an active VMM";
                           "instance_id" => %authz_instance.id(),
                           "propolis_id" => %propolis_id);

                    return Err(Error::internal_error(
                        "instance is stopped but still has an active VMM",
                    ));
                }
                _ => {
                    return Err(Error::conflict(&format!(
                        "instance is in state {} but must be {} to be started",
                        vmm.runtime.state.0,
                        InstanceState::Stopped
                    )));
                }
            }
        }

        let saga_params = sagas::instance_start::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            db_instance: instance.clone(),
        };

        self.execute_saga::<sagas::instance_start::SagaInstanceStart>(
            saga_params,
        )
        .await?;

        self.db_datastore.instance_fetch_with_vmm(opctx, &authz_instance).await
    }

    /// Make sure the given Instance is stopped.
    pub(crate) async fn instance_stop(
        &self,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
    ) -> UpdateResult<InstanceAndActiveVmm> {
        let (.., authz_instance) =
            instance_lookup.lookup_for(authz::Action::Modify).await?;

        let state = self
            .db_datastore
            .instance_fetch_with_vmm(opctx, &authz_instance)
            .await?;

        self.instance_request_state(
            opctx,
            &authz_instance,
            state.instance(),
            state.vmm(),
            InstanceStateChangeRequest::Stop,
        )
        .await?;

        self.db_datastore.instance_fetch_with_vmm(opctx, &authz_instance).await
    }

    /// Idempotently ensures that the sled specified in `db_instance` does not
    /// have a record of the instance. If the instance is currently running on
    /// this sled, this operation rudely terminates it.
    pub(crate) async fn instance_ensure_unregistered(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        sled_id: &Uuid,
        prev_instance_runtime: &db::model::InstanceRuntimeState,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, authz_instance).await?;
        let sa = self.sled_client(&sled_id).await?;
        let result = sa
            .instance_unregister(&authz_instance.id())
            .await
            .map(|res| res.into_inner().updated_runtime);

        self.handle_instance_put_result(
            &authz_instance.id(),
            prev_instance_runtime,
            result.map(|state| state.map(Into::into)),
        )
        .await
        .map(|_| ())
    }

    /// Determines the action to take on an instance's active VMM given a
    /// request to change its state.
    ///
    /// # Arguments
    ///
    /// - instance_state: The prior state of the instance as recorded in CRDB
    ///   and obtained by the caller.
    /// - vmm_state: The prior state of the instance's active VMM as recorded in
    ///   CRDB and obtained by the caller. `None` if the instance has no active
    ///   VMM.
    /// - requested: The state change being requested.
    ///
    /// # Return value
    ///
    /// - `Ok(action)` if the request is allowed to proceed. The result payload
    ///   specifies how to handle the request.
    /// - `Err` if the request should be denied.
    fn select_runtime_change_action(
        &self,
        instance_state: &db::model::Instance,
        vmm_state: &Option<db::model::Vmm>,
        requested: &InstanceStateChangeRequest,
    ) -> Result<InstanceStateChangeRequestAction, Error> {
        let effective_state = if let Some(vmm) = vmm_state {
            vmm.runtime.state.0
        } else {
            instance_state.runtime().nexus_state.0
        };

        // Requests that operate on active instances have to be directed to the
        // instance's current sled agent. If there is none, the request needs to
        // be handled specially based on its type.
        let sled_id = if let Some(vmm) = vmm_state {
            vmm.sled_id
        } else {
            match effective_state {
                // If there's no active sled because the instance is stopped,
                // allow requests to stop to succeed silently for idempotency,
                // but reject requests to do anything else.
                InstanceState::Stopped => match requested {
                    InstanceStateChangeRequest::Run => {
                        return Err(Error::invalid_request(&format!(
                            "cannot run an instance in state {} with no VMM",
                            effective_state
                        )))
                    }
                    InstanceStateChangeRequest::Stop => {
                        return Ok(InstanceStateChangeRequestAction::AlreadyDone);
                    }
                    InstanceStateChangeRequest::Reboot => {
                        return Err(Error::invalid_request(&format!(
                            "cannot reboot an instance in state {} with no VMM",
                            effective_state
                        )))
                    }
                    InstanceStateChangeRequest::Migrate(_) => {
                        return Err(Error::invalid_request(&format!(
                            "cannot migrate an instance in state {} with no VMM",
                            effective_state
                        )))
                    }
                },

                // If the instance is still being created (such that it hasn't
                // even begun to start yet), no runtime state change is valid.
                // Return a specific error message explaining the problem.
                InstanceState::Creating => {
                    return Err(Error::invalid_request(
                                "cannot change instance state while it is \
                                still being created"
                                ))
                }

                // If the instance has no sled beacuse it's been destroyed or
                // has fallen over, reject the state change.
                //
                // TODO(#2825): Failed instances should be allowed to stop, but
                // this requires a special action because there is no sled to
                // send the request to.
                InstanceState::Failed | InstanceState::Destroyed => {
                    return Err(Error::invalid_request(&format!(
                        "instance state cannot be changed from {}",
                        effective_state
                    )))
                }

                // In other states, the instance should have a sled, and an
                // internal invariant has been violated if it doesn't have one.
                _ => {
                    error!(self.log, "instance has no sled but isn't halted";
                           "instance_id" => %instance_state.id(),
                           "state" => ?effective_state);

                    return Err(Error::internal_error(
                        "instance is active but not resident on a sled"
                    ));
                }
            }
        };

        // The instance has an active sled. Allow the sled agent to decide how
        // to handle the request unless the instance is being recovered or the
        // underlying VMM has been destroyed.
        //
        // TODO(#2825): Failed instances should be allowed to stop. See above.
        let allowed = match requested {
            InstanceStateChangeRequest::Run
            | InstanceStateChangeRequest::Reboot
            | InstanceStateChangeRequest::Stop => match effective_state {
                InstanceState::Creating
                | InstanceState::Starting
                | InstanceState::Running
                | InstanceState::Stopping
                | InstanceState::Stopped
                | InstanceState::Rebooting
                | InstanceState::Migrating => true,
                InstanceState::Repairing | InstanceState::Failed => false,
                InstanceState::Destroyed => false,
            },
            InstanceStateChangeRequest::Migrate(_) => match effective_state {
                InstanceState::Running
                | InstanceState::Rebooting
                | InstanceState::Migrating => true,
                InstanceState::Creating
                | InstanceState::Starting
                | InstanceState::Stopping
                | InstanceState::Stopped
                | InstanceState::Repairing
                | InstanceState::Failed
                | InstanceState::Destroyed => false,
            },
        };

        if allowed {
            Ok(InstanceStateChangeRequestAction::SendToSled(sled_id))
        } else {
            Err(Error::InvalidRequest {
                message: format!(
                    "instance state cannot be changed from state \"{}\"",
                    effective_state
                ),
            })
        }
    }

    pub(crate) async fn instance_request_state(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        prev_instance_state: &db::model::Instance,
        prev_vmm_state: &Option<db::model::Vmm>,
        requested: InstanceStateChangeRequest,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, authz_instance).await?;
        let instance_id = authz_instance.id();

        match self.select_runtime_change_action(
            prev_instance_state,
            prev_vmm_state,
            &requested,
        )? {
            InstanceStateChangeRequestAction::AlreadyDone => Ok(()),
            InstanceStateChangeRequestAction::SendToSled(sled_id) => {
                let sa = self.sled_client(&sled_id).await?;
                let instance_put_result = sa
                    .instance_put_state(
                        &instance_id,
                        &InstancePutStateBody { state: requested.into() },
                    )
                    .await
                    .map(|res| res.into_inner().updated_runtime)
                    .map(|state| state.map(Into::into));

                self.handle_instance_put_result(
                    &instance_id,
                    prev_instance_state.runtime(),
                    instance_put_result,
                )
                .await
                .map(|_| ())
            }
        }
    }

    /// Modifies the runtime state of the Instance as requested.  This generally
    /// means booting or halting the Instance.
    pub(crate) async fn instance_ensure_registered(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        db_instance: &db::model::Instance,
        propolis_id: &Uuid,
        initial_vmm: &db::model::Vmm,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, authz_instance).await?;

        // Gather disk information and turn that into DiskRequests
        let disks = self
            .db_datastore
            .instance_list_disks(
                &opctx,
                &authz_instance,
                &PaginatedBy::Name(DataPageParams {
                    marker: None,
                    direction: dropshot::PaginationOrder::Ascending,
                    limit: std::num::NonZeroU32::new(MAX_DISKS_PER_INSTANCE)
                        .unwrap(),
                }),
            )
            .await?;

        let mut disk_reqs = vec![];
        for disk in &disks {
            // Disks that are attached to an instance should always have a slot
            // assignment, but if for some reason this one doesn't, return an
            // error instead of taking down the whole process.
            let slot = match disk.slot {
                Some(s) => s,
                None => {
                    error!(self.log, "attached disk has no PCI slot assignment";
                       "disk_id" => %disk.id(),
                       "disk_name" => disk.name().to_string(),
                       "instance_id" => ?disk.runtime_state.attach_instance_id);

                    return Err(Error::internal_error(&format!(
                        "disk {} is attached but has no PCI slot assignment",
                        disk.id()
                    )));
                }
            };

            let volume =
                self.db_datastore.volume_checkout(disk.volume_id).await?;
            disk_reqs.push(sled_agent_client::types::DiskRequest {
                name: disk.name().to_string(),
                slot: sled_agent_client::types::Slot(slot.0),
                read_only: false,
                device: "nvme".to_string(),
                volume_construction_request: serde_json::from_str(
                    &volume.data(),
                )?,
            });
        }

        let nics = self
            .db_datastore
            .derive_guest_network_interface_info(&opctx, &authz_instance)
            .await?;

        // Collect the external IPs for the instance.
        // TODO-correctness: Handle Floating IPs, see
        //  https://github.com/oxidecomputer/omicron/issues/1334
        let (snat_ip, external_ips): (Vec<_>, Vec<_>) = self
            .db_datastore
            .instance_lookup_external_ips(&opctx, authz_instance.id())
            .await?
            .into_iter()
            .partition(|ip| ip.kind == IpKind::SNat);

        // Sanity checks on the number and kind of each IP address.
        // TODO-correctness: Handle multiple IP addresses, see
        //  https://github.com/oxidecomputer/omicron/issues/1467
        if external_ips.len() > MAX_EXTERNAL_IPS_PER_INSTANCE {
            return Err(Error::internal_error(
                format!(
                    "Expected the number of external IPs to be limited to \
                    {}, but found {}",
                    MAX_EXTERNAL_IPS_PER_INSTANCE,
                    external_ips.len(),
                )
                .as_str(),
            ));
        }
        let external_ips =
            external_ips.into_iter().map(|model| model.ip.ip()).collect();
        if snat_ip.len() != 1 {
            return Err(Error::internal_error(
                "Expected exactly one SNAT IP address for an instance",
            ));
        }
        let source_nat =
            SourceNatConfig::from(snat_ip.into_iter().next().unwrap());

        // Gather the firewall rules for the VPC this instance is in.
        // The NIC info we gathered above doesn't have VPC information
        // because the sled agent doesn't care about that directly,
        // so we fetch it via the first interface's VNI. (It doesn't
        // matter which one we use because all NICs must be in the
        // same VPC; see the check in project_create_instance.)
        let firewall_rules = if let Some(nic) = nics.first() {
            let vni = Vni::try_from(nic.vni.0)?;
            let vpc = self
                .db_datastore
                .resolve_vni_to_vpc(opctx, db::model::Vni(vni))
                .await?;
            let (.., authz_vpc) = LookupPath::new(opctx, &self.db_datastore)
                .vpc_id(vpc.id())
                .lookup_for(authz::Action::Read)
                .await?;
            let rules = self
                .db_datastore
                .vpc_list_firewall_rules(opctx, &authz_vpc)
                .await?;
            self.resolve_firewall_rules_for_sled_agent(opctx, &vpc, &rules)
                .await?
        } else {
            vec![]
        };

        // Gather the SSH public keys of the actor make the request so
        // that they may be injected into the new image via cloud-init.
        // TODO-security: this should be replaced with a lookup based on
        // on `SiloUser` role assignments once those are in place.
        let actor = opctx.authn.actor_required().internal_context(
            "loading current user's ssh keys for new Instance",
        )?;
        let (.., authz_user) = LookupPath::new(opctx, &self.db_datastore)
            .silo_user_id(actor.actor_id())
            .lookup_for(authz::Action::ListChildren)
            .await?;
        let public_keys = self
            .db_datastore
            .ssh_keys_list(
                opctx,
                &authz_user,
                &PaginatedBy::Name(DataPageParams {
                    marker: None,
                    direction: dropshot::PaginationOrder::Ascending,
                    limit: std::num::NonZeroU32::new(MAX_KEYS_PER_INSTANCE)
                        .unwrap(),
                }),
            )
            .await?
            .into_iter()
            .map(|ssh_key| ssh_key.public_key)
            .collect::<Vec<String>>();

        // Ask the sled agent to begin the state change.  Then update the
        // database to reflect the new intermediate state.  If this update is
        // not the newest one, that's fine.  That might just mean the sled agent
        // beat us to it.

        let instance_hardware = sled_agent_client::types::InstanceHardware {
            properties: InstanceProperties {
                ncpus: db_instance.ncpus.into(),
                memory: db_instance.memory.into(),
                hostname: db_instance.hostname.clone(),
            },
            nics,
            source_nat,
            external_ips,
            firewall_rules,
            dhcp_config: sled_agent_client::types::DhcpConfig {
                dns_servers: self.external_dns_servers.clone(),
                // TODO: finish designing instance DNS
                host_domain: None,
                search_domains: Vec::new(),
            },
            disks: disk_reqs,
            cloud_init_bytes: Some(base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                db_instance.generate_cidata(&public_keys)?,
            )),
        };

        let sa = self.sled_client(&initial_vmm.sled_id).await?;
        let instance_register_result = sa
            .instance_register(
                &db_instance.id(),
                &sled_agent_client::types::InstanceEnsureBody {
                    hardware: instance_hardware,
                    instance_runtime: db_instance.runtime().clone().into(),
                    vmm_runtime: initial_vmm.clone().into(),
                    propolis_id: *propolis_id,
                    propolis_addr: SocketAddr::new(
                        initial_vmm.propolis_ip.ip(),
                        PROPOLIS_PORT,
                    )
                    .to_string(),
                },
            )
            .await
            .map(|res| Some(res.into_inner()));

        self.handle_instance_put_result(
            &db_instance.id(),
            db_instance.runtime(),
            instance_register_result.map(|state| state.map(Into::into)),
        )
        .await
        .map(|_| ())
    }

    /// Updates an instance's CRDB record based on the result of a call to sled
    /// agent that tried to update the instance's state.
    ///
    /// # Parameters
    ///
    /// - `db_instance`: The CRDB instance record observed by the caller before
    ///   it attempted to update the instance's state.
    /// - `result`: The result of the relevant sled agent operation. If this is
    ///   `Ok`, the payload is the updated instance runtime state returned from
    ///   sled agent, if there was one.
    ///
    /// # Return value
    ///
    /// - `Ok(true)` if the caller supplied an updated instance record and this
    ///   routine successfully wrote it to CRDB.
    /// - `Ok(false)` if the sled agent call succeeded, but this routine did not
    ///   update CRDB.
    ///   This can happen either because sled agent didn't return an updated
    ///   record or because the updated record was superseded by a state update
    ///   with a more advanced generation number.
    /// - `Err` if the sled agent operation failed or this routine received an
    ///   error while trying to update CRDB.
    async fn handle_instance_put_result(
        &self,
        instance_id: &Uuid,
        prev_instance_runtime: &db::model::InstanceRuntimeState,
        result: Result<
            Option<nexus::SledInstanceState>,
            sled_agent_client::Error<sled_agent_client::types::Error>,
        >,
    ) -> Result<(bool, bool), Error> {
        slog::debug!(&self.log, "Handling sled agent instance PUT result";
                     "instance_id" => %instance_id,
                     "result" => ?result);

        match result {
            Ok(Some(new_state)) => {
                let update_result = self
                    .db_datastore
                    .instance_and_vmm_update_runtime(
                        instance_id,
                        &new_state.instance_state.into(),
                        &new_state.propolis_id,
                        &new_state.vmm_state.into(),
                    )
                    .await;

                slog::debug!(&self.log,
                             "Attempted DB update after instance PUT";
                             "instance_id" => %instance_id,
                             "propolis_id" => %new_state.propolis_id,
                             "result" => ?update_result);

                update_result
            }
            Ok(None) => Ok((false, false)),
            Err(e) => {
                // The sled-agent has told us that it can't do what we
                // requested, but does that mean a failure? One example would be
                // if we try to "reboot" a stopped instance. That shouldn't
                // transition the instance to failed. But if the sled-agent
                // *can't* boot a stopped instance, that should transition
                // to failed.
                //
                // Without a richer error type, let the sled-agent tell Nexus
                // what to do with status codes.
                error!(self.log, "received error from instance PUT";
                       "instance_id" => %instance_id,
                       "error" => ?e);

                // Convert to the Omicron API error type.
                //
                // TODO(#3238): This is an extremely lossy conversion: if the
                // operation failed without getting a response from sled agent,
                // this unconditionally converts to Error::InternalError.
                let e = e.into();

                match &e {
                    // Bad request shouldn't change the instance state.
                    Error::InvalidRequest { .. } => Err(e),

                    // Internal server error (or anything else) should change
                    // the instance state to failed, we don't know what state
                    // the instance is in.
                    //
                    // TODO(#4226): This logic needs to be revisited:
                    // - Some errors that don't get classified as
                    //   Error::InvalidRequest (timeouts, disconnections due to
                    //   network weather, etc.) are not necessarily fatal to the
                    //   instance and shouldn't mark it as Failed.
                    // - If the instance still has a running VMM, this operation
                    //   won't terminate it or reclaim its resources. (The
                    //   resources will be reclaimed if the sled later reports
                    //   that the VMM is gone, however.)
                    _ => {
                        let new_runtime = db::model::InstanceRuntimeState {
                            nexus_state: db::model::InstanceState::new(
                                InstanceState::Failed,
                            ),

                            // TODO(#4226): Clearing the Propolis ID is required
                            // to allow the instance to be deleted, but this
                            // doesn't actually terminate the VMM (see above).
                            propolis_id: None,
                            gen: prev_instance_runtime.gen.next().into(),
                            ..prev_instance_runtime.clone()
                        };

                        // XXX what if this fails?
                        let result = self
                            .db_datastore
                            .instance_update_runtime(&instance_id, &new_runtime)
                            .await;

                        error!(
                            self.log,
                            "attempted to set instance to Failed after bad put";
                            "instance_id" => %instance_id,
                            "result" => ?result,
                        );

                        Err(e)
                    }
                }
            }
        }
    }

    /// Lists disks attached to the instance.
    pub(crate) async fn instance_list_disks(
        &self,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::Disk> {
        let (.., authz_instance) =
            instance_lookup.lookup_for(authz::Action::ListChildren).await?;
        self.db_datastore
            .instance_list_disks(opctx, &authz_instance, pagparams)
            .await
    }

    /// Attach a disk to an instance.
    pub(crate) async fn instance_attach_disk(
        &self,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
        disk: NameOrId,
    ) -> UpdateResult<db::model::Disk> {
        let (.., authz_project, authz_instance) =
            instance_lookup.lookup_for(authz::Action::Modify).await?;
        let (.., authz_project_disk, authz_disk) = self
            .disk_lookup(
                opctx,
                params::DiskSelector {
                    project: match disk {
                        NameOrId::Name(_) => Some(authz_project.id().into()),
                        NameOrId::Id(_) => None,
                    },
                    disk,
                },
            )?
            .lookup_for(authz::Action::Modify)
            .await?;

        // TODO-v1: Write test to verify this case
        // Because both instance and disk can be provided by ID it's possible
        // for someone to specify resources from different projects. The lookups
        // would resolve the resources (assuming the user had sufficient
        // permissions on both) without verifying the shared hierarchy. To
        // mitigate that we verify that their parent projects have the same ID.
        if authz_project.id() != authz_project_disk.id() {
            return Err(Error::InvalidRequest {
                message: "disk must be in the same project as the instance"
                    .to_string(),
            });
        }

        // TODO(https://github.com/oxidecomputer/omicron/issues/811):
        // Disk attach is only implemented for instances that are not
        // currently running. This operation therefore can operate exclusively
        // on database state.
        //
        // To implement hot-plug support, we should do the following in a saga:
        // - Update the state to "Attaching", rather than "Attached".
        // - If the instance is running...
        //   - Issue a request to "disk attach" to the associated sled agent,
        //   using the "state generation" value from the moment we attached.
        //   - Update the DB if the request succeeded (hopefully to "Attached").
        // - If the instance is not running...
        //   - Update the disk state in the DB to "Attached".
        let (_instance, disk) = self
            .db_datastore
            .instance_attach_disk(
                &opctx,
                &authz_instance,
                &authz_disk,
                MAX_DISKS_PER_INSTANCE,
            )
            .await?;
        Ok(disk)
    }

    /// Detach a disk from an instance.
    pub(crate) async fn instance_detach_disk(
        &self,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
        disk: NameOrId,
    ) -> UpdateResult<db::model::Disk> {
        let (.., authz_project, authz_instance) =
            instance_lookup.lookup_for(authz::Action::Modify).await?;
        let (.., authz_disk) = self
            .disk_lookup(
                opctx,
                params::DiskSelector {
                    project: match disk {
                        NameOrId::Name(_) => Some(authz_project.id().into()),
                        NameOrId::Id(_) => None,
                    },
                    disk,
                },
            )?
            .lookup_for(authz::Action::Modify)
            .await?;
        // TODO(https://github.com/oxidecomputer/omicron/issues/811):
        // Disk detach is only implemented for instances that are not
        // currently running. This operation therefore can operate exclusively
        // on database state.
        //
        // To implement hot-unplug support, we should do the following in a saga:
        // - Update the state to "Detaching", rather than "Detached".
        // - If the instance is running...
        //   - Issue a request to "disk detach" to the associated sled agent,
        //   using the "state generation" value from the moment we attached.
        //   - Update the DB if the request succeeded (hopefully to "Detached").
        // - If the instance is not running...
        //   - Update the disk state in the DB to "Detached".
        let disk = self
            .db_datastore
            .instance_detach_disk(&opctx, &authz_instance, &authz_disk)
            .await?;
        Ok(disk)
    }

    /// Invoked by a sled agent to publish an updated runtime state for an
    /// Instance.
    pub(crate) async fn notify_instance_updated(
        &self,
        opctx: &OpContext,
        instance_id: &Uuid,
        new_runtime_state: &nexus::SledInstanceState,
    ) -> Result<(), Error> {
        let log = &self.log;
        let propolis_id = new_runtime_state.propolis_id;

        info!(log, "received new runtime state from sled agent";
              "instance_id" => %instance_id,
              "instance_state" => ?new_runtime_state.instance_state,
              "propolis_id" => %propolis_id,
              "vmm_state" => ?new_runtime_state.vmm_state);

        // Grab the current state of the instance in the DB to reason about
        // whether this update is stale or not.
        let (.., authz_instance, db_instance) =
            LookupPath::new(&opctx, &self.db_datastore)
                .instance_id(*instance_id)
                .fetch()
                .await?;

        // Update OPTE and Dendrite if the instance's active sled assignment
        // changed or a migration was retired. If these actions fail, sled agent
        // is expected to retry this update.
        //
        // This configuration must be updated before updating any state in CRDB
        // so that, if the instance was migrating or has shut down, it will not
        // appear to be able to migrate or start again until the appropriate
        // networking state has been written. Without this interlock, another
        // thread or another Nexus can race with this routine to write
        // conflicting configuration.
        //
        // In the future, this should be replaced by a call to trigger a
        // networking state update RPW.
        self.ensure_updated_instance_network_config(
            opctx,
            &authz_instance,
            db_instance.runtime(),
            &new_runtime_state.instance_state,
        )
        .await?;

        // If the supplied instance state indicates that the instance no longer
        // has an active VMM, attempt to delete the virtual provisioning record,
        // and the assignment of the Propolis metric producer to an oximeter
        // collector.
        //
        // As with updating networking state, this must be done before
        // committing the new runtime state to the database: once the DB is
        // written, a new start saga can arrive and start the instance, which
        // will try to create its own virtual provisioning charges, which will
        // race with this operation.
        if new_runtime_state.instance_state.propolis_id.is_none() {
            self.db_datastore
                .virtual_provisioning_collection_delete_instance(
                    opctx,
                    *instance_id,
                    db_instance.project_id,
                    i64::from(db_instance.ncpus.0 .0),
                    db_instance.memory,
                    (&new_runtime_state.instance_state.gen).into(),
                )
                .await?;

            // TODO-correctness: The `notify_instance_updated` method can run
            // concurrently with itself in some situations, such as where a
            // sled-agent attempts to update Nexus about a stopped instance;
            // that times out; and it makes another request to a different
            // Nexus. The call to `unassign_producer` is racy in those
            // situations, and we may end with instances with no metrics.
            //
            // This unfortunate case should be handled as part of
            // instance-lifecycle improvements, notably using a reliable
            // persistent workflow to correctly update the oximete assignment as
            // an instance's state changes.
            //
            // Tracked in https://github.com/oxidecomputer/omicron/issues/3742.
            self.unassign_producer(instance_id).await?;
        }

        // Write the new instance and VMM states back to CRDB. This needs to be
        // done before trying to clean up the VMM, since the datastore will only
        // allow a VMM to be marked as deleted if it is already in a terminal
        // state.
        let result = self
            .db_datastore
            .instance_and_vmm_update_runtime(
                instance_id,
                &db::model::InstanceRuntimeState::from(
                    new_runtime_state.instance_state.clone(),
                ),
                &propolis_id,
                &db::model::VmmRuntimeState::from(
                    new_runtime_state.vmm_state.clone(),
                ),
            )
            .await;

        // If the VMM is now in a terminal state, make sure its resources get
        // cleaned up.
        //
        // For idempotency, only check to see if the update was successfully
        // processed and ignore whether the VMM record was actually updated.
        // This is required to handle the case where this routine is called
        // once, writes the terminal VMM state, fails before all per-VMM
        // resources are released, returns a retriable error, and is retried:
        // the per-VMM resources still need to be cleaned up, but the DB update
        // will return Ok(_, false) because the database was already updated.
        //
        // Unlike the pre-update cases, it is legal to do this cleanup *after*
        // committing state to the database, because a terminated VMM cannot be
        // reused (restarting or migrating its former instance will use new VMM
        // IDs).
        if result.is_ok() {
            let propolis_terminated = matches!(
                new_runtime_state.vmm_state.state,
                InstanceState::Destroyed | InstanceState::Failed
            );

            if propolis_terminated {
                info!(log, "vmm is terminated, cleaning up resources";
                      "instance_id" => %instance_id,
                      "propolis_id" => %propolis_id);

                self.db_datastore
                    .sled_reservation_delete(opctx, propolis_id)
                    .await?;

                if !self
                    .db_datastore
                    .vmm_mark_deleted(opctx, &propolis_id)
                    .await?
                {
                    warn!(log, "failed to mark vmm record as deleted";
                      "instance_id" => %instance_id,
                      "propolis_id" => %propolis_id,
                      "vmm_state" => ?new_runtime_state.vmm_state);
                }
            }
        }

        match result {
            Ok((instance_updated, vmm_updated)) => {
                info!(log, "instance and vmm updated by sled agent";
                      "instance_id" => %instance_id,
                      "propolis_id" => %propolis_id,
                      "instance_updated" => instance_updated,
                      "vmm_updated" => vmm_updated);
                Ok(())
            }

            // The update command should swallow object-not-found errors and
            // return them back as failures to update, so this error case is
            // unexpected. There's no work to do if this occurs, however.
            Err(Error::ObjectNotFound { .. }) => {
                error!(log, "instance/vmm update unexpectedly returned \
                       an object not found error";
                       "instance_id" => %instance_id,
                       "propolis_id" => %propolis_id);
                Ok(())
            }

            // If the datastore is unavailable, propagate that to the caller.
            // TODO-robustness Really this should be any _transient_ error.  How
            // can we distinguish?  Maybe datastore should emit something
            // different from Error with an Into<Error>.
            Err(error) => {
                warn!(log, "failed to update instance from sled agent";
                      "instance_id" => %instance_id,
                      "propolis_id" => %propolis_id,
                      "error" => ?error);
                Err(error)
            }
        }
    }

    /// Returns the requested range of serial console output bytes,
    /// provided they are still in the propolis-server's cache.
    pub(crate) async fn instance_serial_console_data(
        &self,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
        params: &params::InstanceSerialConsoleRequest,
    ) -> Result<params::InstanceSerialConsoleData, Error> {
        let client = self
            .propolis_client_for_instance(
                opctx,
                instance_lookup,
                authz::Action::Read,
            )
            .await?;
        let mut request = client.instance_serial_history_get();
        if let Some(max_bytes) = params.max_bytes {
            request = request.max_bytes(max_bytes);
        }
        if let Some(from_start) = params.from_start {
            request = request.from_start(from_start);
        }
        if let Some(most_recent) = params.most_recent {
            request = request.most_recent(most_recent);
        }
        let data = request
            .send()
            .await
            .map_err(|e| {
                Error::internal_error(&format!(
                    "websocket connection to instance's serial port failed: \
                        {:?}",
                    e,
                ))
            })?
            .into_inner();
        Ok(params::InstanceSerialConsoleData {
            data: data.data,
            last_byte_offset: data.last_byte_offset,
        })
    }

    pub(crate) async fn instance_serial_console_stream(
        &self,
        opctx: &OpContext,
        mut client_stream: WebSocketStream<impl AsyncRead + AsyncWrite + Unpin>,
        instance_lookup: &lookup::Instance<'_>,
        params: &params::InstanceSerialConsoleStreamRequest,
    ) -> Result<(), Error> {
        let client_addr = match self
            .propolis_addr_for_instance(
                opctx,
                instance_lookup,
                authz::Action::Modify,
            )
            .await
        {
            Ok(x) => x,
            Err(e) => {
                let _ = client_stream
                    .close(Some(CloseFrame {
                        code: CloseCode::Error,
                        reason: e.to_string().into(),
                    }))
                    .await
                    .is_ok();
                return Err(e);
            }
        };

        let offset = match params.most_recent {
            Some(most_recent) => WSClientOffset::MostRecent(most_recent),
            None => WSClientOffset::FromStart(0),
        };

        match propolis_client::support::InstanceSerialConsoleHelper::new(
            client_addr,
            offset,
            Some(self.log.clone()),
        )
        .await
        {
            Ok(propolis_conn) => {
                Self::proxy_instance_serial_ws(client_stream, propolis_conn)
                    .await
                    .map_err(|e| Error::internal_error(&format!("{}", e)))
            }
            Err(e) => {
                let message = format!(
                    "websocket connection to instance's serial port failed: {}",
                    e
                );
                let _ = client_stream
                    .close(Some(CloseFrame {
                        code: CloseCode::Error,
                        reason: message.clone().into(),
                    }))
                    .await
                    .is_ok();
                Err(Error::internal_error(&message))
            }
        }
    }

    async fn propolis_addr_for_instance(
        &self,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
        action: authz::Action,
    ) -> Result<SocketAddr, Error> {
        let (.., authz_instance) = instance_lookup.lookup_for(action).await?;

        let state = self
            .db_datastore
            .instance_fetch_with_vmm(opctx, &authz_instance)
            .await?;

        let (instance, vmm) = (state.instance(), state.vmm());
        if let Some(vmm) = vmm {
            match vmm.runtime.state.0 {
                InstanceState::Running
                | InstanceState::Rebooting
                | InstanceState::Migrating
                | InstanceState::Repairing => {
                    Ok(SocketAddr::new(vmm.propolis_ip.ip(), PROPOLIS_PORT))
                }
                InstanceState::Creating
                | InstanceState::Starting
                | InstanceState::Stopping
                | InstanceState::Stopped
                | InstanceState::Failed => Err(Error::ServiceUnavailable {
                    internal_message: format!(
                        "cannot connect to serial console of instance in state \
                            {:?}",
                        vmm.runtime.state.0
                    ),
                }),
                InstanceState::Destroyed => Err(Error::ServiceUnavailable {
                    internal_message: format!(
                        "cannot connect to serial console of instance in state \
                        {:?}",
                        InstanceState::Stopped),
                }),
            }
        } else {
            Err(Error::ServiceUnavailable {
                internal_message: format!(
                    "instance is in state {:?} and has no active serial console \
                    server",
                    instance.runtime().nexus_state
                )
            })
        }
    }

    async fn propolis_client_for_instance(
        &self,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
        action: authz::Action,
    ) -> Result<propolis_client::Client, Error> {
        let client_addr = self
            .propolis_addr_for_instance(opctx, instance_lookup, action)
            .await?;
        Ok(propolis_client::Client::new(&format!("http://{}", client_addr)))
    }

    async fn proxy_instance_serial_ws(
        client_stream: WebSocketStream<impl AsyncRead + AsyncWrite + Unpin>,
        mut propolis_conn: InstanceSerialConsoleHelper,
    ) -> Result<(), propolis_client::support::tungstenite::Error> {
        let (mut nexus_sink, mut nexus_stream) = client_stream.split();

        // buffered_input is Some if there's a websocket message waiting to be
        // sent from the client to propolis.
        let mut buffered_input = None;
        // buffered_output is Some if there's a websocket message waiting to be
        // sent from propolis to the client.
        let mut buffered_output = None;

        loop {
            let nexus_read;
            let nexus_reserve;
            let propolis_read;
            let propolis_reserve;

            if buffered_input.is_some() {
                // We already have a buffered input -- do not read any further
                // messages from the client.
                nexus_read = Fuse::terminated();
                propolis_reserve = propolis_conn.reserve().fuse();
                if buffered_output.is_some() {
                    // We already have a buffered output -- do not read any
                    // further messages from propolis.
                    nexus_reserve = nexus_sink.reserve().fuse();
                    propolis_read = Fuse::terminated();
                } else {
                    nexus_reserve = Fuse::terminated();
                    // can't propolis_read simultaneously due to a
                    // &mut propolis_conn being taken above
                    propolis_read = Fuse::terminated();
                }
            } else {
                nexus_read = nexus_stream.next().fuse();
                propolis_reserve = Fuse::terminated();
                if buffered_output.is_some() {
                    // We already have a buffered output -- do not read any
                    // further messages from propolis.
                    nexus_reserve = nexus_sink.reserve().fuse();
                    propolis_read = Fuse::terminated();
                } else {
                    nexus_reserve = Fuse::terminated();
                    propolis_read = propolis_conn.recv().fuse();
                }
            }

            tokio::select! {
                msg = nexus_read => {
                    match msg {
                        None => {
                            propolis_conn.send(WebSocketMessage::Close(Some(CloseFrame {
                                code: CloseCode::Abnormal,
                                reason: std::borrow::Cow::from(
                                    "nexus: websocket connection to client closed unexpectedly"
                                ),
                            }))).await?;
                            break;
                        }
                        Some(Err(e)) => {
                            propolis_conn.send(WebSocketMessage::Close(Some(CloseFrame {
                                code: CloseCode::Error,
                                reason: std::borrow::Cow::from(
                                    format!("nexus: error in websocket connection to client: {}", e)
                                ),
                            }))).await?;
                            return Err(e);
                        }
                        Some(Ok(WebSocketMessage::Close(details))) => {
                            propolis_conn.send(WebSocketMessage::Close(details)).await?;
                            break;
                        }
                        Some(Ok(WebSocketMessage::Text(_text))) => {
                            // TODO: json payloads specifying client-sent metadata?
                        }
                        Some(Ok(WebSocketMessage::Binary(data))) => {
                            debug_assert!(
                                buffered_input.is_none(),
                                "attempted to drop buffered_input message ({buffered_input:?})",
                            );
                            buffered_input = Some(WebSocketMessage::Binary(data))
                        }
                        // Frame won't exist at this level, and ping reply is handled by tungstenite
                        Some(Ok(WebSocketMessage::Frame(_) | WebSocketMessage::Ping(_) | WebSocketMessage::Pong(_))) => {}
                    }
                }
                result = nexus_reserve => {
                    let permit = result?;
                    let message = buffered_output
                        .take()
                        .expect("nexus_reserve is only active when buffered_output is Some");
                    permit.send(message)?.await?;
                }
                msg = propolis_read => {
                    if let Some(msg) = msg {
                        let msg = match msg {
                            Ok(msg) => msg.process().await, // msg.process isn't cancel-safe
                            Err(error) => Err(error),
                        };
                        match msg {
                            Err(e) => {
                                nexus_sink.send(WebSocketMessage::Close(Some(CloseFrame {
                                    code: CloseCode::Error,
                                    reason: std::borrow::Cow::from(
                                        format!("nexus: error in websocket connection to serial port: {}", e)
                                    ),
                                }))).await?;
                                return Err(e);
                            }
                            Ok(WebSocketMessage::Close(details)) => {
                                nexus_sink.send(WebSocketMessage::Close(details)).await?;
                                break;
                            }
                            Ok(WebSocketMessage::Text(_json)) => {
                                // connecting to new propolis-server is handled
                                // within InstanceSerialConsoleHelper already.
                                // we might consider sending the nexus client
                                // an informational event for UX polish.
                            }
                            Ok(WebSocketMessage::Binary(data)) => {
                                debug_assert!(
                                    buffered_output.is_none(),
                                    "attempted to drop buffered_output message ({buffered_output:?})",
                                );
                                buffered_output = Some(WebSocketMessage::Binary(data))
                            }
                            // Frame won't exist at this level, and ping reply is handled by tungstenite
                            Ok(WebSocketMessage::Frame(_) | WebSocketMessage::Ping(_) | WebSocketMessage::Pong(_)) => {}
                        }
                    } else {
                        nexus_sink.send(WebSocketMessage::Close(Some(CloseFrame {
                            code: CloseCode::Abnormal,
                            reason: std::borrow::Cow::from(
                                "nexus: websocket connection to serial port closed unexpectedly"
                            ),
                        }))).await?;
                        break;
                    }
                }
                result = propolis_reserve => {
                    let permit = result?;
                    let message = buffered_input
                        .take()
                        .expect("propolis_reserve is only active when buffered_input is Some");
                    permit.send(message)?.await?;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::Nexus;
    use super::{CloseCode, CloseFrame, WebSocketMessage, WebSocketStream};
    use core::time::Duration;
    use futures::{SinkExt, StreamExt};
    use omicron_test_utils::dev::test_setup_log;
    use propolis_client::support::tungstenite::protocol::Role;
    use propolis_client::support::{
        InstanceSerialConsoleHelper, WSClientOffset,
    };
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    #[tokio::test]
    async fn test_serial_console_stream_proxying() {
        let logctx = test_setup_log("test_serial_console_stream_proxying");
        let (nexus_client_conn, nexus_server_conn) = tokio::io::duplex(1024);
        let (propolis_client_conn, propolis_server_conn) =
            tokio::io::duplex(1024);
        // The address doesn't matter -- it's just a key to look up the connection with.
        let address =
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 0));
        let propolis_conn = InstanceSerialConsoleHelper::new_test(
            [(address, propolis_client_conn)],
            address,
            WSClientOffset::FromStart(0),
            Some(logctx.log.clone()),
        )
        .await
        .unwrap();

        let jh = tokio::spawn(async move {
            let nexus_client_stream = WebSocketStream::from_raw_socket(
                nexus_server_conn,
                Role::Server,
                None,
            )
            .await;
            Nexus::proxy_instance_serial_ws(nexus_client_stream, propolis_conn)
                .await
        });
        let mut nexus_client_ws = WebSocketStream::from_raw_socket(
            nexus_client_conn,
            Role::Client,
            None,
        )
        .await;
        let mut propolis_server_ws = WebSocketStream::from_raw_socket(
            propolis_server_conn,
            Role::Server,
            None,
        )
        .await;

        slog::info!(logctx.log, "sending messages to nexus client");
        let sent1 = WebSocketMessage::Binary(vec![1, 2, 3, 42, 5]);
        nexus_client_ws.send(sent1.clone()).await.unwrap();
        let sent2 = WebSocketMessage::Binary(vec![5, 42, 3, 2, 1]);
        nexus_client_ws.send(sent2.clone()).await.unwrap();
        slog::info!(
            logctx.log,
            "messages sent, receiving them via propolis server"
        );
        let received1 = propolis_server_ws.next().await.unwrap().unwrap();
        assert_eq!(sent1, received1);
        let received2 = propolis_server_ws.next().await.unwrap().unwrap();
        assert_eq!(sent2, received2);

        slog::info!(logctx.log, "sending messages to propolis server");
        let sent3 = WebSocketMessage::Binary(vec![6, 7, 8, 90]);
        propolis_server_ws.send(sent3.clone()).await.unwrap();
        let sent4 = WebSocketMessage::Binary(vec![90, 8, 7, 6]);
        propolis_server_ws.send(sent4.clone()).await.unwrap();
        slog::info!(logctx.log, "messages sent, receiving it via nexus client");
        let received3 = nexus_client_ws.next().await.unwrap().unwrap();
        assert_eq!(sent3, received3);
        let received4 = nexus_client_ws.next().await.unwrap().unwrap();
        assert_eq!(sent4, received4);

        slog::info!(logctx.log, "sending close message to nexus client");
        let sent = WebSocketMessage::Close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: std::borrow::Cow::from("test done"),
        }));
        nexus_client_ws.send(sent.clone()).await.unwrap();
        slog::info!(
            logctx.log,
            "close message sent, receiving it via propolis"
        );
        let received = propolis_server_ws.next().await.unwrap().unwrap();
        assert_eq!(sent, received);

        slog::info!(
            logctx.log,
            "propolis server closed, waiting \
             1s for proxy task to shut down"
        );
        tokio::time::timeout(Duration::from_secs(1), jh)
            .await
            .expect("proxy task shut down within 1s")
            .expect("task successfully completed")
            .expect("proxy task exited successfully");
        logctx.cleanup_successful();
    }
}
