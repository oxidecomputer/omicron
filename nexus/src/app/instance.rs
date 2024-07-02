// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Virtual Machine Instances

use super::MAX_DISKS_PER_INSTANCE;
use super::MAX_EPHEMERAL_IPS_PER_INSTANCE;
use super::MAX_EXTERNAL_IPS_PER_INSTANCE;
use super::MAX_MEMORY_BYTES_PER_INSTANCE;
use super::MAX_NICS_PER_INSTANCE;
use super::MAX_SSH_KEYS_PER_INSTANCE;
use super::MAX_VCPU_PER_INSTANCE;
use super::MIN_MEMORY_BYTES_PER_INSTANCE;
use crate::app::sagas;
use crate::cidata::InstanceCiData;
use crate::external_api::params;
use cancel_safe_futures::prelude::*;
use futures::future::Fuse;
use futures::{FutureExt, SinkExt, StreamExt};
use nexus_db_model::IpAttachState;
use nexus_db_model::IpKind;
use nexus_db_model::Vmm;
use nexus_db_model::VmmState as DbVmmState;
use nexus_db_queries::authn;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::datastore::instance::InstanceUpdateResult;
use nexus_db_queries::db::datastore::InstanceAndActiveVmm;
use nexus_db_queries::db::identity::Resource;
use nexus_db_queries::db::lookup;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::DataStore;
use nexus_types::external_api::views;
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
use omicron_common::api::internal::nexus;
use omicron_common::api::internal::nexus::VmmState;
use omicron_common::api::internal::shared::SourceNatConfig;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::PropolisUuid;
use omicron_uuid_kinds::SledUuid;
use propolis_client::support::tungstenite::protocol::frame::coding::CloseCode;
use propolis_client::support::tungstenite::protocol::CloseFrame;
use propolis_client::support::tungstenite::Message as WebSocketMessage;
use propolis_client::support::InstanceSerialConsoleHelper;
use propolis_client::support::WSClientOffset;
use propolis_client::support::WebSocketStream;
use sagas::instance_common::ExternalIpAttach;
use sled_agent_client::types::InstanceMigrationSourceParams;
use sled_agent_client::types::InstanceMigrationTargetParams;
use sled_agent_client::types::InstanceProperties;
use sled_agent_client::types::InstancePutMigrationIdsBody;
use sled_agent_client::types::InstancePutStateBody;
use std::matches;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use uuid::Uuid;

type SledAgentClientError =
    sled_agent_client::Error<sled_agent_client::types::Error>;

// Newtype wrapper to avoid the orphan type rule.
#[derive(Debug, thiserror::Error)]
pub struct SledAgentInstancePutError(pub SledAgentClientError);

impl std::fmt::Display for SledAgentInstancePutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<SledAgentClientError> for SledAgentInstancePutError {
    fn from(value: SledAgentClientError) -> Self {
        Self(value)
    }
}

impl From<SledAgentInstancePutError> for omicron_common::api::external::Error {
    fn from(value: SledAgentInstancePutError) -> Self {
        value.0.into()
    }
}

impl SledAgentInstancePutError {
    /// Returns `true` if this error is of a class that indicates that Nexus
    /// cannot assume anything about the health of the instance or its sled.
    pub fn instance_unhealthy(&self) -> bool {
        // TODO(#3238) TODO(#4226) For compatibility, this logic is lifted from
        // the From impl that converts Progenitor client errors to
        // `omicron_common::api::external::Error`s and from previous logic in
        // this module that inferred instance health from those converted
        // errors. In particular, some of the outer Progenitor client error
        // variants (e.g. CommunicationError) can indicate transient conditions
        // that aren't really fatal to an instance and don't otherwise indicate
        // that it's unhealthy.
        //
        // To match old behavior until this is revisited, however, treat all
        // Progenitor errors except for explicit error responses as signs of an
        // unhealthy instance, and then only treat an instance as healthy if its
        // sled returned a 400-level status code.
        match &self.0 {
            progenitor_client::Error::ErrorResponse(rv) => {
                !rv.status().is_client_error()
            }
            _ => true,
        }
    }
}

/// An error that can be returned from an operation that changes the state of an
/// instance on a specific sled.
#[derive(Debug, thiserror::Error)]
pub enum InstanceStateChangeError {
    /// Sled agent returned an error from one of its instance endpoints.
    #[error("sled agent client error")]
    SledAgent(#[source] SledAgentInstancePutError),

    /// Some other error occurred outside of the attempt to communicate with
    /// sled agent.
    #[error(transparent)]
    Other(#[from] omicron_common::api::external::Error),
}

// Allow direct conversion of instance state change errors into API errors for
// callers who don't care about the specific reason the update failed and just
// need to return an API error.
impl From<InstanceStateChangeError> for omicron_common::api::external::Error {
    fn from(value: InstanceStateChangeError) -> Self {
        match value {
            InstanceStateChangeError::SledAgent(e) => e.into(),
            InstanceStateChangeError::Other(e) => e,
        }
    }
}

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
    SendToSled(SledUuid),
}

/// What is the higher level operation that is calling
/// `instance_ensure_registered`?
pub(crate) enum InstanceRegisterReason {
    Start { vmm_id: PropolisUuid },
    Migrate { vmm_id: PropolisUuid, target_vmm_id: PropolisUuid },
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
        if params
            .external_ips
            .iter()
            .filter(|v| matches!(v, params::ExternalIpCreate::Ephemeral { .. }))
            .count()
            > MAX_EPHEMERAL_IPS_PER_INSTANCE
        {
            return Err(Error::invalid_request(&format!(
                "An instance may not have more than {} ephemeral IP address",
                MAX_EPHEMERAL_IPS_PER_INSTANCE,
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
        if params.memory.to_bytes() < u64::from(MIN_MEMORY_BYTES_PER_INSTANCE) {
            return Err(Error::invalid_value(
                "size",
                format!(
                    "memory must be at least {}",
                    ByteCount::from(MIN_MEMORY_BYTES_PER_INSTANCE)
                ),
            ));
        }

        // Reject instances where the memory is not divisible by
        // MIN_MEMORY_BYTES_PER_INSTANCE
        if (params.memory.to_bytes() % u64::from(MIN_MEMORY_BYTES_PER_INSTANCE))
            != 0
        {
            return Err(Error::invalid_value(
                "size",
                format!(
                    "memory must be divisible by {}",
                    ByteCount::from(MIN_MEMORY_BYTES_PER_INSTANCE)
                ),
            ));
        }

        // Reject instances where the memory is greater than the limit
        if params.memory.to_bytes() > MAX_MEMORY_BYTES_PER_INSTANCE {
            return Err(Error::invalid_value(
                "size",
                format!(
                    "memory must be less than or equal to {}",
                    ByteCount::try_from(MAX_MEMORY_BYTES_PER_INSTANCE).unwrap()
                ),
            ));
        }

        let actor = opctx.authn.actor_required().internal_context(
            "loading current user's ssh keys for new Instance",
        )?;
        let (.., authz_user) = LookupPath::new(opctx, &self.db_datastore)
            .silo_user_id(actor.actor_id())
            .lookup_for(authz::Action::ListChildren)
            .await?;

        let ssh_keys = match &params.ssh_public_keys {
            Some(keys) => Some(
                self.db_datastore
                    .ssh_keys_batch_lookup(opctx, &authz_user, keys)
                    .await?
                    .iter()
                    .map(|id| NameOrId::Id(*id))
                    .collect::<Vec<NameOrId>>(),
            ),
            None => None,
        };
        if let Some(ssh_keys) = &ssh_keys {
            if ssh_keys.len() > MAX_SSH_KEYS_PER_INSTANCE.try_into().unwrap() {
                return Err(Error::invalid_request(format!(
                    "cannot attach more than {} ssh keys to the instance",
                    MAX_SSH_KEYS_PER_INSTANCE
                )));
            }
        }

        let saga_params = sagas::instance_create::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            project_id: authz_project.id(),
            create_params: params::InstanceCreate {
                ssh_public_keys: ssh_keys,
                ..params.clone()
            },
            boundary_switches: self
                .boundary_switches(&self.opctx_alloc)
                .await?,
        };

        let saga_outputs = self
            .sagas
            .saga_execute::<sagas::instance_create::SagaInstanceCreate>(
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
        self.sagas
            .saga_execute::<sagas::instance_delete::SagaInstanceDelete>(
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
            || vmm.as_ref().unwrap().runtime.state != DbVmmState::Running
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
            return Err(Error::conflict("instance is already migrating"));
        }

        // Kick off the migration saga
        let saga_params = sagas::instance_migrate::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            instance: instance.clone(),
            src_vmm: vmm.clone(),
            migrate_params: params,
        };
        self.sagas
            .saga_execute::<sagas::instance_migrate::SagaInstanceMigrate>(
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
        instance_id: InstanceUuid,
        sled_id: SledUuid,
        prev_instance_runtime: &db::model::InstanceRuntimeState,
        migration_params: InstanceMigrationSourceParams,
    ) -> UpdateResult<db::model::Instance> {
        assert!(prev_instance_runtime.migration_id.is_none());
        assert!(prev_instance_runtime.dst_propolis_id.is_none());

        let (.., authz_instance) = LookupPath::new(opctx, &self.db_datastore)
            .instance_id(instance_id.into_untyped_uuid())
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
            .map(|res| Some(res.into_inner().into()))
            .map_err(|e| SledAgentInstancePutError(e));

        // Write the updated instance runtime state back to CRDB. If this
        // outright fails, this operation fails. If the operation nominally
        // succeeds but nothing was updated, this action is outdated and the
        // caller should not proceed with migration.
        let InstanceUpdateResult { instance_updated, .. } =
            match instance_put_result {
                Ok(state) => {
                    self.write_returned_instance_state(&instance_id, state)
                        .await?
                }
                Err(e) => {
                    if e.instance_unhealthy() {
                        let _ = self
                            .mark_instance_failed(
                                &instance_id,
                                &prev_instance_runtime,
                                &e,
                            )
                            .await;
                    }
                    return Err(e.into());
                }
            };

        if instance_updated {
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
        instance_id: InstanceUuid,
        sled_id: SledUuid,
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
            .map(|res| Some(res.into_inner().into()))
            .map_err(|e| SledAgentInstancePutError(e));

        match instance_put_result {
            Ok(state) => {
                self.write_returned_instance_state(&instance_id, state).await?;
            }
            Err(e) => {
                if e.instance_unhealthy() {
                    let _ = self
                        .mark_instance_failed(
                            &instance_id,
                            &prev_instance_runtime,
                            &e,
                        )
                        .await;
                }
                return Err(e.into());
            }
        }

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

        if let Err(e) = self
            .instance_request_state(
                opctx,
                &authz_instance,
                state.instance(),
                state.vmm(),
                InstanceStateChangeRequest::Reboot,
            )
            .await
        {
            if let InstanceStateChangeError::SledAgent(inner) = &e {
                if inner.instance_unhealthy() {
                    let _ = self
                        .mark_instance_failed(
                            &InstanceUuid::from_untyped_uuid(
                                authz_instance.id(),
                            ),
                            state.instance().runtime(),
                            inner,
                        )
                        .await;
                }
            }

            return Err(e.into());
        }

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
            match vmm.runtime.state {
                DbVmmState::Starting
                | DbVmmState::Running
                | DbVmmState::Rebooting => {
                    debug!(self.log, "asked to start an active instance";
                           "instance_id" => %authz_instance.id());

                    return Ok(state);
                }
                DbVmmState::Stopped => {
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
                        vmm.runtime.state,
                        InstanceState::Stopped
                    )));
                }
            }
        }

        let saga_params = sagas::instance_start::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            db_instance: instance.clone(),
        };

        self.sagas
            .saga_execute::<sagas::instance_start::SagaInstanceStart>(
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

        if let Err(e) = self
            .instance_request_state(
                opctx,
                &authz_instance,
                state.instance(),
                state.vmm(),
                InstanceStateChangeRequest::Stop,
            )
            .await
        {
            if let InstanceStateChangeError::SledAgent(inner) = &e {
                if inner.instance_unhealthy() {
                    let _ = self
                        .mark_instance_failed(
                            &InstanceUuid::from_untyped_uuid(
                                authz_instance.id(),
                            ),
                            state.instance().runtime(),
                            inner,
                        )
                        .await;
                }
            }

            return Err(e.into());
        }

        self.db_datastore.instance_fetch_with_vmm(opctx, &authz_instance).await
    }

    /// Idempotently ensures that the sled specified in `db_instance` does not
    /// have a record of the instance. If the instance is currently running on
    /// this sled, this operation rudely terminates it.
    pub(crate) async fn instance_ensure_unregistered(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        sled_id: &SledUuid,
    ) -> Result<Option<nexus::SledInstanceState>, InstanceStateChangeError>
    {
        opctx.authorize(authz::Action::Modify, authz_instance).await?;
        let sa = self.sled_client(&sled_id).await?;
        sa.instance_unregister(&InstanceUuid::from_untyped_uuid(
            authz_instance.id(),
        ))
        .await
        .map(|res| res.into_inner().updated_runtime.map(Into::into))
        .map_err(|e| {
            InstanceStateChangeError::SledAgent(SledAgentInstancePutError(e))
        })
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
            vmm.runtime.state.into()
        } else {
            instance_state.runtime().nexus_state.into()
        };

        // Requests that operate on active instances have to be directed to the
        // instance's current sled agent. If there is none, the request needs to
        // be handled specially based on its type.
        let sled_id = if let Some(vmm) = vmm_state {
            SledUuid::from_untyped_uuid(vmm.sled_id)
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
            Err(Error::invalid_request(format!(
                "instance state cannot be changed from state \"{}\"",
                effective_state
            )))
        }
    }

    pub(crate) async fn instance_request_state(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        prev_instance_state: &db::model::Instance,
        prev_vmm_state: &Option<db::model::Vmm>,
        requested: InstanceStateChangeRequest,
    ) -> Result<(), InstanceStateChangeError> {
        opctx.authorize(authz::Action::Modify, authz_instance).await?;
        let instance_id = InstanceUuid::from_untyped_uuid(authz_instance.id());

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
                    .map(|res| res.into_inner().updated_runtime.map(Into::into))
                    .map_err(|e| SledAgentInstancePutError(e));

                // If the operation succeeded, write the instance state back,
                // returning any subsequent errors that occurred during that
                // write.
                //
                // If the operation failed, kick the sled agent error back up to
                // the caller to let it decide how to handle it.
                //
                // When creating the zone for the first time, we just get
                // Ok(None) here, which is a no-op in write_returned_instance_state.
                match instance_put_result {
                    Ok(state) => self
                        .write_returned_instance_state(&instance_id, state)
                        .await
                        .map(|_| ())
                        .map_err(Into::into),
                    Err(e) => Err(InstanceStateChangeError::SledAgent(e)),
                }
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
        propolis_id: &PropolisUuid,
        initial_vmm: &db::model::Vmm,
        operation: InstanceRegisterReason,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, authz_instance).await?;

        // Check that the hostname is valid.
        //
        // TODO-cleanup: This can be removed when we are confident that no
        // instances exist prior to the addition of strict hostname validation
        // in the API.
        let Ok(hostname) = db_instance.hostname.parse() else {
            let msg = format!(
                "The instance hostname '{}' is no longer valid. \
                To access the data on its disks, this instance \
                must be deleted, and a new one created with the \
                relevant disks. The new hostname will be validated \
                at that time.",
                db_instance.hostname,
            );
            return Err(Error::invalid_request(&msg));
        };

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

            let volume = self
                .db_datastore
                .volume_checkout(
                    disk.volume_id,
                    match operation {
                        InstanceRegisterReason::Start { vmm_id } =>
                            db::datastore::VolumeCheckoutReason::InstanceStart { vmm_id },
                        InstanceRegisterReason::Migrate { vmm_id, target_vmm_id } =>
                            db::datastore::VolumeCheckoutReason::InstanceMigrate { vmm_id, target_vmm_id },
                    }
                )
                .await?;

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
        let (snat_ip, external_ips): (Vec<_>, Vec<_>) = self
            .db_datastore
            .instance_lookup_external_ips(
                &opctx,
                InstanceUuid::from_untyped_uuid(authz_instance.id()),
            )
            .await?
            .into_iter()
            .partition(|ip| ip.kind == IpKind::SNat);

        // Sanity checks on the number and kind of each IP address.
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

        // If there are any external IPs not yet fully attached/detached,then
        // there are attach/detach sagas in progress. That should complete in
        // its own time, so return a 503 to indicate a possible retry.
        if external_ips.iter().any(|v| v.state != IpAttachState::Attached) {
            return Err(Error::unavail(
                "External IP attach/detach is in progress during instance_ensure_registered"
            ));
        }

        // Partition remaining external IPs by class: we can have at most
        // one ephemeral ip.
        let (ephemeral_ips, floating_ips): (Vec<_>, Vec<_>) = external_ips
            .into_iter()
            .partition(|ip| ip.kind == IpKind::Ephemeral);

        if ephemeral_ips.len() > MAX_EPHEMERAL_IPS_PER_INSTANCE {
            return Err(Error::internal_error(
                format!(
                "Expected at most {} ephemeral IP for an instance, found {}",
                MAX_EPHEMERAL_IPS_PER_INSTANCE,
                ephemeral_ips.len()
            )
                .as_str(),
            ));
        }

        let ephemeral_ip = ephemeral_ips.get(0).map(|model| model.ip.ip());

        let floating_ips =
            floating_ips.into_iter().map(|model| model.ip.ip()).collect();
        if snat_ip.len() != 1 {
            return Err(Error::internal_error(
                "Expected exactly one SNAT IP address for an instance",
            ));
        }
        let source_nat =
            SourceNatConfig::try_from(snat_ip.into_iter().next().unwrap())
                .map_err(|err| {
                    Error::internal_error(&format!(
                        "read invalid SNAT config from db: {err}"
                    ))
                })?;

        // Gather the firewall rules for the VPC this instance is in.
        // The NIC info we gathered above doesn't have VPC information
        // because the sled agent doesn't care about that directly,
        // so we fetch it via the first interface's VNI. (It doesn't
        // matter which one we use because all NICs must be in the
        // same VPC; see the check in project_create_instance.)
        let firewall_rules = if let Some(nic) = nics.first() {
            let vni = nic.vni;
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

        let ssh_keys = self
            .db_datastore
            .instance_ssh_keys_list(
                opctx,
                authz_instance,
                &PaginatedBy::Name(DataPageParams {
                    marker: None,
                    direction: dropshot::PaginationOrder::Ascending,
                    limit: std::num::NonZeroU32::new(MAX_SSH_KEYS_PER_INSTANCE)
                        .unwrap(),
                }),
            )
            .await?
            .into_iter();

        let ssh_keys: Vec<String> =
            ssh_keys.map(|ssh_key| ssh_key.public_key).collect();

        // Construct instance metadata used to track its statistics.
        //
        // This requires another fetch on the silo and project, to extract their
        // IDs.
        let (.., db_project) = self
            .project_lookup(
                opctx,
                params::ProjectSelector {
                    project: NameOrId::Id(db_instance.project_id),
                },
            )?
            .fetch()
            .await?;
        let (_, db_silo) = self.current_silo_lookup(opctx)?.fetch().await?;
        let metadata = sled_agent_client::types::InstanceMetadata {
            silo_id: db_silo.id(),
            project_id: db_project.id(),
        };

        // Ask the sled agent to begin the state change.  Then update the
        // database to reflect the new intermediate state.  If this update is
        // not the newest one, that's fine.  That might just mean the sled agent
        // beat us to it.

        let instance_hardware = sled_agent_client::types::InstanceHardware {
            properties: InstanceProperties {
                ncpus: db_instance.ncpus.into(),
                memory: db_instance.memory.into(),
                hostname,
            },
            nics,
            source_nat,
            ephemeral_ip,
            floating_ips,
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
                db_instance.generate_cidata(&ssh_keys)?,
            )),
        };

        let instance_id = InstanceUuid::from_untyped_uuid(db_instance.id());
        let sa = self
            .sled_client(&SledUuid::from_untyped_uuid(initial_vmm.sled_id))
            .await?;
        let instance_register_result = sa
            .instance_register(
                &instance_id,
                &sled_agent_client::types::InstanceEnsureBody {
                    hardware: instance_hardware,
                    instance_runtime: db_instance.runtime().clone().into(),
                    vmm_runtime: initial_vmm.clone().into(),
                    propolis_id: *propolis_id,
                    propolis_addr: SocketAddr::new(
                        initial_vmm.propolis_ip.ip(),
                        initial_vmm.propolis_port.into(),
                    )
                    .to_string(),
                    metadata,
                },
            )
            .await
            .map(|res| Some(res.into_inner().into()))
            .map_err(|e| SledAgentInstancePutError(e));

        match instance_register_result {
            Ok(state) => {
                self.write_returned_instance_state(&instance_id, state).await?;
            }
            Err(e) => {
                if e.instance_unhealthy() {
                    let _ = self
                        .mark_instance_failed(
                            &instance_id,
                            db_instance.runtime(),
                            &e,
                        )
                        .await;
                }
                return Err(e.into());
            }
        }

        Ok(())
    }

    /// Takes an updated instance state returned from a call to sled agent and
    /// writes it back to the database.
    ///
    /// # Return value
    ///
    /// - `Ok((instance_updated, vmm_updated))` if no failures occurred. The
    ///   tuple fields indicate which database records (if any) were updated.
    ///   Note that it is possible for sled agent not to return an updated
    ///   instance state from a particular API call. In that case, the `state`
    ///   parameter is `None` and this routine returns `Ok((false, false))`.
    /// - `Err` if an error occurred while writing state to the database. A
    ///   database operation that succeeds but doesn't update anything (e.g.
    ///   owing to an outdated generation number) will return `Ok`.
    async fn write_returned_instance_state(
        &self,
        instance_id: &InstanceUuid,
        state: Option<nexus::SledInstanceState>,
    ) -> Result<InstanceUpdateResult, Error> {
        slog::debug!(&self.log,
                     "writing instance state returned from sled agent";
                     "instance_id" => %instance_id,
                     "new_state" => ?state);

        if let Some(state) = state {
            let update_result = self
                .db_datastore
                .instance_and_vmm_update_runtime(
                    instance_id,
                    &state.instance_state.into(),
                    &state.propolis_id,
                    &state.vmm_state.into(),
                    &state.migration_state,
                )
                .await;

            slog::debug!(&self.log,
                         "attempted to write instance state from sled agent";
                         "instance_id" => %instance_id,
                         "propolis_id" => %state.propolis_id,
                         "result" => ?update_result);

            update_result
        } else {
            // There was no instance state to write back, so --- perhaps
            // obviously --- nothing happened.
            Ok(InstanceUpdateResult {
                instance_updated: false,
                vmm_updated: false,
                migration_updated: None,
            })
        }
    }

    /// Attempts to move an instance from `prev_instance_runtime` to the
    /// `Failed` state in response to an error returned from a call to a sled
    /// agent instance API, supplied in `reason`.
    pub(crate) async fn mark_instance_failed(
        &self,
        instance_id: &InstanceUuid,
        prev_instance_runtime: &db::model::InstanceRuntimeState,
        reason: &SledAgentInstancePutError,
    ) -> Result<(), Error> {
        error!(self.log, "marking instance failed due to sled agent API error";
               "instance_id" => %instance_id,
               "error" => ?reason);

        let new_runtime = db::model::InstanceRuntimeState {
            nexus_state: db::model::InstanceState::Failed,

            // TODO(#4226): Clearing the Propolis ID is required to allow the
            // instance to be deleted, but this doesn't actually terminate the
            // VMM.
            propolis_id: None,
            gen: prev_instance_runtime.gen.next().into(),
            ..prev_instance_runtime.clone()
        };

        match self
            .db_datastore
            .instance_update_runtime(&instance_id, &new_runtime)
            .await
        {
            Ok(_) => info!(self.log, "marked instance as Failed";
                           "instance_id" => %instance_id),
            // XXX: It's not clear what to do with this error; should it be
            // bubbled back up to the caller?
            Err(e) => error!(self.log,
                            "failed to write Failed instance state to DB";
                            "instance_id" => %instance_id,
                            "error" => ?e),
        }

        Ok(())
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
            return Err(Error::invalid_request(
                "disk must be in the same project as the instance",
            ));
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
        instance_id: &InstanceUuid,
        new_runtime_state: &nexus::SledInstanceState,
    ) -> Result<(), Error> {
        notify_instance_updated(
            &self.datastore(),
            &self.resolver().await,
            &self.opctx_alloc,
            opctx,
            &self.log,
            instance_id,
            new_runtime_state,
            self.v2p_notification_tx.clone(),
        )
        .await?;
        self.vpc_needed_notify_sleds();
        Ok(())
    }

    /// Returns the requested range of serial console output bytes,
    /// provided they are still in the propolis-server's cache.
    pub(crate) async fn instance_serial_console_data(
        &self,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
        params: &params::InstanceSerialConsoleRequest,
    ) -> Result<params::InstanceSerialConsoleData, Error> {
        let (_, client) = self
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
        let (_, client_addr) = match self
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

    /// Return a propolis address for the instance, along with the VMM identity
    /// that it's for.
    async fn propolis_addr_for_instance(
        &self,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
        action: authz::Action,
    ) -> Result<(Vmm, SocketAddr), Error> {
        let (.., authz_instance) = instance_lookup.lookup_for(action).await?;

        let state = self
            .db_datastore
            .instance_fetch_with_vmm(opctx, &authz_instance)
            .await?;

        let (instance, vmm) = (state.instance(), state.vmm());
        if let Some(vmm) = vmm {
            match vmm.runtime.state {
                DbVmmState::Running
                | DbVmmState::Rebooting
                | DbVmmState::Migrating => {
                    Ok((vmm.clone(), SocketAddr::new(vmm.propolis_ip.ip(), vmm.propolis_port.into())))
                }

                DbVmmState::Starting
                | DbVmmState::Stopping
                | DbVmmState::Stopped
                | DbVmmState::Failed => {
                    Err(Error::invalid_request(format!(
                        "cannot connect to serial console of instance in state \"{}\"",
                        vmm.runtime.state,
                    )))
                }

                DbVmmState::Destroyed | DbVmmState::SagaUnwound => Err(Error::invalid_request(
                    "cannot connect to serial console of instance in state \"Stopped\"",
                )),
            }
        } else {
            Err(Error::invalid_request(format!(
                "instance is {} and has no active serial console \
                    server",
                instance.runtime().nexus_state
            )))
        }
    }

    /// Return a propolis client for the instance, along with the VMM identity
    /// that it's for.
    pub(crate) async fn propolis_client_for_instance(
        &self,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
        action: authz::Action,
    ) -> Result<(Vmm, propolis_client::Client), Error> {
        let (vmm, client_addr) = self
            .propolis_addr_for_instance(opctx, instance_lookup, action)
            .await?;
        Ok((
            vmm,
            propolis_client::Client::new(&format!("http://{}", client_addr)),
        ))
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

    /// Attach an ephemeral IP to an instance.
    pub(crate) async fn instance_attach_ephemeral_ip(
        self: &Arc<Self>,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
        pool: Option<NameOrId>,
    ) -> UpdateResult<views::ExternalIp> {
        let (.., authz_project, authz_instance) =
            instance_lookup.lookup_for(authz::Action::Modify).await?;

        self.instance_attach_external_ip(
            opctx,
            authz_instance,
            authz_project.id(),
            ExternalIpAttach::Ephemeral { pool },
        )
        .await
    }

    /// Attach an ephemeral IP to an instance.
    pub(crate) async fn instance_attach_floating_ip(
        self: &Arc<Self>,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
        authz_fip: authz::FloatingIp,
        authz_fip_project: authz::Project,
    ) -> UpdateResult<views::ExternalIp> {
        let (.., authz_project, authz_instance) =
            instance_lookup.lookup_for(authz::Action::Modify).await?;

        if authz_fip_project.id() != authz_project.id() {
            return Err(Error::invalid_request(
                "floating IP must be in the same project as the instance",
            ));
        }

        self.instance_attach_external_ip(
            opctx,
            authz_instance,
            authz_project.id(),
            ExternalIpAttach::Floating { floating_ip: authz_fip },
        )
        .await
    }

    /// Attach an external IP to an instance.
    pub(crate) async fn instance_attach_external_ip(
        self: &Arc<Self>,
        opctx: &OpContext,
        authz_instance: authz::Instance,
        project_id: Uuid,
        ext_ip: ExternalIpAttach,
    ) -> UpdateResult<views::ExternalIp> {
        let saga_params = sagas::instance_ip_attach::Params {
            create_params: ext_ip.clone(),
            authz_instance,
            project_id,
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
        };

        let saga_outputs = self
            .sagas
            .saga_execute::<sagas::instance_ip_attach::SagaInstanceIpAttach>(
                saga_params,
            )
            .await?;

        saga_outputs
            .lookup_node_output::<views::ExternalIp>("output")
            .map_err(|e| Error::internal_error(&format!("{:#}", &e)))
            .internal_context("looking up output from ip attach saga")
    }

    /// Detach an external IP from an instance.
    pub(crate) async fn instance_detach_external_ip(
        self: &Arc<Self>,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
        ext_ip: &params::ExternalIpDetach,
    ) -> UpdateResult<views::ExternalIp> {
        let (.., authz_project, authz_instance) =
            instance_lookup.lookup_for(authz::Action::Modify).await?;

        let saga_params = sagas::instance_ip_detach::Params {
            delete_params: ext_ip.clone(),
            authz_instance,
            project_id: authz_project.id(),
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
        };

        let saga_outputs = self
            .sagas
            .saga_execute::<sagas::instance_ip_detach::SagaInstanceIpDetach>(
                saga_params,
            )
            .await?;

        saga_outputs
            .lookup_node_output::<Option<views::ExternalIp>>("output")
            .map_err(|e| Error::internal_error(&format!("{:#}", &e)))
            .internal_context("looking up output from ip detach saga")
            .and_then(|eip| {
                // Saga idempotency means we'll get Ok(None) on double detach
                // of an ephemeral IP. Convert this case to an error here.
                eip.ok_or_else(|| {
                    Error::invalid_request(
                        "instance does not have an ephemeral IP attached",
                    )
                })
            })
    }
}

/// Invoked by a sled agent to publish an updated runtime state for an
/// Instance.
#[allow(clippy::too_many_arguments)] // :(
pub(crate) async fn notify_instance_updated(
    datastore: &DataStore,
    resolver: &internal_dns::resolver::Resolver,
    opctx_alloc: &OpContext,
    opctx: &OpContext,
    log: &slog::Logger,
    instance_id: &InstanceUuid,
    new_runtime_state: &nexus::SledInstanceState,
    v2p_notification_tx: tokio::sync::watch::Sender<()>,
) -> Result<Option<InstanceUpdateResult>, Error> {
    let propolis_id = new_runtime_state.propolis_id;

    info!(log, "received new runtime state from sled agent";
            "instance_id" => %instance_id,
            "instance_state" => ?new_runtime_state.instance_state,
            "propolis_id" => %propolis_id,
            "vmm_state" => ?new_runtime_state.vmm_state,
            "migration_state" => ?new_runtime_state.migration_state);

    // Grab the current state of the instance in the DB to reason about
    // whether this update is stale or not.
    let (.., authz_instance, db_instance) = LookupPath::new(&opctx, &datastore)
        .instance_id(instance_id.into_untyped_uuid())
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
    super::instance_network::ensure_updated_instance_network_config(
        datastore,
        log,
        resolver,
        opctx,
        opctx_alloc,
        &authz_instance,
        db_instance.runtime(),
        &new_runtime_state.instance_state,
        v2p_notification_tx.clone(),
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
        datastore
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
        super::oximeter::unassign_producer(
            datastore,
            log,
            opctx,
            &instance_id.into_untyped_uuid(),
        )
        .await?;
    }

    // Write the new instance and VMM states back to CRDB. This needs to be
    // done before trying to clean up the VMM, since the datastore will only
    // allow a VMM to be marked as deleted if it is already in a terminal
    // state.
    let result = datastore
        .instance_and_vmm_update_runtime(
            instance_id,
            &db::model::InstanceRuntimeState::from(
                new_runtime_state.instance_state.clone(),
            ),
            &propolis_id,
            &db::model::VmmRuntimeState::from(
                new_runtime_state.vmm_state.clone(),
            ),
            &new_runtime_state.migration_state,
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
            VmmState::Destroyed | VmmState::Failed
        );

        if propolis_terminated {
            info!(log, "vmm is terminated, cleaning up resources";
                    "instance_id" => %instance_id,
                    "propolis_id" => %propolis_id);

            datastore
                .sled_reservation_delete(opctx, propolis_id.into_untyped_uuid())
                .await?;

            if !datastore.vmm_mark_deleted(opctx, &propolis_id).await? {
                warn!(log, "failed to mark vmm record as deleted";
                    "instance_id" => %instance_id,
                    "propolis_id" => %propolis_id,
                    "vmm_state" => ?new_runtime_state.vmm_state);
            }
        }
    }

    match result {
        Ok(result) => {
            info!(log, "instance and vmm updated by sled agent";
                    "instance_id" => %instance_id,
                    "propolis_id" => %propolis_id,
                    "instance_updated" => result.instance_updated,
                    "vmm_updated" => result.vmm_updated,
                    "migration_updated" => ?result.migration_updated);
            Ok(Some(result))
        }

        // The update command should swallow object-not-found errors and
        // return them back as failures to update, so this error case is
        // unexpected. There's no work to do if this occurs, however.
        Err(Error::ObjectNotFound { .. }) => {
            error!(log, "instance/vmm update unexpectedly returned \
                    an object not found error";
                    "instance_id" => %instance_id,
                    "propolis_id" => %propolis_id);
            Ok(None)
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
