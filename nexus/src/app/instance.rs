// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Virtual Machine Instances

use super::MAX_DISKS_PER_INSTANCE;
use super::MAX_EXTERNAL_IPS_PER_INSTANCE;
use super::MAX_NICS_PER_INSTANCE;
use crate::app::sagas;
use crate::app::sagas::retry_until_known_result;
use crate::authn;
use crate::authz;
use crate::cidata::InstanceCiData;
use crate::db;
use crate::db::identity::Resource;
use crate::db::lookup;
use crate::db::lookup::LookupPath;
use crate::external_api::params;
use cancel_safe_futures::prelude::*;
use futures::future::Fuse;
use futures::{FutureExt, SinkExt, StreamExt};
use nexus_db_model::IpKind;
use nexus_db_queries::authz::ApiResource;
use nexus_db_queries::context::OpContext;
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
use sled_agent_client::types::InstancePutMigrationIdsBody;
use sled_agent_client::types::InstancePutStateBody;
use sled_agent_client::types::InstanceStateRequested;
use sled_agent_client::types::SourceNatConfig;
use sled_agent_client::Client as SledAgentClient;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use uuid::Uuid;

const MAX_KEYS_PER_INSTANCE: u32 = 8;

pub(crate) enum WriteBackUpdatedInstance {
    WriteBack,
    Drop,
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

    pub async fn project_create_instance(
        self: &Arc<Self>,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        params: &params::InstanceCreate,
    ) -> CreateResult<db::model::Instance> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::CreateChild).await?;

        // Validate parameters
        if params.disks.len() > MAX_DISKS_PER_INSTANCE as usize {
            return Err(Error::invalid_request(&format!(
                "cannot attach more than {} disks to instance!",
                MAX_DISKS_PER_INSTANCE
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
        // MIN_MEMORY_SIZE_BYTES
        if params.memory.to_bytes() < params::MIN_MEMORY_SIZE_BYTES as u64 {
            return Err(Error::InvalidValue {
                label: String::from("size"),
                message: format!(
                    "memory must be at least {}",
                    ByteCount::from(params::MIN_MEMORY_SIZE_BYTES)
                ),
            });
        }

        // Reject instances where the memory is not divisible by
        // MIN_MEMORY_SIZE_BYTES
        if (params.memory.to_bytes() % params::MIN_MEMORY_SIZE_BYTES as u64)
            != 0
        {
            return Err(Error::InvalidValue {
                label: String::from("size"),
                message: format!(
                    "memory must be divisible by {}",
                    ByteCount::from(params::MIN_MEMORY_SIZE_BYTES)
                ),
            });
        }

        let saga_params = sagas::instance_create::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            project_id: authz_project.id(),
            create_params: params.clone(),
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

        // TODO-correctness TODO-robustness TODO-design It's not quite correct
        // to take this instance id and look it up again.  It's possible that
        // it's been modified or even deleted since the saga executed.  In that
        // case, we might return a different state of the Instance than the one
        // that the user created or even fail with a 404!  Both of those are
        // wrong behavior -- we should be returning the very instance that the
        // user created.
        //
        // How can we fix this?  Right now we have internal representations like
        // Instance and analaogous end-user-facing representations like
        // Instance.  The former is not even serializable.  The saga
        // _could_ emit the View version, but that's not great for two (related)
        // reasons: (1) other sagas might want to provision instances and get
        // back the internal representation to do other things with the
        // newly-created instance, and (2) even within a saga, it would be
        // useful to pass a single Instance representation along the saga,
        // but they probably would want the internal representation, not the
        // view.
        //
        // The saga could emit an Instance directly.  Today, Instance
        // etc. aren't supposed to even be serializable -- we wanted to be able
        // to have other datastore state there if needed.  We could have a third
        // InstanceInternalView...but that's starting to feel pedantic.  We
        // could just make Instance serializable, store that, and call it a
        // day.  Does it matter that we might have many copies of the same
        // objects in memory?
        //
        // If we make these serializable, it would be nice if we could leverage
        // the type system to ensure that we never accidentally send them out a
        // dropshot endpoint.  (On the other hand, maybe we _do_ want to do
        // that, for internal interfaces!  Can we do this on a
        // per-dropshot-server-basis?)
        //
        // TODO Even worse, post-authz, we do two lookups here instead of one.
        // Maybe sagas should be able to emit `authz::Instance`-type objects.
        let (.., db_instance) = LookupPath::new(opctx, &self.db_datastore)
            .instance_id(instance_id)
            .fetch()
            .await?;
        Ok(db_instance)
    }

    pub async fn instance_list(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::Instance> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::ListChildren).await?;
        self.db_datastore.instance_list(opctx, &authz_project, pagparams).await
    }

    // This operation may only occur on stopped instances, which implies that
    // the attached disks do not have any running "upstairs" process running
    // within the sled.
    pub async fn project_destroy_instance(
        self: &Arc<Self>,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
    ) -> DeleteResult {
        // TODO-robustness We need to figure out what to do with Destroyed
        // instances?  Presumably we need to clean them up at some point, but
        // not right away so that callers can see that they've been destroyed.
        let (.., authz_instance, instance) =
            instance_lookup.fetch_for(authz::Action::Delete).await?;

        let saga_params = sagas::instance_delete::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            authz_instance,
            instance,
        };
        self.execute_saga::<sagas::instance_delete::SagaInstanceDelete>(
            saga_params,
        )
        .await?;
        Ok(())
    }

    pub async fn project_instance_migrate(
        self: &Arc<Self>,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
        params: params::InstanceMigrate,
    ) -> UpdateResult<db::model::Instance> {
        let (.., authz_instance, db_instance) =
            instance_lookup.fetch_for(authz::Action::Modify).await?;

        if db_instance.runtime().state.0 != InstanceState::Running {
            return Err(Error::invalid_request(
                "instance must be running before it can migrate",
            ));
        }

        if db_instance.runtime().sled_id == params.dst_sled_id {
            return Err(Error::invalid_request(
                "instance is already running on destination sled",
            ));
        }

        if db_instance.runtime().migration_id.is_some() {
            return Err(Error::unavail("instance is already migrating"));
        }

        // Kick off the migration saga
        let saga_params = sagas::instance_migrate::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            instance: db_instance,
            migrate_params: params,
        };
        self.execute_saga::<sagas::instance_migrate::SagaInstanceMigrate>(
            saga_params,
        )
        .await?;

        // TODO correctness TODO robustness TODO design
        // Should we lookup the instance again here?
        // See comment in project_create_instance.
        self.db_datastore.instance_refetch(opctx, &authz_instance).await
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
    pub async fn instance_set_migration_ids(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
        db_instance: &db::model::Instance,
        migration_params: InstanceMigrationSourceParams,
    ) -> UpdateResult<db::model::Instance> {
        assert!(db_instance.runtime().migration_id.is_none());
        assert!(db_instance.runtime().dst_propolis_id.is_none());

        let (.., authz_instance) = LookupPath::new(opctx, &self.db_datastore)
            .instance_id(instance_id)
            .lookup_for(authz::Action::Modify)
            .await?;

        let sa = self.instance_sled(&db_instance).await?;
        let instance_put_result = sa
            .instance_put_migration_ids(
                &instance_id,
                &InstancePutMigrationIdsBody {
                    old_runtime: db_instance.runtime().clone().into(),
                    migration_params: Some(migration_params),
                },
            )
            .await
            .map(|res| Some(res.into_inner()));

        // Write the updated instance runtime state back to CRDB. If this
        // outright fails, this operation fails. If the operation nominally
        // succeeds but nothing was updated, this action is outdated and the
        // caller should not proceed with migration.
        let updated = self
            .handle_instance_put_result(&db_instance, instance_put_result)
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
    pub async fn instance_clear_migration_ids(
        &self,
        instance_id: Uuid,
        db_instance: &db::model::Instance,
    ) -> Result<(), Error> {
        assert!(db_instance.runtime().migration_id.is_some());
        assert!(db_instance.runtime().dst_propolis_id.is_some());

        let sa = self.instance_sled(&db_instance).await?;
        let instance_put_result = sa
            .instance_put_migration_ids(
                &instance_id,
                &InstancePutMigrationIdsBody {
                    old_runtime: db_instance.runtime().clone().into(),
                    migration_params: None,
                },
            )
            .await
            .map(|res| Some(res.into_inner()));

        self.handle_instance_put_result(&db_instance, instance_put_result)
            .await?;

        Ok(())
    }

    /// Reboot the specified instance.
    pub async fn instance_reboot(
        &self,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
    ) -> UpdateResult<db::model::Instance> {
        let (.., authz_instance, db_instance) = instance_lookup.fetch().await?;
        self.instance_request_state(
            opctx,
            &authz_instance,
            &db_instance,
            InstanceStateRequested::Reboot,
        )
        .await?;
        self.db_datastore.instance_refetch(opctx, &authz_instance).await
    }

    /// Make sure the given Instance is running.
    pub async fn instance_start(
        &self,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
    ) -> UpdateResult<db::model::Instance> {
        // TODO(#2824): This needs to be a saga for crash resiliency
        // purposes (otherwise the instance can be leaked if Nexus crashes
        // between registration and instance start).
        let (.., authz_instance, mut db_instance) =
            instance_lookup.fetch().await?;

        // The instance is not really being "created" (it already exists from
        // the caller's perspective), but if it does not exist on its sled, the
        // target sled agent will populate its instance manager with the
        // contents of this modified record, and that record needs to allow a
        // transition to the Starting state.
        //
        // If the instance does exist on this sled, this initial runtime state
        // is ignored.
        let initial_runtime = nexus_db_model::InstanceRuntimeState {
            state: nexus_db_model::InstanceState(InstanceState::Creating),
            ..db_instance.runtime_state
        };
        db_instance.runtime_state = initial_runtime;
        self.instance_ensure_registered(opctx, &authz_instance, &db_instance)
            .await?;

        self.instance_request_state(
            opctx,
            &authz_instance,
            &db_instance,
            InstanceStateRequested::Running,
        )
        .await?;
        self.db_datastore.instance_refetch(opctx, &authz_instance).await
    }

    /// Make sure the given Instance is stopped.
    pub async fn instance_stop(
        &self,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
    ) -> UpdateResult<db::model::Instance> {
        let (.., authz_instance, db_instance) = instance_lookup.fetch().await?;
        self.instance_request_state(
            opctx,
            &authz_instance,
            &db_instance,
            InstanceStateRequested::Stopped,
        )
        .await?;
        self.db_datastore.instance_refetch(opctx, &authz_instance).await
    }

    /// Idempotently ensures that the sled specified in `db_instance` does not
    /// have a record of the instance. If the instance is currently running on
    /// this sled, this operation rudely terminates it.
    pub(crate) async fn instance_ensure_unregistered(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        db_instance: &db::model::Instance,
        write_back: WriteBackUpdatedInstance,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, authz_instance).await?;
        let sa = self.instance_sled(&db_instance).await?;
        let result = sa
            .instance_unregister(&db_instance.id())
            .await
            .map(|res| res.into_inner().updated_runtime);

        match write_back {
            WriteBackUpdatedInstance::WriteBack => self
                .handle_instance_put_result(db_instance, result)
                .await
                .map(|_| ()),
            WriteBackUpdatedInstance::Drop => {
                result?;
                Ok(())
            }
        }
    }

    /// Returns the SledAgentClient for the host where this Instance is running.
    pub(crate) async fn instance_sled(
        &self,
        instance: &db::model::Instance,
    ) -> Result<Arc<SledAgentClient>, Error> {
        let sa_id = &instance.runtime().sled_id;
        self.sled_client(&sa_id).await
    }

    fn check_runtime_change_allowed(
        &self,
        runtime: &nexus::InstanceRuntimeState,
        requested: &InstanceStateRequested,
    ) -> Result<(), Error> {
        // Users are allowed to request a start or stop even if the instance is
        // already in the desired state (or moving to it), and we will issue a
        // request to the SA to make the state change in these cases in case the
        // runtime state we saw here was stale.
        //
        // Users cannot change the state of a failed or destroyed instance.
        // TODO(#2825): Failed instances should be allowed to stop.
        //
        // Migrating instances can't change state until they're done migrating,
        // but for idempotency, a request to make an incarnation of an instance
        // into a migration target is allowed if the incarnation is already a
        // migration target.
        let allowed = match runtime.run_state {
            InstanceState::Creating => true,
            InstanceState::Starting => true,
            InstanceState::Running => true,
            InstanceState::Stopping => true,
            InstanceState::Stopped => true,
            InstanceState::Rebooting => true,
            InstanceState::Migrating => {
                matches!(requested, InstanceStateRequested::MigrationTarget(_))
            }
            InstanceState::Repairing => false,
            InstanceState::Failed => false,
            InstanceState::Destroyed => false,
        };

        if allowed {
            Ok(())
        } else {
            Err(Error::InvalidRequest {
                message: format!(
                    "instance state cannot be changed from state \"{}\"",
                    runtime.run_state
                ),
            })
        }
    }

    pub(crate) async fn instance_request_state(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        db_instance: &db::model::Instance,
        requested: InstanceStateRequested,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, authz_instance).await?;
        self.check_runtime_change_allowed(
            &db_instance.runtime().clone().into(),
            &requested,
        )?;

        let sa = self.instance_sled(&db_instance).await?;
        let instance_put_result = sa
            .instance_put_state(
                &db_instance.id(),
                &InstancePutStateBody { state: requested },
            )
            .await
            .map(|res| res.into_inner().updated_runtime);

        self.handle_instance_put_result(db_instance, instance_put_result)
            .await
            .map(|_| ())
    }

    /// Modifies the runtime state of the Instance as requested.  This generally
    /// means booting or halting the Instance.
    pub(crate) async fn instance_ensure_registered(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        db_instance: &db::model::Instance,
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
                       "instance" => ?disk.runtime_state.attach_instance_id);

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
                slot: sled_agent_client::types::Slot(slot as u8),
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
            runtime: sled_agent_client::types::InstanceRuntimeState::from(
                db_instance.runtime().clone(),
            ),
            nics,
            source_nat,
            external_ips,
            firewall_rules,
            disks: disk_reqs,
            cloud_init_bytes: Some(base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                db_instance.generate_cidata(&public_keys)?,
            )),
        };

        let sa = self.instance_sled(&db_instance).await?;

        let instance_register_result = sa
            .instance_register(
                &db_instance.id(),
                &sled_agent_client::types::InstanceEnsureBody {
                    initial: instance_hardware,
                },
            )
            .await
            .map(|res| Some(res.into_inner()));

        self.handle_instance_put_result(db_instance, instance_register_result)
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
        db_instance: &db::model::Instance,
        result: Result<
            Option<sled_agent_client::types::InstanceRuntimeState>,
            sled_agent_client::Error<sled_agent_client::types::Error>,
        >,
    ) -> Result<bool, Error> {
        slog::debug!(&self.log, "Handling sled agent instance PUT result";
                     "result" => ?result);

        match result {
            Ok(Some(new_runtime)) => {
                let new_runtime: nexus::InstanceRuntimeState =
                    new_runtime.into();

                let update_result = self
                    .db_datastore
                    .instance_update_runtime(
                        &db_instance.id(),
                        &new_runtime.into(),
                    )
                    .await;

                slog::debug!(&self.log,
                             "Attempted DB update after instance PUT";
                             "result" => ?update_result);
                update_result
            }
            Ok(None) => Ok(false),
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
                error!(self.log, "saw {} from instance_put!", e);

                // Convert to the Omicron API error type.
                //
                // N.B. The match below assumes that this conversion will turn
                //      any 400-level error status from sled agent into an
                //      `Error::InvalidRequest`.
                let e = e.into();

                match &e {
                    // Bad request shouldn't change the instance state.
                    Error::InvalidRequest { .. } => Err(e),

                    // Internal server error (or anything else) should change
                    // the instance state to failed, we don't know what state
                    // the instance is in.
                    _ => {
                        let new_runtime = db::model::InstanceRuntimeState {
                            state: db::model::InstanceState::new(
                                InstanceState::Failed,
                            ),
                            gen: db_instance.runtime_state.gen.next().into(),
                            ..db_instance.runtime_state.clone()
                        };

                        // XXX what if this fails?
                        let result = self
                            .db_datastore
                            .instance_update_runtime(
                                &db_instance.id(),
                                &new_runtime,
                            )
                            .await;

                        error!(
                            self.log,
                            "saw {:?} from setting InstanceState::Failed after bad instance_put",
                            result,
                        );

                        Err(e)
                    }
                }
            }
        }
    }

    /// Lists disks attached to the instance.
    pub async fn instance_list_disks(
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
    pub async fn instance_attach_disk(
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
        // Because both instance and disk can be provided by ID it's possible for someone
        // to specify resources from different projects. The lookups would resolve the resources
        // (assuming the user had sufficient permissions on both) without verifying the shared hierarchy.
        // To mitigate that we verify that their parent projects have the same ID.
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
    pub async fn instance_detach_disk(
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
    pub async fn notify_instance_updated(
        &self,
        opctx: &OpContext,
        id: &Uuid,
        new_runtime_state: &nexus::InstanceRuntimeState,
    ) -> Result<(), Error> {
        let log = &self.log;

        slog::debug!(log, "received new runtime state from sled agent";
                     "instance_id" => %id,
                     "runtime_state" => ?new_runtime_state);

        // If the new state has a newer Propolis ID generation than the current
        // instance state in CRDB, notify interested parties of this change.
        //
        // The synchronization rules here are as follows:
        //
        // - Sled agents own an instance's runtime state while an instance is
        //   running on a sled. Each sled agent prevents concurrent conflicting
        //   Propolis identifier updates from being sent until previous updates
        //   are processed.
        // - Operations that can dispatch an instance to a brand-new sled (e.g.
        //   live migration) can only start if the appropriate instance runtime
        //   state fields are cleared in CRDB. For example, while a live
        //   migration is in progress, the instance's `migration_id` field will
        //   be non-NULL, and a new migration cannot start until it is cleared.
        //   This routine must notify recipients before writing new records
        //   back to CRDB so that these "locks" remain held until all
        //   notifications have been sent. Otherwise, Nexus might allow new
        //   operations to proceed that will produce system updates that might
        //   race with this one.
        // - This work is not done in a saga. The presumption is instead that
        //   if any of these operations fail, the entire update will fail, and
        //   sled agent will retry the update. Unwinding on failure isn't needed
        //   because (a) any partially-applied configuration is correct
        //   configuration, (b) if the instance is migrating, it can't migrate
        //   again until this routine successfully updates configuration and
        //   writes an update back to CRDB, and (c) sled agent won't process any
        //   new instance state changes (e.g. a change that stops an instance)
        //   until this state change is successfully committed.
        let (.., db_instance) = LookupPath::new(&opctx, &self.db_datastore)
            .instance_id(*id)
            .fetch_for(authz::Action::Read)
            .await?;

        if new_runtime_state.propolis_gen > *db_instance.runtime().propolis_gen
        {
            self.handle_instance_propolis_gen_change(
                opctx,
                new_runtime_state,
                &db_instance,
            )
            .await?;
        }

        let result = self
            .db_datastore
            .instance_update_runtime(id, &(new_runtime_state.clone().into()))
            .await;

        match result {
            Ok(true) => {
                info!(log, "instance updated by sled agent";
                    "instance_id" => %id,
                    "propolis_id" => %new_runtime_state.propolis_id,
                    "new_state" => %new_runtime_state.run_state);
                Ok(())
            }

            Ok(false) => {
                info!(log, "instance update from sled agent ignored (old)";
                    "instance_id" => %id,
                    "propolis_id" => %new_runtime_state.propolis_id,
                    "requested_state" => %new_runtime_state.run_state);
                Ok(())
            }

            // If the instance doesn't exist, swallow the error -- there's
            // nothing to do here.
            // TODO-robustness This could only be possible if we've removed an
            // Instance from the datastore altogether.  When would we do that?
            // We don't want to do it as soon as something's destroyed, I think,
            // and in that case, we'd need some async task for cleaning these
            // up.
            Err(Error::ObjectNotFound { .. }) => {
                warn!(log, "non-existent instance updated by sled agent";
                    "instance_id" => %id,
                    "new_state" => %new_runtime_state.run_state);
                Ok(())
            }

            // If the datastore is unavailable, propagate that to the caller.
            // TODO-robustness Really this should be any _transient_ error.  How
            // can we distinguish?  Maybe datastore should emit something
            // different from Error with an Into<Error>.
            Err(error) => {
                warn!(log, "failed to update instance from sled agent";
                    "instance_id" => %id,
                    "new_state" => %new_runtime_state.run_state,
                    "error" => ?error);
                Err(error)
            }
        }
    }

    async fn handle_instance_propolis_gen_change(
        &self,
        opctx: &OpContext,
        new_runtime: &nexus::InstanceRuntimeState,
        db_instance: &nexus_db_model::Instance,
    ) -> Result<(), Error> {
        let log = &self.log;
        let instance_id = db_instance.id();

        info!(log,
              "updating configuration after Propolis generation change";
              "instance_id" => %instance_id,
              "new_sled_id" => %new_runtime.sled_id,
              "old_sled_id" => %db_instance.runtime().sled_id);

        // Push updated V2P mappings to all interested sleds. This needs to be
        // done irrespective of whether the sled ID actually changed, because
        // merely creating the target Propolis on the target sled will create
        // XDE devices for its NICs, and creating an XDE device for a virtual IP
        // creates a V2P mapping that maps that IP to that sled. This is fine if
        // migration succeeded, but if it failed, the instance is running on the
        // source sled, and the incorrect mapping needs to be replaced.
        //
        // TODO(#3107): When XDE no longer creates mappings implicitly, this
        // can be restricted to cases where an instance's sled has actually
        // changed.
        self.create_instance_v2p_mappings(
            opctx,
            instance_id,
            new_runtime.sled_id,
        )
        .await?;

        let (.., sled) = LookupPath::new(opctx, &self.db_datastore)
            .sled_id(new_runtime.sled_id)
            .fetch()
            .await?;

        self.instance_ensure_dpd_config(
            opctx,
            db_instance.id(),
            &sled.address(),
            None,
        )
        .await?;

        Ok(())
    }

    /// Ensures that the Dendrite configuration for the supplied instance is
    /// up-to-date.
    ///
    /// # Parameters
    ///
    /// - `opctx`: An operation context that grants read and list-children
    ///   permissions on the identified instance.
    /// - `instance_id`: The ID of the instance to act on.
    /// - `sled_ip_address`: The internal IP address assigned to the sled's
    ///   sled agent.
    /// - `ip_index_filter`: An optional filter on the index into the instance's
    ///   external IP array.
    ///   - If this is `Some(n)`, this routine configures DPD state for only the
    ///     Nth external IP in the collection returned from CRDB. The caller is
    ///     responsible for ensuring that the IP collection has stable indices
    ///     when making this call.
    ///   - If this is `None`, this routine configures DPD for all external
    ///     IPs.
    pub(crate) async fn instance_ensure_dpd_config(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
        sled_ip_address: &std::net::SocketAddrV6,
        ip_index_filter: Option<usize>,
    ) -> Result<(), Error> {
        let log = &self.log;
        let dpd_client = &self.dpd_client;

        info!(log, "looking up instance's primary network interface";
              "instance_id" => %instance_id);

        let (.., authz_instance) = LookupPath::new(opctx, &self.db_datastore)
            .instance_id(instance_id)
            .lookup_for(authz::Action::ListChildren)
            .await?;

        // All external IPs map to the primary network interface, so find that
        // interface. If there is no such interface, there's no way to route
        // traffic destined to those IPs, so there's nothing to configure and
        // it's safe to return early.
        let network_interface = match self
            .db_datastore
            .derive_guest_network_interface_info(&opctx, &authz_instance)
            .await?
            .into_iter()
            .find(|interface| interface.primary)
        {
            Some(interface) => interface,
            None => {
                info!(log, "Instance has no primary network interface";
                      "instance_id" => %instance_id);
                return Ok(());
            }
        };

        let mac_address =
            macaddr::MacAddr6::from_str(&network_interface.mac.to_string())
                .map_err(|e| {
                    Error::internal_error(&format!(
                        "failed to convert mac address: {e}"
                    ))
                })?;

        let vni: u32 = network_interface.vni.into();

        info!(log, "looking up instance's external IPs";
              "instance_id" => %instance_id);

        let ips = self
            .db_datastore
            .instance_lookup_external_ips(&opctx, instance_id)
            .await?;

        if let Some(wanted_index) = ip_index_filter {
            if let None = ips.get(wanted_index) {
                return Err(Error::internal_error(&format!(
                    "failed to find external ip address at index: {}",
                    wanted_index
                )));
            }
        }

        for target_ip in ips
            .iter()
            .enumerate()
            .filter(|(index, _)| {
                if let Some(wanted_index) = ip_index_filter {
                    *index == wanted_index
                } else {
                    true
                }
            })
            .map(|(_, ip)| ip)
        {
            retry_until_known_result(log, || async {
                dpd_client
                    .ensure_nat_entry(
                        &log,
                        target_ip.ip,
                        dpd_client::types::MacAddr {
                            a: mac_address.into_array(),
                        },
                        *target_ip.first_port,
                        *target_ip.last_port,
                        vni,
                        sled_ip_address.ip(),
                    )
                    .await
            })
            .await
            .map_err(|e| {
                Error::internal_error(&format!(
                    "failed to ensure dpd entry: {e}"
                ))
            })?;
        }

        Ok(())
    }

    /// Returns the requested range of serial console output bytes,
    /// provided they are still in the propolis-server's cache.
    pub(crate) async fn instance_serial_console_data(
        &self,
        instance_lookup: &lookup::Instance<'_>,
        params: &params::InstanceSerialConsoleRequest,
    ) -> Result<params::InstanceSerialConsoleData, Error> {
        let client = self
            .propolis_client_for_instance(instance_lookup, authz::Action::Read)
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
            .map_err(|_| {
                Error::internal_error(
                    "websocket connection to instance's serial port failed",
                )
            })?
            .into_inner();
        Ok(params::InstanceSerialConsoleData {
            data: data.data,
            last_byte_offset: data.last_byte_offset,
        })
    }

    pub(crate) async fn instance_serial_console_stream(
        &self,
        mut client_stream: WebSocketStream<impl AsyncRead + AsyncWrite + Unpin>,
        instance_lookup: &lookup::Instance<'_>,
        params: &params::InstanceSerialConsoleStreamRequest,
    ) -> Result<(), Error> {
        let client_addr = match self
            .propolis_addr_for_instance(instance_lookup, authz::Action::Modify)
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
        instance_lookup: &lookup::Instance<'_>,
        action: authz::Action,
    ) -> Result<SocketAddr, Error> {
        let (.., authz_instance, instance) =
            instance_lookup.fetch_for(action).await?;
        match instance.runtime_state.state.0 {
            InstanceState::Running
            | InstanceState::Rebooting
            | InstanceState::Migrating
            | InstanceState::Repairing => {
                let ip_addr = instance
                    .runtime_state
                    .propolis_ip
                    .ok_or_else(|| {
                        Error::internal_error(
                            "instance's hypervisor IP address not found",
                        )
                    })?
                    .ip();
                Ok(SocketAddr::new(ip_addr, PROPOLIS_PORT))
            }
            InstanceState::Creating
            | InstanceState::Starting
            | InstanceState::Stopping
            | InstanceState::Stopped
            | InstanceState::Failed => Err(Error::ServiceUnavailable {
                internal_message: format!(
                    "Cannot connect to hypervisor of instance in state {:?}",
                    instance.runtime_state.state
                ),
            }),
            InstanceState::Destroyed => Err(authz_instance.not_found()),
        }
    }

    async fn propolis_client_for_instance(
        &self,
        instance_lookup: &lookup::Instance<'_>,
        action: authz::Action,
    ) -> Result<propolis_client::Client, Error> {
        let client_addr =
            self.propolis_addr_for_instance(instance_lookup, action).await?;
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
