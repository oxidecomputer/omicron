// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Virtual Machine Instances

use super::MAX_DISKS_PER_INSTANCE;
use super::MAX_EXTERNAL_IPS_PER_INSTANCE;
use super::MAX_NICS_PER_INSTANCE;
use crate::app::sagas;
use crate::authn;
use crate::authz;
use crate::cidata::InstanceCiData;
use crate::context::OpContext;
use crate::db;
use crate::db::identity::Resource;
use crate::db::lookup;
use crate::db::lookup::LookupPath;
use crate::external_api::params;
use futures::future::Fuse;
use futures::{FutureExt, SinkExt, StreamExt};
use nexus_db_model::IpKind;
use nexus_db_model::Name;
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
use ref_cast::RefCast;
use sled_agent_client::types::InstanceRuntimeStateMigrateParams;
use sled_agent_client::types::InstanceRuntimeStateRequested;
use sled_agent_client::types::InstanceStateRequested;
use sled_agent_client::types::SourceNatConfig;
use sled_agent_client::Client as SledAgentClient;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::Role as WebSocketRole;
use tokio_tungstenite::tungstenite::Message as WebSocketMessage;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

const MAX_KEYS_PER_INSTANCE: u32 = 8;

impl super::Nexus {
    pub fn instance_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        instance_selector: &'a params::InstanceSelector,
    ) -> LookupResult<lookup::Instance<'a>> {
        match instance_selector {
            params::InstanceSelector {
                project_selector: None,
                instance: NameOrId::Id(id),
            } => {
                let instance =
                    LookupPath::new(opctx, &self.db_datastore).instance_id(*id);
                Ok(instance)
            }
            params::InstanceSelector {
                project_selector: Some(project_selector),
                instance: NameOrId::Name(name),
            } => {
                let instance = self
                    .project_lookup(opctx, project_selector)?
                    .instance_name(Name::ref_cast(name));
                Ok(instance)
            }
            params::InstanceSelector {
                project_selector: Some(_),
                instance: NameOrId::Id(_),
            } => {
                Err(Error::invalid_request(
                    "when providing instance as an ID, project should not be specified",
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
        let (.., authz_instance) =
            instance_lookup.lookup_for(authz::Action::Modify).await?;

        // Kick off the migration saga
        let saga_params = sagas::instance_migrate::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            instance_id: authz_instance.id(),
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

    /// Idempotently place the instance in a 'Migrating' state.
    pub async fn instance_start_migrate(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
        migration_id: Uuid,
        dst_propolis_id: Uuid,
    ) -> UpdateResult<db::model::Instance> {
        let (.., authz_instance, db_instance) =
            LookupPath::new(opctx, &self.db_datastore)
                .instance_id(instance_id)
                .fetch()
                .await
                .unwrap();
        let requested = InstanceRuntimeStateRequested {
            run_state: InstanceStateRequested::Migrating,
            migration_params: Some(InstanceRuntimeStateMigrateParams {
                migration_id,
                dst_propolis_id,
            }),
        };
        self.instance_set_runtime(
            opctx,
            &authz_instance,
            &db_instance,
            requested,
        )
        .await?;
        self.db_datastore.instance_refetch(opctx, &authz_instance).await
    }

    /// Reboot the specified instance.
    pub async fn instance_reboot(
        &self,
        opctx: &OpContext,
        instance_lookup: &lookup::Instance<'_>,
    ) -> UpdateResult<db::model::Instance> {
        // To implement reboot, we issue a call to the sled agent to set a
        // runtime state of "reboot". We cannot simply stop the Instance and
        // start it again here because if we crash in the meantime, we might
        // leave it stopped.
        //
        // When an instance is rebooted, the "rebooting" flag remains set on
        // the runtime state as it transitions to "Stopping" and "Stopped".
        // This flag is cleared when the state goes to "Starting".  This way,
        // even if the whole rack powered off while this was going on, we would
        // never lose track of the fact that this Instance was supposed to be
        // running.
        let (.., authz_instance, db_instance) = instance_lookup.fetch().await?;
        let requested = InstanceRuntimeStateRequested {
            run_state: InstanceStateRequested::Reboot,
            migration_params: None,
        };
        self.instance_set_runtime(
            opctx,
            &authz_instance,
            &db_instance,
            requested,
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
        let (.., authz_instance, db_instance) = instance_lookup.fetch().await?;
        let requested = InstanceRuntimeStateRequested {
            run_state: InstanceStateRequested::Running,
            migration_params: None,
        };
        self.instance_set_runtime(
            opctx,
            &authz_instance,
            &db_instance,
            requested,
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
        let requested = InstanceRuntimeStateRequested {
            run_state: InstanceStateRequested::Stopped,
            migration_params: None,
        };
        self.instance_set_runtime(
            opctx,
            &authz_instance,
            &db_instance,
            requested,
        )
        .await?;
        self.db_datastore.instance_refetch(opctx, &authz_instance).await
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
        requested: &InstanceRuntimeStateRequested,
    ) -> Result<(), Error> {
        // Users are allowed to request a start or stop even if the instance is
        // already in the desired state (or moving to it), and we will issue a
        // request to the SA to make the state change in these cases in case the
        // runtime state we saw here was stale.  However, users are not allowed
        // to change the state of an instance that's migrating, failed or
        // destroyed.  But if we're already migrating, requesting a migration is
        // allowed to allow for idempotency.
        let allowed = match runtime.run_state {
            InstanceState::Creating => true,
            InstanceState::Starting => true,
            InstanceState::Running => true,
            InstanceState::Stopping => true,
            InstanceState::Stopped => true,
            InstanceState::Rebooting => true,

            InstanceState::Migrating => {
                requested.run_state == InstanceStateRequested::Migrating
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

    /// Modifies the runtime state of the Instance as requested.  This generally
    /// means booting or halting the Instance.
    pub(crate) async fn instance_set_runtime(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        db_instance: &db::model::Instance,
        requested: InstanceRuntimeStateRequested,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, authz_instance).await?;

        self.check_runtime_change_allowed(
            &db_instance.runtime().clone().into(),
            &requested,
        )?;

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
        for (i, disk) in disks.iter().enumerate() {
            let volume =
                self.db_datastore.volume_checkout(disk.volume_id).await?;
            disk_reqs.push(sled_agent_client::types::DiskRequest {
                name: disk.name().to_string(),
                slot: sled_agent_client::types::Slot(i as u8),
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

        let instance_put_result = sa
            .instance_put(
                &db_instance.id(),
                &sled_agent_client::types::InstanceEnsureBody {
                    initial: instance_hardware,
                    target: requested.clone(),
                    migrate: None,
                },
            )
            .await;

        match instance_put_result {
            Ok(new_runtime) => {
                let new_runtime: nexus::InstanceRuntimeState =
                    new_runtime.into_inner().into();

                self.db_datastore
                    .instance_update_runtime(
                        &db_instance.id(),
                        &new_runtime.into(),
                    )
                    .await
                    .map(|_| ())
            }

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

                // this is unfortunate, but sled_agent_client::Error doesn't
                // implement Copy, and can't be match'ed upon below without this
                // line.
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
                &params::DiskSelector::new(
                    None,
                    Some(authz_project.id().into()),
                    disk,
                ),
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
                &params::DiskSelector::new(
                    None,
                    Some(authz_project.id().into()),
                    disk,
                ),
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
        id: &Uuid,
        new_runtime_state: &nexus::InstanceRuntimeState,
    ) -> Result<(), Error> {
        let log = &self.log;

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

    /// Returns the requested range of serial console output bytes,
    /// provided they are still in the sled-agent's cache.
    pub(crate) async fn instance_serial_console_data(
        &self,
        instance_lookup: &lookup::Instance<'_>,
        params: &params::InstanceSerialConsoleRequest,
    ) -> Result<params::InstanceSerialConsoleData, Error> {
        let (.., db_instance) = instance_lookup.fetch().await?;

        let sa = self.instance_sled(&db_instance).await?;
        let data = sa
            .instance_serial_get(
                &db_instance.identity().id,
                // these parameters are all the same type; OpenAPI puts them in alphabetical order.
                params.from_start,
                params.max_bytes,
                params.most_recent,
            )
            .await?;
        let sa_data: sled_agent_client::types::InstanceSerialConsoleData =
            data.into_inner();
        Ok(params::InstanceSerialConsoleData {
            data: sa_data.data,
            last_byte_offset: sa_data.last_byte_offset,
        })
    }

    pub(crate) async fn instance_serial_console_stream(
        &self,
        conn: dropshot::WebsocketConnection,
        instance_lookup: &lookup::Instance<'_>,
    ) -> Result<(), Error> {
        // TODO: Technically the stream is two way so the user of this method can modify the instance in some way. Should we use different permissions?
        let (.., instance) = instance_lookup.fetch().await?;
        let ip_addr = instance
            .runtime_state
            .propolis_ip
            .ok_or_else(|| {
                Error::internal_error(
                    "instance's propolis server ip address not found",
                )
            })?
            .ip();
        let socket_addr = SocketAddr::new(ip_addr, PROPOLIS_PORT);
        let client =
            propolis_client::Client::new(&format!("http://{}", socket_addr));
        let propolis_upgraded = client
            .instance_serial()
            .send()
            .await
            .map_err(|_| {
                Error::internal_error(
                    "failed to connect to instance's propolis server",
                )
            })?
            .into_inner();

        Self::proxy_instance_serial_ws(conn.into_inner(), propolis_upgraded)
            .await
            .map_err(|e| Error::internal_error(&format!("{}", e)))
    }

    async fn proxy_instance_serial_ws(
        client_upgraded: impl AsyncRead + AsyncWrite + Unpin,
        propolis_upgraded: impl AsyncRead + AsyncWrite + Unpin,
    ) -> Result<(), tokio_tungstenite::tungstenite::Error> {
        let (mut propolis_sink, mut propolis_stream) =
            WebSocketStream::from_raw_socket(
                propolis_upgraded,
                WebSocketRole::Client,
                None,
            )
            .await
            .split();
        let (mut nexus_sink, mut nexus_stream) =
            WebSocketStream::from_raw_socket(
                client_upgraded,
                WebSocketRole::Server,
                None,
            )
            .await
            .split();

        let mut buffered_output = None;
        let mut buffered_input = None;
        loop {
            let (nexus_read, propolis_write) = match buffered_input.take() {
                None => (nexus_stream.next().fuse(), Fuse::terminated()),
                Some(msg) => {
                    (Fuse::terminated(), propolis_sink.send(msg).fuse())
                }
            };
            let (nexus_write, propolis_read) = match buffered_output.take() {
                None => (Fuse::terminated(), propolis_stream.next().fuse()),
                Some(msg) => (nexus_sink.send(msg).fuse(), Fuse::terminated()),
            };
            tokio::select! {
                msg = nexus_read => {
                    match msg {
                        None => {
                            propolis_sink.send(WebSocketMessage::Close(Some(CloseFrame {
                                code: CloseCode::Abnormal,
                                reason: std::borrow::Cow::from(
                                    "nexus: websocket connection to client closed unexpectedly"
                                ),
                            }))).await?;
                            break;
                        }
                        Some(Err(e)) => {
                            propolis_sink.send(WebSocketMessage::Close(Some(CloseFrame {
                                code: CloseCode::Error,
                                reason: std::borrow::Cow::from(
                                    format!("nexus: error in websocket connection to client: {}", e)
                                ),
                            }))).await?;
                            return Err(e);
                        }
                        Some(Ok(WebSocketMessage::Close(details))) => {
                            propolis_sink.send(WebSocketMessage::Close(details)).await?;
                            break;
                        }
                        Some(Ok(WebSocketMessage::Text(_text))) => {
                            // TODO: json payloads specifying client-sent metadata?
                        }
                        Some(Ok(WebSocketMessage::Binary(data))) => {
                            buffered_input = Some(WebSocketMessage::Binary(data))
                        }
                        // Frame won't exist at this level, and ping reply is handled by tungstenite
                        Some(Ok(WebSocketMessage::Frame(_) | WebSocketMessage::Ping(_) | WebSocketMessage::Pong(_))) => {}
                    }
                }
                result = nexus_write => {
                    result?;
                }
                msg = propolis_read => {
                    match msg {
                        None => {
                            nexus_sink.send(WebSocketMessage::Close(Some(CloseFrame {
                                code: CloseCode::Abnormal,
                                reason: std::borrow::Cow::from(
                                    "nexus: websocket connection to propolis closed unexpectedly"
                                ),
                            }))).await?;
                            break;
                        }
                        Some(Err(e)) => {
                            nexus_sink.send(WebSocketMessage::Close(Some(CloseFrame {
                                code: CloseCode::Error,
                                reason: std::borrow::Cow::from(
                                    format!("nexus: error in websocket connection to propolis: {}", e)
                                ),
                            }))).await?;
                            return Err(e);
                        }
                        Some(Ok(WebSocketMessage::Close(details))) => {
                            nexus_sink.send(WebSocketMessage::Close(details)).await?;
                            break;
                        }
                        Some(Ok(WebSocketMessage::Text(_text))) => {
                            // TODO: deserialize a json payload, specifying:
                            //  - event: "migration"
                            //  - address: the address of the new propolis-server
                            //  - offset: what byte offset to start from (the last one sent from old propolis)
                        }
                        Some(Ok(WebSocketMessage::Binary(data))) => {
                            buffered_output = Some(WebSocketMessage::Binary(data))
                        }
                        // Frame won't exist at this level, and ping reply is handled by tungstenite
                        Some(Ok(WebSocketMessage::Frame(_) | WebSocketMessage::Ping(_) | WebSocketMessage::Pong(_))) => {}
                    }
                }
                result = propolis_write => {
                    result?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::Nexus;
    use core::time::Duration;
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
    use tokio_tungstenite::tungstenite::protocol::CloseFrame;
    use tokio_tungstenite::tungstenite::protocol::Role;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::WebSocketStream;

    #[tokio::test]
    async fn test_serial_console_stream_proxying() {
        let (nexus_client_conn, nexus_server_conn) = tokio::io::duplex(1024);
        let (propolis_client_conn, propolis_server_conn) =
            tokio::io::duplex(1024);
        let jh = tokio::spawn(async move {
            Nexus::proxy_instance_serial_ws(
                nexus_server_conn,
                propolis_client_conn,
            )
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

        let sent = Message::Binary(vec![1, 2, 3, 42, 5]);
        nexus_client_ws.send(sent.clone()).await.unwrap();
        let received = propolis_server_ws.next().await.unwrap().unwrap();
        assert_eq!(sent, received);

        let sent = Message::Binary(vec![6, 7, 8, 90]);
        propolis_server_ws.send(sent.clone()).await.unwrap();
        let received = nexus_client_ws.next().await.unwrap().unwrap();
        assert_eq!(sent, received);

        let sent = Message::Close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: std::borrow::Cow::from("test done"),
        }));
        nexus_client_ws.send(sent.clone()).await.unwrap();
        let received = propolis_server_ws.next().await.unwrap().unwrap();
        assert_eq!(sent, received);

        tokio::time::timeout(Duration::from_secs(1), jh)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
