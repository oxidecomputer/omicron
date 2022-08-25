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
use crate::authz::ApiResource;
use crate::cidata::InstanceCiData;
use crate::context::OpContext;
use crate::db;
use crate::db::identity::Resource;
use crate::db::lookup::LookupPath;
use crate::db::queries::network_interface;
use crate::external_api::params;
use nexus_db_model::IpKind;
use nexus_db_model::Name;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::InstanceState;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::internal::nexus;
use sled_agent_client::types::InstanceRuntimeStateMigrateParams;
use sled_agent_client::types::InstanceRuntimeStateRequested;
use sled_agent_client::types::InstanceStateRequested;
use sled_agent_client::types::SourceNatConfig;
use sled_agent_client::Client as SledAgentClient;
use std::sync::Arc;
use uuid::Uuid;

const MAX_KEYS_PER_INSTANCE: u32 = 8;

impl super::Nexus {
    pub async fn project_create_instance(
        self: &Arc<Self>,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        params: &params::InstanceCreate,
    ) -> CreateResult<db::model::Instance> {
        let (.., authz_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::CreateChild)
            .await?;

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
            organization_name: organization_name.clone().into(),
            project_name: project_name.clone().into(),
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

    pub async fn project_list_instances(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Instance> {
        let (.., authz_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        self.db_datastore
            .project_list_instances(opctx, &authz_project, pagparams)
            .await
    }

    pub async fn instance_fetch(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
    ) -> LookupResult<db::model::Instance> {
        let (.., db_instance) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .instance_name(instance_name)
            .fetch()
            .await?;
        Ok(db_instance)
    }

    pub async fn instance_fetch_by_id(
        &self,
        opctx: &OpContext,
        instance_id: &Uuid,
    ) -> LookupResult<db::model::Instance> {
        let (.., db_instance) = LookupPath::new(opctx, &self.db_datastore)
            .instance_id(*instance_id)
            .fetch()
            .await?;
        Ok(db_instance)
    }

    // This operation may only occur on stopped instances, which implies that
    // the attached disks do not have any running "upstairs" process running
    // within the sled.
    pub async fn project_destroy_instance(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
    ) -> DeleteResult {
        // TODO-robustness We need to figure out what to do with Destroyed
        // instances?  Presumably we need to clean them up at some point, but
        // not right away so that callers can see that they've been destroyed.
        let (.., authz_instance, _) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .instance_name(instance_name)
                .fetch()
                .await?;

        self.db_datastore
            .project_delete_instance(opctx, &authz_instance)
            .await?;
        self.db_datastore
            .instance_delete_all_network_interfaces(opctx, &authz_instance)
            .await?;
        // Ignore the count of addresses deleted
        self.db_datastore
            .deallocate_external_ip_by_instance_id(opctx, authz_instance.id())
            .await?;
        Ok(())
    }

    pub async fn project_instance_migrate(
        self: &Arc<Self>,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
        params: params::InstanceMigrate,
    ) -> UpdateResult<db::model::Instance> {
        let (.., authz_instance) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .instance_name(instance_name)
            .lookup_for(authz::Action::Modify)
            .await?;

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
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
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
        let (.., authz_instance, db_instance) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .instance_name(instance_name)
                .fetch()
                .await?;
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
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
        only_provision: bool,
    ) -> UpdateResult<db::model::Instance> {
        let (.., authz_instance, db_instance) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .instance_name(instance_name)
                .fetch()
                .await?;
        let requested = InstanceRuntimeStateRequested {
            run_state: if only_provision {
                InstanceStateRequested::Provisioned
            } else {
                InstanceStateRequested::Running
            },
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
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
    ) -> UpdateResult<db::model::Instance> {
        let (.., authz_instance, db_instance) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .instance_name(instance_name)
                .fetch()
                .await?;
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
            InstanceState::Provisioned => true,
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
                &DataPageParams {
                    marker: None,
                    direction: dropshot::PaginationOrder::Ascending,
                    limit: std::num::NonZeroU32::new(MAX_DISKS_PER_INSTANCE)
                        .unwrap(),
                },
            )
            .await?;

        let mut disk_reqs = vec![];
        for (i, disk) in disks.iter().enumerate() {
            let volume = self.db_datastore.volume_get(disk.volume_id).await?;
            let gen: i64 = (&disk.runtime_state.gen.0).into();
            disk_reqs.push(sled_agent_client::types::DiskRequest {
                name: disk.name().to_string(),
                slot: sled_agent_client::types::Slot(i as u8),
                read_only: false,
                device: "nvme".to_string(),
                gen: gen as u64,
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
                &DataPageParams {
                    marker: None,
                    direction: dropshot::PaginationOrder::Ascending,
                    limit: std::num::NonZeroU32::new(MAX_KEYS_PER_INSTANCE)
                        .unwrap(),
                },
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
            disks: disk_reqs,
            cloud_init_bytes: Some(base64::encode(
                db_instance.generate_cidata(&public_keys)?,
            )),
        };

        let sa = self.instance_sled(&db_instance).await?;

        let new_runtime = sa
            .instance_put(
                &db_instance.id(),
                &sled_agent_client::types::InstanceEnsureBody {
                    initial: instance_hardware,
                    target: requested,
                    migrate: None,
                },
            )
            .await
            .map_err(Error::from)?;

        let new_runtime: nexus::InstanceRuntimeState =
            new_runtime.into_inner().into();

        self.db_datastore
            .instance_update_runtime(&db_instance.id(), &new_runtime.into())
            .await
            .map(|_| ())
    }

    /// Lists disks attached to the instance.
    pub async fn instance_list_disks(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Disk> {
        let (.., authz_instance) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .instance_name(instance_name)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        self.db_datastore
            .instance_list_disks(opctx, &authz_instance, pagparams)
            .await
    }

    /// Attach a disk to an instance.
    pub async fn instance_attach_disk(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
        disk_name: &Name,
    ) -> UpdateResult<db::model::Disk> {
        let (.., authz_project, authz_disk, _) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .disk_name(disk_name)
                .fetch()
                .await?;
        let (.., authz_instance, _) =
            LookupPath::new(opctx, &self.db_datastore)
                .project_id(authz_project.id())
                .instance_name(instance_name)
                .fetch()
                .await?;

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
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
        disk_name: &Name,
    ) -> UpdateResult<db::model::Disk> {
        let (.., authz_project, authz_disk, _) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .disk_name(disk_name)
                .fetch()
                .await?;
        let (.., authz_instance, _) =
            LookupPath::new(opctx, &self.db_datastore)
                .project_id(authz_project.id())
                .instance_name(instance_name)
                .fetch()
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

    /// Create a network interface attached to the provided instance.
    // TODO-performance: Add a version of this that accepts the instance ID
    // directly. This will avoid all the internal database lookups in the event
    // that we create many NICs for the same instance, such as in a saga.
    pub async fn instance_create_network_interface(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
        params: &params::NetworkInterfaceCreate,
    ) -> CreateResult<db::model::NetworkInterface> {
        let (.., authz_project, authz_instance) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .instance_name(instance_name)
                .lookup_for(authz::Action::Modify)
                .await?;

        // NOTE: We need to lookup the VPC and VPC Subnet, since we need both
        // IDs for creating the network interface.
        //
        // TODO-correctness: There are additional races here. The VPC and VPC
        // Subnet could both be deleted between the time we fetch them and
        // actually insert the record for the interface. The solution is likely
        // to make both objects implement `DatastoreCollection` for their
        // children, and then use `VpcSubnet::insert_resource` inside the
        // `instance_create_network_interface` method. See
        // https://github.com/oxidecomputer/omicron/issues/738.
        let vpc_name = db::model::Name(params.vpc_name.clone());
        let subnet_name = db::model::Name(params.subnet_name.clone());
        let (.., authz_vpc, authz_subnet, db_subnet) =
            LookupPath::new(opctx, &self.db_datastore)
                .project_id(authz_project.id())
                .vpc_name(&vpc_name)
                .vpc_subnet_name(&subnet_name)
                .fetch()
                .await?;
        let interface_id = Uuid::new_v4();
        let interface = db::model::IncompleteNetworkInterface::new(
            interface_id,
            authz_instance.id(),
            authz_vpc.id(),
            db_subnet,
            params.identity.clone(),
            params.ip,
        )?;
        self.db_datastore
            .instance_create_network_interface(
                opctx,
                &authz_subnet,
                &authz_instance,
                interface,
            )
            .await
            .map_err(|e| {
                debug!(
                    self.log,
                    "failed to create network interface";
                    "instance_id" => ?authz_instance.id(),
                    "interface_id" => ?interface_id,
                    "error" => ?e,
                );
                if matches!(
                    e,
                    network_interface::InsertError::InstanceNotFound(_)
                ) {
                    // Return the not-found message of the authz interface
                    // object, so that the message reflects how the caller
                    // originally looked it up
                    authz_instance.not_found()
                } else {
                    // Convert other errors into an appropriate client error
                    network_interface::InsertError::into_external(e)
                }
            })
    }

    /// Lists network interfaces attached to the instance.
    pub async fn instance_list_network_interfaces(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::NetworkInterface> {
        let (.., authz_instance) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .instance_name(instance_name)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        self.db_datastore
            .instance_list_network_interfaces(opctx, &authz_instance, pagparams)
            .await
    }

    /// Fetch a network interface attached to the given instance.
    pub async fn network_interface_fetch(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
        interface_name: &Name,
    ) -> LookupResult<db::model::NetworkInterface> {
        let (.., db_interface) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .instance_name(instance_name)
            .network_interface_name(interface_name)
            .fetch()
            .await?;
        Ok(db_interface)
    }

    pub async fn network_interface_fetch_by_id(
        &self,
        opctx: &OpContext,
        interface_id: &Uuid,
    ) -> LookupResult<db::model::NetworkInterface> {
        let (.., db_interface) = LookupPath::new(opctx, &self.db_datastore)
            .network_interface_id(*interface_id)
            .fetch()
            .await?;
        Ok(db_interface)
    }

    /// Update a network interface for the given instance.
    pub async fn network_interface_update(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
        interface_name: &Name,
        updates: params::NetworkInterfaceUpdate,
    ) -> UpdateResult<db::model::NetworkInterface> {
        let (.., authz_instance) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .instance_name(instance_name)
            .lookup_for(authz::Action::Modify)
            .await?;
        let (.., authz_interface) = LookupPath::new(opctx, &self.db_datastore)
            .instance_id(authz_instance.id())
            .network_interface_name(interface_name)
            .lookup_for(authz::Action::Modify)
            .await?;
        self.db_datastore
            .instance_update_network_interface(
                opctx,
                &authz_instance,
                &authz_interface,
                db::model::NetworkInterfaceUpdate::from(updates),
            )
            .await
    }

    /// Delete a network interface from the provided instance.
    ///
    /// Note that the primary interface for an instance cannot be deleted if
    /// there are any secondary interfaces.
    pub async fn instance_delete_network_interface(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
        interface_name: &Name,
    ) -> DeleteResult {
        let (.., authz_instance) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .instance_name(instance_name)
            .lookup_for(authz::Action::Modify)
            .await?;
        let (.., authz_interface) = LookupPath::new(opctx, &self.db_datastore)
            .instance_id(authz_instance.id())
            .network_interface_name(interface_name)
            .lookup_for(authz::Action::Delete)
            .await?;
        self.db_datastore
            .instance_delete_network_interface(
                opctx,
                &authz_instance,
                &authz_interface,
            )
            .await
            .map_err(|e| {
                debug!(
                    self.log,
                    "failed to delete network interface";
                    "instance_id" => ?authz_instance.id(),
                    "interface_id" => ?authz_interface.id(),
                    "error" => ?e,
                );
                if matches!(
                    e,
                    network_interface::DeleteError::InstanceNotFound(_)
                ) {
                    // Return the not-found message of the authz interface
                    // object, so that the message reflects how the caller
                    // originally looked it up
                    authz_instance.not_found()
                } else {
                    // Convert other errors into an appropriate client error
                    network_interface::DeleteError::into_external(e)
                }
            })
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
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
        params: &params::InstanceSerialConsoleRequest,
    ) -> Result<params::InstanceSerialConsoleData, Error> {
        let db_instance = self
            .instance_fetch(
                opctx,
                organization_name,
                project_name,
                instance_name,
            )
            .await?;

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
}
