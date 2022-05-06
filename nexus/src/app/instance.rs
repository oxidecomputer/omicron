// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::MAX_DISKS_PER_INSTANCE;
use crate::authn;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::identity::Resource;
use crate::db::lookup::LookupPath;
use crate::db::model::Name;
use crate::db::subnet_allocation::NetworkInterfaceError;
use crate::external_api::params;
use crate::sagas;
use omicron_common::api::external;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::DiskState;
use omicron_common::api::external::Error;
use omicron_common::api::external::InstanceState;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::internal::nexus;
use sled_agent_client::types::InstanceRuntimeStateMigrateParams;
use sled_agent_client::types::InstanceRuntimeStateRequested;
use sled_agent_client::types::InstanceStateRequested;
use sled_agent_client::Client as SledAgentClient;
use std::sync::Arc;
use uuid::Uuid;

impl super::Nexus {
    pub async fn instance_migrate(
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
        let saga_params = Arc::new(sagas::ParamsInstanceMigrate {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            instance_id: authz_instance.id(),
            migrate_params: params,
        });
        self.execute_saga(
            Arc::clone(&sagas::SAGA_INSTANCE_MIGRATE_TEMPLATE),
            sagas::SAGA_INSTANCE_MIGRATE_NAME,
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
    ) -> UpdateResult<db::model::Instance> {
        let (.., authz_instance, db_instance) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .instance_name(instance_name)
                .fetch()
                .await?;
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
        let sa_id = &instance.runtime().sled_uuid;
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

        // Ask the sled agent to begin the state change.  Then update the
        // database to reflect the new intermediate state.  If this update is
        // not the newest one, that's fine.  That might just mean the sled agent
        // beat us to it.

        let instance_hardware = sled_agent_client::types::InstanceHardware {
            runtime: sled_agent_client::types::InstanceRuntimeState::from(
                db_instance.runtime().clone(),
            ),
            nics,
            disks: disk_reqs,
            cloud_init_bytes: Some(base64::encode(
                db_instance.generate_cidata()?,
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
        let (.., authz_project, authz_disk, db_disk) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .disk_name(disk_name)
                .fetch()
                .await?;
        let (.., authz_instance, db_instance) =
            LookupPath::new(opctx, &self.db_datastore)
                .project_id(authz_project.id())
                .instance_name(instance_name)
                .fetch()
                .await?;
        let instance_id = &authz_instance.id();

        // Enforce attached disks limit
        let attached_disks = self
            .instance_list_disks(
                opctx,
                organization_name,
                project_name,
                instance_name,
                &DataPageParams {
                    marker: None,
                    direction: dropshot::PaginationOrder::Ascending,
                    limit: std::num::NonZeroU32::new(MAX_DISKS_PER_INSTANCE)
                        .unwrap(),
                },
            )
            .await?;

        if attached_disks.len() == MAX_DISKS_PER_INSTANCE as usize {
            return Err(Error::invalid_request(&format!(
                "cannot attach more than {} disks to instance!",
                MAX_DISKS_PER_INSTANCE
            )));
        }

        fn disk_attachment_error(
            disk: &db::model::Disk,
        ) -> CreateResult<db::model::Disk> {
            let disk_status = match disk.runtime().state().into() {
                DiskState::Destroyed => "disk is destroyed",
                DiskState::Faulted => "disk is faulted",
                DiskState::Creating => "disk is detached",
                DiskState::Detached => "disk is detached",

                // It would be nice to provide a more specific message here, but
                // the appropriate identifier to provide the user would be the
                // other instance's name.  Getting that would require another
                // database hit, which doesn't seem worth it for this.
                DiskState::Attaching(_) => {
                    "disk is attached to another instance"
                }
                DiskState::Attached(_) => {
                    "disk is attached to another instance"
                }
                DiskState::Detaching(_) => {
                    "disk is attached to another instance"
                }
            };
            let message = format!(
                "cannot attach disk \"{}\": {}",
                disk.name().as_str(),
                disk_status
            );
            Err(Error::InvalidRequest { message })
        }

        match &db_disk.state().into() {
            // If we're already attaching or attached to the requested instance,
            // there's nothing else to do.
            // TODO-security should it be an error if you're not authorized to
            // do this and we did not actually have to do anything?
            DiskState::Attached(id) if id == instance_id => return Ok(db_disk),

            // If the disk is currently attaching or attached to another
            // instance, fail this request.  Users must explicitly detach first
            // if that's what they want.  If it's detaching, they have to wait
            // for it to become detached.
            // TODO-debug: the error message here could be better.  We'd have to
            // look up the other instance by id (and gracefully handle it not
            // existing).
            DiskState::Attached(id) => {
                assert_ne!(id, instance_id);
                return disk_attachment_error(&db_disk);
            }
            DiskState::Detaching(_) => {
                return disk_attachment_error(&db_disk);
            }
            DiskState::Attaching(id) if id != instance_id => {
                return disk_attachment_error(&db_disk);
            }
            DiskState::Destroyed => {
                return disk_attachment_error(&db_disk);
            }
            DiskState::Faulted => {
                return disk_attachment_error(&db_disk);
            }

            DiskState::Creating => (),
            DiskState::Detached => (),
            DiskState::Attaching(id) => {
                assert_eq!(id, instance_id);
            }
        }

        match &db_instance.runtime_state.state.state() {
            // If there's a propolis zone for this instance, ask the Sled Agent
            // to hot-plug the disk.
            //
            // TODO this will probably involve volume construction requests as
            // well!
            InstanceState::Running | InstanceState::Starting => {
                self.disk_set_runtime(
                    opctx,
                    &authz_disk,
                    &db_disk,
                    self.instance_sled(&db_instance).await?,
                    sled_agent_client::types::DiskStateRequested::Attached(
                        *instance_id,
                    ),
                )
                .await?;
            }

            _ => {
                // If there is not a propolis zone, then disk attach only occurs
                // in the DB.
                let new_runtime = db_disk.runtime().attach(*instance_id);

                self.db_datastore
                    .disk_update_runtime(opctx, &authz_disk, &new_runtime)
                    .await?;
            }
        }

        self.db_datastore.disk_refetch(opctx, &authz_disk).await
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
        let (.., authz_project, authz_disk, db_disk) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .disk_name(disk_name)
                .fetch()
                .await?;
        let (.., authz_instance, db_instance) =
            LookupPath::new(opctx, &self.db_datastore)
                .project_id(authz_project.id())
                .instance_name(instance_name)
                .fetch()
                .await?;
        let instance_id = &authz_instance.id();

        match &db_disk.state().into() {
            // This operation is a noop if the disk is not attached or already
            // detaching from the same instance.
            // TODO-security should it be an error if you're not authorized to
            // do this and we did not actually have to do anything?
            DiskState::Creating => return Ok(db_disk),
            DiskState::Detached => return Ok(db_disk),
            DiskState::Destroyed => return Ok(db_disk),
            DiskState::Faulted => return Ok(db_disk),
            DiskState::Detaching(id) if id == instance_id => {
                return Ok(db_disk)
            }

            // This operation is not allowed if the disk is attached to some
            // other instance.
            DiskState::Attaching(id) if id != instance_id => {
                return Err(Error::InvalidRequest {
                    message: String::from("disk is attached elsewhere"),
                });
            }
            DiskState::Attached(id) if id != instance_id => {
                return Err(Error::InvalidRequest {
                    message: String::from("disk is attached elsewhere"),
                });
            }
            DiskState::Detaching(_) => {
                return Err(Error::InvalidRequest {
                    message: String::from("disk is attached elsewhere"),
                });
            }

            // These are the cases where we have to do something.
            DiskState::Attaching(_) => (),
            DiskState::Attached(_) => (),
        }

        // If there's a propolis zone for this instance, ask the Sled
        // Agent to hot-remove the disk.
        match &db_instance.runtime_state.state.state() {
            InstanceState::Running | InstanceState::Starting => {
                self.disk_set_runtime(
                    opctx,
                    &authz_disk,
                    &db_disk,
                    self.instance_sled(&db_instance).await?,
                    sled_agent_client::types::DiskStateRequested::Detached,
                )
                .await?;
            }

            _ => {
                // If there is not a propolis zone, then disk detach only occurs
                // in the DB.
                let new_runtime = db_disk.runtime().detach();

                self.db_datastore
                    .disk_update_runtime(opctx, &authz_disk, &new_runtime)
                    .await?;
            }
        }

        self.db_datastore.disk_refetch(opctx, &authz_disk).await
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
        let (.., authz_project, authz_instance, db_instance) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .instance_name(instance_name)
                .fetch()
                .await?;

        // TODO-completeness: We'd like to relax this once hot-plug is
        // supported.
        //
        // TODO-correctness: There's a TOCTOU race here. Someone might start the
        // instance between this check and when we actually create the NIC
        // record. One solution is to place the state verification in the query
        // to create the NIC. Unfortunately, that query is already very
        // complicated.
        let stopped =
            db::model::InstanceState::new(external::InstanceState::Stopped);
        if db_instance.runtime_state.state != stopped {
            return Err(external::Error::invalid_request(
                "Instance must be stopped to attach a new network interface",
            ));
        }

        // NOTE: We need to lookup the VPC and VPC Subnet, since we need both
        // IDs for creating the network interface.
        let vpc_name = db::model::Name(params.vpc_name.clone());
        let subnet_name = db::model::Name(params.subnet_name.clone());
        let (.., authz_vpc, authz_subnet, db_subnet) =
            LookupPath::new(opctx, &self.db_datastore)
                .project_id(authz_project.id())
                .vpc_name(&vpc_name)
                .vpc_subnet_name(&subnet_name)
                .fetch()
                .await?;
        let mac = db::model::MacAddr::new()?;
        let interface_id = Uuid::new_v4();
        let interface = db::model::IncompleteNetworkInterface::new(
            interface_id,
            authz_instance.id(),
            authz_vpc.id(),
            db_subnet,
            mac,
            params.identity.clone(),
            params.ip,
        )?;
        let interface = self
            .db_datastore
            .instance_create_network_interface(
                opctx,
                &authz_subnet,
                &authz_instance,
                interface,
            )
            .await
            .map_err(NetworkInterfaceError::into_external)?;
        Ok(interface)
    }

    /// Fetch a network interface attached to the given instance.
    pub async fn instance_network_interface_fetch(
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

    /// Delete a network interface from the provided instance.
    pub async fn instance_delete_network_interface(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
        interface_name: &Name,
    ) -> DeleteResult {
        let (.., authz_instance, db_instance) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .instance_name(instance_name)
                .fetch_for(authz::Action::Modify)
                .await?;
        let (.., authz_interface) = LookupPath::new(opctx, &self.db_datastore)
            .instance_id(authz_instance.id())
            .network_interface_name(interface_name)
            .lookup_for(authz::Action::Delete)
            .await?;

        // TODO-completeness: We'd like to relax this once hot-plug is supported
        let stopped =
            db::model::InstanceState::new(external::InstanceState::Stopped);
        if db_instance.runtime_state.state != stopped {
            return Err(external::Error::invalid_request(
                "Instance must be stopped to detach a network interface",
            ));
        }
        self.db_datastore
            .instance_delete_network_interface(opctx, &authz_interface)
            .await
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
                    "propolis_id" => %new_runtime_state.propolis_uuid,
                    "new_state" => %new_runtime_state.run_state);
                Ok(())
            }

            Ok(false) => {
                info!(log, "instance update from sled agent ignored (old)";
                    "instance_id" => %id,
                    "propolis_id" => %new_runtime_state.propolis_uuid,
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
}
