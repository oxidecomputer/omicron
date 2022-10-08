// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Disks and snapshots

use crate::app::sagas;
use crate::authn;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::lookup::LookupPath;
use crate::db::model::Name;
use crate::external_api::params;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::internal::nexus::DiskRuntimeState;
use sled_agent_client::Client as SledAgentClient;
use std::sync::Arc;
use uuid::Uuid;

impl super::Nexus {
    // Disks

    pub async fn project_create_disk(
        self: &Arc<Self>,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        params: &params::DiskCreate,
    ) -> CreateResult<db::model::Disk> {
        let (.., authz_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::CreateChild)
            .await?;

        match &params.disk_source {
            params::DiskSource::Blank { block_size } => {
                // Reject disks where the block size doesn't evenly divide the
                // total size
                if (params.size.to_bytes() % block_size.0 as u64) != 0 {
                    return Err(Error::InvalidValue {
                        label: String::from("size and block_size"),
                        message: String::from(
                            "total size must be a multiple of block size",
                        ),
                    });
                }

                // Reject disks where the size isn't at least
                // MIN_DISK_SIZE_BYTES
                if params.size.to_bytes() < params::MIN_DISK_SIZE_BYTES as u64 {
                    return Err(Error::InvalidValue {
                        label: String::from("size"),
                        message: format!(
                            "total size must be at least {}",
                            ByteCount::from(params::MIN_DISK_SIZE_BYTES)
                        ),
                    });
                }

                // Reject disks where the MIN_DISK_SIZE_BYTES doesn't evenly
                // divide the size
                if (params.size.to_bytes() % params::MIN_DISK_SIZE_BYTES as u64)
                    != 0
                {
                    return Err(Error::InvalidValue {
                        label: String::from("size"),
                        message: format!(
                            "total size must be a multiple of {}",
                            ByteCount::from(params::MIN_DISK_SIZE_BYTES)
                        ),
                    });
                }
            }
            params::DiskSource::Snapshot { snapshot_id } => {
                let (.., db_snapshot) =
                    LookupPath::new(opctx, &self.db_datastore)
                        .snapshot_id(*snapshot_id)
                        .fetch()
                        .await?;

                // Reject disks where the block size doesn't evenly divide the
                // total size
                if (params.size.to_bytes()
                    % db_snapshot.block_size.to_bytes() as u64)
                    != 0
                {
                    return Err(Error::InvalidValue {
                        label: String::from("size and block_size"),
                        message: String::from(
                            "total size must be a multiple of snapshot's block size",
                        ),
                    });
                }

                // If the size of the snapshot is greater than the size of the
                // disk, return an error.
                if db_snapshot.size.to_bytes() > params.size.to_bytes() {
                    return Err(Error::invalid_request(
                        &format!(
                            "disk size {} must be greater than or equal to snapshot size {}",
                            params.size.to_bytes(),
                            db_snapshot.size.to_bytes(),
                        ),
                    ));
                }

                // Reject disks where the size isn't at least
                // MIN_DISK_SIZE_BYTES
                if params.size.to_bytes() < params::MIN_DISK_SIZE_BYTES as u64 {
                    return Err(Error::InvalidValue {
                        label: String::from("size"),
                        message: format!(
                            "total size must be at least {}",
                            ByteCount::from(params::MIN_DISK_SIZE_BYTES)
                        ),
                    });
                }

                // Reject disks where the MIN_DISK_SIZE_BYTES doesn't evenly divide
                // the size
                if (params.size.to_bytes() % params::MIN_DISK_SIZE_BYTES as u64)
                    != 0
                {
                    return Err(Error::InvalidValue {
                        label: String::from("size"),
                        message: format!(
                            "total size must be a multiple of {}",
                            ByteCount::from(params::MIN_DISK_SIZE_BYTES)
                        ),
                    });
                }
            }
            params::DiskSource::Image { image_id: _ } => {
                // Until we implement project images, do not allow disks to be
                // created from a project image.
                return Err(Error::InvalidValue {
                    label: String::from("image"),
                    message: String::from(
                        "project image are not yet supported",
                    ),
                });
            }
            params::DiskSource::GlobalImage { image_id } => {
                let (.., db_global_image) =
                    LookupPath::new(opctx, &self.db_datastore)
                        .global_image_id(*image_id)
                        .fetch()
                        .await?;

                // Reject disks where the block size doesn't evenly divide the
                // total size
                if (params.size.to_bytes()
                    % db_global_image.block_size.to_bytes() as u64)
                    != 0
                {
                    return Err(Error::InvalidValue {
                        label: String::from("size and block_size"),
                        message: String::from(
                            "total size must be a multiple of global image's block size",
                        ),
                    });
                }

                // If the size of the image is greater than the size of the
                // disk, return an error.
                if db_global_image.size.to_bytes() > params.size.to_bytes() {
                    return Err(Error::invalid_request(
                        &format!(
                            "disk size {} must be greater than or equal to image size {}",
                            params.size.to_bytes(),
                            db_global_image.size.to_bytes(),
                        ),
                    ));
                }

                // Reject disks where the size isn't at least
                // MIN_DISK_SIZE_BYTES
                if params.size.to_bytes() < params::MIN_DISK_SIZE_BYTES as u64 {
                    return Err(Error::InvalidValue {
                        label: String::from("size"),
                        message: format!(
                            "total size must be at least {}",
                            ByteCount::from(params::MIN_DISK_SIZE_BYTES)
                        ),
                    });
                }

                // Reject disks where the MIN_DISK_SIZE_BYTES doesn't evenly
                // divide the size
                if (params.size.to_bytes() % params::MIN_DISK_SIZE_BYTES as u64)
                    != 0
                {
                    return Err(Error::InvalidValue {
                        label: String::from("size"),
                        message: format!(
                            "total size must be a multiple of {}",
                            ByteCount::from(params::MIN_DISK_SIZE_BYTES)
                        ),
                    });
                }
            }
        }

        let saga_params = sagas::disk_create::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            project_id: authz_project.id(),
            create_params: params.clone(),
        };
        let saga_outputs = self
            .execute_saga::<sagas::disk_create::SagaDiskCreate>(saga_params)
            .await?;
        let disk_created = saga_outputs
            .lookup_node_output::<db::model::Disk>("created_disk")
            .map_err(|e| Error::internal_error(&format!("{:#}", &e)))
            .internal_context("looking up output from disk create saga")?;
        Ok(disk_created)
    }

    pub async fn project_list_disks(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Disk> {
        let (.., authz_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        self.db_datastore
            .project_list_disks(opctx, &authz_project, pagparams)
            .await
    }

    pub async fn disk_fetch(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        disk_name: &Name,
    ) -> LookupResult<db::model::Disk> {
        let (.., db_disk) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .disk_name(disk_name)
            .fetch()
            .await?;
        Ok(db_disk)
    }

    pub async fn disk_fetch_by_id(
        &self,
        opctx: &OpContext,
        disk_id: &Uuid,
    ) -> LookupResult<db::model::Disk> {
        let (.., db_disk) = LookupPath::new(opctx, &self.db_datastore)
            .disk_id(*disk_id)
            .fetch()
            .await?;
        Ok(db_disk)
    }

    /// Modifies the runtime state of the Disk as requested.  This generally
    /// means attaching or detaching the disk.
    // TODO(https://github.com/oxidecomputer/omicron/issues/811):
    // This will be unused until we implement hot-plug support.
    // However, it has been left for reference until then, as it will
    // likely be needed once that feature is implemented.
    #[allow(dead_code)]
    pub(crate) async fn disk_set_runtime(
        &self,
        opctx: &OpContext,
        authz_disk: &authz::Disk,
        db_disk: &db::model::Disk,
        sa: Arc<SledAgentClient>,
        requested: sled_agent_client::types::DiskStateRequested,
    ) -> Result<(), Error> {
        let runtime: DiskRuntimeState = db_disk.runtime().into();

        opctx.authorize(authz::Action::Modify, authz_disk).await?;

        // Ask the Sled Agent to begin the state change.  Then update the
        // database to reflect the new intermediate state.
        let new_runtime = sa
            .disk_put(
                &authz_disk.id(),
                &sled_agent_client::types::DiskEnsureBody {
                    initial_runtime:
                        sled_agent_client::types::DiskRuntimeState::from(
                            runtime,
                        ),
                    target: requested,
                },
            )
            .await
            .map_err(Error::from)?;

        let new_runtime: DiskRuntimeState = new_runtime.into_inner().into();

        self.db_datastore
            .disk_update_runtime(opctx, authz_disk, &new_runtime.into())
            .await
            .map(|_| ())
    }

    pub async fn notify_disk_updated(
        &self,
        opctx: &OpContext,
        id: Uuid,
        new_state: &DiskRuntimeState,
    ) -> Result<(), Error> {
        let log = &self.log;
        let (.., authz_disk) = LookupPath::new(&opctx, &self.db_datastore)
            .disk_id(id)
            .lookup_for(authz::Action::Modify)
            .await?;

        let result = self
            .db_datastore
            .disk_update_runtime(opctx, &authz_disk, &new_state.clone().into())
            .await;

        // TODO-cleanup commonize with notify_instance_updated()
        match result {
            Ok(true) => {
                info!(log, "disk updated by sled agent";
                    "disk_id" => %id,
                    "new_state" => ?new_state);
                Ok(())
            }

            Ok(false) => {
                info!(log, "disk update from sled agent ignored (old)";
                    "disk_id" => %id);
                Ok(())
            }

            // If the disk doesn't exist, swallow the error -- there's
            // nothing to do here.
            // TODO-robustness This could only be possible if we've removed a
            // disk from the datastore altogether.  When would we do that?
            // We don't want to do it as soon as something's destroyed, I think,
            // and in that case, we'd need some async task for cleaning these
            // up.
            Err(Error::ObjectNotFound { .. }) => {
                warn!(log, "non-existent disk updated by sled agent";
                    "instance_id" => %id,
                    "new_state" => ?new_state);
                Ok(())
            }

            // If the datastore is unavailable, propagate that to the caller.
            Err(error) => {
                warn!(log, "failed to update disk from sled agent";
                    "disk_id" => %id,
                    "new_state" => ?new_state,
                    "error" => ?error);
                Err(error)
            }
        }
    }

    pub async fn project_delete_disk(
        self: &Arc<Self>,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        disk_name: &Name,
    ) -> DeleteResult {
        let (.., project, authz_disk) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .disk_name(disk_name)
                .lookup_for(authz::Action::Delete)
                .await?;

        let saga_params = sagas::disk_delete::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            project_id: project.id(),
            disk_id: authz_disk.id(),
        };
        self.execute_saga::<sagas::disk_delete::SagaDiskDelete>(saga_params)
            .await?;
        Ok(())
    }

    // Snapshots

    pub async fn project_create_snapshot(
        self: &Arc<Self>,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        params: &params::SnapshotCreate,
    ) -> CreateResult<db::model::Snapshot> {
        let authz_silo: authz::Silo;
        let _authz_org: authz::Organization;
        let authz_project: authz::Project;
        let authz_disk: authz::Disk;

        (authz_silo, _authz_org, authz_project, authz_disk) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .disk_name(&db::model::Name(params.disk.clone()))
                .lookup_for(authz::Action::Read)
                .await?;

        let saga_params = sagas::snapshot_create::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            silo_id: authz_silo.id(),
            project_id: authz_project.id(),
            disk_id: authz_disk.id(),
            create_params: params.clone(),
        };

        let saga_outputs = self
            .execute_saga::<sagas::snapshot_create::SagaSnapshotCreate>(
                saga_params,
            )
            .await?;

        let snapshot_created = saga_outputs
            .lookup_node_output::<db::model::Snapshot>("finalized_snapshot")
            .map_err(|e| Error::InternalError {
                internal_message: e.to_string(),
            })?;

        Ok(snapshot_created)
    }

    pub async fn project_list_snapshots(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Snapshot> {
        let (.., authz_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::ListChildren)
            .await?;

        self.db_datastore
            .project_list_snapshots(opctx, &authz_project, pagparams)
            .await
    }

    pub async fn snapshot_fetch(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        snapshot_name: &Name,
    ) -> LookupResult<db::model::Snapshot> {
        let (.., db_snapshot) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .snapshot_name(snapshot_name)
            .fetch()
            .await?;

        Ok(db_snapshot)
    }

    pub async fn snapshot_fetch_by_id(
        &self,
        opctx: &OpContext,
        snapshot_id: &Uuid,
    ) -> LookupResult<db::model::Snapshot> {
        let (.., db_snapshot) = LookupPath::new(opctx, &self.db_datastore)
            .snapshot_id(*snapshot_id)
            .fetch()
            .await?;

        Ok(db_snapshot)
    }

    pub async fn project_delete_snapshot(
        self: &Arc<Self>,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        snapshot_name: &Name,
    ) -> DeleteResult {
        // TODO-correctness
        // This also requires solving how to clean up the associated resources
        // (on-disk snapshots, running read-only downstairs) because disks
        // *could* still be using them (if the snapshot has not yet been turned
        // into a regular crucible volume). It will involve some sort of
        // reference counting for volumes, and probably means this needs to
        // instead be a saga.

        let (.., project, authz_snapshot, db_snapshot) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .snapshot_name(snapshot_name)
                .fetch()
                .await?;

        // TODO: This should exist within a saga
        self.db_datastore
            .resource_usage_update_disk(
                &opctx,
                project.id(),
                -i64::try_from(db_snapshot.size.to_bytes()).map_err(|e| {
                    Error::internal_error(&format!(
                        "updating resource usage: {e}"
                    ))
                })?,
            )
            .await?;

        self.db_datastore
            .project_delete_snapshot(opctx, &authz_snapshot, &db_snapshot)
            .await?;

        // Kick off volume deletion saga(s)
        self.volume_delete(opctx, project.id(), db_snapshot.volume_id).await?;
        if let Some(volume_id) = db_snapshot.destination_volume_id {
            self.volume_delete(opctx, project.id(), volume_id).await?;
        }

        Ok(())
    }
}
