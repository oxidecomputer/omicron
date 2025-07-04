// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Disks

use crate::app::sagas;
use crate::external_api::params;
use nexus_db_lookup::LookupPath;
use nexus_db_lookup::lookup;
use nexus_db_queries::authn;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::DiskState;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::internal::nexus::DiskRuntimeState;
use sled_agent_client::Client as SledAgentClient;
use std::sync::Arc;
use uuid::Uuid;

use super::MAX_DISK_SIZE_BYTES;
use super::MIN_DISK_SIZE_BYTES;

impl super::Nexus {
    // Disks
    pub fn disk_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        disk_selector: params::DiskSelector,
    ) -> LookupResult<lookup::Disk<'a>> {
        match disk_selector {
            params::DiskSelector { disk: NameOrId::Id(id), project: None } => {
                let disk =
                    LookupPath::new(opctx, &self.db_datastore).disk_id(id);
                Ok(disk)
            }
            params::DiskSelector {
                disk: NameOrId::Name(name),
                project: Some(project),
            } => {
                let disk = self
                    .project_lookup(opctx, params::ProjectSelector { project })?
                    .disk_name_owned(name.into());
                Ok(disk)
            }
            params::DiskSelector { disk: NameOrId::Id(_), .. } => {
                Err(Error::invalid_request(
                    "when providing disk as an ID project should not be specified",
                ))
            }
            _ => Err(Error::invalid_request(
                "disk should either be UUID or project should be specified",
            )),
        }
    }

    pub(super) async fn validate_disk_create_params(
        self: &Arc<Self>,
        opctx: &OpContext,
        authz_project: &authz::Project,
        params: &params::DiskCreate,
    ) -> Result<(), Error> {
        let block_size: u64 = match params.disk_source {
            params::DiskSource::Blank { block_size }
            | params::DiskSource::ImportingBlocks { block_size } => {
                block_size.into()
            }
            params::DiskSource::Snapshot { snapshot_id } => {
                let (.., db_snapshot) =
                    LookupPath::new(opctx, &self.db_datastore)
                        .snapshot_id(snapshot_id)
                        .fetch()
                        .await?;

                // Return an error if the snapshot does not belong to our
                // project.
                if db_snapshot.project_id != authz_project.id() {
                    return Err(Error::invalid_request(
                        "snapshot does not belong to this project",
                    ));
                }

                // If the size of the snapshot is greater than the size of the
                // disk, return an error.
                if db_snapshot.size.to_bytes() > params.size.to_bytes() {
                    return Err(Error::invalid_request(&format!(
                        "disk size {} must be greater than or equal to snapshot size {}",
                        params.size.to_bytes(),
                        db_snapshot.size.to_bytes(),
                    )));
                }

                db_snapshot.block_size.to_bytes().into()
            }
            params::DiskSource::Image { image_id } => {
                let (.., db_image) = LookupPath::new(opctx, &self.db_datastore)
                    .image_id(image_id)
                    .fetch()
                    .await?;

                // The image either needs to belong to our project or be
                // promoted to our silo. If not, return an error.
                if let Some(project) = db_image.project_id {
                    if project != authz_project.id() {
                        return Err(Error::invalid_request(
                            "image does not belong to this project",
                        ));
                    }
                }

                // If the size of the image is greater than the size of the
                // disk, return an error.
                if db_image.size.to_bytes() > params.size.to_bytes() {
                    return Err(Error::invalid_request(&format!(
                        "disk size {} must be greater than or equal to image size {}",
                        params.size.to_bytes(),
                        db_image.size.to_bytes(),
                    )));
                }

                db_image.block_size.to_bytes().into()
            }
        };

        // Reject disks where the block size doesn't evenly divide the
        // total size
        if (params.size.to_bytes() % block_size) != 0 {
            return Err(Error::invalid_value(
                "size and block_size",
                format!(
                    "total size must be a multiple of block size {}",
                    block_size,
                ),
            ));
        }

        // Reject disks where the size isn't at least
        // MIN_DISK_SIZE_BYTES
        if params.size.to_bytes() < u64::from(MIN_DISK_SIZE_BYTES) {
            return Err(Error::invalid_value(
                "size",
                format!(
                    "total size must be at least {}",
                    ByteCount::from(MIN_DISK_SIZE_BYTES)
                ),
            ));
        }

        // Reject disks where the MIN_DISK_SIZE_BYTES doesn't evenly
        // divide the size
        if (params.size.to_bytes() % u64::from(MIN_DISK_SIZE_BYTES)) != 0 {
            return Err(Error::invalid_value(
                "size",
                format!(
                    "total size must be a multiple of {}",
                    ByteCount::from(MIN_DISK_SIZE_BYTES)
                ),
            ));
        }

        // Reject disks where the size is greated than MAX_DISK_SIZE_BYTES
        if params.size.to_bytes() > MAX_DISK_SIZE_BYTES {
            return Err(Error::invalid_value(
                "size",
                format!(
                    "total size must be less than {}",
                    ByteCount::try_from(MAX_DISK_SIZE_BYTES).unwrap()
                ),
            ));
        }

        Ok(())
    }

    pub(crate) async fn project_create_disk(
        self: &Arc<Self>,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        params: &params::DiskCreate,
    ) -> CreateResult<db::model::Disk> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::CreateChild).await?;
        self.validate_disk_create_params(opctx, &authz_project, params).await?;

        let saga_params = sagas::disk_create::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            project_id: authz_project.id(),
            create_params: params.clone(),
        };
        let saga_outputs = self
            .sagas
            .saga_execute::<sagas::disk_create::SagaDiskCreate>(saga_params)
            .await?;
        let disk_created = saga_outputs
            .lookup_node_output::<db::model::Disk>("created_disk")
            .map_err(|e| Error::internal_error(&format!("{:#}", &e)))
            .internal_context("looking up output from disk create saga")?;
        Ok(disk_created)
    }

    pub(crate) async fn disk_list(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::Disk> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::ListChildren).await?;
        self.db_datastore.disk_list(opctx, &authz_project, pagparams).await
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

    pub(crate) async fn notify_disk_updated(
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

    pub(crate) async fn project_delete_disk(
        self: &Arc<Self>,
        opctx: &OpContext,
        disk_lookup: &lookup::Disk<'_>,
    ) -> DeleteResult {
        let (.., project, authz_disk) =
            disk_lookup.lookup_for(authz::Action::Delete).await?;

        let (.., db_disk) = LookupPath::new(opctx, &self.db_datastore)
            .disk_id(authz_disk.id())
            .fetch()
            .await?;

        let saga_params = sagas::disk_delete::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            project_id: project.id(),
            disk_id: authz_disk.id(),
            volume_id: db_disk.volume_id(),
        };
        self.sagas
            .saga_execute::<sagas::disk_delete::SagaDiskDelete>(saga_params)
            .await?;
        Ok(())
    }

    /// Remove a read only parent from a disk.
    /// This is just a wrapper around the volume operation of the same
    /// name, but we provide this interface when all the caller has is
    /// the disk UUID as the internal volume_id is not exposed.
    pub(crate) async fn disk_remove_read_only_parent(
        self: &Arc<Self>,
        opctx: &OpContext,
        disk_id: Uuid,
    ) -> DeleteResult {
        // First get the internal volume ID that is stored in the disk
        // database entry, once we have that just call the volume method
        // to remove the read only parent.
        let (.., db_disk) = LookupPath::new(opctx, &self.db_datastore)
            .disk_id(disk_id)
            .fetch()
            .await?;

        self.volume_remove_read_only_parent(&opctx, db_disk.volume_id())
            .await?;

        Ok(())
    }

    /// Move a disk from the "ImportReady" state to the "Importing" state,
    /// blocking any import from URL jobs.
    pub(crate) async fn disk_manual_import_start(
        self: &Arc<Self>,
        opctx: &OpContext,
        disk_lookup: &lookup::Disk<'_>,
    ) -> UpdateResult<()> {
        let authz_disk: authz::Disk;
        let db_disk: db::model::Disk;

        (.., authz_disk, db_disk) =
            disk_lookup.fetch_for(authz::Action::Modify).await?;

        let disk_state: DiskState = db_disk.state().into();
        match disk_state {
            DiskState::ImportReady => {
                // ok
            }

            _ => {
                return Err(Error::invalid_request(&format!(
                    "cannot set disk in state {:?} to {:?}",
                    disk_state,
                    DiskState::ImportingFromBulkWrites.label()
                )));
            }
        }

        self.db_datastore
            .disk_update_runtime(
                opctx,
                &authz_disk,
                &db_disk.runtime().importing_from_bulk_writes(),
            )
            .await
            .map(|_| ())
    }

    /// Bulk write some bytes into a disk that's in state ImportingFromBulkWrites
    pub(crate) async fn disk_manual_import(
        self: &Arc<Self>,
        disk_lookup: &lookup::Disk<'_>,
        param: params::ImportBlocksBulkWrite,
    ) -> UpdateResult<()> {
        let db_disk: db::model::Disk;

        (.., db_disk) = disk_lookup.fetch_for(authz::Action::Modify).await?;

        let disk_state: DiskState = db_disk.state().into();
        match disk_state {
            DiskState::ImportingFromBulkWrites => {
                // ok
            }

            _ => {
                return Err(Error::invalid_request(&format!(
                    "cannot import blocks with a bulk write for disk in state {:?}",
                    disk_state,
                )));
            }
        }

        if let Some(endpoint) = db_disk.pantry_address() {
            let data: Vec<u8> = base64::Engine::decode(
                &base64::engine::general_purpose::STANDARD,
                &param.base64_encoded_data,
            )
            .map_err(|e| {
                Error::invalid_request(&format!(
                    "error base64 decoding data: {}",
                    e
                ))
            })?;

            info!(
                self.log,
                "bulk write of {} bytes to offset {} of disk {} using pantry endpoint {:?}",
                data.len(),
                param.offset,
                db_disk.id(),
                endpoint,
            );

            // The the disk state can change between the check above and here
            // because there's no disk state change associated with this write.
            // I believe that toggling the disk's state and generation number in
            // the DB is too expensive to be done for every 512k chunk of a disk
            // (or whatever the eventual import chunk size is), however I didn't
            // actually measure it.
            //
            // For example, between the check against state
            // ImportingFromBulkWrites and here, the user could have called the
            // bulk-write-stop endpoint, which would put the disk into state
            // ImportReady. They could have then called the finalize endpoint,
            // which would kick off the disk finalizing saga: set the state to
            // finalzing, optionally take a snapshot, detach it from the
            // associated Pantry, and set the state to Detached.
            //
            // In this scenario the write here would fail because the volume
            // would have been detached from the Pantry, but say that the user
            // instead called bulk-write-stop, then import, thereby kicking off
            // the import blocks from URL saga? The Pantry locks its internal
            // entry by the ID of the volume, so at least the requests would be
            // ordered by their arrival time: either this bulk write would land
            // first, or the import would land first:
            //
            // - if this bulk write landed first, the import would likely
            //   overwrite it with blocks from a URL.
            //
            // - if the import landed first, then the bulk write would overwrite
            //   what the import job imported, probably corrupting the data.
            //
            // I also believe it's not correct to use a saga here. Really, the
            // user's cli (or any other program really) is responsible for this
            // bulk write operation, not Nexus. If the bulk_write call below
            // fails, then that failure would be propagated up to the user, and
            // that user's program can act accordingly. In a way, the user's
            // program is an externally driven saga instead.

            let client = crucible_pantry_client::Client::new_with_client(
                &format!("http://{}", endpoint),
                self.reqwest_client.clone(),
            );
            let request = crucible_pantry_client::types::BulkWriteRequest {
                offset: param.offset,
                base64_encoded_data: param.base64_encoded_data,
            };

            client
                .bulk_write(&db_disk.id().to_string(), &request)
                .await
                .map_err(|e| match e {
                    crucible_pantry_client::Error::ErrorResponse(rv) => {
                        match rv.status() {
                            status if status.is_client_error() => {
                                Error::invalid_request(&rv.message)
                            }

                            _ => Error::internal_error(&rv.message),
                        }
                    }

                    _ => Error::internal_error(&format!(
                        "error sending bulk write to pantry: {}",
                        e,
                    )),
                })?;

            Ok(())
        } else {
            error!(self.log, "disk {} has no pantry address!", db_disk.id());
            Err(Error::internal_error(&format!(
                "disk {} has no pantry address!",
                db_disk.id(),
            )))
        }
    }

    /// Move a disk from the "ImportingFromBulkWrites" state to the
    /// "ImportReady" state, usually signalling the end of manually importing
    /// blocks.
    pub(crate) async fn disk_manual_import_stop(
        self: &Arc<Self>,
        opctx: &OpContext,
        disk_lookup: &lookup::Disk<'_>,
    ) -> UpdateResult<()> {
        let authz_disk: authz::Disk;
        let db_disk: db::model::Disk;

        (.., authz_disk, db_disk) =
            disk_lookup.fetch_for(authz::Action::Modify).await?;

        let disk_state: DiskState = db_disk.state().into();
        match disk_state {
            DiskState::ImportingFromBulkWrites => {
                // ok
            }

            _ => {
                return Err(Error::invalid_request(&format!(
                    "cannot set disk in state {:?} to {:?}",
                    disk_state,
                    DiskState::ImportReady.label()
                )));
            }
        }

        self.db_datastore
            .disk_update_runtime(
                opctx,
                &authz_disk,
                &db_disk.runtime().import_ready(),
            )
            .await
            .map(|_| ())
    }

    /// Move a disk from the "ImportReady" state to the "Detach" state, making
    /// it ready for general use.
    pub(crate) async fn disk_finalize_import(
        self: &Arc<Self>,
        opctx: &OpContext,
        disk_lookup: &lookup::Disk<'_>,
        finalize_params: &params::FinalizeDisk,
    ) -> UpdateResult<()> {
        let (authz_silo, authz_proj, authz_disk) =
            disk_lookup.lookup_for(authz::Action::Modify).await?;

        let saga_params = sagas::finalize_disk::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            silo_id: authz_silo.id(),
            project_id: authz_proj.id(),
            disk_id: authz_disk.id(),
            snapshot_name: finalize_params.snapshot_name.clone(),
        };

        self.sagas
            .saga_execute::<sagas::finalize_disk::SagaFinalizeDisk>(saga_params)
            .await?;

        Ok(())
    }
}
