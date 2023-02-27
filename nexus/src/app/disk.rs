// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Disks and snapshots

use crate::app::sagas;
use crate::authn;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::lookup;
use crate::db::lookup::LookupPath;
use crate::db::model::Name;
use crate::external_api::params;
use nexus_types::identity::Resource;
use omicron_common::api::external::http_pagination::PaginatedBy;
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
use omicron_common::api::internal::nexus::DiskRuntimeState;
use ref_cast::RefCast;
use sled_agent_client::Client as SledAgentClient;
use std::sync::Arc;
use uuid::Uuid;

fn validate_disk_create_params(
    params: &params::DiskCreate,
    block_size: u64,
) -> Result<(), Error> {
    // Reject disks where the block size doesn't evenly divide the
    // total size
    if (params.size.to_bytes() % block_size) != 0 {
        return Err(Error::InvalidValue {
            label: String::from("size and block_size"),
            message: format!(
                "total size must be a multiple of block size {}",
                block_size,
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
    if (params.size.to_bytes() % params::MIN_DISK_SIZE_BYTES as u64) != 0 {
        return Err(Error::InvalidValue {
            label: String::from("size"),
            message: format!(
                "total size must be a multiple of {}",
                ByteCount::from(params::MIN_DISK_SIZE_BYTES)
            ),
        });
    }

    Ok(())
}

impl super::Nexus {
    // Disks
    pub fn disk_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        disk_selector: &'a params::DiskSelector,
    ) -> LookupResult<lookup::Disk<'a>> {
        match disk_selector {
            params::DiskSelector {
                disk: NameOrId::Id(id),
                project_selector: None,
            } => {
                let disk =
                    LookupPath::new(opctx, &self.db_datastore).disk_id(*id);
                Ok(disk)
            }
            params::DiskSelector {
                disk: NameOrId::Name(name),
                project_selector: Some(project_selector),
            } => {
                let disk = self
                    .project_lookup(opctx, project_selector)?
                    .disk_name(Name::ref_cast(name));
                Ok(disk)
            }
            params::DiskSelector {
                disk: NameOrId::Id(_),
                project_selector: Some(_),
            } => Err(Error::invalid_request(
                "when providing disk as an ID, project should not be specified",
            )),
            _ => Err(Error::invalid_request(
                "disk should either be UUID or project should be specified",
            )),
        }
    }

    pub async fn project_create_disk(
        self: &Arc<Self>,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        params: &params::DiskCreate,
    ) -> CreateResult<db::model::Disk> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::CreateChild).await?;

        match &params.disk_source {
            params::DiskSource::Blank { block_size } => {
                validate_disk_create_params(&params, (*block_size).into())?;
            }
            params::DiskSource::Snapshot { snapshot_id } => {
                let (.., db_snapshot) =
                    LookupPath::new(opctx, &self.db_datastore)
                        .snapshot_id(*snapshot_id)
                        .fetch()
                        .await?;

                validate_disk_create_params(
                    &params,
                    db_snapshot.block_size.to_bytes().into(),
                )?;

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

                validate_disk_create_params(
                    &params,
                    db_global_image.block_size.to_bytes().into(),
                )?;

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
            }
            params::DiskSource::ImportingBlocks { block_size } => {
                validate_disk_create_params(&params, (*block_size).into())?;
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

    pub async fn disk_list(
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
        disk_lookup: &lookup::Disk<'_>,
    ) -> DeleteResult {
        let (.., project, authz_disk) =
            disk_lookup.lookup_for(authz::Action::Delete).await?;

        let saga_params = sagas::disk_delete::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            project_id: project.id(),
            disk_id: authz_disk.id(),
        };
        self.execute_saga::<sagas::disk_delete::SagaDiskDelete>(saga_params)
            .await?;
        Ok(())
    }

    /// Remove a read only parent from a disk.
    /// This is just a wrapper around the volume operation of the same
    /// name, but we provide this interface when all the caller has is
    /// the disk UUID as the internal volume_id is not exposed.
    pub async fn disk_remove_read_only_parent(
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

        self.volume_remove_read_only_parent(&opctx, db_disk.volume_id).await?;

        Ok(())
    }

    /// Import blocks from a URL into a disk
    pub async fn import_blocks_from_url_for_disk(
        self: &Arc<Self>,
        opctx: &OpContext,
        disk_selector: &params::DiskSelector,
        params: params::ImportBlocksFromUrl,
    ) -> UpdateResult<()> {
        let authz_disk: authz::Disk;

        (_, _, _, authz_disk) = self
            .disk_lookup(&opctx, &disk_selector)?
            .lookup_for(authz::Action::Modify)
            .await?;

        let saga_params = sagas::import_blocks_from_url::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            disk_id: authz_disk.id(),

            import_params: params.clone(),
        };

        self
            .execute_saga::<sagas::import_blocks_from_url::SagaImportBlocksFromUrl>(saga_params)
            .await?;

        Ok(())
    }

    /// Move a disk from the "ImportReady" state to the "Importing" state,
    /// blocking any import from URL jobs.
    pub async fn disk_manual_import_start(
        self: &Arc<Self>,
        opctx: &OpContext,
        disk_selector: &params::DiskSelector,
    ) -> UpdateResult<()> {
        let authz_disk: authz::Disk;
        let db_disk: db::model::Disk;

        (_, _, _, authz_disk, db_disk) = self
            .disk_lookup(&opctx, &disk_selector)?
            .fetch_for(authz::Action::Modify)
            .await?;

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
    pub async fn disk_manual_import(
        self: &Arc<Self>,
        opctx: &OpContext,
        disk_selector: &params::DiskSelector,
        param: params::ImportBlocksBulkWrite,
    ) -> UpdateResult<()> {
        let db_disk: db::model::Disk;

        (_, _, _, _, db_disk) = self
            .disk_lookup(&opctx, &disk_selector)?
            .fetch_for(authz::Action::Modify)
            .await?;

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

            let client = crucible_pantry_client::Client::new(&format!(
                "http://{}",
                endpoint
            ));
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
    pub async fn disk_manual_import_stop(
        self: &Arc<Self>,
        opctx: &OpContext,
        disk_selector: &params::DiskSelector,
    ) -> UpdateResult<()> {
        let authz_disk: authz::Disk;
        let db_disk: db::model::Disk;

        (_, _, _, authz_disk, db_disk) = self
            .disk_lookup(&opctx, &disk_selector)?
            .fetch_for(authz::Action::Modify)
            .await?;

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
    pub async fn disk_finalize_import(
        self: &Arc<Self>,
        opctx: &OpContext,
        path_disk: NameOrId,
        finalize_params: params::FinalizeDisk,
    ) -> UpdateResult<()> {
        let disk_selector = params::DiskSelector {
            disk: path_disk,
            project_selector: finalize_params.project_selector,
        };

        let authz_silo: authz::Silo;
        let authz_proj: authz::Project;
        let authz_disk: authz::Disk;
        let db_disk: db::model::Disk;

        (authz_silo, _, authz_proj, authz_disk, db_disk) = self
            .disk_lookup(&opctx, &disk_selector)?
            .fetch_for(authz::Action::Modify)
            .await?;

        let saga_params = sagas::finalize_disk::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            silo_id: authz_silo.id(),
            project_id: authz_proj.id(),
            disk_id: authz_disk.id(),
            disk_name: db_disk.name().clone(),
            snapshot_name: finalize_params.snapshot_name,
        };

        self.execute_saga::<sagas::finalize_disk::SagaFinalizeDisk>(
            saga_params,
        )
        .await?;

        Ok(())
    }
}
