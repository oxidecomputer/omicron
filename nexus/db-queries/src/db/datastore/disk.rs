// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Disk`]s.

use super::DataStore;
use crate::authz;
use crate::authz::ApiResource;
use crate::context::OpContext;
use crate::db;
use crate::db::collection_attach::AttachError;
use crate::db::collection_attach::DatastoreAttachTarget;
use crate::db::collection_detach::DatastoreDetachTarget;
use crate::db::collection_detach::DetachError;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::identity::Resource;
use crate::db::model::Disk;
use crate::db::model::DiskRuntimeState;
use crate::db::model::DiskUpdate;
use crate::db::model::Instance;
use crate::db::model::Name;
use crate::db::model::Project;
use crate::db::model::VirtualProvisioningResource;
use crate::db::model::Volume;
use crate::db::model::to_db_typed_uuid;
use crate::db::pagination::paginated;
use crate::db::queries::disk::DiskSetClauseForAttach;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::update_and_check::UpdateStatus;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::DateTime;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::LookupPath;
use omicron_common::api;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::bail_unless;
use omicron_uuid_kinds::VolumeUuid;
use ref_cast::RefCast;
use std::net::SocketAddrV6;
use uuid::Uuid;

impl DataStore {
    /// List disks associated with a given instance by name.
    pub async fn instance_list_disks(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<Disk> {
        use nexus_db_schema::schema::disk::dsl;

        opctx.authorize(authz::Action::ListChildren, authz_instance).await?;

        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::disk, dsl::id, &pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::disk,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::time_deleted.is_null())
        .filter(dsl::attach_instance_id.eq(authz_instance.id()))
        .select(Disk::as_select())
        .load_async::<Disk>(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn project_create_disk(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        disk: Disk,
    ) -> CreateResult<Disk> {
        use nexus_db_schema::schema::disk::dsl;

        opctx.authorize(authz::Action::CreateChild, authz_project).await?;

        let generation = disk.runtime().generation;
        let name = disk.name().clone();
        let project_id = disk.project_id;

        let disk: Disk = Project::insert_resource(
            project_id,
            diesel::insert_into(dsl::disk)
                .values(disk)
                .on_conflict(dsl::id)
                .do_update()
                .set(dsl::time_modified.eq(dsl::time_modified)),
        )
        .insert_and_get_result_async(
            &*self.pool_connection_authorized(opctx).await?,
        )
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => authz_project.not_found(),
            AsyncInsertError::DatabaseError(e) => public_error_from_diesel(
                e,
                ErrorHandler::Conflict(ResourceType::Disk, name.as_str()),
            ),
        })?;

        let runtime = disk.runtime();
        bail_unless!(
            runtime.state().state() == &api::external::DiskState::Creating,
            "newly-created Disk has unexpected state: {:?}",
            runtime.disk_state
        );
        bail_unless!(
            runtime.generation == generation,
            "newly-created Disk has unexpected generation: {:?}",
            runtime.generation
        );
        Ok(disk)
    }

    pub async fn disk_list(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<Disk> {
        opctx.authorize(authz::Action::ListChildren, authz_project).await?;

        use nexus_db_schema::schema::disk::dsl;
        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::disk, dsl::id, &pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::disk,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::time_deleted.is_null())
        .filter(dsl::project_id.eq(authz_project.id()))
        .select(Disk::as_select())
        .load_async::<Disk>(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Attaches a disk to an instance, if both objects:
    /// - Exist
    /// - Are in valid states
    /// - Are under the maximum "attach count" threshold
    pub async fn instance_attach_disk(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        authz_disk: &authz::Disk,
        max_disks: u32,
    ) -> Result<(Instance, Disk), Error> {
        use nexus_db_schema::schema::{disk, instance};

        opctx.authorize(authz::Action::Modify, authz_instance).await?;
        opctx.authorize(authz::Action::Modify, authz_disk).await?;

        let ok_to_attach_disk_states = [
            api::external::DiskState::Creating,
            api::external::DiskState::Detached,
        ];
        let ok_to_attach_disk_state_labels: Vec<_> =
            ok_to_attach_disk_states.iter().map(|s| s.label()).collect();

        // TODO(https://github.com/oxidecomputer/omicron/issues/811):
        // This list of instance attach states is more restrictive than it
        // plausibly could be.
        //
        // We currently only permit attaching disks to stopped instances.
        let ok_to_attach_instance_states = vec![
            db::model::InstanceState::Creating,
            db::model::InstanceState::NoVmm,
        ];

        let attach_update = DiskSetClauseForAttach::new(authz_instance.id());

        let query = Instance::attach_resource(
            authz_instance.id(),
            authz_disk.id(),
            instance::table.into_boxed().filter(
                instance::dsl::state
                    .eq_any(ok_to_attach_instance_states)
                    .and(instance::dsl::active_propolis_id.is_null()),
            ),
            disk::table.into_boxed().filter(
                disk::dsl::disk_state.eq_any(ok_to_attach_disk_state_labels),
            ),
            max_disks,
            diesel::update(disk::dsl::disk).set(attach_update),
        );

        let (instance, disk) = query.attach_and_get_result_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .or_else(|e: AttachError<Disk, _, _>| {
            match e {
                AttachError::CollectionNotFound => {
                    Err(Error::not_found_by_id(
                        ResourceType::Instance,
                        &authz_instance.id(),
                    ))
                },
                AttachError::ResourceNotFound => {
                    Err(Error::not_found_by_id(
                        ResourceType::Disk,
                        &authz_disk.id(),
                    ))
                },
                AttachError::NoUpdate { attached_count, resource, collection } => {
                    let disk_state = resource.state().into();
                    match disk_state {
                        // Idempotent errors: We did not perform an update,
                        // because we're already in the process of attaching.
                        api::external::DiskState::Attached(id) if id == authz_instance.id() => {
                            return Ok((collection, resource));
                        }
                        api::external::DiskState::Attaching(id) if id == authz_instance.id() => {
                            return Ok((collection, resource));
                        }
                        // Ok-to-attach disk states: Inspect the state to infer
                        // why we did not attach.
                        api::external::DiskState::Creating |
                        api::external::DiskState::Detached => {
                            if collection.runtime_state.propolis_id.is_some() {
                                return Err(
                                    Error::invalid_request(
                                        "cannot attach disk: instance is not \
                                        fully stopped"
                                    )
                                );
                            }
                            match collection.runtime_state.nexus_state.state() {
                                // Ok-to-be-attached instance states:
                                api::external::InstanceState::Creating |
                                api::external::InstanceState::Stopped => {
                                    // The disk is ready to be attached, and the
                                    // instance is ready to be attached. Perhaps
                                    // we are at attachment capacity?
                                    if attached_count == i64::from(max_disks) {
                                        return Err(Error::invalid_request(&format!(
                                            "cannot attach more than {} disks to instance",
                                            max_disks
                                        )));
                                    }

                                    // We can't attach, but the error hasn't
                                    // helped us infer why.
                                    return Err(Error::internal_error(
                                        "cannot attach disk"
                                    ));
                                }
                                // Not okay-to-be-attached instance states:
                                _ => {
                                    Err(Error::invalid_request(&format!(
                                        "cannot attach disk to instance in {} state",
                                        collection.runtime_state.nexus_state.state(),
                                    )))
                                }
                            }
                        },
                        // Not-okay-to-attach disk states: The disk is attached elsewhere.
                        api::external::DiskState::Attached(_) |
                        api::external::DiskState::Attaching(_) |
                        api::external::DiskState::Detaching(_) => {
                            Err(Error::invalid_request(&format!(
                                "cannot attach disk \"{}\": disk is attached to another instance",
                                resource.name().as_str(),
                            )))
                        }
                        _ => {
                            Err(Error::invalid_request(&format!(
                                "cannot attach disk \"{}\": invalid state {}",
                                resource.name().as_str(),
                                disk_state,
                            )))
                        }
                    }
                },
                AttachError::DatabaseError(e) => {
                    Err(public_error_from_diesel(e, ErrorHandler::Server))
                },
            }
        })?;

        Ok((instance, disk))
    }

    pub async fn instance_detach_disk(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        authz_disk: &authz::Disk,
    ) -> Result<Disk, Error> {
        use nexus_db_schema::schema::{disk, instance};

        opctx.authorize(authz::Action::Modify, authz_instance).await?;
        opctx.authorize(authz::Action::Modify, authz_disk).await?;

        let ok_to_detach_disk_states =
            [api::external::DiskState::Attached(authz_instance.id())];
        let ok_to_detach_disk_state_labels: Vec<_> =
            ok_to_detach_disk_states.iter().map(|s| s.label()).collect();

        // TODO(https://github.com/oxidecomputer/omicron/issues/811):
        // This list of instance detach states is more restrictive than it
        // plausibly could be.
        //
        // We currently only permit detaching disks from stopped instances.
        let ok_to_detach_instance_states = vec![
            db::model::InstanceState::Creating,
            db::model::InstanceState::NoVmm,
            db::model::InstanceState::Failed,
        ];

        let detached_label = api::external::DiskState::Detached.label();

        let disk = Instance::detach_resource(
            authz_instance.id(),
            authz_disk.id(),
            instance::table
                .into_boxed()
                .filter(instance::dsl::state
                        .eq_any(ok_to_detach_instance_states)
                        .and(instance::dsl::active_propolis_id.is_null())
                        .and(
                            instance::dsl::boot_disk_id.ne(authz_disk.id())
                                .or(instance::dsl::boot_disk_id.is_null())
                        )),
            disk::table
                .into_boxed()
                .filter(disk::dsl::disk_state.eq_any(ok_to_detach_disk_state_labels)),
            diesel::update(disk::dsl::disk)
                .set((
                    disk::dsl::disk_state.eq(detached_label),
                    disk::dsl::attach_instance_id.eq(Option::<Uuid>::None),
                    disk::dsl::slot.eq(Option::<i16>::None)
                ))
        )
        .detach_and_get_result_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .or_else(|e: DetachError<Disk, _, _>| {
            match e {
                DetachError::CollectionNotFound => {
                    Err(Error::not_found_by_id(
                        ResourceType::Instance,
                        &authz_instance.id(),
                    ))
                },
                DetachError::ResourceNotFound => {
                    Err(Error::not_found_by_id(
                        ResourceType::Disk,
                        &authz_disk.id(),
                    ))
                },
                DetachError::NoUpdate { resource, collection } => {
                    let disk_state = resource.state().into();
                    match disk_state {
                        // Idempotent errors: We did not perform an update,
                        // because we're already in the process of detaching.
                        api::external::DiskState::Detached => {
                            return Ok(resource);
                        }
                        api::external::DiskState::Detaching(id) if id == authz_instance.id() => {
                            return Ok(resource);
                        }
                        // Ok-to-detach disk states: Inspect the state to infer
                        // why we did not detach.
                        api::external::DiskState::Attached(id) if id == authz_instance.id() => {
                            if collection.runtime_state.propolis_id.is_some() {
                                return Err(
                                    Error::invalid_request(
                                        "cannot detach disk: instance is not \
                                        fully stopped"
                                    )
                                );
                            }
                            match collection.runtime_state.nexus_state.state() {
                                // Ok-to-be-detached instance states:
                                api::external::InstanceState::Creating |
                                api::external::InstanceState::Stopped => {
                                    if collection.boot_disk_id == Some(authz_disk.id()) {
                                        return Err(Error::conflict(
                                            "boot disk cannot be detached"
                                        ));
                                    }

                                    // We can't detach, but the error hasn't
                                    // helped us infer why.
                                    return Err(Error::internal_error(
                                        "cannot detach disk"
                                    ));
                                }
                                // Not okay-to-be-detached instance states:
                                _ => {
                                    Err(Error::invalid_request(&format!(
                                        "cannot detach disk from instance in {} state",
                                        collection.runtime_state.nexus_state.state(),
                                    )))
                                }
                            }
                        },
                        api::external::DiskState::Attaching(id) if id == authz_instance.id() => {
                            Err(Error::invalid_request(&format!(
                                "cannot detach disk \"{}\": disk is currently being attached",
                                resource.name().as_str(),
                            )))
                        },
                        // Not-okay-to-detach disk states: The disk is attached elsewhere.
                        api::external::DiskState::Attached(_) |
                        api::external::DiskState::Attaching(_) |
                        api::external::DiskState::Detaching(_) => {
                            Err(Error::invalid_request(&format!(
                                "cannot detach disk \"{}\": disk is attached to another instance",
                                resource.name().as_str(),
                            )))
                        }
                        _ => {
                            Err(Error::invalid_request(&format!(
                                "cannot detach disk \"{}\": invalid state {}",
                                resource.name().as_str(),
                                disk_state,
                            )))
                        }
                    }
                },
                DetachError::DatabaseError(e) => {
                    Err(public_error_from_diesel(e, ErrorHandler::Server))
                },
            }
        })?;

        Ok(disk)
    }

    pub async fn disk_update_runtime(
        &self,
        opctx: &OpContext,
        authz_disk: &authz::Disk,
        new_runtime: &DiskRuntimeState,
    ) -> Result<bool, Error> {
        // TODO-security This permission might be overloaded here.  The way disk
        // runtime updates work is that the caller in Nexus first updates the
        // Sled Agent to make a change, then updates to the database to reflect
        // that change.  So by the time we get here, we better have already done
        // an authz check, or we will have already made some unauthorized change
        // to the system!  At the same time, we don't want just anybody to be
        // able to modify the database state.  So we _do_ still want an authz
        // check here.  Arguably it's for a different kind of action, but it
        // doesn't seem that useful to split it out right now.
        opctx.authorize(authz::Action::Modify, authz_disk).await?;

        let disk_id = authz_disk.id();
        use nexus_db_schema::schema::disk::dsl;
        let updated = diesel::update(dsl::disk)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(disk_id))
            .filter(dsl::state_generation.lt(new_runtime.generation))
            .set(new_runtime.clone())
            .check_if_exists::<Disk>(disk_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map(|r| match r.status {
                UpdateStatus::Updated => true,
                UpdateStatus::NotUpdatedButExists => false,
            })
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_disk),
                )
            })?;

        Ok(updated)
    }

    pub async fn disk_set_pantry(
        &self,
        opctx: &OpContext,
        authz_disk: &authz::Disk,
        pantry_address: SocketAddrV6,
    ) -> UpdateResult<bool> {
        opctx.authorize(authz::Action::Modify, authz_disk).await?;

        let disk_id = authz_disk.id();
        use nexus_db_schema::schema::disk::dsl;
        let updated = diesel::update(dsl::disk)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(disk_id))
            .set(dsl::pantry_address.eq(pantry_address.to_string()))
            .check_if_exists::<Disk>(disk_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map(|r| match r.status {
                UpdateStatus::Updated => true,
                UpdateStatus::NotUpdatedButExists => false,
            })
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_disk),
                )
            })?;

        Ok(updated)
    }

    pub async fn disk_clear_pantry(
        &self,
        opctx: &OpContext,
        authz_disk: &authz::Disk,
    ) -> UpdateResult<bool> {
        opctx.authorize(authz::Action::Modify, authz_disk).await?;

        let disk_id = authz_disk.id();
        use nexus_db_schema::schema::disk::dsl;
        let updated = diesel::update(dsl::disk)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(disk_id))
            .set(&DiskUpdate { pantry_address: None })
            .check_if_exists::<Disk>(disk_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map(|r| match r.status {
                UpdateStatus::Updated => true,
                UpdateStatus::NotUpdatedButExists => false,
            })
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_disk),
                )
            })?;

        Ok(updated)
    }

    /// Fetches information about a Disk that the caller has previously fetched
    ///
    /// The only difference between this function and a new fetch by id is that
    /// this function preserves the `authz_disk` that you started with -- which
    /// keeps track of how you looked it up.  So if you looked it up by name,
    /// the authz you get back will reflect that, whereas if you did a fresh
    /// lookup by id, it wouldn't.
    /// TODO-cleanup this could be provided by the Lookup API for any resource
    pub async fn disk_refetch(
        &self,
        opctx: &OpContext,
        authz_disk: &authz::Disk,
    ) -> LookupResult<Disk> {
        let (.., db_disk) = LookupPath::new(opctx, self)
            .disk_id(authz_disk.id())
            .fetch()
            .await
            .map_err(|e| match e {
                // Use the "not found" message of the authz object we were
                // given, which will reflect however the caller originally
                // looked it up.
                Error::ObjectNotFound { .. } => authz_disk.not_found(),
                e => e,
            })?;
        Ok(db_disk)
    }

    /// Updates a disk record to indicate it has been deleted.
    ///
    /// Returns the disk before any modifications are made by this function.
    ///
    /// Does not attempt to modify any resources (e.g. regions) which may
    /// belong to the disk.
    // TODO: Delete me (this function, not the disk!), ensure all datastore
    // access is auth-checked.
    //
    // Here's the deal: We have auth checks on access to the database - at the
    // time of writing this comment, only a subset of access is protected, and
    // "Delete Disk" is actually one of the first targets of this auth check.
    //
    // However, there are contexts where we want to delete disks *outside* of
    // calling the HTTP API-layer "delete disk" endpoint. As one example, during
    // the "undo" part of the disk creation saga, we want to allow users to
    // delete the disk they (partially) created.
    //
    // This gets a little tricky mapping back to user permissions - a user
    // SHOULD be able to create a disk with the "create" permission, without the
    // "delete" permission. To still make the call internally, we'd basically
    // need to manufacture a token that identifies the ability to "create a
    // disk, or delete a very specific disk with ID = ...".
    pub async fn project_delete_disk_no_auth(
        &self,
        disk_id: &Uuid,
        ok_to_delete_states: &[api::external::DiskState],
    ) -> Result<db::model::Disk, Error> {
        use nexus_db_schema::schema::disk::dsl;
        let conn = self.pool_connection_unauthorized().await?;
        let now = Utc::now();

        let ok_to_delete_state_labels: Vec<_> =
            ok_to_delete_states.iter().map(|s| s.label()).collect();
        let destroyed = api::external::DiskState::Destroyed.label();

        let result = diesel::update(dsl::disk)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(*disk_id))
            .filter(dsl::disk_state.eq_any(ok_to_delete_state_labels))
            .filter(dsl::attach_instance_id.is_null())
            .set((dsl::disk_state.eq(destroyed), dsl::time_deleted.eq(now)))
            .check_if_exists::<Disk>(*disk_id)
            .execute_and_check(&conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Disk,
                        LookupType::ById(*disk_id),
                    ),
                )
            })?;

        match result.status {
            UpdateStatus::Updated => Ok(result.found),
            UpdateStatus::NotUpdatedButExists => {
                let disk = result.found;
                let disk_state = disk.state();
                if disk.time_deleted().is_some()
                    && disk_state.state()
                        == &api::external::DiskState::Destroyed
                {
                    // To maintain idempotency, if the disk has already been
                    // destroyed, don't throw an error.
                    return Ok(disk);
                } else if !ok_to_delete_states.contains(disk_state.state()) {
                    return Err(Error::invalid_request(format!(
                        "disk cannot be deleted in state \"{}\"",
                        disk.runtime_state.disk_state
                    )));
                } else if disk_state.is_attached() {
                    return Err(Error::invalid_request("disk is attached"));
                } else {
                    // NOTE: This is a "catch-all" error case, more specific
                    // errors should be preferred as they're more actionable.
                    return Err(Error::InternalError {
                        internal_message: String::from(
                            "disk exists, but cannot be deleted",
                        ),
                    });
                }
            }
        }
    }

    /// Set a disk to faulted and un-delete it
    ///
    /// If the disk delete saga unwinds, then the disk should _not_ remain
    /// deleted: disk delete saga should be triggered again in order to fully
    /// complete, and the only way to do that is to un-delete the disk. Set it
    /// to faulted to ensure that it won't be used. Use the disk's UUID as part
    /// of its new name to ensure that even if a user created another disk that
    /// shadows this "phantom" disk the original can still be un-deleted and
    /// faulted.
    ///
    /// It's worth pointing out that it's possible that the user created a disk,
    /// then used that disk's ID to make a new disk with the same name as this
    /// function would have picked when undeleting the original disk. In the
    /// event that the original disk's delete saga unwound, this would cause
    /// that unwind to fail at this step, and would cause a stuck saga that
    /// requires manual intervention. The fixes as part of addressing issue 3866
    /// should greatly reduce the number of disk delete sagas that unwind, but
    /// this possibility does exist. To any customer reading this: please don't
    /// name your disks `deleted-{another disk's id}` :)
    pub async fn project_undelete_disk_set_faulted_no_auth(
        &self,
        disk_id: &Uuid,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::disk::dsl;
        let conn = self.pool_connection_unauthorized().await?;

        let faulted = api::external::DiskState::Faulted.label();

        // If only the UUID is used, you will hit "name cannot be a UUID to
        // avoid ambiguity with IDs". Add a small prefix to avoid this, and use
        // "deleted" to be unambigious to the user about what they should do
        // with this disk.
        let new_name = format!("deleted-{disk_id}");

        let result = diesel::update(dsl::disk)
            .filter(dsl::time_deleted.is_not_null())
            .filter(dsl::id.eq(*disk_id))
            .set((
                dsl::time_deleted.eq(None::<DateTime<Utc>>),
                dsl::disk_state.eq(faulted),
                dsl::name.eq(new_name),
            ))
            .check_if_exists::<Disk>(*disk_id)
            .execute_and_check(&conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Disk,
                        LookupType::ById(*disk_id),
                    ),
                )
            })?;

        match result.status {
            UpdateStatus::Updated => Ok(()),
            UpdateStatus::NotUpdatedButExists => {
                let disk = result.found;
                let disk_state = disk.state();

                if disk.time_deleted().is_none()
                    && disk_state.state() == &api::external::DiskState::Faulted
                {
                    // To maintain idempotency, if the disk has already been
                    // faulted, don't throw an error.
                    return Ok(());
                } else {
                    // NOTE: This is a "catch-all" error case, more specific
                    // errors should be preferred as they're more actionable.
                    return Err(Error::InternalError {
                        internal_message: String::from(
                            "disk exists, but cannot be faulted",
                        ),
                    });
                }
            }
        }
    }

    /// Find disks that have been deleted but still have a
    /// `virtual_provisioning_resource` record: this indicates that a disk
    /// delete saga partially succeeded, then unwound, which (before the fixes
    /// in customer-support#58) would mean the disk was deleted but the project
    /// it was in could not be deleted (due to an erroneous number of bytes
    /// "still provisioned").
    pub async fn find_phantom_disks(&self) -> ListResultVec<Disk> {
        use nexus_db_schema::schema::disk::dsl;
        use nexus_db_schema::schema::virtual_provisioning_resource::dsl as resource_dsl;
        use nexus_db_schema::schema::volume::dsl as volume_dsl;

        let conn = self.pool_connection_unauthorized().await?;

        let potential_phantom_disks: Vec<(
            Disk,
            Option<VirtualProvisioningResource>,
            Option<Volume>,
        )> = dsl::disk
            .filter(dsl::time_deleted.is_not_null())
            .left_join(
                resource_dsl::virtual_provisioning_resource
                    .on(resource_dsl::id.eq(dsl::id)),
            )
            .left_join(volume_dsl::volume.on(dsl::volume_id.eq(volume_dsl::id)))
            .select((
                Disk::as_select(),
                Option::<VirtualProvisioningResource>::as_select(),
                Option::<Volume>::as_select(),
            ))
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        // The first forward steps of the disk delete saga (plus the volume
        // delete sub saga) are as follows:
        //
        // 1. soft-delete the disk
        // 2. call virtual_provisioning_collection_delete_disk
        // 3. soft-delete the disk's volume
        //
        // Before the fixes as part of customer-support#58, steps 1 and 3 did
        // not have undo steps, where step 2 did. In order to detect when the
        // disk delete saga unwound, find entries where
        //
        // 1. the disk and volume are soft-deleted
        // 2. the `virtual_provisioning_resource` exists
        //
        // It's important not to conflict with any currently running disk delete
        // saga.

        Ok(potential_phantom_disks
            .into_iter()
            .filter(|(disk, resource, volume)| {
                if let Some(volume) = volume {
                    // In this branch, the volume record exists. Because it was
                    // returned by the query above, if it is soft-deleted we
                    // then know the saga unwound before the volume record could
                    // be hard deleted. This won't conflict with a running disk
                    // delete saga, because the resource record should be None
                    // if the disk and volume were already soft deleted (if
                    // there is one, the saga will be at or past step 3).
                    disk.time_deleted().is_some()
                        && volume.time_deleted.is_some()
                        && resource.is_some()
                } else {
                    // In this branch, the volume record was hard-deleted. The
                    // saga could still have unwound after hard deleting the
                    // volume record, so proceed with filtering. This won't
                    // conflict with a running disk delete saga because the
                    // resource record should be None if the disk was soft
                    // deleted and the volume was hard deleted (if there is one,
                    // the saga should be almost finished as the volume hard
                    // delete is the last thing it does).
                    disk.time_deleted().is_some() && resource.is_some()
                }
            })
            .map(|(disk, _, _)| disk)
            .collect())
    }

    pub async fn disk_for_volume_id(
        &self,
        volume_id: VolumeUuid,
    ) -> LookupResult<Option<Disk>> {
        let conn = self.pool_connection_unauthorized().await?;

        use nexus_db_schema::schema::disk::dsl;
        dsl::disk
            .filter(dsl::volume_id.eq(to_db_typed_uuid(volume_id)))
            .select(Disk::as_select())
            .first_async(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::db::pub_test_utils::TestDatabase;
    use nexus_types::external_api::params;
    use omicron_common::api::external;
    use omicron_test_utils::dev;

    #[tokio::test]
    async fn test_undelete_disk_set_faulted_idempotent() {
        let logctx =
            dev::test_setup_log("test_undelete_disk_set_faulted_idempotent");
        let log = logctx.log.new(o!());
        let db = TestDatabase::new_with_datastore(&log).await;
        let (opctx, db_datastore) = (db.opctx(), db.datastore());

        let silo_id = opctx.authn.actor().unwrap().silo_id().unwrap();

        let (authz_project, _db_project) = db_datastore
            .project_create(
                &opctx,
                Project::new(
                    silo_id,
                    params::ProjectCreate {
                        identity: external::IdentityMetadataCreateParams {
                            name: "testpost".parse().unwrap(),
                            description: "please ignore".to_string(),
                        },
                    },
                ),
            )
            .await
            .unwrap();

        let disk = db_datastore
            .project_create_disk(
                &opctx,
                &authz_project,
                Disk::new(
                    Uuid::new_v4(),
                    authz_project.id(),
                    VolumeUuid::new_v4(),
                    params::DiskCreate {
                        identity: external::IdentityMetadataCreateParams {
                            name: "first-post".parse().unwrap(),
                            description: "just trying things out".to_string(),
                        },
                        disk_source: params::DiskSource::Blank {
                            block_size: params::BlockSize::try_from(512)
                                .unwrap(),
                        },
                        size: external::ByteCount::from(2147483648),
                    },
                    db::model::BlockSize::Traditional,
                    DiskRuntimeState::new(),
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let (.., authz_disk, db_disk) = LookupPath::new(&opctx, db_datastore)
            .disk_id(disk.id())
            .fetch()
            .await
            .unwrap();

        db_datastore
            .disk_update_runtime(
                &opctx,
                &authz_disk,
                &db_disk.runtime().detach(),
            )
            .await
            .unwrap();

        db_datastore
            .project_delete_disk_no_auth(
                &authz_disk.id(),
                &[external::DiskState::Detached],
            )
            .await
            .unwrap();

        // Assert initial state - deleting the Disk will make LookupPath::fetch
        // not work.
        {
            LookupPath::new(&opctx, db_datastore)
                .disk_id(disk.id())
                .fetch()
                .await
                .unwrap_err();
        }

        // Function under test: call this twice to ensure it's idempotent

        db_datastore
            .project_undelete_disk_set_faulted_no_auth(&authz_disk.id())
            .await
            .unwrap();

        // Assert state change

        {
            let (.., db_disk) = LookupPath::new(&opctx, db_datastore)
                .disk_id(disk.id())
                .fetch()
                .await
                .unwrap();

            assert!(db_disk.time_deleted().is_none());
            assert_eq!(
                db_disk.runtime().disk_state,
                external::DiskState::Faulted.label().to_string()
            );
        }

        db_datastore
            .project_undelete_disk_set_faulted_no_auth(&authz_disk.id())
            .await
            .unwrap();

        // Assert state is the same after the second call

        {
            let (.., db_disk) = LookupPath::new(&opctx, db_datastore)
                .disk_id(disk.id())
                .fetch()
                .await
                .unwrap();

            assert!(db_disk.time_deleted().is_none());
            assert_eq!(
                db_disk.runtime().disk_state,
                external::DiskState::Faulted.label().to_string()
            );
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
