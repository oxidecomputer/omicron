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
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::identity::Resource;
use crate::db::lookup::LookupPath;
use crate::db::model::Disk;
use crate::db::model::DiskRuntimeState;
use crate::db::model::DiskUpdate;
use crate::db::model::Instance;
use crate::db::model::Name;
use crate::db::model::Project;
use crate::db::pagination::paginated;
use crate::db::queries::disk::DiskSetClauseForAttach;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::update_and_check::UpdateStatus;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use omicron_common::api;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use omicron_common::bail_unless;
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
        use db::schema::disk::dsl;

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
        use db::schema::disk::dsl;

        opctx.authorize(authz::Action::CreateChild, authz_project).await?;

        let gen = disk.runtime().gen;
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
            runtime.gen == gen,
            "newly-created Disk has unexpected generation: {:?}",
            runtime.gen
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

        use db::schema::disk::dsl;
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
        use db::schema::{disk, instance};

        opctx.authorize(authz::Action::Modify, authz_instance).await?;
        opctx.authorize(authz::Action::Modify, authz_disk).await?;

        let ok_to_attach_disk_states = vec![
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
            db::model::InstanceState(api::external::InstanceState::Creating),
            db::model::InstanceState(api::external::InstanceState::Stopped),
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
        .or_else(|e| {
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
        use db::schema::{disk, instance};

        opctx.authorize(authz::Action::Modify, authz_instance).await?;
        opctx.authorize(authz::Action::Modify, authz_disk).await?;

        let ok_to_detach_disk_states =
            vec![api::external::DiskState::Attached(authz_instance.id())];
        let ok_to_detach_disk_state_labels: Vec<_> =
            ok_to_detach_disk_states.iter().map(|s| s.label()).collect();

        // TODO(https://github.com/oxidecomputer/omicron/issues/811):
        // This list of instance detach states is more restrictive than it
        // plausibly could be.
        //
        // We currently only permit detaching disks from stopped instances.
        let ok_to_detach_instance_states = vec![
            db::model::InstanceState(api::external::InstanceState::Creating),
            db::model::InstanceState(api::external::InstanceState::Stopped),
            db::model::InstanceState(api::external::InstanceState::Failed),
        ];

        let detached_label = api::external::DiskState::Detached.label();

        let disk = Instance::detach_resource(
            authz_instance.id(),
            authz_disk.id(),
            instance::table
                .into_boxed()
                .filter(instance::dsl::state
                        .eq_any(ok_to_detach_instance_states)
                        .and(instance::dsl::active_propolis_id.is_null())),
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
        .or_else(|e| {
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
                                        "cannot attach disk: instance is not \
                                        fully stopped"
                                    )
                                );
                            }
                            match collection.runtime_state.nexus_state.state() {
                                // Ok-to-be-detached instance states:
                                api::external::InstanceState::Creating |
                                api::external::InstanceState::Stopped => {
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
        use db::schema::disk::dsl;
        let updated = diesel::update(dsl::disk)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(disk_id))
            .filter(dsl::state_generation.lt(new_runtime.gen))
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
        use db::schema::disk::dsl;
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
        use db::schema::disk::dsl;
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
    /// Returns the volume ID of associated with the deleted disk.
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
        use db::schema::disk::dsl;
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
                    return Err(Error::InvalidRequest {
                        message: format!(
                            "disk cannot be deleted in state \"{}\"",
                            disk.runtime_state.disk_state
                        ),
                    });
                } else if disk_state.is_attached() {
                    return Err(Error::InvalidRequest {
                        message: String::from("disk is attached"),
                    });
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
}
