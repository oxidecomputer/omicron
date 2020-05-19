/*!
 * HTTP-agnostic interface to an Oxide system
 */

use crate::api_error::ApiError;
use crate::api_model::ApiDisk;
use crate::api_model::ApiDiskAttachment;
use crate::api_model::ApiDiskCreateParams;
use crate::api_model::ApiDiskRuntimeState;
use crate::api_model::ApiDiskState;
use crate::api_model::ApiDiskStateRequested;
use crate::api_model::ApiIdentityMetadata;
use crate::api_model::ApiInstance;
use crate::api_model::ApiInstanceCreateParams;
use crate::api_model::ApiInstanceRuntimeState;
use crate::api_model::ApiInstanceRuntimeStateParams;
use crate::api_model::ApiInstanceState;
use crate::api_model::ApiName;
use crate::api_model::ApiObject;
use crate::api_model::ApiProject;
use crate::api_model::ApiProjectCreateParams;
use crate::api_model::ApiProjectUpdateParams;
use crate::api_model::ApiRack;
use crate::api_model::ApiResourceType;
use crate::api_model::ApiServer;
use crate::datastore::collection_page;
use crate::datastore::ControlDataStore;
use crate::server_controller::ServerController;
use async_trait::async_trait;
use chrono::Utc;
use dropshot::ExtractedParameter;
use futures::future::ready;
use futures::future::TryFutureExt;
use futures::lock::Mutex;
use futures::stream::Stream;
use futures::stream::StreamExt;
use serde::Deserialize;
use slog::Logger;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use uuid::Uuid;

/*
 * These type aliases exist primarily to make it easier to be consistent about
 * return values from this module.
 */

/** Result of a create operation for the specified type. */
pub type CreateResult<T> = Result<Arc<T>, ApiError>;
/** Result of a delete operation for the specified type. */
pub type DeleteResult = Result<(), ApiError>;
/** Result of a list operation that returns an ObjectStream. */
pub type ListResult<T> = Result<ObjectStream<T>, ApiError>;
/** Result of a lookup operation for the specified type. */
pub type LookupResult<T> = Result<Arc<T>, ApiError>;
/** Result of an update operation for the specified type. */
pub type UpdateResult<T> = Result<Arc<T>, ApiError>;

/** A stream of Results, each potentially representing an object in the API. */
pub type ObjectStream<T> =
    Pin<Box<dyn Stream<Item = Result<Arc<T>, ApiError>> + Send>>;

#[derive(Deserialize, ExtractedParameter)]
pub struct PaginationParams<NameType> {
    pub marker: Option<NameType>,
    pub limit: Option<usize>,
}

/**
 * Given an `ObjectStream<ApiObject>` (for some specific `ApiObject` type),
 * return a vector of the objects' views.  Any failures are ignored.
 * TODO-hardening: Consider how to better deal with these failures.  We should
 * probably at least log something.
 */
pub async fn to_view_list<T: ApiObject>(
    object_stream: ObjectStream<T>,
) -> Vec<T::View> {
    object_stream
        .filter(|maybe_object| ready(maybe_object.is_ok()))
        .map(|maybe_object| maybe_object.unwrap().to_view())
        .collect::<Vec<T::View>>()
        .await
}

/**
 * This trait is used to expose interfaces that we only want made available to
 * the test suite.
 */
#[async_trait]
pub trait OxideControllerTestInterfaces {
    /**
     * Returns the ServerController for an Instance from its id.  We may also
     * want to split this up into instance_lookup_by_id() and instance_sc(), but
     * after all it's a test suite special to begin with.
     */
    async fn instance_server_by_id(
        &self,
        id: &Uuid,
    ) -> Result<Arc<ServerController>, ApiError>;
}

/**
 * Represents the state of the Oxide system that we're managing.
 *
 * Right now, this is mostly a wrapper around the data store because this server
 * doesn't do much beyond CRUD operations.  However, higher-level functionality
 * could go here, including caching of objects.
 */
pub struct OxideController {
    /** uuid for this rack (TODO should also be in persistent storage) */
    id: Uuid,

    /** general server log */
    log: Logger,

    /** cached ApiRack structure representing the single rack. */
    api_rack: Arc<ApiRack>,

    /** persistent storage for resources in the control plane */
    datastore: ControlDataStore,

    /**
     * List of controllers in this server.
     * TODO This ought to have some representation in the data store as well so
     * that we don't simply forget about servers that aren't currently up.
     * We'll need to think about the interface between this program and the
     * servers and how we discover them, both when they initially show up and
     * when we come up.
     */
    server_controllers: Mutex<BTreeMap<Uuid, Arc<ServerController>>>,
}

/*
 * TODO Is it possible to make some of these operations more generic?  A
 * particularly good example is probably list() (or even lookup()), where
 * with the right type parameters, generic code can be written to work on all
 * types.
 * TODO update and delete need to accommodate both with-etag and don't-care
 * TODO audit logging ought to be part of this structure and its functions
 */
impl OxideController {
    pub fn new_with_id(id: &Uuid, log: Logger) -> OxideController {
        OxideController {
            id: id.clone(),
            log: log,
            api_rack: Arc::new(ApiRack {
                id: id.clone(),
            }),
            datastore: ControlDataStore::new(),
            server_controllers: Mutex::new(BTreeMap::new()),
        }
    }

    pub async fn add_server_controller(&self, sc: Arc<ServerController>) {
        let mut scs = self.server_controllers.lock().await;
        assert!(!scs.contains_key(&sc.id));
        info!(self.log, "registered server controller";
            "server_uuid" => sc.id.to_string());
        scs.insert(sc.id.clone(), sc);
    }

    /*
     * Projects
     */

    pub async fn project_create(
        &self,
        new_project: &ApiProjectCreateParams,
    ) -> CreateResult<ApiProject> {
        self.datastore.project_create(new_project).await
    }

    pub async fn project_create_with_id(
        &self,
        new_uuid: Uuid,
        new_project: &ApiProjectCreateParams,
    ) -> CreateResult<ApiProject> {
        self.datastore.project_create_with_id(new_uuid, new_project).await
    }

    pub async fn project_lookup(
        &self,
        name: &ApiName,
    ) -> LookupResult<ApiProject> {
        self.datastore.project_lookup(name).await
    }

    pub async fn projects_list(
        &self,
        pagparams: &PaginationParams<ApiName>,
    ) -> ListResult<ApiProject> {
        self.datastore.projects_list(pagparams).await
    }

    pub async fn project_delete(&self, name: &ApiName) -> DeleteResult {
        self.datastore.project_delete(name).await
    }

    pub async fn project_update(
        &self,
        name: &ApiName,
        new_params: &ApiProjectUpdateParams,
    ) -> UpdateResult<ApiProject> {
        self.datastore.project_update(name, new_params).await
    }

    /*
     * Disks
     */

    pub async fn project_list_disks(
        &self,
        project_name: &ApiName,
        pagparams: &PaginationParams<ApiName>,
    ) -> ListResult<ApiDisk> {
        self.datastore.project_list_disks(project_name, pagparams).await
    }

    pub async fn project_create_disk(
        &self,
        project_name: &ApiName,
        params: &ApiDiskCreateParams,
    ) -> CreateResult<ApiDisk> {
        let now = Utc::now();
        let project = self.project_lookup(project_name).await?;
        let disk = Arc::new(ApiDisk {
            identity: ApiIdentityMetadata {
                id: Uuid::new_v4(),
                name: params.identity.name.clone(),
                description: params.identity.description.clone(),
                time_created: now.clone(),
                time_modified: now.clone(),
            },
            project_id: project.identity.id.clone(),
            create_snapshot_id: params.snapshot_id.clone(),
            size: params.size.clone(),
            runtime: ApiDiskRuntimeState {
                disk_state: ApiDiskState::Creating,
                gen: 1,
                time_updated: Utc::now(),
            },
        });

        let disk_created = self.datastore.disk_create(disk).await?;

        /*
         * This is a little hokey.  We'd like to simulate an asynchronous
         * transition from "Creating" to "Detached".  For instances, the
         * simulation lives in a simulated server controller.  Here, the analog
         * might be a simulated disk control plane.  But that doesn't exist yet,
         * and we don't even know what APIs it would provide yet.  So we just
         * carry out the simplest possible "simulation" here: we'll return to
         * the client a structure describing a disk in state "Creating", but by
         * the time we do so, we've already updated the internal representation
         * to "Created".
         */
        let mut new_disk = (*disk_created).clone();
        new_disk.runtime.disk_state = ApiDiskState::Detached;
        new_disk.runtime.gen = new_disk.runtime.gen + 1;
        new_disk.runtime.time_updated = Utc::now();
        self.datastore.disk_update(Arc::new(new_disk)).await?;

        Ok(disk_created)
    }

    pub async fn project_lookup_disk(
        &self,
        project_name: &ApiName,
        disk_name: &ApiName,
    ) -> LookupResult<ApiDisk> {
        self.datastore.project_lookup_disk(project_name, disk_name).await
    }

    /*
     * Instances
     */

    async fn server_allocate_instance<'se, 'params>(
        &'se self,
        servers: &'se BTreeMap<Uuid, Arc<ServerController>>,
        _project: Arc<ApiProject>,
        _params: &'params ApiInstanceCreateParams,
    ) -> Result<&'se Arc<ServerController>, ApiError> {
        /* TODO replace this with a real allocation policy. */
        servers.values().nth(0).ok_or_else(|| ApiError::ResourceNotAvailable {
            message: String::from("no servers available for new Instance"),
        })
    }

    pub async fn project_list_instances(
        &self,
        project_name: &ApiName,
        pagparams: &PaginationParams<ApiName>,
    ) -> ListResult<ApiInstance> {
        self.datastore.project_list_instances(project_name, pagparams).await
    }

    pub async fn project_create_instance(
        &self,
        project_name: &ApiName,
        params: &ApiInstanceCreateParams,
    ) -> CreateResult<ApiInstance> {
        let project = self.project_lookup(project_name).await?;

        /*
         * Allocate a server and retrieve our handle to its SC.
         */
        let sc = {
            let servers = self.server_controllers.lock().await;
            let arc = self
                .server_allocate_instance(&servers, project, params)
                .await?;
            Arc::clone(arc)
        };

        /*
         * The order of operations here is slightly subtle:
         *
         * 1. We want to record the new instance in the database before telling
         *    the SC about it.  This record will have state "Creating" to
         *    distinguish it from other runtime states.  If we crash after this
         *    point, there will be a record in the database for the instance,
         *    but it won't be running, since no SC knows about it.
         *    TODO-robustness How do we want to handle a crash at this point?
         *    One approach would be to say that while an Instance exists in the
         *    "Creating" state, it's not logically present yet.  Thus, if
         *    we crash and leave a record here, there's a small resource leak of
         *    sorts, but there's no problem from the user's perspective.  What
         *    about use of the "name", which is user-controlled and unique?  We
         *    could have the creation process look for an existing instance with
         *    the same name in state "Creating", decide if it appears to be
         *    abandoned by the control plane instance that was working on it,
         *    and then simply delete it and proceed.  We might need to do this
         *    in the Instance rename case as well.  Reliably deciding whether
         *    the record is abandoned seems hard, though.  We would also
         *    probably want something that proactively looks for these abandoned
         *    records and removes them to eliminate the leakage.  A case to
         *    consider is two concurrent attempts to create an Instance with the
         *    same name.  Exactly one should win every time and we shouldn't
         *    wind up with two Instances running at any point.
         *
         * 2. We want to tell the SC about this Instance and have the SC start
         *    it up.  The SC should reply immediately that it's starting the
         *    instance, and we immediately record this into the database, now
         *    with state "Starting".
         *
         * 3. Some time later (after this function has completed), the SC will
         *    notify us that the Instance has changed states to "Running".
         *    We'll record this into the database.
         */
        let runtime = ApiInstanceRuntimeState {
            run_state: ApiInstanceState::Creating,
            reboot_in_progress: false,
            server_uuid: sc.id,
            gen: 1,
            time_updated: Utc::now(),
        };

        /*
         * Store the first revision of the Instance into the database.  This
         * will have state "Creating".
         */
        let instance_created = self
            .datastore
            .project_create_instance(project_name, params, &runtime)
            .await?;
        self.instance_set_runtime(
            instance_created,
            sc,
            ApiInstanceRuntimeStateParams {
                run_state: ApiInstanceState::Running,
                reboot_wanted: false,
            },
        )
        .await
    }

    pub async fn project_destroy_instance(
        &self,
        project_name: &ApiName,
        instance_name: &ApiName,
    ) -> DeleteResult {
        /*
         * TODO-robustness We need to figure out what to do with Destroyed
         * instances?  Presumably we need to clean them up at some point, but
         * not right away so that callers can see that they've been destroyed.
         */
        let instance =
            self.project_lookup_instance(project_name, instance_name).await?;
        let sc = self.instance_sc(&instance).await?;
        let runtime_params = ApiInstanceRuntimeStateParams {
            run_state: ApiInstanceState::Destroyed,
            reboot_wanted: false,
        };
        self.instance_set_runtime(instance, sc, runtime_params).await?;
        Ok(())
    }

    pub async fn project_lookup_instance(
        &self,
        project_name: &ApiName,
        instance_name: &ApiName,
    ) -> LookupResult<ApiInstance> {
        self.datastore
            .project_lookup_instance(project_name, instance_name)
            .await
    }

    pub async fn project_delete_instance(
        &self,
        project_name: &ApiName,
        instance_name: &ApiName,
    ) -> DeleteResult {
        self.datastore
            .project_delete_instance(project_name, instance_name)
            .await
    }

    fn check_runtime_change_allowed(
        &self,
        instance: &Arc<ApiInstance>,
    ) -> Result<(), ApiError> {
        /*
         * Users are allowed to request a start or stop even if the instance is
         * already in the desired state (or moving to it), and we will issue a
         * request to the SC to make the state change in these cases in case the
         * runtime state we saw here was stale.  However, users are not allowed
         * to change the state of an instance that's failed or destroyed.
         */
        let run_state = &instance.runtime.run_state;
        let allowed = match run_state {
            ApiInstanceState::Creating => true,
            ApiInstanceState::Starting => true,
            ApiInstanceState::Running => true,
            ApiInstanceState::Stopping => true,
            ApiInstanceState::Stopped => true,

            ApiInstanceState::Repairing => false,
            ApiInstanceState::Failed => false,
            ApiInstanceState::Destroyed => false,
        };

        if allowed {
            Ok(())
        } else {
            Err(ApiError::InvalidRequest {
                message: format!(
                    "instance state cannot be changed from state \"{}\"",
                    run_state
                ),
            })
        }
    }

    /**
     * Returns the ServerController for the host where this Instance is running.
     */
    async fn instance_sc(
        &self,
        instance: &Arc<ApiInstance>,
    ) -> Result<Arc<ServerController>, ApiError> {
        let controllers = self.server_controllers.lock().await;
        let scid = &instance.runtime.server_uuid;
        Ok(Arc::clone(controllers.get(scid).ok_or_else(|| {
            let message =
                format!("no server controller for server_uuid \"{}\"", scid);
            ApiError::ResourceNotAvailable {
                message: message,
            }
        })?))
    }

    /**
     * Reboot the specified instance.
     */
    pub async fn instance_reboot(
        &self,
        project_name: &ApiName,
        instance_name: &ApiName,
    ) -> UpdateResult<ApiInstance> {
        /*
         * To implement reboot, we issue a call to the server controller to
         * set a runtime state with "reboot_wanted".  We cannot simply stop the
         * Instance and start it again here because if we crash in the meantime,
         * we might leave it stopped.
         *
         * When an instance is rebooted, the "reboot_in_progress" remains set on
         * the runtime state as it transitions to "Stopping" and "Stopped".  This
         * flag is cleared when the state goes to "Starting".  This way, even if
         * the whole rack powered off while this was going on, we would never
         * lose track of the fact that this Instance was supposed to be running.
         */
        let instance = self
            .datastore
            .project_lookup_instance(project_name, instance_name)
            .await?;

        self.check_runtime_change_allowed(&instance)?;
        self.instance_set_runtime(
            Arc::clone(&instance),
            self.instance_sc(&instance).await?,
            ApiInstanceRuntimeStateParams {
                run_state: ApiInstanceState::Running,
                reboot_wanted: true,
            },
        )
        .await
    }

    /**
     * Make sure the given Instance is running.
     */
    pub async fn instance_start(
        &self,
        project_name: &ApiName,
        instance_name: &ApiName,
    ) -> UpdateResult<ApiInstance> {
        let instance = self
            .datastore
            .project_lookup_instance(project_name, instance_name)
            .await?;

        self.check_runtime_change_allowed(&instance)?;
        self.instance_set_runtime(
            Arc::clone(&instance),
            self.instance_sc(&instance).await?,
            ApiInstanceRuntimeStateParams {
                run_state: ApiInstanceState::Running,
                reboot_wanted: false,
            },
        )
        .await
    }

    /**
     * Make sure the given Instance is stopped.
     */
    pub async fn instance_stop(
        &self,
        project_name: &ApiName,
        instance_name: &ApiName,
    ) -> UpdateResult<ApiInstance> {
        let instance = self
            .datastore
            .project_lookup_instance(project_name, instance_name)
            .await?;

        self.check_runtime_change_allowed(&instance)?;
        self.instance_set_runtime(
            Arc::clone(&instance),
            self.instance_sc(&instance).await?,
            ApiInstanceRuntimeStateParams {
                run_state: ApiInstanceState::Stopped,
                reboot_wanted: false,
            },
        )
        .await
    }

    /**
     * Modifies the runtime state of the Instance as requested.  This generally
     * means booting or halting the Instance.
     */
    async fn instance_set_runtime(
        &self,
        mut instance: Arc<ApiInstance>,
        sc: Arc<ServerController>,
        runtime_params: ApiInstanceRuntimeStateParams,
    ) -> UpdateResult<ApiInstance> {
        /*
         * Ask the SC to begin the state change.  Then update the database to
         * reflect the new intermediate state.
         */
        let new_runtime_state =
            sc.instance_ensure(Arc::clone(&instance), runtime_params).await?;

        let instance_ref = Arc::make_mut(&mut instance);
        instance_ref.runtime = new_runtime_state.clone();
        self.datastore.instance_update(Arc::clone(&instance)).await?;
        Ok(instance)
    }

    /**
     * Lists disks attached to the instance.
     */
    pub async fn instance_list_disks(
        &self,
        project_name: &ApiName,
        instance_name: &ApiName,
        pagparams: &PaginationParams<ApiName>,
    ) -> ListResult<ApiDiskAttachment> {
        let instance = self
            .datastore
            .project_lookup_instance(project_name, instance_name)
            .await?;
        let disks =
            self.datastore.instance_list_disks(&instance, pagparams).await?;
        let attachments = disks
            .filter(|maybe_disk| ready(maybe_disk.is_ok()))
            .map(|maybe_disk| {
                let disk = maybe_disk.unwrap();
                Ok(Arc::new(ApiDiskAttachment {
                    instance_name: instance.identity.name.clone(),
                    instance_id: instance.identity.id.clone(),
                    disk_name: disk.identity.name.clone(),
                    disk_id: disk.identity.id.clone(),
                    disk_state: disk.runtime.disk_state.clone(),
                }))
            })
            .collect::<Vec<Result<Arc<ApiDiskAttachment>, ApiError>>>()
            .await;
        Ok(futures::stream::iter(attachments).boxed())
    }

    fn check_disk_change_allowed(
        &self,
        disk: &Arc<ApiDisk>,
    ) -> Result<(), ApiError> {
        /*
         * If another request has been made to destroy or fault the disk, no
         * further requests are allowed.
         * XXX How do we know if one is pending?
         */
        let label = match &disk.runtime.disk_state {
            ApiDiskState::Destroyed => Some("destroyed"),
            ApiDiskState::Faulted => Some("faulted"),
            _ => None,
        };

        if let Some(state) = label {
            Err(ApiError::InvalidRequest {
                message: format!("disk is {}", state),
            })
        } else {
            Ok(())
        }
    }

    /**
     * Attach a disk to an instance.
     */
    pub async fn instance_attach_disk(
        &self,
        project_name: &ApiName,
        instance_name: &ApiName,
        disk_name: &ApiName,
    ) -> CreateResult<ApiDiskAttachment> {
        let instance = self
            .datastore
            .project_lookup_instance(project_name, instance_name)
            .await?;
        let disk =
            self.datastore.project_lookup_disk(project_name, disk_name).await?;
        let instance_id = &instance.identity.id;

        fn disk_attachment_for(
            instance: &Arc<ApiInstance>,
            disk: &Arc<ApiDisk>,
        ) -> CreateResult<ApiDiskAttachment> {
            let instance_id = &instance.identity.id;
            assert_eq!(
                instance_id,
                disk.runtime.disk_state.attached_instance_id()
            );
            Ok(Arc::new(ApiDiskAttachment {
                instance_name: instance.identity.name.clone(),
                instance_id: instance_id.clone(),
                disk_id: disk.identity.id.clone(),
                disk_name: disk.identity.name.clone(),
                disk_state: disk.runtime.disk_state.clone(),
            }))
        }

        fn disk_attachment_error(
            disk: &Arc<ApiDisk>,
        ) -> CreateResult<ApiDiskAttachment> {
            let disk_status = match disk.runtime.disk_state {
                /*
                 * We won't actually get to these two cases because they're
                 * checked in check_disk_change_allowed().
                 */
                ApiDiskState::Destroyed => "disk is destroyed",
                ApiDiskState::Faulted => "disk is faulted",

                ApiDiskState::Creating => "disk is detached",
                ApiDiskState::Detached => "disk is detached",

                /*
                 * It would be nice to provide a more specific message here, but
                 * the appropriate identifier to provide the user would be the
                 * other instance's name.  Getting that would require another
                 * database hit, which doesn't seem worth it for this.
                 */
                ApiDiskState::Attaching(_) => {
                    "disk is attached to another instance"
                }
                ApiDiskState::Attached(_) => {
                    "disk is attached to another instance"
                }
                ApiDiskState::Detaching(_) => {
                    "disk is attached to another instance"
                }
            };
            let message = format!(
                "cannot attach disk \"{}\": {}",
                String::from(disk.identity.name.clone()),
                disk_status
            );
            Err(ApiError::InvalidRequest {
                message: message,
            })
        }

        /*
         * Check common cases, like the disk being destroyed or faulted.
         */
        self.check_disk_change_allowed(&disk)?;

        match &disk.runtime.disk_state {
            /*
             * If we're already attaching or attached to the requested instance,
             * there's nothing else to do.
             */
            ApiDiskState::Attached(id) if id == instance_id => {
                return disk_attachment_for(&instance, &disk);
            }

            /*
             * If the disk is currently attaching or attached to another
             * instance, fail this request.  Users must explicitly detach first
             * if that's what they want.  If it's detaching, they have to wait
             * for it to become detached.
             * TODO-debug: the error message here could be better.  We'd have to
             * look up the other instance by id (and gracefully handle it not
             * existing).
             */
            ApiDiskState::Attached(id) => {
                assert_ne!(id, instance_id);
                return disk_attachment_error(&disk);
            }
            ApiDiskState::Detaching(_) => {
                return disk_attachment_error(&disk);
            }
            ApiDiskState::Attaching(id) if id != instance_id => {
                return disk_attachment_error(&disk);
            }
            ApiDiskState::Destroyed => {
                return disk_attachment_error(&disk);
            }
            ApiDiskState::Faulted => {
                return disk_attachment_error(&disk);
            }

            ApiDiskState::Creating => (),
            ApiDiskState::Detached => (),
            ApiDiskState::Attaching(id) => {
                assert_eq!(id, instance_id);
                ()
            }
        }

        /* XXX this code could use some cleanup! */
        let mut new_disk = (*disk).clone();
        let requested = ApiDiskStateRequested::Attached(instance_id.clone());
        let sc = self.instance_sc(&instance).await?;
        let new_state =
            sc.disk_ensure(Arc::new(new_disk.clone()), requested).await?;

        new_disk.runtime = new_state;
        let new_disk = Arc::new(new_disk);
        self.datastore.disk_update(Arc::clone(&new_disk)).await?;
        disk_attachment_for(&instance, &new_disk)
    }

    /**
     * Detach a disk from an instance.
     */
    pub async fn instance_detach_disk(
        &self,
        project_name: &ApiName,
        instance_name: &ApiName,
        disk_name: &ApiName,
    ) -> DeleteResult {
        let instance = self
            .datastore
            .project_lookup_instance(project_name, instance_name)
            .await?;
        let disk =
            self.datastore.project_lookup_disk(project_name, disk_name).await?;
        let instance_id = &instance.identity.id;

        match &disk.runtime.disk_state {
            /*
             * This operation is a noop if the disk is not attached or already
             * detaching from the same instance.
             */
            ApiDiskState::Creating => return Ok(()),
            ApiDiskState::Detached => return Ok(()),
            ApiDiskState::Destroyed => return Ok(()),
            ApiDiskState::Faulted => return Ok(()),
            ApiDiskState::Detaching(id) if id == instance_id => return Ok(()),

            /*
             * This operation is not allowed if the disk is attached to some
             * other instance.
             */
            ApiDiskState::Attaching(id) => if id != instance_id {
                return Err(ApiError::InvalidRequest {
                    message: String::from("disk is attached elsewhere"),
                });
            },
            ApiDiskState::Attached(id) => if id != instance_id {
                return Err(ApiError::InvalidRequest {
                    message: String::from("disk is attached elsewhere"),
                });
            },
            ApiDiskState::Detaching(id) => if id != instance_id {
                return Err(ApiError::InvalidRequest {
                    message: String::from("disk is attached elsewhere"),
                });
            },

            /* These are the cases where we have to do something. */
            ApiDiskState::Attaching(_) => (),
            ApiDiskState::Attached(_) => (),
        }

        /* XXX commonize with attach */
        let sc = self.instance_sc(&instance).await?;
        let mut new_disk = (*disk).clone();
        let requested = ApiDiskStateRequested::Detached;
        let new_state =
            sc.disk_ensure(Arc::new(new_disk.clone()), requested).await?;
        new_disk.runtime = new_state;
        let new_disk = Arc::new(new_disk);
        self.datastore.disk_update(Arc::clone(&new_disk)).await?;
        Ok(())
    }

    /*
     * Racks.  We simulate just one for now.
     */

    fn as_rack(&self) -> Arc<ApiRack> {
        Arc::clone(&self.api_rack)
    }

    pub async fn racks_list(
        &self,
        pagparams: &PaginationParams<Uuid>,
    ) -> ListResult<ApiRack> {
        if let Some(marker) = pagparams.marker {
            if marker > self.id {
                return Ok(futures::stream::empty().boxed());
            }
        }

        Ok(futures::stream::once(ready(Ok(self.as_rack()))).boxed())
    }

    pub async fn rack_lookup(&self, rack_id: &Uuid) -> LookupResult<ApiRack> {
        if *rack_id == self.id {
            Ok(self.as_rack())
        } else {
            Err(ApiError::not_found_by_id(ApiResourceType::Rack, rack_id))
        }
    }

    /*
     * Servers.
     * TODO-completeness: Eventually, we'll want servers to be stored in the
     * database, with a controlled process for adopting them, decommissioning
     * them, etc.  For now, we expose an ApiServer for each ServerController
     * that we've got.
     */
    pub async fn servers_list(
        &self,
        pagparams: &PaginationParams<Uuid>,
    ) -> ListResult<ApiServer> {
        let controllers = self.server_controllers.lock().await;
        let servers = collection_page(&controllers, pagparams)?
            .filter(|maybe_object| ready(maybe_object.is_ok()))
            .map(|sc| {
                Ok(Arc::new(ApiServer {
                    id: sc.unwrap().id,
                }))
            })
            .collect::<Vec<Result<Arc<ApiServer>, ApiError>>>()
            .await;
        Ok(futures::stream::iter(servers).boxed())
    }

    pub async fn server_lookup(
        &self,
        server_id: &Uuid,
    ) -> LookupResult<ApiServer> {
        let controllers = self.server_controllers.lock().await;
        let sc = controllers.get(server_id).ok_or_else(|| {
            ApiError::not_found_by_id(ApiResourceType::Server, server_id)
        })?;

        Ok(Arc::new(ApiServer {
            id: sc.id,
        }))
    }

    pub fn as_sc_api(self: &Arc<OxideController>) -> ControllerScApi {
        ControllerScApi {
            controller: Arc::clone(self),
        }
    }
}

#[async_trait]
impl OxideControllerTestInterfaces for OxideController {
    async fn instance_server_by_id(
        &self,
        id: &Uuid,
    ) -> Result<Arc<ServerController>, ApiError> {
        let instance = self.datastore.instance_lookup_by_id(id).await?;
        self.instance_sc(&instance).await
    }
}

/**
 * `ControllerScApi` represents the API exposed by the OxideController (OXCP)
 * for use by ServerControllers.  Like `ServerController`, this is currently
 * implemented directly in Rust, but the intent is for this to be a network call
 * of some kind, so we should be careful about the kinds of interfaces exposed
 * here.
 */
pub struct ControllerScApi {
    controller: Arc<OxideController>,
}

impl ControllerScApi {
    /**
     * Invoked by a server controller to publish an updated runtime state for an
     * Instance.
     */
    pub async fn notify_instance_updated(
        &self,
        id: &Uuid,
        new_runtime_state: &ApiInstanceRuntimeState,
    ) -> Result<(), ApiError> {
        let datastore = &self.controller.datastore;
        let log = &self.controller.log;

        let result = datastore
            .instance_lookup_by_id(id)
            .and_then(|old_instance| {
                let mut new_instance = (*old_instance).clone();
                new_instance.runtime = new_runtime_state.clone();
                datastore.instance_update(Arc::new(new_instance))
            })
            .await;

        match result {
            Ok(_) => {
                debug!(log, "instance updated by server controller";
                    "instance_id" => %id,
                    "new_state" => %new_runtime_state.run_state);
                Ok(())
            }

            /*
             * If the instance doesn't exist, swallow the error -- there's
             * nothing to do here.
             * TODO-robustness This could only be possible if we've removed an
             * Instance from the datastore altogether.  When would we do that?
             * We don't want to do it as soon as something's destroyed, I think,
             * and in that case, we'd need some async task for cleaning these
             * up.
             */
            Err(ApiError::ObjectNotFound {
                ..
            }) => {
                warn!(log, "non-existent instance updated by server controller";
                    "instance_id" => %id,
                    "new_state" => %new_runtime_state.run_state);
                Ok(())
            }

            /*
             * If the datastore is unavailable, propagate that to the caller.
             */
            Err(error) => {
                warn!(log, "failed to update instance from server controller";
                    "instance_id" => %id,
                    "new_state" => %new_runtime_state.run_state,
                    "error" => ?error);
                Err(error.into())
            }
        }
    }

    pub async fn notify_disk_updated(
        &self,
        id: &Uuid,
        new_state: &ApiDiskRuntimeState,
    ) -> Result<(), ApiError> {
        let datastore = &self.controller.datastore;
        let log = &self.controller.log;

        let result = datastore
            .disk_lookup_by_id(id)
            .and_then(|old_disk| {
                let mut new_disk = (*old_disk).clone();
                new_disk.runtime = new_state.clone();
                datastore.disk_update(Arc::new(new_disk))
            })
            .await;

        /* TODO-cleanup commonize with notify_instance_updated() */
        match result {
            Ok(_) => {
                debug!(log, "disk updated by server controller";
                    "disk_id" => %id,
                    "new_state" => ?new_state);
                Ok(())
            }

            /*
             * If the disk doesn't exist, swallow the error -- there's
             * nothing to do here.
             * TODO-robustness This could only be possible if we've removed a
             * disk from the datastore altogether.  When would we do that?
             * We don't want to do it as soon as something's destroyed, I think,
             * and in that case, we'd need some async task for cleaning these
             * up.
             */
            Err(ApiError::ObjectNotFound {
                ..
            }) => {
                warn!(log, "non-existent disk updated by server controller";
                    "instance_id" => %id,
                    "new_state" => ?new_state);
                Ok(())
            }

            /*
             * If the datastore is unavailable, propagate that to the caller.
             */
            Err(error) => {
                warn!(log, "failed to update disk from server controller";
                    "disk_id" => %id,
                    "new_state" => ?new_state,
                    "error" => ?error);
                Err(error.into())
            }
        }
    }
}
