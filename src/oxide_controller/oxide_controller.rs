/*!
 * Heart of OXC, the Oxide Controller, which operates much of the control plane
 * in an Oxide fleet
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
use crate::api_model::ApiInstanceRuntimeStateRequested;
use crate::api_model::ApiInstanceState;
use crate::api_model::ApiName;
use crate::api_model::ApiProject;
use crate::api_model::ApiProjectCreateParams;
use crate::api_model::ApiProjectUpdateParams;
use crate::api_model::ApiRack;
use crate::api_model::ApiResourceType;
use crate::api_model::ApiSled;
use crate::api_model::CreateResult;
use crate::api_model::DataPageParams;
use crate::api_model::DeleteResult;
use crate::api_model::ListResult;
use crate::api_model::LookupResult;
use crate::api_model::UpdateResult;
use crate::datastore::collection_page;
use crate::datastore::ControlDataStore;
use crate::oxide_controller::saga_interface::OxcSagaContext;
use crate::oxide_controller::sagas;
use crate::SledAgentClient;
use async_trait::async_trait;
use chrono::Utc;
use futures::future::ready;
use futures::future::TryFutureExt;
use futures::lock::Mutex;
use futures::stream::StreamExt;
use slog::Logger;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::sync::Arc;
use steno::SagaExecutor;
use uuid::Uuid;

/**
 * Exposes additional [`OxideController`] interfaces for use by the test suite
 */
#[async_trait]
pub trait OxideControllerTestInterfaces {
    /**
     * Returns the SledAgentClient for an Instance from its id.  We may also
     * want to split this up into instance_lookup_by_id() and instance_sled(),
     * but after all it's a test suite special to begin with.
     */
    async fn instance_sled_by_id(
        &self,
        id: &Uuid,
    ) -> Result<Arc<SledAgentClient>, ApiError>;

    /**
     * Returns the SledAgentClient for a Disk from its id.
     */
    async fn disk_sled_by_id(
        &self,
        id: &Uuid,
    ) -> Result<Arc<SledAgentClient>, ApiError>;
}

/**
 * (OXC) Manages an Oxide fleet -- the heart of the control plane
 */
pub struct OxideController {
    /** uuid for this rack (TODO should also be in persistent storage) */
    id: Uuid,

    /** general server log */
    log: Logger,

    /** cached ApiRack structure representing the single rack. */
    api_rack: Arc<ApiRack>,

    /** persistent storage for resources in the control plane */
    // XXX not pub
    pub datastore: ControlDataStore,

    /**
     * List of sled agents known by this controller.
     * TODO This ought to have some representation in the data store as well so
     * that we don't simply forget about sleds that aren't currently up.  We'll
     * need to think about the interface between this program and the sleds and
     * how we discover them, both when they initially show up and when we come
     * up.
     */
    // XXX not pub
    pub sled_agents: Mutex<BTreeMap<Uuid, Arc<SledAgentClient>>>,
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
    /**
     * Create a new OXC instance for the given rack id `id`
     *
     * The state of the system is maintained in memory, so we always start from
     * a clean slate.
     */
    /* TODO-polish revisit rack metadata */
    pub fn new_with_id(id: &Uuid, log: Logger) -> OxideController {
        OxideController {
            id: *id,
            log,
            api_rack: Arc::new(ApiRack {
                identity: ApiIdentityMetadata {
                    id: *id,
                    name: ApiName::try_from(format!("rack-{}", *id)).unwrap(),
                    description: String::from(""),
                    time_created: Utc::now(),
                    time_modified: Utc::now(),
                },
            }),
            datastore: ControlDataStore::new(),
            sled_agents: Mutex::new(BTreeMap::new()),
        }
    }

    /*
     * TODO-robustness we should have a limit on how many sled agents there can
     * be (for graceful degradation at large scale).
     */
    pub async fn upsert_sled_agent(&self, sa: Arc<SledAgentClient>) {
        let mut scs = self.sled_agents.lock().await;
        info!(self.log, "registered sled agent";
            "sled_uuid" => sa.id.to_string());
        scs.insert(sa.id, sa);
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

    pub async fn projects_list_by_name(
        &self,
        pagparams: &DataPageParams<'_, ApiName>,
    ) -> ListResult<ApiProject> {
        self.datastore.projects_list_by_name(pagparams).await
    }

    pub async fn projects_list_by_id(
        &self,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResult<ApiProject> {
        self.datastore.projects_list_by_id(pagparams).await
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
        pagparams: &DataPageParams<'_, ApiName>,
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

        /*
         * Until we implement snapshots, do not allow disks to be created with a
         * snapshot id.
         */
        if params.snapshot_id.is_some() {
            return Err(ApiError::InvalidValue {
                label: String::from("snapshot_id"),
                message: String::from("snapshots are not yet supported"),
            });
        }

        let disk = Arc::new(ApiDisk {
            identity: ApiIdentityMetadata {
                id: Uuid::new_v4(),
                name: params.identity.name.clone(),
                description: params.identity.description.clone(),
                time_created: now,
                time_modified: now,
            },
            project_id: project.identity.id,
            create_snapshot_id: params.snapshot_id,
            size: params.size,
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
         * simulation lives in a simulated sled agent.  Here, the analog might
         * be a simulated disk control plane.  But that doesn't exist yet, and
         * we don't even know what APIs it would provide yet.  So we just carry
         * out the simplest possible "simulation" here: we'll return to the
         * client a structure describing a disk in state "Creating", but by the
         * time we do so, we've already updated the internal representation to
         * "Created".
         */
        let mut new_disk = (*disk_created).clone();
        new_disk.runtime.disk_state = ApiDiskState::Detached;
        new_disk.runtime.gen += 1;
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

    pub async fn project_delete_disk(
        &self,
        project_name: &ApiName,
        disk_name: &ApiName,
    ) -> DeleteResult {
        let disk = self.project_lookup_disk(project_name, disk_name).await?;
        if disk.runtime.disk_state == ApiDiskState::Destroyed {
            /*
             * TODO-correctness In general, this program is inconsistent about
             * deleting things that are already destroyed.  Should this succeed
             * or not?  We should decide once and validate it everywhere.  (Even
             * in this case, most of the time this request would not succeed,
             * despite this branch, because we will have failed to locate the
             * disk above.)
             */
            return Ok(());
        }

        if disk.runtime.disk_state.is_attached() {
            return Err(ApiError::InvalidRequest {
                message: String::from("disk is attached"),
            });
        }

        /*
         * TODO-robustness It's not clear how this handles the case where we
         * begin this delete operation while some other request is ongoing to
         * attach the disk.  We won't be able to see that in the state here.  We
         * might be able to detect this when we go update the disk's state to
         * Attaching (because a SQL UPDATE will update 0 rows), but we'd sort of
         * already be in a bad state because the destroyed disk will be
         * attaching (and eventually attached) on some sled, and if the wrong
         * combination of components crash at this point, we could wind up not
         * fixing that state.
         *
         * This is a consequence of the choice _not_ to record the Attaching
         * state in the database before beginning the attach process.  If we did
         * that, we wouldn't have this problem, but we'd have a similar problem
         * of dealing with the case of a crash after recording this state and
         * before actually beginning the attach process.
         *
         * TODO-debug Do we actually want to remove this right away or mark it
         * Destroyed and not show it?  I think the latter, but then we need some
         * way to clean these up later.  We also need to avoid camping on the
         * name in the namespace.  (In traditional RDBMS terms, we want the
         * unique index on the disk's name to be a partial index for state !=
         * "destroyed".)
         */
        self.datastore.disk_delete(disk).await
    }

    /*
     * Instances
     */

    // XXX should not be pub
    pub async fn sled_allocate_instance<'se, 'params>(
        &'se self,
        sleds: &'se BTreeMap<Uuid, Arc<SledAgentClient>>,
        _project: &ApiProject,
        _params: &'params ApiInstanceCreateParams,
    ) -> Result<&'se Arc<SledAgentClient>, ApiError> {
        /* TODO replace this with a real allocation policy. */
        sleds.values().next().ok_or_else(|| ApiError::ServiceUnavailable {
            message: String::from("no sleds available for new Instance"),
        })
    }

    pub async fn project_list_instances(
        &self,
        project_name: &ApiName,
        pagparams: &DataPageParams<'_, ApiName>,
    ) -> ListResult<ApiInstance> {
        self.datastore.project_list_instances(project_name, pagparams).await
    }

    pub async fn project_create_instance(
        self: &Arc<Self>,
        project_name: &ApiName,
        params: &ApiInstanceCreateParams,
    ) -> CreateResult<ApiInstance> {
        let saga_context = Arc::new(OxcSagaContext::new(Arc::clone(self)));
        let saga_template =
            sagas::saga_instance_create(saga_context, project_name, params);
        // XXX fill in saga exec creator
        let saga_exec = SagaExecutor::new(Arc::new(saga_template), "tmp");
        saga_exec.run().await;
        let result = saga_exec.result();
        // XXX Need to produce a better error message here based on how the saga
        // failed.
        let instance_id = result
            .lookup_output::<Uuid>("instance_id")
            .map_err(|e| ApiError::InternalError { message: e.to_string() })?;
        // XXX If the instance doesn't exist at this point, that's going to be
        // strange!
        let instance =
            self.datastore.instance_lookup_by_id(&instance_id).await?;
        Ok(instance)

        //        let project = self.project_lookup(project_name).await?;
        //
        //        /*
        //         * Allocate a sled and retrieve our handle to its SA.
        //         */
        //        let sa = {
        //            let sleds = self.sled_agents.lock().await;
        //            let arc =
        //                self.sled_allocate_instance(&sleds, &project, params).await?;
        //            Arc::clone(arc)
        //        };
        //
        //        /*
        //         * The order of operations here is slightly subtle:
        //         *
        //         * 1. We want to record the new instance in the database before telling
        //         *    the SA about it.  This record will have state "Creating" to
        //         *    distinguish it from other runtime states.  If we crash after this
        //         *    point, there will be a record in the database for the instance,
        //         *    but it won't be running, since no SA knows about it.
        //         *    TODO-robustness How do we want to handle a crash at this point?
        //         *    One approach would be to say that while an Instance exists in the
        //         *    "Creating" state, it's not logically present yet.  Thus, if
        //         *    we crash and leave a record here, there's a small resource leak of
        //         *    sorts, but there's no problem from the user's perspective.  What
        //         *    about use of the "name", which is user-controlled and unique?  We
        //         *    could have the creation process look for an existing instance with
        //         *    the same name in state "Creating", decide if it appears to be
        //         *    abandoned by the control plane instance that was working on it,
        //         *    and then simply delete it and proceed.  We might need to do this
        //         *    in the Instance rename case as well.  Reliably deciding whether
        //         *    the record is abandoned seems hard, though.  We would also
        //         *    probably want something that proactively looks for these abandoned
        //         *    records and removes them to eliminate the leakage.  A case to
        //         *    consider is two concurrent attempts to create an Instance with the
        //         *    same name.  Exactly one should win every time and we shouldn't
        //         *    wind up with two Instances running at any point.
        //         *
        //         * 2. We want to tell the SA about this Instance and have the SA start
        //         *    it up.  The SA should reply immediately that it's starting the
        //         *    instance, and we immediately record this into the database, now
        //         *    with state "Starting".
        //         *
        //         * 3. Some time later (after this function has completed), the SA will
        //         *    notify us that the Instance has changed states to "Running".
        //         *    We'll record this into the database.
        //         */
        //        let runtime = ApiInstanceRuntimeState {
        //            run_state: ApiInstanceState::Creating,
        //            reboot_in_progress: false,
        //            sled_uuid: sa.id,
        //            gen: 1,
        //            time_updated: Utc::now(),
        //        };
        //
        //        /*
        //         * Store the first revision of the Instance into the database.  This
        //         * will have state "Creating".
        //         */
        //        let instance_created = self
        //            .datastore
        //            .project_create_instance(project_name, params, &runtime)
        //            .await?;
        //        self.instance_set_runtime(
        //            instance_created,
        //            sa,
        //            ApiInstanceRuntimeStateRequested {
        //                run_state: ApiInstanceState::Running,
        //                reboot_wanted: false,
        //            },
        //        )
        //        .await
    }

    /*
     * TODO-correctness It's not totally clear what the semantics and behavior
     * should be here.  It might be nice to say that you can only do this
     * operation if the Instance is already stopped, in which case we can
     * execute this immediately by just removing it from the database, with the
     * same race we have with disk delete (i.e., if someone else is requesting
     * an instance boot, we may wind up in an inconsistent state).  On the other
     * hand, we could always allow this operation, issue the request to the SA
     * to destroy the instance (not just stop it), and proceed with deletion
     * when that finishes.  But in that case, although the HTTP DELETE request
     * completed, the object will still appear for a little while, which kind of
     * sucks.
     */
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
        let sa = self.instance_sled(&instance).await?;
        let runtime_params = ApiInstanceRuntimeStateRequested {
            run_state: ApiInstanceState::Destroyed,
            reboot_wanted: false,
        };
        self.instance_set_runtime(instance, sa, runtime_params).await?;
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
         * request to the SA to make the state change in these cases in case the
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

    pub async fn sled_client(
        &self,
        sled_uuid: &Uuid,
    ) -> Result<Arc<SledAgentClient>, ApiError> {
        let sled_agents = self.sled_agents.lock().await;
        Ok(Arc::clone(sled_agents.get(sled_uuid).ok_or_else(|| {
            let message =
                format!("no sled agent for sled_uuid \"{}\"", sled_uuid);
            ApiError::ServiceUnavailable { message }
        })?))
    }

    /**
     * Returns the SledAgentClient for the host where this Instance is running.
     */
    async fn instance_sled(
        &self,
        instance: &Arc<ApiInstance>,
    ) -> Result<Arc<SledAgentClient>, ApiError> {
        let said = &instance.runtime.sled_uuid;
        self.sled_client(&said).await
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
         * To implement reboot, we issue a call to the sled agent to set a
         * runtime state with "reboot_wanted".  We cannot simply stop the
         * Instance and start it again here because if we crash in the meantime,
         * we might leave it stopped.
         *
         * When an instance is rebooted, the "reboot_in_progress" remains set on
         * the runtime state as it transitions to "Stopping" and "Stopped".
         * This flag is cleared when the state goes to "Starting".  This way,
         * even if the whole rack powered off while this was going on, we would
         * never lose track of the fact that this Instance was supposed to be
         * running.
         */
        let instance = self
            .datastore
            .project_lookup_instance(project_name, instance_name)
            .await?;

        self.check_runtime_change_allowed(&instance)?;
        self.instance_set_runtime(
            Arc::clone(&instance),
            self.instance_sled(&instance).await?,
            ApiInstanceRuntimeStateRequested {
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
            self.instance_sled(&instance).await?,
            ApiInstanceRuntimeStateRequested {
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
            self.instance_sled(&instance).await?,
            ApiInstanceRuntimeStateRequested {
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
        sa: Arc<SledAgentClient>,
        runtime_params: ApiInstanceRuntimeStateRequested,
    ) -> UpdateResult<ApiInstance> {
        /*
         * Ask the SA to begin the state change.  Then update the database to
         * reflect the new intermediate state.
         */
        let new_runtime_state = sa
            .instance_ensure(
                instance.identity.id,
                instance.runtime.clone(),
                runtime_params,
            )
            .await?;

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
        pagparams: &DataPageParams<'_, ApiName>,
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
                    instance_id: instance.identity.id,
                    disk_name: disk.identity.name.clone(),
                    disk_id: disk.identity.id,
                    disk_state: disk.runtime.disk_state.clone(),
                }))
            })
            .collect::<Vec<Result<Arc<ApiDiskAttachment>, ApiError>>>()
            .await;
        Ok(futures::stream::iter(attachments).boxed())
    }

    /**
     * Fetch information about whether this disk is attached to this instance.
     */
    pub async fn instance_get_disk(
        &self,
        project_name: &ApiName,
        instance_name: &ApiName,
        disk_name: &ApiName,
    ) -> LookupResult<ApiDiskAttachment> {
        let instance = self
            .datastore
            .project_lookup_instance(project_name, instance_name)
            .await?;
        let disk =
            self.datastore.project_lookup_disk(project_name, disk_name).await?;
        if let Some(instance_id) =
            disk.runtime.disk_state.attached_instance_id()
        {
            if instance_id == &instance.identity.id {
                return Ok(Arc::new(ApiDiskAttachment {
                    instance_name: instance.identity.name.clone(),
                    instance_id: instance.identity.id,
                    disk_name: disk.identity.name.clone(),
                    disk_id: disk.identity.id,
                    disk_state: disk.runtime.disk_state.clone(),
                }));
            }
        }

        Err(ApiError::not_found_other(
            ApiResourceType::DiskAttachment,
            format!(
                "disk \"{}\" is not attached to instance \"{}\"",
                String::from(disk_name.clone()),
                String::from(instance_name.clone())
            ),
        ))
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
                disk.runtime.disk_state.attached_instance_id().unwrap()
            );
            Ok(Arc::new(ApiDiskAttachment {
                instance_name: instance.identity.name.clone(),
                instance_id: *instance_id,
                disk_id: disk.identity.id,
                disk_name: disk.identity.name.clone(),
                disk_state: disk.runtime.disk_state.clone(),
            }))
        }

        fn disk_attachment_error(
            disk: &Arc<ApiDisk>,
        ) -> CreateResult<ApiDiskAttachment> {
            let disk_status = match disk.runtime.disk_state {
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
            Err(ApiError::InvalidRequest { message })
        }

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
            }
        }

        let disk = self
            .disk_set_runtime(
                disk,
                self.instance_sled(&instance).await?,
                ApiDiskStateRequested::Attached(*instance_id),
            )
            .await?;
        disk_attachment_for(&instance, &disk)
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
            ApiDiskState::Attaching(id) if id != instance_id => {
                return Err(ApiError::InvalidRequest {
                    message: String::from("disk is attached elsewhere"),
                });
            }
            ApiDiskState::Attached(id) if id != instance_id => {
                return Err(ApiError::InvalidRequest {
                    message: String::from("disk is attached elsewhere"),
                });
            }
            ApiDiskState::Detaching(_) => {
                return Err(ApiError::InvalidRequest {
                    message: String::from("disk is attached elsewhere"),
                });
            }

            /* These are the cases where we have to do something. */
            ApiDiskState::Attaching(_) => (),
            ApiDiskState::Attached(_) => (),
        }

        self.disk_set_runtime(
            disk,
            self.instance_sled(&instance).await?,
            ApiDiskStateRequested::Detached,
        )
        .await?;
        Ok(())
    }

    /**
     * Modifies the runtime state of the Disk as requested.  This generally
     * means attaching or detaching the disk.
     */
    async fn disk_set_runtime(
        &self,
        mut disk: Arc<ApiDisk>,
        sa: Arc<SledAgentClient>,
        requested: ApiDiskStateRequested,
    ) -> UpdateResult<ApiDisk> {
        /*
         * Ask the SA to begin the state change.  Then update the database to
         * reflect the new intermediate state.
         */
        let new_runtime = sa
            .disk_ensure(disk.identity.id, disk.runtime.clone(), requested)
            .await?;
        let disk_ref = Arc::make_mut(&mut disk);
        disk_ref.runtime = new_runtime;
        self.datastore.disk_update(Arc::clone(&disk)).await?;
        Ok(disk)
    }

    /*
     * Racks.  We simulate just one for now.
     */

    fn as_rack(&self) -> Arc<ApiRack> {
        Arc::clone(&self.api_rack)
    }

    pub async fn racks_list(
        &self,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResult<ApiRack> {
        if let Some(marker) = pagparams.marker {
            if *marker >= self.id {
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
     * Sleds.
     * TODO-completeness: Eventually, we'll want sleds to be stored in the
     * database, with a controlled process for adopting them, decommissioning
     * them, etc.  For now, we expose an ApiSled for each SledAgentClient that
     * we've got.
     */
    pub async fn sleds_list(
        &self,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResult<ApiSled> {
        let sled_agents = self.sled_agents.lock().await;
        let sleds = collection_page(&sled_agents, pagparams)?
            .filter(|maybe_object| ready(maybe_object.is_ok()))
            .map(|sa| {
                let sa = sa.unwrap();
                Ok(Arc::new(ApiSled {
                    identity: ApiIdentityMetadata {
                        /* TODO-correctness cons up real metadata here */
                        id: sa.id,
                        name: ApiName::try_from(format!("sled-{}", sa.id))
                            .unwrap(),
                        description: String::from(""),
                        time_created: Utc::now(),
                        time_modified: Utc::now(),
                    },
                    service_address: sa.service_address,
                }))
            })
            .collect::<Vec<Result<Arc<ApiSled>, ApiError>>>()
            .await;
        Ok(futures::stream::iter(sleds).boxed())
    }

    pub async fn sled_lookup(&self, sled_id: &Uuid) -> LookupResult<ApiSled> {
        let controllers = self.sled_agents.lock().await;
        let sa = controllers.get(sled_id).ok_or_else(|| {
            ApiError::not_found_by_id(ApiResourceType::Sled, sled_id)
        })?;

        Ok(Arc::new(ApiSled {
            identity: ApiIdentityMetadata {
                /* TODO-correctness cons up real metadata here */
                id: sa.id,
                name: ApiName::try_from(format!("sled-{}", sa.id)).unwrap(),
                description: String::from(""),
                time_created: Utc::now(),
                time_modified: Utc::now(),
            },
            service_address: sa.service_address,
        }))
    }

    /*
     * Internal control plane interfaces.
     */

    /**
     * Invoked by a sled agent to publish an updated runtime state for an
     * Instance.
     */
    pub async fn notify_instance_updated(
        &self,
        id: &Uuid,
        new_runtime_state: &ApiInstanceRuntimeState,
    ) -> Result<(), ApiError> {
        let datastore = &self.datastore;
        let log = &self.log;

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
                info!(log, "instance updated by sled agent";
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
            Err(ApiError::ObjectNotFound { .. }) => {
                warn!(log, "non-existent instance updated by sled agent";
                    "instance_id" => %id,
                    "new_state" => %new_runtime_state.run_state);
                Ok(())
            }

            /*
             * If the datastore is unavailable, propagate that to the caller.
             */
            Err(error) => {
                warn!(log, "failed to update instance from sled agent";
                    "instance_id" => %id,
                    "new_state" => %new_runtime_state.run_state,
                    "error" => ?error);
                Err(error)
            }
        }
    }

    pub async fn notify_disk_updated(
        &self,
        id: &Uuid,
        new_state: &ApiDiskRuntimeState,
    ) -> Result<(), ApiError> {
        let datastore = &self.datastore;
        let log = &self.log;

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
                info!(log, "disk updated by sled agent";
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
            Err(ApiError::ObjectNotFound { .. }) => {
                warn!(log, "non-existent disk updated by sled agent";
                    "instance_id" => %id,
                    "new_state" => ?new_state);
                Ok(())
            }

            /*
             * If the datastore is unavailable, propagate that to the caller.
             */
            Err(error) => {
                warn!(log, "failed to update disk from sled agent";
                    "disk_id" => %id,
                    "new_state" => ?new_state,
                    "error" => ?error);
                Err(error)
            }
        }
    }
}

#[async_trait]
impl OxideControllerTestInterfaces for OxideController {
    async fn instance_sled_by_id(
        &self,
        id: &Uuid,
    ) -> Result<Arc<SledAgentClient>, ApiError> {
        let instance = self.datastore.instance_lookup_by_id(id).await?;
        self.instance_sled(&instance).await
    }

    async fn disk_sled_by_id(
        &self,
        id: &Uuid,
    ) -> Result<Arc<SledAgentClient>, ApiError> {
        let disk = self.datastore.disk_lookup_by_id(id).await?;
        let instance_id =
            disk.runtime.disk_state.attached_instance_id().unwrap();
        let instance =
            self.datastore.instance_lookup_by_id(instance_id).await?;
        self.instance_sled(&instance).await
    }
}
