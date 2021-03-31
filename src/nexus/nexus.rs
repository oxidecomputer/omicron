/*!
 * Nexus, the service that operates much of the control plane in an Oxide fleet
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
use crate::api_model::CreateResult2;
use crate::api_model::DataPageParams;
use crate::api_model::DeleteResult;
use crate::api_model::ListResult;
use crate::api_model::LookupResult;
use crate::api_model::LookupResult2;
use crate::api_model::UpdateResult;
use crate::api_model::UpdateResult2;
use crate::nexus::datastore::collection_page;
use crate::nexus::datastore::DataStore;
use crate::nexus::db;
use crate::nexus::saga_interface::SagaContext;
use crate::nexus::sagas;
use crate::sled_agent;
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
use steno::SagaId;
use steno::SagaResultOk;
use steno::SagaTemplate;
use steno::SagaType;
use uuid::Uuid;

/**
 * Exposes additional [`Nexus`] interfaces for use by the test suite
 */
#[async_trait]
pub trait TestInterfaces {
    /**
     * Returns the sled_agent::Client for an Instance from its id.  We may also
     * want to split this up into instance_lookup_by_id() and instance_sled(),
     * but after all it's a test suite special to begin with.
     */
    async fn instance_sled_by_id(
        &self,
        id: &Uuid,
    ) -> Result<Arc<sled_agent::Client>, ApiError>;

    /**
     * Returns the sled_agent::Client for a Disk from its id.
     */
    async fn disk_sled_by_id(
        &self,
        id: &Uuid,
    ) -> Result<Arc<sled_agent::Client>, ApiError>;
}

/**
 * Manages an Oxide fleet -- the heart of the control plane
 */
pub struct Nexus {
    /** uuid for this rack (TODO should also be in persistent storage) */
    id: Uuid,

    /** general server log */
    log: Logger,

    /** cached ApiRack structure representing the single rack. */
    api_rack: Arc<ApiRack>,

    /** persistent storage for resources in the control plane */
    datastore: DataStore,

    /** persistent storage for resources in the control plane */
    db_datastore: db::DataStore,

    /**
     * List of sled agents known by this nexus.
     * TODO This ought to have some representation in the data store as well so
     * that we don't simply forget about sleds that aren't currently up.  We'll
     * need to think about the interface between this program and the sleds and
     * how we discover them, both when they initially show up and when we come
     * up.
     */
    sled_agents: Mutex<BTreeMap<Uuid, Arc<sled_agent::Client>>>,
}

/*
 * TODO Is it possible to make some of these operations more generic?  A
 * particularly good example is probably list() (or even lookup()), where
 * with the right type parameters, generic code can be written to work on all
 * types.
 * TODO update and delete need to accommodate both with-etag and don't-care
 * TODO audit logging ought to be part of this structure and its functions
 */
impl Nexus {
    /**
     * Create a new Nexus instance for the given rack id `id`
     *
     * The state of the system is maintained in memory, so we always start from
     * a clean slate.
     */
    /* TODO-polish revisit rack metadata */
    pub fn new_with_id(id: &Uuid, log: Logger, pool: db::Pool) -> Nexus {
        Nexus {
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
            datastore: DataStore::new_empty(),
            db_datastore: db::DataStore::new(Arc::new(pool)),
            sled_agents: Mutex::new(BTreeMap::new()),
        }
    }

    /*
     * TODO-robustness we should have a limit on how many sled agents there can
     * be (for graceful degradation at large scale).
     */
    pub async fn upsert_sled_agent(&self, sa: Arc<sled_agent::Client>) {
        let mut scs = self.sled_agents.lock().await;
        info!(self.log, "registered sled agent";
            "sled_uuid" => sa.id.to_string());
        scs.insert(sa.id, sa);
    }

    pub fn datastore(&self) -> &db::DataStore {
        &self.db_datastore
    }

    /**
     * Given a saga template and parameters, create a new saga and execute it.
     */
    /*
     * TODO-debugging It would be nice to keep a list of the outstanding sagas
     * and maybe even provide APIs to report their status.
     * Maybe steno could even have an interface for keeping track of a bunch of
     * sagas, maybe as part of a SagaExecCoordinator or something.
     */
    async fn execute_saga<P, S>(
        self: &Arc<Self>,
        saga_template: SagaTemplate<S>,
        saga_params: P,
    ) -> Result<SagaResultOk, ApiError>
    where
        S: SagaType<
            ExecContextType = Arc<SagaContext>,
            SagaParamsType = Arc<P>,
        >,
    {
        let saga_context = Arc::new(SagaContext::new(Arc::clone(self)));
        let saga_id = SagaId(Uuid::new_v4());
        let saga_exec = SagaExecutor::new(
            &saga_id,
            Arc::new(saga_template),
            &self.id.to_string(),
            Arc::new(saga_context),
            Arc::new(saga_params),
        )
        .map_err(|e| {
            /* TODO-error more context would be useful */
            ApiError::InternalError { message: e.to_string() }
        })?;
        saga_exec.run().await;
        saga_exec.result().kind.map_err(|saga_error| {
            saga_error.error_source.convert::<ApiError>().unwrap_or_else(|e| {
                /* TODO-error more context would be useful */
                ApiError::InternalError { message: e.to_string() }
            })
        })
    }

    /*
     * Projects
     */

    pub async fn project_create(
        &self,
        new_project: &ApiProjectCreateParams,
    ) -> CreateResult<ApiProject> {
        let id = Uuid::new_v4();
        Ok(Arc::new(
            self.db_datastore.project_create_with_id(&id, new_project).await?,
        ))
    }

    pub async fn project_fetch(
        &self,
        name: &ApiName,
    ) -> LookupResult<ApiProject> {
        Ok(Arc::new(self.db_datastore.project_fetch(name).await?))
    }

    pub async fn projects_list_by_name(
        &self,
        pagparams: &DataPageParams<'_, ApiName>,
    ) -> ListResult<ApiProject> {
        self.db_datastore.projects_list_by_name(pagparams).await
    }

    pub async fn projects_list_by_id(
        &self,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResult<ApiProject> {
        self.db_datastore.projects_list_by_id(pagparams).await
    }

    pub async fn project_delete(&self, name: &ApiName) -> DeleteResult {
        self.db_datastore.project_delete(name).await
    }

    pub async fn project_update(
        &self,
        name: &ApiName,
        new_params: &ApiProjectUpdateParams,
    ) -> UpdateResult<ApiProject> {
        Ok(Arc::new(self.db_datastore.project_update(name, new_params).await?))
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
        let project = self.project_fetch(project_name).await?;

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

    /*
     * TODO-design This interface should not exist.  See
     * SagaContext::alloc_server().
     */
    pub async fn sled_allocate(&self) -> Result<Uuid, ApiError> {
        let sleds = self.sled_agents.lock().await;

        /* TODO replace this with a real allocation policy. */
        sleds
            .values()
            .next()
            .ok_or_else(|| ApiError::ServiceUnavailable {
                message: String::from("no sleds available for new Instance"),
            })
            .map(|s| s.id)
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
    ) -> CreateResult2<ApiInstance> {
        let project_id =
            self.db_datastore.project_lookup_id_by_name(project_name).await?;

        let saga_template = sagas::saga_instance_create();
        let saga_params = sagas::ParamsInstanceCreate {
            project_id,
            create_params: params.clone(),
        };

        let saga_outputs =
            self.execute_saga(saga_template, saga_params).await?;
        /* TODO-error more context would be useful  */
        let instance_id = saga_outputs
            .lookup_output::<Uuid>("instance_id")
            .map_err(|e| ApiError::InternalError { message: e.to_string() })?;
        /*
         * TODO-correctness TODO-robustness TODO-design It's not quite correct
         * to take this instance id and look it up again.  It's possible that
         * it's been modified or even deleted since the saga executed.  In that
         * case, we might return a different state of the Instance than the one
         * that the user created or even fail with a 404!  Both of those are
         * wrong behavior -- we should be returning the very instance that the
         * user created.
         *
         * How can we fix this?  Right now we have internal representations like
         * ApiInstance and analaogous end-user-facing representations like
         * ApiInstanceView.  The former is not even serializable.  The saga
         * _could_ emit the View version, but that's not great for two (related)
         * reasons: (1) other sagas might want to provision instances and get
         * back the internal representation to do other things with the
         * newly-created instance, and (2) even within a saga, it would be
         * useful to pass a single ApiInstance representation along the saga,
         * but they probably would want the internal representation, not the
         * view.
         *
         * The saga could emit an ApiInstance directly.  Today, ApiInstance
         * etc. aren't supposed to even be serializable -- we wanted to be able
         * to have other datastore state there if needed.  We could have a third
         * ApiInstanceInternalView...but that's starting to feel pedantic.  We
         * could just make ApiInstance serializable, store that, and call it a
         * day.  Does it matter that we might have many copies of the same
         * objects in memory?
         *
         * If we make these serializable, it would be nice if we could leverage
         * the type system to ensure that we never accidentally send them out a
         * dropshot endpoint.  (On the other hand, maybe we _do_ want to do
         * that, for internal interfaces!  Can we do this on a
         * per-dropshot-server-basis?)
         */
        let instance = self.db_datastore.instance_fetch(&instance_id).await?;
        Ok(instance)
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
        self.instance_set_runtime(&instance, sa, runtime_params).await?;
        Ok(())
    }

    pub async fn project_lookup_instance(
        &self,
        project_name: &ApiName,
        instance_name: &ApiName,
    ) -> LookupResult2<ApiInstance> {
        /* XXX Can/should we restructure this to be done with one query? */
        let project_id =
            self.db_datastore.project_lookup_id_by_name(project_name).await?;
        self.db_datastore
            .instance_fetch_by_name(&project_id, instance_name)
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
    ) -> Result<Arc<sled_agent::Client>, ApiError> {
        let sled_agents = self.sled_agents.lock().await;
        Ok(Arc::clone(sled_agents.get(sled_uuid).ok_or_else(|| {
            let message =
                format!("no sled agent for sled_uuid \"{}\"", sled_uuid);
            ApiError::ServiceUnavailable { message }
        })?))
    }

    /**
     * Returns the sled_agent::Client for the host where this Instance is running.
     */
    async fn instance_sled(
        &self,
        instance: &ApiInstance,
    ) -> Result<Arc<sled_agent::Client>, ApiError> {
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
    ) -> UpdateResult2<ApiInstance> {
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
            &instance,
            self.instance_sled(&instance).await?,
            ApiInstanceRuntimeStateRequested {
                run_state: ApiInstanceState::Running,
                reboot_wanted: true,
            },
        )
        .await?;
        self.db_datastore.instance_fetch(&instance.identity.id).await
    }

    /**
     * Make sure the given Instance is running.
     */
    pub async fn instance_start(
        &self,
        project_name: &ApiName,
        instance_name: &ApiName,
    ) -> UpdateResult2<ApiInstance> {
        let instance = self
            .datastore
            .project_lookup_instance(project_name, instance_name)
            .await?;

        self.check_runtime_change_allowed(&instance)?;
        self.instance_set_runtime(
            &instance,
            self.instance_sled(&instance).await?,
            ApiInstanceRuntimeStateRequested {
                run_state: ApiInstanceState::Running,
                reboot_wanted: false,
            },
        )
        .await?;
        self.db_datastore.instance_fetch(&instance.identity.id).await
    }

    /**
     * Make sure the given Instance is stopped.
     */
    pub async fn instance_stop(
        &self,
        project_name: &ApiName,
        instance_name: &ApiName,
    ) -> UpdateResult2<ApiInstance> {
        let instance = self
            .datastore
            .project_lookup_instance(project_name, instance_name)
            .await?;

        self.check_runtime_change_allowed(&instance)?;
        self.instance_set_runtime(
            &instance,
            self.instance_sled(&instance).await?,
            ApiInstanceRuntimeStateRequested {
                run_state: ApiInstanceState::Stopped,
                reboot_wanted: false,
            },
        )
        .await?;
        self.db_datastore.instance_fetch(&instance.identity.id).await
    }

    /**
     * Modifies the runtime state of the Instance as requested.  This generally
     * means booting or halting the Instance.
     */
    async fn instance_set_runtime(
        &self,
        instance: &ApiInstance,
        sa: Arc<sled_agent::Client>,
        runtime_params: ApiInstanceRuntimeStateRequested,
    ) -> Result<(), ApiError> {
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

        self.db_datastore
            .instance_update_runtime(&instance.identity.id, &new_runtime_state)
            .await?;
        Ok(())
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
        sa: Arc<sled_agent::Client>,
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
     * them, etc.  For now, we expose an ApiSled for each sled_agent::Client that
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
        let nexuses = self.sled_agents.lock().await;
        let sa = nexuses.get(sled_id).ok_or_else(|| {
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
        let log = &self.log;

        let result = self
            .db_datastore
            .instance_update_runtime(id, new_runtime_state)
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
             * XXX It's not clear that we can even get here any more because
             * instance_update_runtime() does not distinguish this error.
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
             * XXX Really this should be any _transient_ error.  How can we
             * distinguish?  Maybe datastore should emit something different
             * from ApiError with an Into<ApiError>.
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
impl TestInterfaces for Nexus {
    async fn instance_sled_by_id(
        &self,
        id: &Uuid,
    ) -> Result<Arc<sled_agent::Client>, ApiError> {
        let instance = self.datastore.instance_lookup_by_id(id).await?;
        self.instance_sled(&instance).await
    }

    async fn disk_sled_by_id(
        &self,
        id: &Uuid,
    ) -> Result<Arc<sled_agent::Client>, ApiError> {
        let disk = self.datastore.disk_lookup_by_id(id).await?;
        let instance_id =
            disk.runtime.disk_state.attached_instance_id().unwrap();
        let instance =
            self.datastore.instance_lookup_by_id(instance_id).await?;
        self.instance_sled(&instance).await
    }
}
