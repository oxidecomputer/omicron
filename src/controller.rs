/*!
 * HTTP-agnostic interface to an Oxide system
 */

use crate::api_error::ApiError;
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
use crate::datastore::ControlDataStore;
use crate::server_controller::ServerController;
use chrono::Utc;
use dropshot::ExtractedParameter;
use futures::future::ready;
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

    pub async fn add_server_controller(&self, sc: ServerController) {
        let mut scs = self.server_controllers.lock().await;
        assert!(!scs.contains_key(&sc.id));
        info!(self.log, "registered server controller";
            "server_uuid" => sc.id.to_string());
        scs.insert(sc.id.clone(), Arc::new(sc));
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
         *    TODO-robustness we could look for it here and resume creation if
         *    we find it.  That would potentially handle a case where the
         *    client is retrying their create request.  On the other hand, we
         *    need some way to either clean this up or resume creation even if
         *    the caller _doesn't_ retry this request.  How will we know it's
         *    been abandoned?  (Maybe it doesn't matter that much, since the
         *    following operations are either idempotent or we can tell that
         *    they've conflicted with somebody else who's fixing the problem.)
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
        let id = instance_created.identity.id.clone();

        /*
         * Notify the SC, which will return an updated runtime state (which
         * should be "Starting".
         */
        let runtime_state = sc
            .instance_ensure(instance_created, &ApiInstanceRuntimeStateParams {
                run_state: ApiInstanceState::Running,
            })
            .await?;

        /*
         * Update the database to reflect the new "Starting" state.
         */
        let instance_updated = self
            .datastore
            .instance_update_runtime(&id, &runtime_state)
            .await?;
        Ok(instance_updated)
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

    pub fn as_sc_api(self: &Arc<OxideController>) -> ControllerScApi {
        ControllerScApi {
            controller: Arc::clone(self),
        }
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
    ) {
        let datastore = &self.controller.datastore;
        let log = &self.controller.log;
        let result =
            datastore.instance_update_runtime(id, &new_runtime_state).await;

        if let Err(error) = result {
            warn!(log, "failed to update instance from server controller";
                "instance_id" => %id,
                "new_state" => %new_runtime_state.run_state,
                "error" => ?error);
        } else {
            debug!(log, "instance updated by server controller";
                "instance_id" => %id,
                "new_state" => %new_runtime_state.run_state);
        }
    }
}
