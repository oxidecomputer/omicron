/*!
 * HTTP-agnostic interface to an Oxide Rack
 */

use crate::api_error::ApiError;
use crate::api_model::ApiInstance;
use crate::api_model::ApiInstanceCreateParams;
use crate::api_model::ApiName;
use crate::api_model::ApiObject;
use crate::api_model::ApiProject;
use crate::api_model::ApiProjectCreateParams;
use crate::api_model::ApiProjectUpdateParams;
use crate::datastore::RackDataStore;
use dropshot_endpoint::ExtractorParameter;
use futures::future::ready;
use futures::stream::Stream;
use futures::stream::StreamExt;
use serde::Deserialize;
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

#[derive(Deserialize, ExtractorParameter)]
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
 * Represents the state of the Oxide rack that we're managing.
 *
 * Right now, this is mostly a wrapper around the data store because this server
 * doesn't do much beyond CRUD operations.  However, higher-level functionality
 * could go here, including caching of objects.
 */
pub struct OxideRack {
    /** persistent storage for resources in the rack */
    datastore: RackDataStore,
}

/*
 * TODO Is it possible to make some of these operations more generic?  A
 * particularly good example is probably list() (or even lookup()), where
 * with the right type parameters, generic code can be written to work on all
 * types.
 * TODO update and delete need to accommodate both with-etag and don't-care
 * TODO audit logging ought to be part of this structure and its functions
 */
impl OxideRack {
    pub fn new() -> OxideRack {
        OxideRack {
            datastore: RackDataStore::new(),
        }
    }

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
        self.datastore.project_create_instance(project_name, params).await
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
}
