/*!
 * facilities for working with objects in the API (agnostic to both the HTTP
 * transport through which consumers interact with them and the backend
 * implementation (simulator or a real rack)).
 */

use async_trait::async_trait;
use futures::stream::Stream;
use serde::Deserialize;
use serde::Serialize;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;

use crate::api_error::ApiError;

/**
 * A stream of Results, each potentially representing an object in the API.
 */
pub type ObjectStream<T> = Pin<Box<
    dyn Stream<Item = Result<Arc<T>, ApiError>>
>>;

/**
 * Result of a list operation that returns an ObjectStream.
 */
pub type ListResult<T> = Result<ObjectStream<T>, ApiError>;

/**
 * Result of a create operation for the specified type.
 */
pub type CreateResult<T> = Result<Arc<T>, ApiError>;

/**
 * Result of a delete operation for the specified type.
 */
pub type DeleteResult = Result<(), ApiError>;

/**
 * Result of a lookup operation for the specified type.
 */
pub type LookupResult<T> = Result<Arc<T>, ApiError>;

/**
 * Result of an update operation for the specified type.
 */
pub type UpdateResult<T> = Result<Arc<T>, ApiError>;


/**
 * XXX update this comment
 * ApiObject is a trait implemented by the types used to represent objects in
 * the API.  It's helpful to start with a concrete example, so let's consider
 * a Project, which is about as simple a resource as we have.
 *
 * The `ApiProject` struct represents a project as understood by the API.  It
 * contains all the fields necessary to implement a Project.  It has several
 * associated types:
 *
 * * `ApiProjectView`, which is what gets emitted by the API when a user asks
 *    for a Project
 * * `ApiProjectCreateParams`, which is what must be provided to the API when a
 *   user wants to create a new project
 * * `ApiProjectUpdate`, which is what must be provided to the API when a user
 *   wants to update a project.
 *
 * There are generic functions provided at the model layer for creating,
 * deleting, updating, fetching, and listing projects:
 * `api_model_object_create()`, `api_model_object_delete()`,
 * `api_model_object_update()`, `api_model_object_delete()`, and
 * `api_model_object_lookup()`.  Different types support different type-specific
 * "list" functions.
 *
 * All types that implement ApiObject must support lookup().  Other traits are
 * implemented to support create/delete and update:
 *
 * * `ApiObjectCreateable` objects support create _and_ delete.
 *   XXX could use a better name
 * * `ApiObjectUpdateable` objects support update.
 *   XXX could use a better name
 */

pub trait ApiObject {
    type View: Serialize;
    fn to_view(&self) -> Self::View;
}

/**
 * Represents a Project in the API.
 */
pub struct ApiProject {
    pub backend_impl: Box<dyn Any + Send + Sync>,

    pub id: String,
    pub name: String,
    pub description: String,
    pub generation: u64,
}

impl ApiObject for ApiProject {
    type View = ApiProjectView;

    fn to_view(&self)
        -> ApiProjectView
    {
        ApiProjectView {
            id: self.id.clone(),
            name: self.name.clone(),
            description: self.description.clone()
        }
    }
}

/**
 * Represents the properties of a Project that can be seen by end users.
 */
#[derive(Debug, Serialize)]
pub struct ApiProjectView {
    pub id: String,
    pub name: String,
    pub description: String
}

/**
 * Represents the create-time parameters for a Project.
 */
#[derive(Debug, Deserialize)]
pub struct ApiProjectCreateParams {
    pub name: String,
    pub description: String,
}

/**
 * Represents the properties of a Project that can be updated by end users.
 */
#[derive(Debug, Deserialize)]
pub struct ApiProjectUpdateParams {
    pub name: Option<String>,
    pub description: Option<String>,
}

/**
 * Represents a backend implementation of the API.
 * TODO Is it possible to make some of these operations more generic?  A
 * particularly good example is probably list() (or even lookup()), where
 * with the right type parameters, generic code can be written to work on all
 * types.
 * TODO update and delete need to accommodate both with-etag and don't-care
 */
#[async_trait]
pub trait ApiBackend: Send + Sync {
    async fn project_create(&self, params: &ApiProjectCreateParams)
        -> CreateResult<ApiProject>;
    async fn project_lookup(&self, name: String)
        -> LookupResult<ApiProject>;
    async fn project_delete(&self, name: String)
        -> DeleteResult;
    async fn project_update(&self, name: String,
        params: &ApiProjectUpdateParams)
        -> UpdateResult<ApiProject>;
    async fn projects_list(&self, marker: Option<String>, limit: usize)
        -> ListResult<ApiProject>;
}
