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
 * Recall that we intend to support two backends: one backed by a real Oxide
 * rack and the other backed by a simulator.  The interfaces to these backends
 * is defined by the `ApiBackend` trait, which provides functions for operating
 * on resources like projects.  For example, `ApiBackend` provides
 * `project_lookup(primary key)`,
 * `project_list(marker: Option, limit: usize)`,
 * `project_create(project, ApiProjectCreateParams)`,
 * `project_update(project, ApiProjectUpdateParams)`, and
 * `project_delete(project)`.  These are all `async` functions.
 *
 * We expect to add many more types to the API for things like instances, disks,
 * images, networking abstractions, organizations, teams, users, system
 * components, and the like.  See RFD 4 for details.  The current plan is to add
 * types and supporting backend functions for each of these resources.  However,
 * different types may support different operations.  For examples, instances
 * will have additional operations (like "boot" and "halt").  System component
 * resources may be immutable (i.e., they won't define a "CreateParams" type, an
 * "UpdateParams" type, nor create or update functions on the Backend).
 *
 * The only thing guaranteed by the `ApiObject` trait is that the type can be
 * converted to a View, which is something that can be serialized.
 */
pub trait ApiObject {
    type View: Serialize;
    fn to_view(&self) -> Self::View;
}

/*
 * PROJECTS
 */

/**
 * Represents a Project in the API.  See RFD for field details.
 */
pub struct ApiProject {
    /** private data used by the backend implementation */
    pub backend_impl: Box<dyn Any + Send + Sync>,

    /** unique identifier assigned by the system for the project */
    pub id: String,
    /** unique identifier assigned by the user for the project */
    pub name: String,
    /** human-readable label for the project */
    pub description: String,

    /*
     * TODO
     * We define a generation number here at the model layer so that in theory
     * the model layer can handle optimistic concurrency control (i.e.,
     * put-only-if-matches-etag and the like).  It's not yet clear if this is
     * better handled in the backend or if a generation number is the right way
     * to express this.
     */
    /** generation number for this version of the object. */
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
 * TODO Is this where the OpenAPI documentation should go?
 */
#[derive(Debug, Deserialize, Serialize)]
pub struct ApiProjectView {
    pub id: String,
    pub name: String,
    pub description: String
}

/**
 * Represents the create-time parameters for a Project.
 * TODO Is this where the OpenAPI documentation should go?
 */
#[derive(Debug, Deserialize, Serialize)]
pub struct ApiProjectCreateParams {
    pub name: String,
    pub description: String,
}

/**
 * Represents the properties of a Project that can be updated by end users.
 * TODO Is this where the OpenAPI documentation should go?
 */
#[derive(Debug, Deserialize, Serialize)]
pub struct ApiProjectUpdateParams {
    pub name: Option<String>,
    pub description: Option<String>,
}

/*
 * BACKEND INTERFACES
 *
 * TODO: Currently, the HTTP layer calls directly into the backend layer.
 * That's probably not what we want.  A good example where we don't really want
 * that is where the user requests to delete a project.  We need to go delete
 * everything _in_ that project first.  That's common code that ought to live
 * outside the backend.  It's also not HTTP-specific, if we were to throw some
 * other control interface on this server.  Hence, it belongs in this model
 * layer.
 */

/*
 * These type aliases exist primarily to make it easier to be consistent in the
 * way these functions look.
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
pub type ObjectStream<T> = Pin<Box<
    dyn Stream<Item = Result<Arc<T>, ApiError>> + Send
>>;

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
