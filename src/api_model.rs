/*!
 * facilities for working with objects in the API (agnostic to both the HTTP
 * transport through which consumers interact with them and the backend
 * implementation (simulator or a real rack)).
 */

use async_trait::async_trait;
use futures::stream::Stream;
use serde::Deserialize;
use serde::Serialize;
use std::pin::Pin;

use crate::api_error::ApiError;

/**
 * A stream of Results, each potentially representing an object in the API.
 */
pub type ApiObjectStream<T> = Pin<Box<
    dyn Stream<Item = Result<T, ApiError>>
>>;

/**
 * Result of a list operation that returns an ApiObjectStream.
 */
pub type ApiListResult<T> = Result<ApiObjectStream<T>, ApiError>;

/**
 * Result of a create operation for the specified type.
 */
pub type ApiCreateResult<T> = Result<T, ApiError>;


/**
 * Represents a Project in the Oxide API.
 */
#[derive(Debug, Serialize)]
pub struct ApiModelProject {
    pub name: String,
}

/**
 * Represents the create-time parameters for a Project.
 */
#[derive(Debug, Deserialize)]
pub struct ApiModelProjectCreate {
    pub name: String
}

/**
 * Represents a backend implementation of the API.
 */
#[async_trait]
pub trait ApiBackend: Send + Sync {
    async fn projects_list(&self) -> ApiListResult<ApiModelProject>;
    async fn project_create(&self,
        new_project: &ApiModelProjectCreate)
        -> ApiCreateResult<ApiModelProject>;
    async fn project_delete(&self, project_id: &String)
        -> Result<(), ApiError>;
}

pub async fn api_model_projects_list(backend: &dyn ApiBackend)
    -> ApiListResult<ApiModelProject>
{
    backend.projects_list().await
}

pub async fn api_model_project_create(backend: &dyn ApiBackend,
    new_project: &ApiModelProjectCreate)
    -> ApiCreateResult<ApiModelProject>
{
    backend.project_create(new_project).await
}

pub async fn api_model_project_delete(backend: &dyn ApiBackend,
    project_id: &String)
    -> Result<(), ApiError>
{
    backend.project_delete(project_id).await
}
