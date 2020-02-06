/*!
 * facilities for working with objects in the API (agnostic to both the HTTP
 * transport through which consumers interact with them and the backend
 * implementation (simulator or a real rack)).
 */

use async_trait::async_trait;
use futures::stream::Stream;
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
 * Represents a Project in the Oxide API.
 */
#[derive(Debug, Serialize)]
pub struct ApiModelProject {
    pub name: String,
}

/**
 * Represents a backend implementation of the API.
 */
#[async_trait]
pub trait ApiBackend: Send + Sync {
    async fn projects_list<>(&'static self) -> ApiListResult<ApiModelProject>;
}

pub async fn api_model_list_projects(backend: &'static dyn ApiBackend)
    -> ApiListResult<ApiModelProject>
{
    backend.projects_list().await
}
