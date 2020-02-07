/*!
 * playground-area implementation of a few resources to experiment with
 * different ways of organizing this code.
 */

use actix_web::HttpResponse;
use actix_web::web::Data;
use actix_web::web::Json;
use actix_web::web::Path;
use actix_web::web::ServiceConfig;
use futures::stream::StreamExt;

use crate::api_error;
use crate::api_http;
use crate::api_model;

use api_error::ApiError;
use api_model::ApiModelProjectCreate;

/**
 * Stores shared state used by API endpoints
 */
pub struct ApiServerState {
    /** the API backend to use for servicing requests */
    pub backend: Box<dyn api_model::ApiBackend>
}

pub fn register_actix_api(config: &mut ServiceConfig)
{
    config.service(actix_web::web::resource("/projects")
        .route(actix_web::web::get().to(api_projects_get))
        .route(actix_web::web::post().to(api_projects_post)));
    config.service(actix_web::web::resource("/projects/{projectId}")
        .route(actix_web::web::delete().to(api_projects_delete_project)));
}

async fn api_projects_get(server: Data<ApiServerState>)
    -> Result<HttpResponse, ApiError>
{
    let backend = &*server.backend;
    let project_stream = api_model::api_model_projects_list(backend).await?;
    let byte_stream = project_stream.map(|project|
        api_http::api_serialize_object_for_stream(&project));

    /*
     * TODO Figure out if this is the right format (newline-separated JSON) and
     * if so whether it's a good content-type for this.
     * Is it important to be able to support different formats later?  (or
     * useful to factor the code so that we could?)
     */
    let response = HttpResponse::Ok()
        .content_type("application/x-json-stream")
        .streaming(byte_stream);
    Ok(response)
}

async fn api_projects_post(
    server: Data<ApiServerState>,
    new_project: Json<ApiModelProjectCreate>)
    -> Result<HttpResponse, ApiError>
{
    let backend = &*server.backend;
    api_model::api_model_project_create(backend, &new_project).await?;
    Ok(HttpResponse::NoContent().finish())
}

async fn api_projects_delete_project(
    server: Data<ApiServerState>,
    project_id: Path<String>)
    -> Result<HttpResponse, ApiError>
{
    let backend = &*server.backend;
    api_model::api_model_project_delete(backend, &*project_id).await?;
    Ok(HttpResponse::NoContent().finish())
}
