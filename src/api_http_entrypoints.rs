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
use crate::api_http_util;
use crate::api_model;
use crate::api_server;

use api_error::ApiError;
use api_model::ApiModelProjectCreate;
use api_server::ApiServerState;

pub fn register_api_entrypoints(config: &mut ServiceConfig)
{
    config.service(actix_web::web::resource("/projects")
        .route(actix_web::web::get().to(api_projects_get))
        .route(actix_web::web::post().to(api_projects_post)));
    config.service(actix_web::web::resource("/projects/{projectId}")
        .route(actix_web::web::delete().to(api_projects_delete_project))
        .route(actix_web::web::get().to(api_projects_get_project)));
}

/*
 * Generally, HTTP resources are grouped within some collection.  For a
 * relatively simple example:
 *
 *   GET    /projects               (list the projects in the collection)
 *   POST   /projects               (create a project in the collection)
 *   GET    /projects/{project_id}  (look up a project in the collection)
 *   DELETE /projects/{project_id}  (delete a project in the collection)
 *   PUT    /projects/{project_id}  (update a project in the collection)
 *
 * There's a naming convention for the functions that implement these API entry
 * points.  When operating on the collection itself, we use:
 *
 *    api_{collection_path}_{verb}
 *
 * For examples:
 *
 *    GET  /projects                    -> api_projects_get()
 *    POST /projects                    -> api_projects_post()
 *
 * For operations on items within the collection, we use:
 *
 *    api_{collection_path}_{verb}_{object}
 *
 * For examples:
 *
 *    GET    /projects/{project_id}     -> api_projects_get_project()
 *    DELETE /projects/{project_id}     -> api_projects_delete_project()
 */

async fn api_projects_get(server: Data<ApiServerState>)
    -> Result<HttpResponse, ApiError>
{
    let backend = &*server.backend;
    let project_stream = api_model::api_model_projects_list(backend).await?;
    let byte_stream = project_stream.map(|project|
        api_http_util::api_http_serialize_for_stream(&project));

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

async fn api_projects_get_project(
    server: Data<ApiServerState>,
    project_id: Path<String>)
    -> Result<HttpResponse, ApiError>
{
    let backend = &*server.backend;
    let project = api_model::api_model_project_lookup(
        backend, &*project_id).await?;
    let serialized = api_http_util::api_http_serialize_for_stream(&Ok(project))?;

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(serialized))
}
