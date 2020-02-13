/*!
 * playground-area implementation of a few resources to experiment with
 * different ways of organizing this code.
 */

use std::sync::Arc;

use actix_web::HttpResponse;
use actix_web::web::Data;
use actix_web::web::Json;
use actix_web::web::Path;
use actix_web::web::ServiceConfig;
use serde::Serialize;

use crate::api_error::ApiError;
use crate::api_http_util::api_http_serialize_for_stream;
use crate::api_model::ApiProject;
use crate::api_model::ApiProjectCreateParams;
use crate::api_model::ApiProjectUpdateParams;
use crate::api_server::ApiServerState;

pub fn register_api_entrypoints(config: &mut ServiceConfig)
{
    config.service(actix_web::web::resource("/projects")
        .route(actix_web::web::get().to(api_projects_get))
        .route(actix_web::web::post().to(api_projects_post)));
    config.service(actix_web::web::resource("/projects/{projectId}")
        .route(actix_web::web::delete().to(api_projects_delete_project))
        .route(actix_web::web::put().to(api_projects_put_project))
        .route(actix_web::web::get().to(api_projects_get_project)));
}

/*
 * Helper functions for emitting responses
 */

fn api_http_create<T>(object: T)
    -> Result<HttpResponse, ApiError>
    where T: Serialize
{
    let serialized = api_http_serialize_for_stream(&Ok(object))?;
    Ok(HttpResponse::Created()
        .content_type("application/json")
        .body(serialized))
}

fn api_http_delete()
    -> Result<HttpResponse, ApiError>
{
    Ok(HttpResponse::NoContent().finish())
}

fn api_http_emit_one<T>(object: T)
    -> Result<HttpResponse, ApiError>
    where T: Serialize
{
    let serialized = api_http_serialize_for_stream(&Ok(object))?;
    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(serialized))
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

async fn api_projects_get()
    -> Result<HttpResponse, ApiError>
{
    /*
     * TODO Figure out if this is the right format (newline-separated JSON) and
     * if so whether it's a good content-type for this.
     * Is it important to be able to support different formats later?  (or
     * useful to factor the code so that we could?)
     */
    unimplemented!("list projects");
}

async fn api_projects_post(
    server: Data<ApiServerState>,
    new_project: Json<ApiProjectCreateParams>)
    -> Result<HttpResponse, ApiError>
{
    let backend = &*server.backend;
    let project = backend.project_create(&*new_project).await?;
    api_http_create(project.to_view())
}

async fn api_projects_get_project(
    server: Data<ApiServerState>,
    project_id: Path<String>)
    -> Result<HttpResponse, ApiError>
{
    let backend = &*server.backend;
    let project_id = project_id.to_string();
    let project : Arc<dyn ApiProject> = backend.project_lookup(project_id).await?;
    api_http_emit_one(project.to_view())
}

async fn api_projects_delete_project(
    server: Data<ApiServerState>,
    project_id: Path<String>)
    -> Result<HttpResponse, ApiError>
{
    let backend = &*server.backend;
    let project_id = project_id.to_string();
    let project : Arc<dyn ApiProject> = backend.project_lookup(project_id).await?;
    project.delete().await?;
    api_http_delete()
}

async fn api_projects_put_project(
    server: Data<ApiServerState>,
    project_id: Path<String>,
    updated_project: Json<ApiProjectUpdateParams>)
    -> Result<HttpResponse, ApiError>
{
    let backend = &*server.backend;
    let project_id = project_id.to_string();
    let oldproject : Arc<dyn ApiProject> = backend.project_lookup(project_id).await?;
    let newproject = oldproject.update(&*updated_project).await?;
    api_http_emit_one(newproject.to_view())
}
