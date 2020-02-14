/*!
 * playground-area implementation of a few resources to experiment with
 * different ways of organizing this code.
 */

use std::sync::Arc;

use actix_web::HttpResponse;
use actix_web::web::Data;
use actix_web::web::Json;
use actix_web::web::Path;
use actix_web::web::Query;
use actix_web::web::ServiceConfig;
use futures::stream::StreamExt;
use serde::Deserialize;

use crate::api_error::ApiError;
use crate::api_http_util::api_http_serialize_for_stream;
use crate::api_model::ApiObject;
use crate::api_model::ApiProject;
use crate::api_model::ApiProjectCreateParams;
use crate::api_model::ApiProjectUpdateParams;
use crate::api_model::ObjectStream;
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

fn api_http_create<T>(object: Arc<T>)
    -> Result<HttpResponse, ApiError>
    where T: ApiObject
{
    let serialized = api_http_serialize_for_stream(&Ok(object.to_view()))?;
    Ok(HttpResponse::Created()
        .content_type("application/json")
        .body(serialized))
}

fn api_http_delete()
    -> Result<HttpResponse, ApiError>
{
    Ok(HttpResponse::NoContent().finish())
}

fn api_http_emit_one<T>(object: Arc<T>)
    -> Result<HttpResponse, ApiError>
    where T: ApiObject
{
    let serialized = api_http_serialize_for_stream(&Ok(object.to_view()))?;
    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(serialized))
}

fn api_http_emit_stream<T: 'static>(object_stream: ObjectStream<T>)
    -> Result<HttpResponse, ApiError>
    where T: ApiObject
{
    let byte_stream = object_stream
        .map(|maybe_object| maybe_object.map(|object| object.to_view()))
        .map(|maybe_object| api_http_serialize_for_stream(&maybe_object));
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

#[derive(Deserialize)]
struct ListQueryParams {
    marker: Option<String>,
    limit: Option<usize>
}

async fn api_projects_get(
    server: Data<ApiServerState>,
    params: Query<ListQueryParams>)
    -> Result<HttpResponse, ApiError>
{
    let backend = &*server.backend;
    let limit = params.limit.unwrap_or(3); // XXX
    let marker = params.marker.as_ref().map(|s| s.clone());
    let project_stream = backend.projects_list(marker, limit).await?;
    api_http_emit_stream(project_stream)
}

async fn api_projects_post(
    server: Data<ApiServerState>,
    new_project: Json<ApiProjectCreateParams>)
    -> Result<HttpResponse, ApiError>
{
    let backend = &*server.backend;
    let project = backend.project_create(&*new_project).await?;
    api_http_create(project)
}

async fn api_projects_get_project(
    server: Data<ApiServerState>,
    project_id: Path<String>)
    -> Result<HttpResponse, ApiError>
{
    let backend = &*server.backend;
    let project_id = project_id.to_string();
    let project : Arc<ApiProject> = backend.project_lookup(project_id).await?;
    api_http_emit_one(project)
}

async fn api_projects_delete_project(
    server: Data<ApiServerState>,
    project_id: Path<String>)
    -> Result<HttpResponse, ApiError>
{
    let backend = &*server.backend;
    let project_id = project_id.to_string();
    backend.project_delete(project_id).await?;
    api_http_delete()
}

/*
 * TODO: Is it valid for PUT to accept application/json that's a subset of what
 * the resource actually represents?  If not, is that a problem?  (HTTP may
 * require that this be idempotent.)  If so, can we get around that having this
 * be a slightly different content-type (e.g., "application/json-patch")?  We
 * should see what other APIs do.
 */
async fn api_projects_put_project(
    server: Data<ApiServerState>,
    project_id: Path<String>,
    updated_project: Json<ApiProjectUpdateParams>)
    -> Result<HttpResponse, ApiError>
{
    let backend = &*server.backend;
    let project_id = project_id.to_string();
    let newproject = backend.project_update(project_id, &*updated_project).await?;
    api_http_emit_one(newproject)
}
