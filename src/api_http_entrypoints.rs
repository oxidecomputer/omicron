/*!
 * playground-area implementation of a few resources to experiment with
 * different ways of organizing this code.
 */

use std::sync::Arc;

use hyper::Response;
use hyper::Body;
// use actix_web::HttpResponse;
// use actix_web::web::Data;
// use actix_web::web::Json;
// use actix_web::web::Path;
// use actix_web::web::Query;
// use actix_web::web::ServiceConfig;
use serde::Deserialize;

use crate::api_error::ApiError;
use crate::api_http_util::api_http_create;
//use crate::api_http_util::api_http_delete;
//use crate::api_http_util::api_http_emit_one;
//use crate::api_http_util::api_http_emit_stream;
use crate::api_model::ApiProject;
use crate::api_model::ApiProjectCreateParams;
use crate::api_model::ApiProjectUpdateParams;
use crate::api_server::ApiServerState;

/*
 * API ENDPOINT FUNCTION NAMING CONVENTIONS
 *
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
 *    DELETE /projects/{project_id}     -> api_projects_delete_project()
 *    GET    /projects/{project_id}     -> api_projects_get_project()
 *    PUT    /projects/{project_id}     -> api_projects_put_project()
 */

// /* TODO-cleanup: should not be public */
// #[derive(Deserialize)]
// pub struct ListQueryParams {
//     marker: Option<String>,
//     limit: Option<usize>
// }

// /*
//  * "GET /projects": list all projects
//  */
// async fn api_projects_get(
//     server: Data<ApiServerState>,
//     params: Query<ListQueryParams>)
//     -> Result<HttpResponse, ApiError>
// {
//     let backend = &*server.backend;
//     let limit = params.limit.unwrap_or(3); // XXX
//     let marker = params.marker.as_ref().map(|s| s.clone());
//     let project_stream = backend.projects_list(marker, limit).await?;
//     api_http_emit_stream(project_stream)
// }

/*
 * "POST /projects": create a new project
 * TODO-cleanup: none of the endpoints should be public
 */
pub async fn api_projects_post(
    server: &ApiServerState,
    new_project: &ApiProjectCreateParams)
    -> Result<Response<Body>, ApiError>
{
    let backend = &*server.backend;
    let project = backend.project_create(&*new_project).await?;
    api_http_create(project)
}

// /*
//  * "GET /project/{project_id}": fetch a specific project
//  */
// async fn api_projects_get_project(
//     server: Data<ApiServerState>,
//     project_id: Path<String>)
//     -> Result<HttpResponse, ApiError>
// {
//     let backend = &*server.backend;
//     let project_id = project_id.to_string();
//     let project : Arc<ApiProject> = backend.project_lookup(project_id).await?;
//     api_http_emit_one(project)
// }
// 
// /*
//  * "DELETE /project/{project_id}": delete a specific project
//  */
// async fn api_projects_delete_project(
//     server: Data<ApiServerState>,
//     project_id: Path<String>)
//     -> Result<HttpResponse, ApiError>
// {
//     let backend = &*server.backend;
//     let project_id = project_id.to_string();
//     backend.project_delete(project_id).await?;
//     api_http_delete()
// }
// 
// /*
//  * "PUT /project/{project_id}": update a specific project
//  *
//  * TODO: Is it valid for PUT to accept application/json that's a subset of what
//  * the resource actually represents?  If not, is that a problem?  (HTTP may
//  * require that this be idempotent.)  If so, can we get around that having this
//  * be a slightly different content-type (e.g., "application/json-patch")?  We
//  * should see what other APIs do.
//  */
// async fn api_projects_put_project(
//     server: Data<ApiServerState>,
//     project_id: Path<String>,
//     updated_project: Json<ApiProjectUpdateParams>)
//     -> Result<HttpResponse, ApiError>
// {
//     let backend = &*server.backend;
//     let project_id = project_id.to_string();
//     let newproject = backend.project_update(
//         project_id, &*updated_project).await?;
//     api_http_emit_one(newproject)
// }
