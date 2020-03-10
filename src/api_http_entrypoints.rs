/*!
 * Handler functions (entrypoints) for HTTP APIs
 */

use std::sync::Arc;

use hyper::Body;
use hyper::Method;
use hyper::Response;
use serde::Deserialize;

use crate::api_backend;
use crate::api_http_util::api_http_create;
use crate::api_http_util::api_http_delete;
use crate::api_http_util::api_http_emit_one;
use crate::api_http_util::api_http_emit_stream;
use crate::api_model::ApiProject;
use crate::api_model::ApiProjectCreateParams;
use crate::api_model::ApiProjectUpdateParams;
use crate::httpapi::http_extract_path_params;
use crate::httpapi::HttpError;
use crate::httpapi::HttpRouteHandler;
use crate::httpapi::HttpRouter;
use crate::httpapi::Json;
use crate::httpapi::Query;
use crate::httpapi::RequestContext;

/** Default maximum number of items per page of "list" results */
const DEFAULT_LIST_PAGE_SIZE: usize = 100;

pub fn api_register_entrypoints(router: &mut HttpRouter) {
    router.insert(
        Method::GET,
        "/projects",
        HttpRouteHandler::new(api_projects_get),
    );
    router.insert(
        Method::POST,
        "/projects",
        HttpRouteHandler::new(api_projects_post),
    );
    router.insert(
        Method::GET,
        "/projects/{project_id}",
        HttpRouteHandler::new(api_projects_get_project),
    );
    router.insert(
        Method::DELETE,
        "/projects/{project_id}",
        HttpRouteHandler::new(api_projects_delete_project),
    );
    router.insert(
        Method::PUT,
        "/projects/{project_id}",
        HttpRouteHandler::new(api_projects_put_project),
    );
}

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

#[derive(Deserialize)]
struct ListQueryParams {
    pub marker: Option<String>,
    pub limit: Option<usize>,
}

/*
 * "GET /projects": list all projects
 */
async fn api_projects_get(
    rqctx: Arc<RequestContext>,
    params_raw: Query<ListQueryParams>,
) -> Result<Response<Body>, HttpError> {
    let backend = api_backend(&rqctx);
    let params = params_raw.into_inner();
    let limit = params.limit.unwrap_or(DEFAULT_LIST_PAGE_SIZE);
    let marker = params.marker.as_ref().map(|s| s.clone());
    let project_stream = backend.projects_list(marker, limit).await?;
    api_http_emit_stream(project_stream).await
}

/*
 * "POST /projects": create a new project
 */
async fn api_projects_post(
    rqctx: Arc<RequestContext>,
    new_project: Json<ApiProjectCreateParams>,
) -> Result<Response<Body>, HttpError> {
    let backend = api_backend(&rqctx);
    let project = backend.project_create(&new_project.into_inner()).await?;
    api_http_create(project)
}

#[derive(Deserialize)]
struct ProjectPathParam {
    project_id: String,
}

/*
 * "GET /project/{project_id}": fetch a specific project
 */
async fn api_projects_get_project(
    rqctx: Arc<RequestContext>,
) -> Result<Response<Body>, HttpError> {
    let backend = api_backend(&rqctx);
    let params: ProjectPathParam =
        http_extract_path_params(&rqctx.path_variables)?;
    let project_id = &params.project_id;
    let project: Arc<ApiProject> = backend.project_lookup(project_id).await?;
    api_http_emit_one(project)
}

/*
 * "DELETE /project/{project_id}": delete a specific project
 */
async fn api_projects_delete_project(
    rqctx: Arc<RequestContext>,
) -> Result<Response<Body>, HttpError> {
    let backend = api_backend(&rqctx);
    let params: ProjectPathParam =
        http_extract_path_params(&rqctx.path_variables)?;
    let project_id = &params.project_id;
    backend.project_delete(project_id).await?;
    api_http_delete()
}

/*
 * "PUT /project/{project_id}": update a specific project
 *
 * TODO-correctness: Is it valid for PUT to accept application/json that's a
 * subset of what the resource actually represents?  If not, is that a problem?
 * (HTTP may require that this be idempotent.)  If so, can we get around that
 * having this be a slightly different content-type (e.g.,
 * "application/json-patch")?  We should see what other APIs do.
 */
async fn api_projects_put_project(
    rqctx: Arc<RequestContext>,
    updated_project: Json<ApiProjectUpdateParams>,
) -> Result<Response<Body>, HttpError> {
    let backend = api_backend(&rqctx);
    let params: ProjectPathParam =
        http_extract_path_params(&rqctx.path_variables)?;
    let project_id = &params.project_id;
    let newproject = backend
        .project_update(project_id, &updated_project.into_inner())
        .await?;
    api_http_emit_one(newproject)
}
