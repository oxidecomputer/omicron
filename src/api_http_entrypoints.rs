/*!
 * playground-area implementation of a few resources to experiment with
 * different ways of organizing this code.
 */

use std::sync::Arc;

use hyper::Body;
use hyper::Method;
use hyper::Response;
use serde::Deserialize;

use crate::api_error::ApiHttpError;
use crate::api_handler::api_handler_create;
use crate::api_handler::Json;
use crate::api_handler::Query;
use crate::api_handler::RequestContext;
use crate::api_http_router::HttpRouter;
use crate::api_http_util::api_http_create;
use crate::api_http_util::api_http_delete;
use crate::api_http_util::api_http_emit_one;
use crate::api_http_util::api_http_emit_stream;
use crate::api_model::ApiProject;
use crate::api_model::ApiProjectCreateParams;
use crate::api_model::ApiProjectUpdateParams;

pub fn api_register_entrypoints(router: &mut HttpRouter)
{
    router.insert(Method::GET, "/projects",
        api_handler_create(api_projects_get));
    router.insert(Method::POST, "/projects",
        api_handler_create(api_projects_post));
    router.insert(Method::GET, "/projects/{project_id}",
        api_handler_create(api_projects_get_project));
    router.insert(Method::DELETE, "/projects/{project_id}",
        api_handler_create(api_projects_delete_project));
    router.insert(Method::PUT, "/projects/{project_id}",
        api_handler_create(api_projects_put_project));
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
    pub limit: Option<usize>
}

/*
 * "GET /projects": list all projects
 */
async fn api_projects_get(
    rqctx: Arc<RequestContext>,
    params_raw: Query<ListQueryParams>
)
    -> Result<Response<Body>, ApiHttpError>
{
    let backend = &rqctx.server.backend;
    let params = params_raw.into_inner();
    let limit = params.limit.unwrap_or(3); // XXX
    let marker = params.marker.as_ref().map(|s| s.clone());
    let project_stream = backend.projects_list(marker, limit).await?;
    api_http_emit_stream(project_stream).await
}

/*
 * "POST /projects": create a new project
 */
async fn api_projects_post(
    rqctx: Arc<RequestContext>,
    new_project: Json<ApiProjectCreateParams>
)
    -> Result<Response<Body>, ApiHttpError>
{
    let backend = &*rqctx.server.backend;
    let project = backend.project_create(&new_project.into_inner()).await?;
    api_http_create(project)
}

/*
 * "GET /project/{project_id}": fetch a specific project
 * TODO-cleanup: Seeing the code below, it would be kind of nice if we could
 * take the path parameters as a function argument because it would mean that we
 * could statically verify here that we had the appropriate path parameters.
 * (Actually, it would still be a runtime check in that we'd only end up
 * matching up the path parameters during routing, but we could put the
 * unwrap()/expect() in one place instead of every handler.  Is there any way to
 * make it fail at compile time?)
 */
async fn api_projects_get_project(rqctx: Arc<RequestContext>)
    -> Result<Response<Body>, ApiHttpError>
{
    let backend = &*rqctx.server.backend;
    let project_id = &rqctx.path_variables.get(&"project_id".to_string()).
        expect("handler function invoked with route missing project_id");
    let project : Arc<ApiProject> = backend.project_lookup(project_id).await?;
    api_http_emit_one(project)
}

/*
 * "DELETE /project/{project_id}": delete a specific project
 */
async fn api_projects_delete_project(rqctx: Arc<RequestContext>)
    -> Result<Response<Body>, ApiHttpError>
{
    let backend = &*rqctx.server.backend;
    let project_id = &rqctx.path_variables.get(&"project_id".to_string()).
        expect("handler function invoked with route missing project_id");
    backend.project_delete(project_id).await?;
    api_http_delete()
}

/*
 * "PUT /project/{project_id}": update a specific project
 *
 * TODO: Is it valid for PUT to accept application/json that's a subset of what
 * the resource actually represents?  If not, is that a problem?  (HTTP may
 * require that this be idempotent.)  If so, can we get around that having this
 * be a slightly different content-type (e.g., "application/json-patch")?  We
 * should see what other APIs do.
 */
async fn api_projects_put_project(
    rqctx: Arc<RequestContext>,
    updated_project: Json<ApiProjectUpdateParams>
)
    -> Result<Response<Body>, ApiHttpError>
{
    let backend = &*rqctx.server.backend;
    let project_id = &rqctx.path_variables.get(&"project_id".to_string()).
        expect("handler function invoked with route missing project_id");
    let newproject = backend.project_update(
        project_id, &updated_project.into_inner()).await?;
    api_http_emit_one(newproject)
}
