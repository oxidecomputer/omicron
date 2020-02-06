/*!
 * playground-area implementation of a few resources to experiment with
 * different ways of organizing this code.
 */

use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::web::Data;
use futures::stream::StreamExt;

use crate::api_error;
use crate::api_http;
use crate::api_model;

use api_error::ApiError;

/**
 * Stores shared state used by API endpoints
 */
pub struct ApiServerState {
    /** the API backend to use for servicing requests */
    pub backend: &'static dyn api_model::ApiBackend
}

pub fn register_actix_api(config: &mut actix_web::web::ServiceConfig)
{
    config.service(actix_web::web::resource("/projects")
        .route(actix_web::web::get().to(api_projects_get)));
}

async fn api_projects_get(server: Data<ApiServerState>,
    _req: HttpRequest)
    -> Result<HttpResponse, ApiError>
{
    let backend = server.backend;
    let project_stream = api_model::api_model_list_projects(backend).await?;
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
