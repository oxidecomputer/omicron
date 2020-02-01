/*!
 * demo.rs: playground-area implementation of a few resources to experiment with
 * different ways of organizing this code.
 */

pub fn register_actix_demo(config: &mut actix_web::web::ServiceConfig)
{
    config.service(actix_web::web::resource("/projects")
        .route(actix_web::web::get().to(api_projects_get)));
}

/*
 * XXX need to take a closer look at what error handling looks like.  What makes
 * this a little complicated is that it looks like each API function needs to
 * return a Result<_, E> where E must be an actual struct that implements the
 * actix_web::error::ResponseError trait.  That is, it can't be "dyn
 * actix_web::error::ResponseError", nor can it be a Box of that.
 *
 * We should probably look at the "fail"/"failure" crate, as that seems
 * widely used and there's some support for it in Actix.
 *
 * We'll also want to carefully design what our errors look like:
 *
 * - each error class should be associated with an HTTP status code
 *   - should there be a hierarchy descending from ClientError and ServerError?
 * - all errors should have a string code and string description
 * - it would be nice if some errors could include additional information (e.g.,
 *   validation errors could indicate which property was invalid)
 */
#[derive(Debug)]
struct ApiError {
}
impl actix_web::error::ResponseError for ApiError {
}
impl std::fmt::Display for ApiError {
    fn fmt(&self, _f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // XXX What is this used for?  Should this emit JSON?
        // (We have to implement it in order to implement the
        // actix_web::error::ResponseError trait.)
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
enum ApiModelType {
    Project,
}

// StreamExt is used for 'boxed()'
use futures::stream::StreamExt;

async fn api_model_list_begin(resource_type: &ApiModelType)
    -> Result<std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<bytes::Bytes, ApiError>>>>, ApiError>
{
    assert_eq!(*resource_type, ApiModelType::Project);
    Ok(futures::stream::iter(
        vec![
            Ok(bytes::Bytes::from_static("project1".as_bytes())),
            Ok(bytes::Bytes::from_static("project2".as_bytes())),
            Ok(bytes::Bytes::from_static("project3".as_bytes()))]).boxed())
}

async fn api_projects_get(_req: actix_web::HttpRequest)
    -> Result<actix_web::HttpResponse, ApiError>
{
    let project_stream = api_model_list_begin(&ApiModelType::Project).await?;
    let response = actix_web::HttpResponse::Ok()
        .content_type("application/json")
        .streaming(project_stream);
    Ok(response)
}
