/*!
 * api_model.rs: facilities for working with objects in the API (agnostic to
 * both the HTTP transport through which consumers interact with them and
 * the backend implementation (simulator or a real rack)).
 */

use futures::stream::StreamExt;

use crate::api_error;

/**
 * A stream of Results, each potentially representing an object in the API.
 */
pub type ApiObjectStream<T> = std::pin::Pin<Box<
    dyn futures::stream::Stream<Item = Result<T, api_error::ApiError>>>>;


/**
 * Represents a Project in the Oxide API.
 */
#[derive(Debug, serde::Serialize)]
pub struct ApiModelProject {
    name: String,
}

pub async fn api_model_list_projects()
    -> Result<ApiObjectStream<ApiModelProject>, api_error::ApiError>
{
    /*
     * TODO This is currently hardcoded to return a particular set of projects,
     * including an Error to exercise that case.
     */
    Ok(futures::stream::iter(
        vec![
            Ok(ApiModelProject { name: "project1".to_string() }),
            // Ok(ApiModelProject { name: "project2".to_string() }),
            Err(api_error::ApiError {}),
            Ok(ApiModelProject { name: "project3".to_string() }),
        ]).boxed())
}
