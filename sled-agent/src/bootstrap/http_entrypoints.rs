//! HTTP entrypoint functions for the bootstrap agent's exposed API

use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::RequestContext;
use dropshot::TypedBody;
use omicron_common::api::external::Error as ExternalError;
use omicron_common::api::internal::bootstrap_agent::{
    ShareRequest, ShareResponse,
};
use std::sync::Arc;

use super::agent::Agent;

/// Returns a description of the bootstrap agent API
pub fn ba_api() -> ApiDescription<Arc<Agent>> {
    fn register_endpoints(
        api: &mut ApiDescription<Arc<Agent>>,
    ) -> Result<(), String> {
        api.register(api_request_share)?;
        Ok(())
    }

    let mut api = ApiDescription::new();
    if let Err(err) = register_endpoints(&mut api) {
        panic!("failed to register entrypoints: {}", err);
    }
    api
}

#[endpoint {
    method = GET,
    path = "/request_share",
}]
async fn api_request_share(
    rqctx: Arc<RequestContext<Arc<Agent>>>,
    request: TypedBody<ShareRequest>,
) -> Result<HttpResponseOk<ShareResponse>, HttpError> {
    let bootstrap_agent = rqctx.context();

    let request = request.into_inner();
    Ok(HttpResponseOk(
        bootstrap_agent
            .request_share(request.identity)
            .await
            .map_err(|e| ExternalError::from(e))?,
    ))
}
