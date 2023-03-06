// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Dropshot server for configuring DNS namespace
// XXX-dap should this use HTTP conditional requests?

use crate::dns_types::DnsConfig;
use crate::storage;
use dropshot::{endpoint, RequestContext};

pub struct Context {
    client: storage::Client,
}

impl Context {
    pub fn new(client: storage::Client) -> Context {
        Context { client }
    }
}

pub fn api() -> dropshot::ApiDescription<Context> {
    let mut api = dropshot::ApiDescription::new();

    api.register(dns_config_get).expect("register dns_config_get");
    api.register(dns_config_put).expect("register dns_config_update");
    api
}

#[endpoint(
    method = GET,
    path = "/config",
)]
async fn dns_config_get(
    rqctx: RequestContext<Context>,
) -> Result<dropshot::HttpResponseOk<DnsConfig>, dropshot::HttpError> {
    let apictx = rqctx.context();
    let config = apictx.client.dns_config().await.map_err(|e| {
        dropshot::HttpError::for_internal_error(format!(
            "internal error: {:?}",
            e
        ))
    })?;
    Ok(dropshot::HttpResponseOk(config))
}

#[endpoint(
    method = PUT,
    path = "/config",
)]
async fn dns_config_put(
    rqctx: RequestContext<Context>,
    rq: dropshot::TypedBody<DnsConfig>,
) -> Result<dropshot::HttpResponseUpdatedNoContent, dropshot::HttpError> {
    let apictx = rqctx.context();
    apictx.client.dns_config_update(&rq.into_inner()).await.map_err(|e| {
        dropshot::HttpError::for_internal_error(format!(
            "internal error: {:?}",
            e
        ))
    })?;
    Ok(dropshot::HttpResponseUpdatedNoContent())
}
