// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Dropshot server for configuring DNS namespace
// XXX-dap should this use HTTP conditional requests?

use crate::dns_types::DnsConfig;
use crate::storage;
use dropshot::{endpoint, RequestContext};

pub struct Context {
    store: storage::Store,
}

impl Context {
    pub fn new(store: storage::Store) -> Context {
        Context { store }
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
    let config = apictx.store.dns_config().await.map_err(|e| {
        dropshot::HttpError::for_internal_error(format!(
            "internal error: {:?}",
            e
        ))
    })?;
    Ok(dropshot::HttpResponseOk(config))
}

// XXX-dap tune up maximum input size
#[endpoint(
    method = PUT,
    path = "/config",
)]
async fn dns_config_put(
    rqctx: RequestContext<Context>,
    rq: dropshot::TypedBody<DnsConfig>,
) -> Result<dropshot::HttpResponseUpdatedNoContent, dropshot::HttpError> {
    let apictx = rqctx.context();
    apictx
        .store
        .dns_config_update(&rq.into_inner(), &rqctx.request_id)
        .await
        .map_err(|e| {
        dropshot::HttpError::for_internal_error(format!(
            "internal error: {:?}",
            e
        ))
    })?;
    Ok(dropshot::HttpResponseUpdatedNoContent())
}
