// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Dropshot server for configuring DNS namespace
// XXX-dap should this use HTTP conditional requests?

use crate::dns_types::{DnsConfig, DnsConfigParams};
use crate::storage::{self, UpdateError};
use dns_service_client::{
    ERROR_CODE_BAD_UPDATE_GENERATION, ERROR_CODE_UPDATE_IN_PROGRESS,
};
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
    rq: dropshot::TypedBody<DnsConfigParams>,
) -> Result<dropshot::HttpResponseUpdatedNoContent, dropshot::HttpError> {
    let apictx = rqctx.context();
    apictx.store.dns_config_update(&rq.into_inner(), &rqctx.request_id).await?;
    Ok(dropshot::HttpResponseUpdatedNoContent())
}

impl From<UpdateError> for dropshot::HttpError {
    fn from(error: UpdateError) -> Self {
        let message = format!("{:#}", error);
        match &error {
            UpdateError::BadUpdateGeneration { .. } => dropshot::HttpError {
                status_code: http::StatusCode::CONFLICT,
                error_code: Some(String::from(
                    ERROR_CODE_BAD_UPDATE_GENERATION,
                )),
                external_message: message.clone(),
                internal_message: message,
            },

            UpdateError::UpdateInProgress { .. } => dropshot::HttpError {
                status_code: http::StatusCode::CONFLICT,
                error_code: Some(String::from(ERROR_CODE_UPDATE_IN_PROGRESS)),
                external_message: message.clone(),
                internal_message: message,
            },

            UpdateError::InternalError(_) => {
                dropshot::HttpError::for_internal_error(message)
            }
        }
    }
}
