// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Dropshot server for configuring DNS namespace

use crate::dns_data::{self, DnsKV, DnsRecordKey};
use dropshot::endpoint;
use std::sync::Arc;

pub struct Context {
    client: dns_data::Client,
}

impl Context {
    pub fn new(client: dns_data::Client) -> Context {
        Context { client }
    }
}

pub fn api() -> dropshot::ApiDescription<Arc<Context>> {
    let mut api = dropshot::ApiDescription::new();

    api.register(dns_records_list).expect("register dns_records_list");
    api.register(dns_records_create).expect("register dns_records_create");
    api.register(dns_records_delete).expect("register dns_records_delete");
    api
}

#[endpoint(
    method = GET,
    path = "/records",
)]
async fn dns_records_list(
    rqctx: Arc<dropshot::RequestContext<Arc<Context>>>,
) -> Result<dropshot::HttpResponseOk<Vec<DnsKV>>, dropshot::HttpError> {
    let apictx = rqctx.context();
    // XXX record key
    let records = apictx.client.get_records(None).await.map_err(|e| {
        dropshot::HttpError::for_internal_error(format!("uh oh: {:?}", e))
    })?;
    Ok(dropshot::HttpResponseOk(records))
}

#[endpoint(
    method = POST,
    path = "/records",
)]
async fn dns_records_create(
    rqctx: Arc<dropshot::RequestContext<Arc<Context>>>,
    rq: dropshot::TypedBody<Vec<DnsKV>>,
) -> Result<dropshot::HttpResponseUpdatedNoContent, dropshot::HttpError> {
    let apictx = rqctx.context();
    apictx.client.set_records(rq.into_inner()).await.map_err(|e| {
        dropshot::HttpError::for_internal_error(format!("uh oh: {:?}", e))
    })?;
    Ok(dropshot::HttpResponseUpdatedNoContent())
}

#[endpoint(
    method = DELETE,
    path = "/records",
)]
async fn dns_records_delete(
    rqctx: Arc<dropshot::RequestContext<Arc<Context>>>,
    rq: dropshot::TypedBody<Vec<DnsRecordKey>>,
) -> Result<dropshot::HttpResponseDeleted, dropshot::HttpError> {
    let apictx = rqctx.context();
    apictx.client.delete_records(rq.into_inner()).await.map_err(|e| {
        dropshot::HttpError::for_internal_error(format!("uh oh: {:?}", e))
    })?;
    Ok(dropshot::HttpResponseDeleted())
}
