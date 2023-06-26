// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A fake implementation of (some) of the internal Nexus interface
//!
//! This must be an exact subset of the Nexus internal interface
//! to operate correctly.

use dropshot::{
    endpoint, ApiDescription, FreeformBody, HttpError, HttpResponseOk, Path,
    RequestContext,
};
use hyper::Body;
use omicron_common::api::external::Error;
use omicron_common::api::internal::nexus::UpdateArtifactId;

pub trait FakeNexusServer: Send + Sync {
    fn cpapi_artifact_download(
        &self,
        _artifact_id: UpdateArtifactId,
    ) -> Result<Vec<u8>, Error> {
        Err(Error::internal_error("Not implemented"))
    }
}

pub type ServerContext = Box<dyn FakeNexusServer>;

#[endpoint {
    method = GET,
    path = "/artifacts/{kind}/{name}/{version}",
}]
async fn cpapi_artifact_download(
    request_context: RequestContext<ServerContext>,
    path_params: Path<UpdateArtifactId>,
) -> Result<HttpResponseOk<FreeformBody>, HttpError> {
    let context = request_context.context();

    Ok(HttpResponseOk(
        Body::from(context.cpapi_artifact_download(path_params.into_inner())?)
            .into(),
    ))
}

fn api() -> ApiDescription<ServerContext> {
    let mut api = ApiDescription::new();
    api.register(cpapi_artifact_download).unwrap();
    api
}

pub fn start_test_server(
    log: slog::Logger,
    label: ServerContext,
) -> dropshot::HttpServer<ServerContext> {
    let config_dropshot = dropshot::ConfigDropshot {
        bind_address: "[::1]:0".parse().unwrap(),
        ..Default::default()
    };
    dropshot::HttpServerStarter::new(&config_dropshot, api(), label, &log)
        .unwrap()
        .start()
}
