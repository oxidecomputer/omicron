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
use internal_dns::ServiceName;
use omicron_common::api::external::Error;
use omicron_common::api::internal::nexus::UpdateArtifactId;

/// Implements a fake Nexus.
///
/// - All methods should match Nexus' interface, if they exist.
/// - Not all methods should be called by all tests. By default,
/// each method, representing an endpoint, should return an error.
pub trait FakeNexusServer: Send + Sync {
    fn cpapi_artifact_download(
        &self,
        _artifact_id: UpdateArtifactId,
    ) -> Result<Vec<u8>, Error> {
        Err(Error::internal_error("Not implemented"))
    }
}

/// Describes the server context type.
///
/// If you're writing a test, this is a type you should create when calling
/// [`start_test_server`].
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

/// Creates a fake Nexus test server.
///
/// Uses a [`ServerContext`] type to represent the faked Nexus server.
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

/// Creates a transient DNS server pointing to a fake Nexus dropshot server.
#[allow(unused)]
pub async fn start_dns_server(
    log: &slog::Logger,
    nexus: &dropshot::HttpServer<ServerContext>,
) -> dns_server::TransientServer {
    let dns = dns_server::TransientServer::new(log).await.unwrap();
    let mut dns_config_builder = internal_dns::DnsConfigBuilder::new();

    let nexus_addr = match nexus.local_addr() {
        std::net::SocketAddr::V6(addr) => addr,
        _ => panic!("Expected IPv6 address"),
    };

    let nexus_zone = dns_config_builder
        .host_zone(uuid::Uuid::new_v4(), *nexus_addr.ip())
        .expect("failed to set up DNS");
    dns_config_builder
        .service_backend_zone(
            ServiceName::Nexus,
            &nexus_zone,
            nexus_addr.port(),
        )
        .expect("failed to set up DNS");
    let dns_config = dns_config_builder.build();
    dns.initialize_with_config(log, &dns_config).await.unwrap();
    dns
}
