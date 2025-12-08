// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A fake implementation of (some) of the internal Nexus interface
//!
//! This must be an exact subset of the Nexus internal interface
//! to operate correctly.

use dropshot::{
    ApiDescription, HttpError, HttpResponseOk, HttpResponseUpdatedNoContent,
    Path, RequestContext, TypedBody, endpoint,
};
use internal_dns_types::config::DnsConfigBuilder;
use internal_dns_types::names::ServiceName;
use nexus_client::types::SledAgentInfo;
use omicron_common::api::external::Error;
use omicron_common::api::internal::nexus::SledVmmState;
use omicron_uuid_kinds::{OmicronZoneUuid, PropolisUuid, SledUuid};
use schemars::JsonSchema;
use serde::Deserialize;
use sled_agent_types_migrations::v1::params::VmmPathParam;

/// Implements a fake Nexus.
///
/// - All methods should match Nexus' interface, if they exist.
/// - Not all methods should be called by all tests. By default,
/// each method, representing an endpoint, should return an error.
pub trait FakeNexusServer: Send + Sync {
    fn sled_agent_get(
        &self,
        _sled_id: SledUuid,
    ) -> Result<SledAgentInfo, Error> {
        Err(Error::internal_error("Not implemented"))
    }

    fn sled_agent_put(
        &self,
        _sled_id: SledUuid,
        _info: SledAgentInfo,
    ) -> Result<(), Error> {
        Err(Error::internal_error("Not implemented"))
    }

    fn cpapi_instances_put(
        &self,
        _propolis_id: PropolisUuid,
        _new_runtime_state: SledVmmState,
    ) -> Result<(), Error> {
        Err(Error::internal_error("Not implemented"))
    }
}

/// Describes the server context type.
///
/// If you're writing a test, this is a type you should create when calling
/// [`start_test_server`].
pub type ServerContext = Box<dyn FakeNexusServer>;

/// Path parameters for Sled Agent requests (internal API)
#[derive(Deserialize, JsonSchema)]
struct SledAgentPathParam {
    sled_id: SledUuid,
}

/// Return information about the given sled agent
#[endpoint {
     method = GET,
     path = "/sled-agents/{sled_id}",
 }]
async fn sled_agent_get(
    request_context: RequestContext<ServerContext>,
    path_params: Path<SledAgentPathParam>,
) -> Result<HttpResponseOk<SledAgentInfo>, HttpError> {
    let context = request_context.context();

    Ok(HttpResponseOk(
        context.sled_agent_get(path_params.into_inner().sled_id)?,
    ))
}

#[endpoint {
     method = POST,
     path = "/sled-agents/{sled_id}",
 }]
async fn sled_agent_put(
    request_context: RequestContext<ServerContext>,
    path_params: Path<SledAgentPathParam>,
    sled_info: TypedBody<SledAgentInfo>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let context = request_context.context();
    context.sled_agent_put(
        path_params.into_inner().sled_id,
        sled_info.into_inner(),
    )?;
    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = PUT,
    path = "/vmms/{propolis_id}",
}]
async fn cpapi_instances_put(
    request_context: RequestContext<ServerContext>,
    path_params: Path<VmmPathParam>,
    new_runtime_state: TypedBody<SledVmmState>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let context = request_context.context();
    context.cpapi_instances_put(
        path_params.into_inner().propolis_id,
        new_runtime_state.into_inner(),
    )?;
    Ok(HttpResponseUpdatedNoContent())
}

fn api() -> ApiDescription<ServerContext> {
    let mut api = ApiDescription::new();
    api.register(sled_agent_get).unwrap();
    api.register(sled_agent_put).unwrap();
    api.register(cpapi_instances_put).unwrap();
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
    dropshot::ServerBuilder::new(api(), label, log)
        .config(config_dropshot)
        .start()
        .unwrap()
}

/// Creates a transient DNS server pointing to a fake Nexus dropshot server.
#[allow(unused)]
pub async fn start_dns_server(
    log: &slog::Logger,
    nexus: &dropshot::HttpServer<ServerContext>,
) -> dns_server::TransientServer {
    let dns = dns_server::TransientServer::new(log).await.unwrap();
    let mut dns_config_builder = DnsConfigBuilder::new();

    let nexus_addr = match nexus.local_addr() {
        std::net::SocketAddr::V6(addr) => addr,
        _ => panic!("Expected IPv6 address"),
    };

    let nexus_zone = dns_config_builder
        .host_zone(OmicronZoneUuid::new_v4(), *nexus_addr.ip())
        .expect("failed to set up DNS");
    dns_config_builder
        .service_backend_zone(
            ServiceName::Nexus,
            &nexus_zone,
            nexus_addr.port(),
        )
        .expect("failed to set up DNS");
    let dns_config =
        dns_config_builder.build_full_config_for_initial_generation();
    dns.initialize_with_config(log, &dns_config).await.unwrap();
    dns
}
