// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::{HttpError, HttpResponseCreated, HttpResponseOk, RequestContext, TypedBody};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::SocketAddrV6;

#[dropshot::api_description]
pub trait ClickhouseAdminApi {
    type Context;

    /// Retrieve the address the ClickHouse server or keeper node is listening on.
    #[endpoint {
        method = GET,
        path = "/node/address",
    }]
    async fn clickhouse_address(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<ClickhouseAddress>, HttpError>;

    /// Generate a ClickHouse configuration file for a server node on a specified
    /// directory.
    #[endpoint {
        method = POST,
        // TODO: I'm not sure about this endpoint, could be better?
        path = "/server-node/generate-config",
    }]
    async fn generate_server_config(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<ServerSettings>,
    ) -> Result<HttpResponseCreated<ServerConfigGenerateResponse>, HttpError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ClickhouseAddress {
    pub clickhouse_address: SocketAddrV6,
}

// TODO: Perhaps change this response type for something better
// like an object with all the settings or something like that
/// Success response for server node configuration file generation
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ServerConfigGenerateResponse {
    pub success: bool,
}

impl ServerConfigGenerateResponse {
    pub fn success() -> Self {
        Self { success: true }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ServerSettings {
    pub node_id: u64,
}