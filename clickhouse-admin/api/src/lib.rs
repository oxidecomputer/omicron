// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use clickhouse_admin_types::config::{ClickhouseHost, ReplicaConfig};
use dropshot::{HttpError, HttpResponseCreated, RequestContext, TypedBody};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::Ipv6Addr;

#[dropshot::api_description]
pub trait ClickhouseAdminApi {
    type Context;

    /// Generate a ClickHouse configuration file for a server node on a specified
    /// directory.
    #[endpoint {
        method = POST,
        path = "/node/server/generate-config",
    }]
    async fn generate_server_config(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<ServerSettings>,
    ) -> Result<HttpResponseCreated<ReplicaConfig>, HttpError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ServerSettings {
    pub node_id: u64,
    pub keepers: Vec<ClickhouseHost>,
    pub remote_servers: Vec<ClickhouseHost>,
    pub config_dir: String,
    pub datastore_path: String,
    pub listen_addr: Ipv6Addr,
}
