// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::Utf8PathBuf;
use clickhouse_admin_types::config::{
    path_schema, KeeperConfig, RaftServerSettings, ReplicaConfig,
};
use clickhouse_admin_types::{ServerSettings, KeeperId};
use dropshot::{
    HttpError, HttpResponseCreated, Path, RequestContext, TypedBody,
};
use omicron_common::api::external::Generation;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::Ipv6Addr;

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GenerationNum {
    /// A unique identifier for the configuration generation.
    pub generation: Generation,
}

#[dropshot::api_description]
pub trait ClickhouseAdminApi {
    type Context;

    /// Generate a ClickHouse configuration file for a server node on a specified
    /// directory.
    #[endpoint {
        method = POST,
        path = "/node/server/generate-config/{generation}",
    }]
    async fn generate_server_config(
        rqctx: RequestContext<Self::Context>,
        path: Path<GenerationNum>,
        body: TypedBody<ServerSettings>,
    ) -> Result<HttpResponseCreated<ReplicaConfig>, HttpError>;

    /// Generate a ClickHouse configuration file for a keeper node on a specified
    /// directory.
    #[endpoint {
        method = POST,
        path = "/node/keeper/generate-config/{generation}",
    }]
    async fn generate_keeper_config(
        rqctx: RequestContext<Self::Context>,
        path: Path<GenerationNum>,
        body: TypedBody<KeeperSettings>,
    ) -> Result<HttpResponseCreated<KeeperConfig>, HttpError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct KeeperSettings {
    #[schemars(schema_with = "path_schema")]
    pub config_dir: Utf8PathBuf,
    #[schemars(schema_with = "path_schema")]
    pub datastore_path: Utf8PathBuf,
    pub listen_addr: Ipv6Addr,
    pub node_id: KeeperId,
    pub keepers: Vec<RaftServerSettings>,
}
