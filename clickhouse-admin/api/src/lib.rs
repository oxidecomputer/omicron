// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use clickhouse_admin_types::config::{KeeperConfig, ReplicaConfig};
use clickhouse_admin_types::{KeeperSettings, ServerSettings};
use dropshot::{
    HttpError, HttpResponseCreated, Path, RequestContext, TypedBody,
};
use omicron_common::api::external::Generation;
use schemars::JsonSchema;
use serde::Deserialize;

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
