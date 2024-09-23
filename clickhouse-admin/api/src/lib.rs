// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use clickhouse_admin_types::config::{KeeperConfig, ReplicaConfig};
use clickhouse_admin_types::{KeeperSettings, Lgif, ServerSettings};
use dropshot::{
    HttpError, HttpResponseCreated, HttpResponseOk, RequestContext, TypedBody,
};
use omicron_common::api::external::Generation;
use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ServerConfigurableSettings {
    /// A unique identifier for the configuration generation.
    pub generation: Generation,
    /// Configurable settings for a ClickHouse replica server node.
    pub settings: ServerSettings,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct KeeperConfigurableSettings {
    /// A unique identifier for the configuration generation.
    pub generation: Generation,
    /// Configurable settings for a ClickHouse keeper node.
    pub settings: KeeperSettings,
}

#[dropshot::api_description]
pub trait ClickhouseAdminApi {
    type Context;

    /// Generate a ClickHouse configuration file for a server node on a specified
    /// directory.
    #[endpoint {
        method = PUT,
        path = "/server/config",
    }]
    async fn generate_server_config(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<ServerConfigurableSettings>,
    ) -> Result<HttpResponseCreated<ReplicaConfig>, HttpError>;

    /// Generate a ClickHouse configuration file for a keeper node on a specified
    /// directory.
    #[endpoint {
        method = PUT,
        path = "/keeper/config",
    }]
    async fn generate_keeper_config(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<KeeperConfigurableSettings>,
    ) -> Result<HttpResponseCreated<KeeperConfig>, HttpError>;

    /// Retrieve a logically grouped information file from a keeper node.
    /// This information is used internally by ZooKeeper to manage snapshots
    /// and logs for consistency and recovery.
    #[endpoint {
        method = GET,
        path = "/keeper/lgif",
    }]
    async fn lgif(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Lgif>, HttpError>;
}
