// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use clickhouse_admin_types::{
    ClickhouseKeeperClusterMembership, KeeperConf, KeeperConfig,
    KeeperConfigurableSettings, Lgif, RaftConfig, ReplicaConfig,
    ServerConfigurableSettings,
};
use dropshot::{
    HttpError, HttpResponseCreated, HttpResponseOk, RequestContext, TypedBody,
};

#[dropshot::api_description]
pub trait ClickhouseAdminKeeperApi {
    type Context;

    /// Generate a ClickHouse configuration file for a keeper node on a specified
    /// directory and enable the SMF service if not currently enabled.
    ///
    /// Note that we cannot start the keeper service until there is an initial
    /// configuration set via this endpoint.
    #[endpoint {
        method = PUT,
        path = "/config",
    }]
    async fn generate_config(
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

    /// Retrieve information from ClickHouse virtual node /keeper/config which
    /// contains last committed cluster configuration.
    #[endpoint {
        method = GET,
        path = "/keeper/raft-config",
    }]
    async fn raft_config(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<RaftConfig>, HttpError>;

    /// Retrieve configuration information from a keeper node.
    #[endpoint {
        method = GET,
        path = "/keeper/conf",
    }]
    async fn keeper_conf(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<KeeperConf>, HttpError>;

    /// Retrieve cluster membership information from a keeper node.
    #[endpoint {
        method = GET,
        path = "/keeper/cluster-membership",
    }]
    async fn keeper_cluster_membership(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<ClickhouseKeeperClusterMembership>, HttpError>;
}

#[dropshot::api_description]
pub trait ClickhouseAdminServerApi {
    type Context;

    /// Generate a ClickHouse configuration file for a server node on a specified
    /// directory and enable the SMF service.
    #[endpoint {
        method = PUT,
        path = "/config"
    }]
    async fn generate_config(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<ServerConfigurableSettings>,
    ) -> Result<HttpResponseCreated<ReplicaConfig>, HttpError>;
}
