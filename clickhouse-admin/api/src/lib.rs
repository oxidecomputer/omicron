// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use clickhouse_admin_types::{
    ClickhouseKeeperClusterMembership, DistributedDdlQueue, KeeperConf,
    KeeperConfig, KeeperConfigurableSettings, Lgif, MetricInfoPath, RaftConfig,
    ReplicaConfig, ServerConfigurableSettings, SystemTimeSeries,
    TimeSeriesSettingsQuery,
};
use dropshot::{
    HttpError, HttpResponseCreated, HttpResponseOk,
    HttpResponseUpdatedNoContent, Path, Query, RequestContext, TypedBody,
};

/// API interface for our clickhouse-admin-keeper server
///
/// We separate the admin interface for the keeper and server APIs because they
/// are completely disjoint. We only run a clickhouse keeper *or* clickhouse
/// server in a given zone, and therefore each admin api is only useful in one
/// of the zones. Using separate APIs and clients prevents us from having to
/// mark a given endpoint `unimplemented` in the case of it not being usable
/// with one of the zone types.
///
/// Nonetheless, the interfaces themselves are small and serve a similar
/// purpose. Therfore we combine them into the same crate.
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
    async fn generate_config_and_enable_svc(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<KeeperConfigurableSettings>,
    ) -> Result<HttpResponseCreated<KeeperConfig>, HttpError>;

    /// Retrieve a logically grouped information file from a keeper node.
    /// This information is used internally by ZooKeeper to manage snapshots
    /// and logs for consistency and recovery.
    #[endpoint {
        method = GET,
        path = "/4lw-lgif",
    }]
    async fn lgif(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Lgif>, HttpError>;

    /// Retrieve information from ClickHouse virtual node /keeper/config which
    /// contains last committed cluster configuration.
    #[endpoint {
        method = GET,
        path = "/raft-config",
    }]
    async fn raft_config(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<RaftConfig>, HttpError>;

    /// Retrieve configuration information from a keeper node.
    #[endpoint {
        method = GET,
        path = "/4lw-conf",
    }]
    async fn keeper_conf(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<KeeperConf>, HttpError>;

    /// Retrieve cluster membership information from a keeper node.
    #[endpoint {
        method = GET,
        path = "/cluster-membership",
    }]
    async fn keeper_cluster_membership(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<ClickhouseKeeperClusterMembership>, HttpError>;
}

/// API interface for our clickhouse-admin-server server
///
/// We separate the admin interface for the keeper and server APIs because they
/// are completely disjoint. We only run a clickhouse keeper *or* clickhouse
/// server in a given zone, and therefore each admin api is only useful in one
/// of the zones. Using separate APIs and clients prevents us from having to
/// mark a given endpoint `unimplemented` in the case of it not being usable
/// with one of the zone types.
///
/// Nonetheless, the interfaces themselves are small and serve a similar
/// purpose. Therfore we combine them into the same crate.
#[dropshot::api_description]
pub trait ClickhouseAdminServerApi {
    type Context;

    /// Generate a ClickHouse configuration file for a server node on a specified
    /// directory and enable the SMF service.
    #[endpoint {
        method = PUT,
        path = "/config"
    }]
    async fn generate_config_and_enable_svc(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<ServerConfigurableSettings>,
    ) -> Result<HttpResponseCreated<ReplicaConfig>, HttpError>;

    /// Contains information about distributed ddl queries (ON CLUSTER clause)
    /// that were executed on a cluster.
    #[endpoint {
        method = GET,
        path = "/distributed-ddl-queue",
    }]
    async fn distributed_ddl_queue(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<DistributedDdlQueue>>, HttpError>;

    /// Retrieve time series from the system.metric_log table.
    /// These are internal ClickHouse metrics.
    #[endpoint {
        method = GET,
        path = "/timeseries/{table}/{metric}"
    }]
    async fn system_metric_log_timeseries(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<MetricInfoPath>,
        query_params: Query<TimeSeriesSettingsQuery>,
    ) -> Result<HttpResponseOk<Vec<SystemTimeSeries>>, HttpError>;

 //   /// Retrieve time series from the system.asynchronous_metric_log table.
 //   /// These are internal ClickHouse metrics.
 //   #[endpoint {
 //       method = GET,
 //       path = "/timeseries/async-metric-log/{metric}"
 //   }]
 //   async fn system_async_metric_log_timeseries(
 //       rqctx: RequestContext<Self::Context>,
 //       path_params: Path<MetricNamePath>,
 //       query_params: Query<TimeSeriesSettingsQuery>,
 //   ) -> Result<HttpResponseOk<Vec<SystemTimeSeries>>, HttpError>;
}

/// API interface for our clickhouse-admin-single server
///
/// The single-node server is distinct from the both the multi-node servers
/// and its keepers. The sole purpose of this API is to serialize database
/// initialization requests from reconfigurator execution. Multi-node clusters
/// must provide a similar interface via [`ClickhouseAdminServerApi`].
#[dropshot::api_description]
pub trait ClickhouseAdminSingleApi {
    type Context;

    /// Idempotently initialize a single-node ClickHouse database.
    #[endpoint {
        method = PUT,
        path = "/init"
    }]
    async fn init_db(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    // TODO: Retrieve time series here too
}
