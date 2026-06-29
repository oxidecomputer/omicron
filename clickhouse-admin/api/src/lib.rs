// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use clickhouse_admin_types_versions::{latest, v1, v2};
use dropshot::{
    HttpError, HttpResponseCreated, HttpResponseOk,
    HttpResponseUpdatedNoContent, Path, Query, RequestContext, TypedBody,
};
use dropshot_api_manager_types::api_versions;
use omicron_common::api::external::Generation;

api_versions!([
    // NOTE: These versions will be used across **all three** APIs defined in
    // this file. When we need to add the next version, consider carefully if
    // these APIs should be split into separate modules or crates with their
    // own versions.

    // WHEN CHANGING THE API (part 1 of 2):
    //
    // +- Pick a new semver and define it in the list below.  The list MUST
    // |  remain sorted, which generally means that your version should go at
    // |  the very top.
    // |
    // |  Duplicate this line, uncomment the *second* copy, update that copy for
    // |  your new API version, and leave the first copy commented out as an
    // |  example for the next person.
    // v
    // (next_int, IDENT), // NOTE: read the note at the start of this macro!
    (4, LOG_TO_CONSOLE),
    (3, ADD_RETENTION_POLICY_FOR_ALL_TABLES),
    (2, ADD_RETENTION_POLICY_AND_TABLE_USAGE),
    (1, INITIAL),
]);

// WHEN CHANGING THE API (part 2 of 2):
//
// The call to `api_versions!` above defines constants of type
// `semver::Version` that you can use in your Dropshot API definition to specify
// the version when a particular endpoint was added or removed.  For example, if
// you used:
//
//     (2, ADD_FOOBAR)
//
// Then you could use `VERSION_ADD_FOOBAR` as the version in which endpoints
// were added or removed.

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
        versions = VERSION_LOG_TO_CONSOLE..,
    }]
    async fn generate_config_and_enable_svc(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<latest::keeper::KeeperConfigurableSettings>,
    ) -> Result<
        HttpResponseCreated<latest::config::GenerateConfigResult>,
        HttpError,
    >;

    /// Generate a ClickHouse configuration file for a keeper node on a specified
    /// directory and enable the SMF service if not currently enabled.
    #[endpoint {
        operation_id = "generate_config_and_enable_svc",
        method = PUT,
        path = "/config",
        versions = ..VERSION_LOG_TO_CONSOLE,
    }]
    async fn generate_config_and_enable_svc_v1(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<v1::keeper::KeeperConfigurableSettings>,
    ) -> Result<HttpResponseCreated<v1::config::GenerateConfigResult>, HttpError>
    {
        let HttpResponseCreated(result) =
            Self::generate_config_and_enable_svc(rqctx, body.map(Into::into))
                .await?;
        Ok(HttpResponseCreated(result.into()))
    }

    /// Retrieve the generation number of a configuration
    #[endpoint {
        method = GET,
        path = "/generation",
    }]
    async fn generation(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Generation>, HttpError>;

    /// Retrieve a logically grouped information file from a keeper node.
    /// This information is used internally by ZooKeeper to manage snapshots
    /// and logs for consistency and recovery.
    #[endpoint {
        method = GET,
        path = "/4lw-lgif",
    }]
    async fn lgif(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<latest::keeper::Lgif>, HttpError>;

    /// Retrieve information from ClickHouse virtual node /keeper/config which
    /// contains last committed cluster configuration.
    #[endpoint {
        method = GET,
        path = "/raft-config",
    }]
    async fn raft_config(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<latest::keeper::RaftConfig>, HttpError>;

    /// Retrieve configuration information from a keeper node.
    #[endpoint {
        method = GET,
        path = "/4lw-conf",
        versions = VERSION_LOG_TO_CONSOLE..,
    }]
    async fn keeper_conf(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<latest::keeper::KeeperConf>, HttpError>;

    /// Retrieve configuration information from a keeper node.
    #[endpoint {
        operation_id = "keeper_conf",
        method = GET,
        path = "/4lw-conf",
        versions = ..VERSION_LOG_TO_CONSOLE,
    }]
    async fn keeper_conf_v1(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<v1::keeper::KeeperConf>, HttpError> {
        let HttpResponseOk(conf) = Self::keeper_conf(rqctx).await?;
        Ok(HttpResponseOk(conf.into()))
    }

    /// Retrieve cluster membership information from a keeper node.
    #[endpoint {
        method = GET,
        path = "/cluster-membership",
    }]
    async fn keeper_cluster_membership(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<
        HttpResponseOk<latest::keeper::ClickhouseKeeperClusterMembership>,
        HttpError,
    >;
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
        path = "/config",
        versions = VERSION_LOG_TO_CONSOLE..,
    }]
    async fn generate_config_and_enable_svc(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<latest::server::ServerConfigurableSettings>,
    ) -> Result<
        HttpResponseCreated<latest::config::GenerateConfigResult>,
        HttpError,
    >;

    /// Generate a ClickHouse configuration file for a server node on a specified
    /// directory and enable the SMF service.
    #[endpoint {
        operation_id = "generate_config_and_enable_svc",
        method = PUT,
        path = "/config",
        versions = ..VERSION_LOG_TO_CONSOLE,
    }]
    async fn generate_config_and_enable_svc_v1(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<v1::server::ServerConfigurableSettings>,
    ) -> Result<HttpResponseCreated<v1::config::GenerateConfigResult>, HttpError>
    {
        let HttpResponseCreated(result) =
            Self::generate_config_and_enable_svc(rqctx, body.map(Into::into))
                .await?;
        Ok(HttpResponseCreated(result.into()))
    }

    /// Retrieve the generation number of a configuration
    #[endpoint {
        method = GET,
        path = "/generation",
    }]
    async fn generation(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Generation>, HttpError>;

    /// Contains information about distributed ddl queries (ON CLUSTER clause)
    /// that were executed on a cluster.
    #[endpoint {
        method = GET,
        path = "/distributed-ddl-queue",
    }]
    async fn distributed_ddl_queue(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<
        HttpResponseOk<Vec<latest::server::DistributedDdlQueue>>,
        HttpError,
    >;

    /// Retrieve time series from the system database.
    ///
    /// The value of each data point is the average of all stored data points
    /// within the interval.
    /// These are internal ClickHouse metrics.
    #[endpoint {
        method = GET,
        path = "/timeseries/{table}/{metric}/avg"
    }]
    async fn system_timeseries_avg(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::server::MetricInfoPath>,
        query_params: Query<latest::server::TimeSeriesSettingsQuery>,
    ) -> Result<HttpResponseOk<Vec<latest::server::SystemTimeSeries>>, HttpError>;

    /// Idempotently initialize a replicated ClickHouse cluster database.
    #[endpoint {
        method = PUT,
        path = "/init"
    }]
    async fn init_db(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Set the retention policy for timeseries data.
    #[endpoint {
        method = PUT,
        path = "/retention-policy",
        versions = VERSION_ADD_RETENTION_POLICY_FOR_ALL_TABLES..,
    }]
    async fn set_retention_policy(
        rqctx: RequestContext<Self::Context>,
        policy: TypedBody<latest::retention::RetentionPolicyRequest>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Set the retention policy for timeseries data.
    #[endpoint {
        method = PUT,
        path = "/retention-policy",
        versions =
            VERSION_ADD_RETENTION_POLICY_AND_TABLE_USAGE..VERSION_ADD_RETENTION_POLICY_FOR_ALL_TABLES,
    }]
    async fn set_retention_policy_v2(
        rqctx: RequestContext<Self::Context>,
        policy: TypedBody<v2::retention::RetentionPolicy>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        Self::set_retention_policy(rqctx, policy.map(Into::into)).await
    }

    /// Get the retention policy for timeseries data from the database
    #[endpoint {
        method = GET,
        path = "/retention-policy",
        versions = VERSION_ADD_RETENTION_POLICY_FOR_ALL_TABLES..,
    }]
    async fn retention_policy(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<
        HttpResponseOk<latest::retention::DatabaseRetentionPolicy>,
        HttpError,
    >;

    /// Get the retention policy for timeseries data from the database
    #[endpoint {
        method = GET,
        path = "/retention-policy",
        versions =
            VERSION_ADD_RETENTION_POLICY_AND_TABLE_USAGE..VERSION_ADD_RETENTION_POLICY_FOR_ALL_TABLES,
    }]
    async fn retention_policy_v2(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<v2::retention::RetentionPolicy>, HttpError> {
        let HttpResponseOk(mut policy) = Self::retention_policy(rqctx).await?;
        policy
            .tables
            .pop_first()
            .ok_or_else(|| {
                HttpError::for_unavail(
                    None,
                    "Database is not yet populated".to_string(),
                )
            })
            .map(|pol| HttpResponseOk(pol.into()))
    }

    /// Return the resource usage of database tables.
    #[endpoint {
        method = GET,
        path = "/usage/database",
        versions = VERSION_ADD_RETENTION_POLICY_AND_TABLE_USAGE..,
    }]
    async fn database_usage(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<latest::usage::DatabaseUsageResult>, HttpError>;
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

    /// Retrieve time series from the system database.
    ///
    /// The value of each data point is the average of all stored data points
    /// within the interval.
    /// These are internal ClickHouse metrics.
    #[endpoint {
        method = GET,
        path = "/timeseries/{table}/{metric}/avg"
    }]
    async fn system_timeseries_avg(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<latest::server::MetricInfoPath>,
        query_params: Query<latest::server::TimeSeriesSettingsQuery>,
    ) -> Result<HttpResponseOk<Vec<latest::server::SystemTimeSeries>>, HttpError>;

    /// Set the retention policy for timeseries data.
    #[endpoint {
        method = PUT,
        path = "/retention-policy",
        versions = VERSION_ADD_RETENTION_POLICY_FOR_ALL_TABLES..,
    }]
    async fn set_retention_policy(
        rqctx: RequestContext<Self::Context>,
        policy: TypedBody<latest::retention::RetentionPolicyRequest>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Set the retention policy for timeseries data.
    #[endpoint {
        method = PUT,
        path = "/retention-policy",
        versions =
            VERSION_ADD_RETENTION_POLICY_AND_TABLE_USAGE..VERSION_ADD_RETENTION_POLICY_FOR_ALL_TABLES,
    }]
    async fn set_retention_policy_v2(
        rqctx: RequestContext<Self::Context>,
        policy: TypedBody<v2::retention::RetentionPolicy>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        Self::set_retention_policy(rqctx, policy.map(Into::into)).await
    }

    /// Get the retention policy for timeseries data from the database
    #[endpoint {
        method = GET,
        path = "/retention-policy",
        versions = VERSION_ADD_RETENTION_POLICY_FOR_ALL_TABLES..,
    }]
    async fn retention_policy(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<
        HttpResponseOk<latest::retention::DatabaseRetentionPolicy>,
        HttpError,
    >;

    /// Get the retention policy for timeseries data from the database
    #[endpoint {
        method = GET,
        path = "/retention-policy",
        versions =
            VERSION_ADD_RETENTION_POLICY_AND_TABLE_USAGE..VERSION_ADD_RETENTION_POLICY_FOR_ALL_TABLES,
    }]
    async fn retention_policy_v2(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<v2::retention::RetentionPolicy>, HttpError> {
        let HttpResponseOk(mut policy) = Self::retention_policy(rqctx).await?;
        policy
            .tables
            .pop_first()
            .ok_or_else(|| {
                HttpError::for_unavail(
                    None,
                    "Database is not yet populated".to_string(),
                )
            })
            .map(|pol| HttpResponseOk(pol.into()))
    }

    /// Return the resource usage of database tables.
    #[endpoint {
        method = GET,
        path = "/usage/database",
        versions = VERSION_ADD_RETENTION_POLICY_AND_TABLE_USAGE..,
    }]
    async fn database_usage(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<latest::usage::DatabaseUsageResult>, HttpError>;
}
