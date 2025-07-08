// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Client for CockroachDB's built-in HTTP interface
//!
//! This accesses CockroachDB's native HTTP API, which provides Prometheus metrics
//! at /_status/vars and other endpoints.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use cockroach_admin_client::Client;
use futures::stream::{FuturesUnordered, StreamExt};
use parallel_task_set::ParallelTaskSet;
use serde::{Deserialize, Serialize};
use slog::{Logger, debug, warn};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use strum::{Display, EnumIter, EnumString, IntoStaticStr};
use tokio::sync::RwLock;

/// A CockroachDB client for accessing metrics and node status
///
/// Only accesses a single client at a time. To query from multiple nodes
/// in a cluster concurrently, use [CockroachClusterAdminClient].
#[derive(Clone)]
struct CockroachAdminClient {
    client: Client,
}

impl CockroachAdminClient {
    /// Create a new CockroachDB HTTP client
    ///
    /// "timeout" is used as both a connection and request timeout duration.
    fn new(log: Logger, address: SocketAddr, timeout: Duration) -> Self {
        let reqwest_client = reqwest::ClientBuilder::new()
            .connect_timeout(timeout)
            .timeout(timeout)
            .build()
            .expect("Failed to build HTTP client");

        let client = Client::new_with_client(
            &format!("http://{address}"),
            reqwest_client,
            log,
        );

        Self { client }
    }

    /// Fetch Prometheus metrics from the /_status/vars endpoint
    ///
    /// This API is (and must remain) cancel-safe
    async fn fetch_prometheus_metrics(&self) -> Result<PrometheusMetrics> {
        let response = self
            .client
            .status_vars()
            .await
            .with_context(|| "Failed to get status/vars")?;

        if !response.status().is_success() {
            anyhow::bail!(
                "HTTP request failed with status {}: {}",
                response.status(),
                response.into_inner(),
            );
        }
        let text = response.into_inner();
        PrometheusMetrics::parse(&text)
            .with_context(|| "Failed to parse Prometheus metrics")
    }

    /// Fetch node status information for all nodes
    ///
    /// Note that although we're asking a single node for this information, the
    /// response should describe all nodes in the cluster.
    ///
    /// This API is (and must remain) cancel-safe
    async fn fetch_node_status(&self) -> Result<NodesResponse> {
        let response = self
            .client
            .status_nodes()
            .await
            .with_context(|| "Failed to get node status")?;

        if !response.status().is_success() {
            anyhow::bail!(
                "HTTP request failed with status {}: {}",
                response.status(),
                response.into_inner(),
            );
        }

        let response_text = response.into_inner();
        let nodes_response =
            serde_json::from_str::<NodesResponse>(&response_text)
                .with_context(|| "Failed to parse nodes response JSON")?;
        Ok(nodes_response)
    }
}

/// A cluster client for CockroachDB HTTP endpoints that can contact multiple backends concurrently
/// and cache clients for efficiency.
///
/// ## Usage Pattern
///
/// ```rust,no_run
/// # use omicron_cockroach_metrics::CockroachClusterAdminClient;
/// # use std::net::SocketAddr;
/// # use slog::Logger;
/// # async fn example(log: Logger) -> anyhow::Result<()> {
/// let cluster = CockroachClusterAdminClient::new(log);
///
/// // Update backends when addresses change (e.g., from DNS resolution)
/// let backends: Vec<SocketAddr> = vec![
///     "192.168.1.1:8080".parse()?,
///     "192.168.1.2:8080".parse()?
/// ];
/// cluster.update_backends(&backends).await;
///
/// // Fetch metrics - will try all backends concurrently, return first success
/// let metrics = cluster.fetch_prometheus_metrics_from_any_node().await?;
///
/// // Later, if backends change, just update the cluster
/// let new_backends: Vec<SocketAddr> = vec![
///     "192.168.1.2:8080".parse()?,
///     "192.168.1.3:8080".parse()?
/// ];
///
/// // Keeps 192.168.1.2, drops 192.168.1.1, adds 192.168.1.3
/// cluster.update_backends(&new_backends).await;
///
/// let status = cluster.fetch_node_status_from_any_node().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct CockroachClusterAdminClient {
    /// Cached clients for each backend address
    clients: Arc<RwLock<BTreeMap<SocketAddr, CockroachAdminClient>>>,
    timeout: Duration,
    log: Logger,
}

impl CockroachClusterAdminClient {
    /// Create a new cluster client
    pub fn new(log: Logger, timeout: Duration) -> Self {
        Self {
            clients: Arc::new(RwLock::new(BTreeMap::new())),
            timeout,
            log: log
                .new(slog::o!("component" => "CockroachClusterAdminClient")),
        }
    }

    /// Update the set of backend admin addresses, adding new clients and removing old ones
    pub async fn update_backends(&self, addresses: &[SocketAddr]) {
        let mut clients = self.clients.write().await;

        // Keep only clients whose addresses are still in the new set
        let before_count = clients.len();
        clients.retain(|addr, _| addresses.contains(addr));
        let removed_count = before_count - clients.len();

        // Add new clients for addresses we don't have yet
        let mut added_count = 0;
        for &addr in addresses {
            if let std::collections::btree_map::Entry::Vacant(e) =
                clients.entry(addr)
            {
                e.insert(CockroachAdminClient::new(
                    self.log.clone(),
                    addr,
                    self.timeout,
                ));
                added_count += 1;
            }
        }

        if added_count > 0 || removed_count > 0 {
            debug!(
                self.log,
                "Updated CockroachDB cluster backends";
                "added" => added_count,
                "removed" => removed_count,
                "total" => clients.len(),
                "addresses" => ?addresses
            );
        }
    }

    /// Fetch Prometheus metrics from all backends concurrently, returning the first successful result
    pub async fn fetch_prometheus_metrics_from_any_node(
        &self,
    ) -> Result<PrometheusMetrics> {
        let clients = self.clients.read().await;

        if clients.is_empty() {
            anyhow::bail!("No CockroachDB backends configured");
        }

        // Create futures for all requests
        let mut futures = FuturesUnordered::new();
        for (&addr, client) in clients.iter() {
            let future =
                async move { (addr, client.fetch_prometheus_metrics().await) };
            futures.push(future);
        }

        // Wait for the first successful result
        while let Some((addr, result)) = futures.next().await {
            match result {
                Ok(metrics) => {
                    debug!(
                        self.log,
                        "Successfully fetched metrics from CockroachDB cluster backend";
                        "address" => %addr
                    );
                    return Ok(metrics);
                }
                Err(e) => {
                    // Log the error but continue trying other backends
                    warn!(
                        self.log,
                        "Failed to fetch metrics from CockroachDB cluster backend";
                        "address" => %addr,
                        "error" => %e
                    );
                }
            }
        }

        anyhow::bail!(
            "All CockroachDB cluster backends failed to return metrics"
        )
    }

    /// Fetch node status from all backends concurrently, returning the first successful result
    pub async fn fetch_node_status_from_any_node(
        &self,
    ) -> Result<NodesResponse> {
        let clients = self.clients.read().await;

        if clients.is_empty() {
            anyhow::bail!("No CockroachDB backends configured");
        }

        // Create futures for all requests
        let mut futures = FuturesUnordered::new();
        for (&addr, client) in clients.iter() {
            let future =
                async move { (addr, client.fetch_node_status().await) };
            futures.push(future);
        }

        // Wait for the first successful result
        while let Some((addr, result)) = futures.next().await {
            match result {
                Ok(status) => {
                    debug!(
                        self.log,
                        "Successfully fetched node status from CockroachDB cluster backend";
                        "address" => %addr
                    );
                    return Ok(status);
                }
                Err(e) => {
                    // Log the error but continue trying other backends
                    warn!(
                        self.log,
                        "Failed to fetch node status from CockroachDB cluster backend";
                        "address" => %addr,
                        "error" => %e
                    );
                }
            }
        }

        anyhow::bail!(
            "All CockroachDB cluster backends failed to return node status"
        )
    }

    /// Get the current set of cached client addresses
    pub async fn get_cached_addresses(&self) -> Vec<SocketAddr> {
        let clients = self.clients.read().await;
        clients.keys().copied().collect()
    }

    /// Fetch Prometheus metrics from all backends, returning all successful results
    pub async fn fetch_prometheus_metrics_from_all_nodes(
        &self,
    ) -> Vec<(SocketAddr, PrometheusMetrics)> {
        let clients = self.clients.read().await;

        if clients.is_empty() {
            return Vec::new();
        }

        // Collect tasks from all nodes in parallel
        let mut results = Vec::new();
        let mut tasks = ParallelTaskSet::new();
        for (addr, client) in clients.iter() {
            let addr = *addr;
            let client = client.clone();
            if let Some(result) =
                tasks
                    .spawn({
                        async move {
                            (addr, client.fetch_prometheus_metrics().await)
                        }
                    })
                    .await
            {
                results.push(result);
            }
        }
        results.append(&mut tasks.join_all().await);

        // Collect all successful results
        let mut successful_results = Vec::new();
        let mut results_iter = results.into_iter();
        while let Some((addr, result)) = results_iter.next() {
            match result {
                Ok(metrics) => {
                    debug!(
                        self.log,
                        "Successfully fetched metrics from CockroachDB node";
                        "address" => %addr
                    );
                    successful_results.push((addr, metrics));
                }
                Err(e) => {
                    // Log the error but continue trying other backends
                    warn!(
                        self.log,
                        "Failed to fetch metrics from CockroachDB node";
                        "address" => %addr,
                        "error" => %e
                    );
                }
            }
        }

        successful_results
    }

    /// Fetch node status from all backends, returning all successful results
    pub async fn fetch_node_status_from_all_nodes(
        &self,
    ) -> Vec<(SocketAddr, NodesResponse)> {
        let clients = self.clients.read().await;

        if clients.is_empty() {
            return Vec::new();
        }

        // Create futures for all requests
        let mut results = Vec::new();
        let mut tasks = ParallelTaskSet::new();
        for (addr, client) in clients.iter() {
            let addr = *addr;
            let client = client.clone();
            if let Some(result) = tasks
                .spawn({
                    async move { (addr, client.fetch_node_status().await) }
                })
                .await
            {
                results.push(result);
            }
        }
        results.append(&mut tasks.join_all().await);

        // Collect all successful results
        let mut successful_results = Vec::new();
        let mut results_iter = results.into_iter();
        while let Some((addr, result)) = results_iter.next() {
            match result {
                Ok(status) => {
                    debug!(
                        self.log,
                        "Successfully fetched node status from CockroachDB node";
                        "address" => %addr
                    );
                    successful_results.push((addr, status));
                }
                Err(e) => {
                    // Log the error but continue trying other backends
                    warn!(
                        self.log,
                        "Failed to fetch node status from CockroachDB node";
                        "address" => %addr,
                        "error" => %e
                    );
                }
            }
        }

        successful_results
    }
}

/// A single metric value, which can be a counter, gauge, etc.
#[derive(Debug, Clone, PartialEq)]
pub enum MetricValue {
    /// An unsigned value
    Unsigned(u64),
    /// A floating point value
    Float(f64),
    /// A histogram with buckets
    Histogram(Vec<HistogramBucket>),
    /// A string value
    String(String),
}

/// A histogram bucket with upper bound and count
#[derive(Debug, Clone, PartialEq)]
pub struct HistogramBucket {
    pub le: f64, // "less than or equal to" upper bound
    pub count: u64,
}

/// The expected type of a CockroachDB metric value
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CockroachMetricType {
    /// An unsigned counter or gauge value
    Unsigned,
    /// A floating point gauge value
    Float,
    /// A histogram with buckets and counts
    Histogram,
    /// A string value
    String,
}

/// Well-known CockroachDB metrics that are important for monitoring
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Display,
    EnumIter,
    EnumString,
    IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum CockroachMetric {
    // Lease metrics - important for understanding lease transfers and leadership
    #[strum(serialize = "leases_error")]
    LeasesError,
    #[strum(serialize = "leases_transfers_error")]
    LeasesTransfersError,

    // Number of live nodes in the cluster
    #[strum(serialize = "liveness_livenodes")]
    LivenessLiveNodes,

    // Range metrics - critical for understanding data distribution and splits
    #[strum(serialize = "range_splits")]
    RangeSplits,
    #[strum(serialize = "range_removes")]
    RangeRemoves,
    #[strum(serialize = "ranges_underreplicated")]
    RangesUnderreplicated,
    #[strum(serialize = "ranges_overreplicated")]
    RangesOverreplicated,

    // Replica metrics - important for replication health monitoring
    #[strum(serialize = "replicas_uninitialized")]
    ReplicasUninitialized,
    #[strum(serialize = "replicas_leaders_not_leaseholders")]
    ReplicasLeadersNotLeaseholders,

    // Transaction restart metrics - crucial for detecting contention issues
    #[strum(serialize = "txn_restarts_unknown")]
    TxnRestartsUnknown,
    #[strum(serialize = "txn_restarts_asyncwritefailure")]
    TxnRestartsAsyncWriteFailure,
    #[strum(serialize = "txn_restarts_commitdeadlineexceeded")]
    TxnRestartsCommitDeadlineExceeded,

    // SQL execution latency histogram - critical for performance monitoring
    #[strum(serialize = "sql_exec_latency")]
    SqlExecLatency,
}

/// Represents a collection of Prometheus metrics
#[derive(Debug, Clone)]
pub struct PrometheusMetrics {
    /// Raw metrics as key-value pairs
    pub metrics: BTreeMap<String, MetricValue>,
}

impl CockroachMetric {
    /// Get the exact metric name as it appears in CockroachDB's Prometheus output
    pub fn metric_name(&self) -> &'static str {
        self.into()
    }

    /// Get the expected type of this metric value
    pub fn expected_type(&self) -> CockroachMetricType {
        match self {
            // All counter/gauge metrics are numbers
            CockroachMetric::LeasesError
            | CockroachMetric::LeasesTransfersError
            | CockroachMetric::LivenessLiveNodes
            | CockroachMetric::RangeSplits
            | CockroachMetric::RangeRemoves
            | CockroachMetric::RangesUnderreplicated
            | CockroachMetric::RangesOverreplicated
            | CockroachMetric::ReplicasUninitialized
            | CockroachMetric::ReplicasLeadersNotLeaseholders
            | CockroachMetric::TxnRestartsUnknown
            | CockroachMetric::TxnRestartsAsyncWriteFailure
            | CockroachMetric::TxnRestartsCommitDeadlineExceeded => {
                CockroachMetricType::Unsigned
            }

            // Histogram metrics
            CockroachMetric::SqlExecLatency => CockroachMetricType::Histogram,
        }
    }

    /// Get a human-readable description of what this metric represents
    pub fn description(&self) -> &'static str {
        match self {
            CockroachMetric::LeasesError => {
                "Number of lease operations that resulted in errors"
            }
            CockroachMetric::LeasesTransfersError => {
                "Number of lease transfer operations that failed"
            }
            CockroachMetric::LivenessLiveNodes => {
                "Number of live nodes in the CockroachDB cluster"
            }
            CockroachMetric::RangeSplits => {
                "Number of range split operations performed"
            }
            CockroachMetric::RangeRemoves => {
                "Number of range removal operations performed"
            }
            CockroachMetric::RangesUnderreplicated => {
                "Number of ranges with fewer replicas than desired"
            }
            CockroachMetric::RangesOverreplicated => {
                "Number of ranges with more replicas than desired"
            }
            CockroachMetric::ReplicasUninitialized => {
                "Number of replicas that are uninitialized"
            }
            CockroachMetric::ReplicasLeadersNotLeaseholders => {
                "Number of replicas that are leaders but not leaseholders"
            }
            CockroachMetric::TxnRestartsUnknown => {
                "Number of transaction restarts due to unknown reasons"
            }
            CockroachMetric::TxnRestartsAsyncWriteFailure => {
                "Number of transaction restarts due to async write failures"
            }
            CockroachMetric::TxnRestartsCommitDeadlineExceeded => {
                "Number of transaction restarts due to commit deadline exceeded"
            }
            CockroachMetric::SqlExecLatency => {
                "Histogram of SQL execution latencies"
            }
        }
    }
}

impl PrometheusMetrics {
    /// Parse Prometheus text format into structured metrics
    pub fn parse(text: &str) -> Result<Self> {
        let mut metrics = BTreeMap::new();

        for line in text.lines() {
            let line = line.trim();

            // Skip comments and empty lines
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            // Parse metric line: metric_name{labels} value
            if let Some((name_and_labels, value_str)) = line.rsplit_once(' ') {
                // Extract metric name (before any labels)
                let metric_name =
                    if let Some((name, _)) = name_and_labels.split_once('{') {
                        name
                    } else {
                        name_and_labels
                    };

                // Check if this is a histogram bucket metric
                if let Some(base_name) = metric_name.strip_suffix("_bucket") {
                    // Parse the 'le' (less than or equal) value from labels
                    if let Some(le_value) =
                        Self::extract_le_label(name_and_labels)
                    {
                        if let (Ok(count), Ok(le)) =
                            (value_str.parse::<u64>(), le_value.parse::<f64>())
                        {
                            let bucket = HistogramBucket { le, count };

                            // Add to existing histogram or create new one
                            match metrics.entry(base_name.to_string()) {
                                std::collections::btree_map::Entry::Occupied(
                                    mut entry,
                                ) => {
                                    if let MetricValue::Histogram(
                                        ref mut buckets,
                                    ) = entry.get_mut()
                                    {
                                        buckets.push(bucket);
                                    }
                                }
                                std::collections::btree_map::Entry::Vacant(
                                    entry,
                                ) => {
                                    entry.insert(MetricValue::Histogram(vec![
                                        bucket,
                                    ]));
                                }
                            }
                            continue;
                        }
                    }
                }

                // Parse regular metrics
                let value = if let Ok(num) = value_str.parse::<u64>() {
                    MetricValue::Unsigned(num)
                } else if let Ok(float) = value_str.parse::<f64>() {
                    MetricValue::Float(float)
                } else {
                    MetricValue::String(value_str.to_string())
                };

                metrics.insert(metric_name.to_string(), value);
            }
        }

        // Sort histogram buckets by their upper bound for consistent ordering
        for (_, metric_value) in metrics.iter_mut() {
            if let MetricValue::Histogram(ref mut buckets) = metric_value {
                buckets.sort_by(|a, b| {
                    a.le.partial_cmp(&b.le).unwrap_or(std::cmp::Ordering::Equal)
                });
            }
        }

        Ok(PrometheusMetrics { metrics })
    }

    /// Extract the 'le' (less than or equal) label value from a Prometheus metric line
    fn extract_le_label(metric_line: &str) -> Option<&str> {
        // Look for le="value" in the labels
        let (_, le_part) = metric_line.split_once("le=\"")?;
        let (label, _) = le_part.split_once("\"")?;
        Some(label)
    }

    /// Get a specific metric by its strongly-typed enum value
    pub fn get_metric(&self, metric: CockroachMetric) -> Option<&MetricValue> {
        let value = self.metrics.get(metric.metric_name())?;

        match (metric.expected_type(), &value) {
            (CockroachMetricType::Unsigned, &MetricValue::Unsigned(_)) => {
                Some(value)
            }
            (CockroachMetricType::Float, &MetricValue::Float(_)) => Some(value),
            (CockroachMetricType::String, &MetricValue::String(_)) => {
                Some(value)
            }
            (CockroachMetricType::Histogram, &MetricValue::Histogram(_)) => {
                Some(value)
            }
            _ => None,
        }
    }

    /// Get a specific metric as a number, if it exists and is unsigned
    pub fn get_metric_unsigned(&self, metric: CockroachMetric) -> Option<u64> {
        match self.get_metric(metric) {
            Some(MetricValue::Unsigned(val)) => Some(*val),
            _ => None,
        }
    }

    /// Get a specific CockroachDB metric as a histogram, if it exists and is a histogram
    pub fn get_metric_histogram(
        &self,
        metric: CockroachMetric,
    ) -> Option<&Vec<HistogramBucket>> {
        match self.get_metric(metric) {
            Some(MetricValue::Histogram(buckets)) => Some(buckets),
            _ => None,
        }
    }
}

/// CockroachDB Node ID
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
)]
#[serde(transparent)]
pub struct NodeId(pub i32);

impl NodeId {
    pub fn new(id: i32) -> Self {
        Self(id)
    }

    pub fn as_i32(&self) -> i32 {
        self.0
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for NodeId {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

/// CockroachDB node liveness status
///
/// From CockroachDB's [NodeLivenessStatus protobuf enum](https://github.com/cockroachdb/cockroach/blob/release-21.1/pkg/kv/kvserver/liveness/livenesspb/liveness.proto#L107-L138)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u32)]
#[serde(try_from = "u32", into = "u32")]
pub enum NodeLiveness {
    Unknown = 0,
    Dead = 1,
    Unavailable = 2,
    Live = 3,
    Decommissioning = 4,
    Decommissioned = 5,
    Draining = 6,
}

impl TryFrom<u32> for NodeLiveness {
    type Error = String;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(NodeLiveness::Unknown),
            1 => Ok(NodeLiveness::Dead),
            2 => Ok(NodeLiveness::Unavailable),
            3 => Ok(NodeLiveness::Live),
            4 => Ok(NodeLiveness::Decommissioning),
            5 => Ok(NodeLiveness::Decommissioned),
            6 => Ok(NodeLiveness::Draining),
            _ => Err(format!("Unknown liveness value: {}", value)),
        }
    }
}

impl From<NodeLiveness> for u32 {
    fn from(liveness: NodeLiveness) -> Self {
        liveness as u32
    }
}

mod cockroach_timestamp {
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(
        dt: &DateTime<Utc>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let nanos = dt.timestamp_nanos_opt().ok_or_else(|| {
            serde::ser::Error::custom("Timestamp out of range")
        })?;
        nanos.to_string().serialize(serializer)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = String::deserialize(deserializer)?;
        let nanos: i64 = s.parse().map_err(serde::de::Error::custom)?;
        Ok(DateTime::from_timestamp_nanos(nanos))
    }
}

fn deserialize_string_to_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    let s: String = String::deserialize(deserializer)?;
    s.parse::<u64>().map_err(serde::de::Error::custom)
}

/// Response from the /_status/nodes endpoint containing all cluster nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodesResponse {
    pub nodes: Vec<NodeStatus>,
    /// Maps node ID to liveness status
    #[serde(rename = "livenessByNodeId")]
    pub liveness_by_node_id: std::collections::BTreeMap<NodeId, NodeLiveness>,
}

/// Node status information from CockroachDB /_status/nodes endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStatus {
    pub desc: NodeDescriptor,
    #[serde(rename = "buildInfo")]
    pub build_info: BuildInfo,
    #[serde(rename = "startedAt", with = "cockroach_timestamp")]
    pub started_at: DateTime<Utc>,
    #[serde(rename = "updatedAt", with = "cockroach_timestamp")]
    pub updated_at: DateTime<Utc>,
    #[serde(
        rename = "totalSystemMemory",
        deserialize_with = "deserialize_string_to_u64"
    )]
    pub total_system_memory: u64,
    #[serde(rename = "numCpus")]
    pub num_cpus: u32,
}

/// Node descriptor containing basic node information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeDescriptor {
    #[serde(rename = "nodeId")]
    pub node_id: NodeId,

    #[serde(rename = "address")]
    pub address: AddressInfo,

    #[serde(rename = "sqlAddress")]
    pub sql_address: AddressInfo,

    #[serde(rename = "httpAddress")]
    pub http_address: AddressInfo,

    #[serde(rename = "buildTag")]
    pub build_tag: String,

    #[serde(rename = "startedAt", with = "cockroach_timestamp")]
    pub started_at: DateTime<Utc>,

    #[serde(rename = "clusterName")]
    pub cluster_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddressInfo {
    #[serde(rename = "networkField")]
    pub network_field: String,

    #[serde(rename = "addressField")]
    pub address_field: std::net::SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildInfo {
    #[serde(rename = "goVersion")]
    pub go_version: String,

    #[serde(rename = "tag")]
    pub tag: String,
    // There's plenty more BuildInfo accessible here, but this is a
    // reduced version of the contents.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prometheus_metrics_parsing() {
        let sample_metrics = "
            # TYPE go_memstats_alloc_bytes gauge
            go_memstats_alloc_bytes 1.234567e+07
            # TYPE cockroach_sql_query_count counter
            cockroach_sql_query_count 42
            cockroach_node_id 1
            cockroach_build_timestamp 1234567890
        ";

        let metrics = PrometheusMetrics::parse(sample_metrics).unwrap();

        // Test raw metric access by name
        assert_eq!(
            metrics.metrics.get("go_memstats_alloc_bytes"),
            Some(&MetricValue::Float(12345670.0))
        );
        assert_eq!(
            metrics.metrics.get("cockroach_sql_query_count"),
            Some(&MetricValue::Unsigned(42))
        );
        assert_eq!(
            metrics.metrics.get("cockroach_node_id"),
            Some(&MetricValue::Unsigned(1))
        );
        assert_eq!(
            metrics.metrics.get("cockroach_build_timestamp"),
            Some(&MetricValue::Unsigned(1234567890))
        );

        // Check that we can access cockroach metrics by raw name
        let cockroach_metrics: Vec<_> = metrics
            .metrics
            .keys()
            .filter(|name| name.starts_with("cockroach_"))
            .collect();
        assert_eq!(cockroach_metrics.len(), 3);
    }

    #[test]
    fn test_prometheus_metrics_empty() {
        let metrics = PrometheusMetrics::parse("").unwrap();
        assert!(metrics.metrics.is_empty());
    }

    #[test]
    fn test_prometheus_metrics_comments_only() {
        let sample_metrics = "
            # This is a comment
            # TYPE some_metric counter
            # Another comment
        ";
        let metrics = PrometheusMetrics::parse(sample_metrics).unwrap();
        assert!(metrics.metrics.is_empty());
    }

    #[test]
    fn test_prometheus_histogram_parsing() {
        let sample_metrics = r#"
# TYPE sql_exec_latency histogram
sql_exec_latency_bucket{le="0.001"} 10
sql_exec_latency_bucket{le="0.01"} 25
sql_exec_latency_bucket{le="0.1"} 100
sql_exec_latency_bucket{le="1.0"} 200
sql_exec_latency_bucket{le="+Inf"} 205
sql_exec_latency_count 205
sql_exec_latency_sum 45.2
"#;

        let metrics = PrometheusMetrics::parse(sample_metrics).unwrap();

        // Check that histogram buckets are properly grouped under the base metric name
        if let Some(MetricValue::Histogram(histogram)) =
            metrics.metrics.get("sql_exec_latency")
        {
            assert_eq!(histogram.len(), 5, "Should have 5 histogram buckets");

            // Buckets should already be sorted by le value
            assert_eq!(histogram[0].le, 0.001);
            assert_eq!(histogram[0].count, 10);
            assert_eq!(histogram[1].le, 0.01);
            assert_eq!(histogram[1].count, 25);
            assert_eq!(histogram[4].le, f64::INFINITY);
            assert_eq!(histogram[4].count, 205);

            // Verify buckets are cumulative (and sorted)
            for i in 1..histogram.len() {
                assert!(
                    histogram[i].count >= histogram[i - 1].count,
                    "Histogram buckets should be cumulative"
                );
                assert!(
                    histogram[i].le >= histogram[i - 1].le,
                    "Histogram buckets should be sorted by le value"
                );
            }
        } else {
            panic!("sql_exec_latency histogram not found");
        }

        // Verify count and sum are stored as separate regular metrics
        // (These are accessed by name since they're not in our CockroachMetric enum)
        assert_eq!(
            metrics.metrics.get("sql_exec_latency_count"),
            Some(&MetricValue::Unsigned(205))
        );
        assert_eq!(
            metrics.metrics.get("sql_exec_latency_sum"),
            Some(&MetricValue::Float(45.2))
        );

        // Test strongly-typed histogram access
        if let Some(histogram) =
            metrics.get_metric_histogram(CockroachMetric::SqlExecLatency)
        {
            assert_eq!(histogram.len(), 5);
        } else {
            panic!(
                "sql_exec_latency histogram not accessible via CockroachMetric enum"
            );
        }
    }

    #[test]
    fn test_typed_cockroach_metrics() {
        let sample_metrics = r#"
leases_error 5
sql_exec_latency_bucket{le="0.001"} 10
sql_exec_latency_bucket{le="0.01"} 25
sql_exec_latency_bucket{le="+Inf"} 30
sql_exec_latency_count 30
sql_exec_latency_sum 2.5
"#;

        let metrics = PrometheusMetrics::parse(sample_metrics).unwrap();

        // Test strongly-typed access for number metrics
        if let Some(MetricValue::Unsigned(val)) =
            metrics.get_metric(CockroachMetric::LeasesError)
        {
            assert_eq!(*val, 5);
        } else {
            panic!("Expected Unsigned variant for leases_error");
        }

        // Test strongly-typed access for histogram metrics
        if let Some(MetricValue::Histogram(buckets)) =
            metrics.get_metric(CockroachMetric::SqlExecLatency)
        {
            assert_eq!(buckets.len(), 3);
        } else {
            panic!("Expected Histogram variant for sql_exec_latency");
        }

        // Test convenience methods
        assert_eq!(
            metrics.get_metric_unsigned(CockroachMetric::LeasesError),
            Some(5)
        );
        assert_eq!(
            metrics
                .get_metric_histogram(CockroachMetric::SqlExecLatency)
                .unwrap()
                .len(),
            3
        );

        // Test type safety - metric that doesn't exist
        let missing = metrics.get_metric(CockroachMetric::RangeSplits);
        assert!(missing.is_none());
    }

    #[test]
    fn test_histogram_bucket_sorting() {
        // Test metrics with buckets in random order to verify sorting
        let sample_metrics = r#"
# Buckets deliberately out of order to test sorting
sql_exec_latency_bucket{le="1.0"} 200
sql_exec_latency_bucket{le="0.001"} 10
sql_exec_latency_bucket{le="+Inf"} 205
sql_exec_latency_bucket{le="0.1"} 100
sql_exec_latency_bucket{le="0.01"} 25
"#;

        let metrics = PrometheusMetrics::parse(sample_metrics).unwrap();

        if let Some(MetricValue::Histogram(buckets)) =
            metrics.metrics.get("sql_exec_latency")
        {
            assert_eq!(buckets.len(), 5);

            // Verify buckets are sorted by le value
            assert_eq!(buckets[0].le, 0.001);
            assert_eq!(buckets[0].count, 10);
            assert_eq!(buckets[1].le, 0.01);
            assert_eq!(buckets[1].count, 25);
            assert_eq!(buckets[2].le, 0.1);
            assert_eq!(buckets[2].count, 100);
            assert_eq!(buckets[3].le, 1.0);
            assert_eq!(buckets[3].count, 200);
            assert_eq!(buckets[4].le, f64::INFINITY);
            assert_eq!(buckets[4].count, 205);

            // Verify they are indeed sorted
            for i in 1..buckets.len() {
                assert!(
                    buckets[i - 1].le <= buckets[i].le,
                    "Bucket {} (le={}) should be <= bucket {} (le={})",
                    i - 1,
                    buckets[i - 1].le,
                    i,
                    buckets[i].le
                );
            }
        } else {
            panic!("Expected histogram for sql_exec_latency");
        }
    }

    #[tokio::test]
    async fn test_cluster_client_caching() {
        let log = slog::Logger::root(slog::Discard, slog::o!());
        let timeout = Duration::from_secs(15);
        let cluster = CockroachClusterAdminClient::new(log, timeout);

        // Initially no cached clients
        assert_eq!(cluster.get_cached_addresses().await.len(), 0);

        // Fetch should fail with no backends configured
        assert!(
            cluster.fetch_prometheus_metrics_from_any_node().await.is_err()
        );
        assert!(cluster.fetch_node_status_from_any_node().await.is_err());

        // Add some backends
        let addr1: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:8081".parse().unwrap();
        let addr3: SocketAddr = "127.0.0.1:8082".parse().unwrap();

        cluster.update_backends(&[addr1, addr2]).await;
        let cached = cluster.get_cached_addresses().await;
        assert_eq!(cached.len(), 2);
        assert!(cached.contains(&addr1));
        assert!(cached.contains(&addr2));

        // Update with different set - should keep addr2, drop addr1, add addr3
        cluster.update_backends(&[addr2, addr3]).await;
        let cached = cluster.get_cached_addresses().await;
        assert_eq!(cached.len(), 2);
        assert!(!cached.contains(&addr1));
        assert!(cached.contains(&addr2));
        assert!(cached.contains(&addr3));

        // Clear all backends
        cluster.update_backends(&[]).await;
        assert_eq!(cluster.get_cached_addresses().await.len(), 0);

        // Fetch should fail again with no backends configured
        assert!(
            cluster.fetch_prometheus_metrics_from_any_node().await.is_err()
        );
        assert!(cluster.fetch_node_status_from_any_node().await.is_err());
    }

    #[test]
    fn test_prometheus_metrics_parse_resilience() {
        // Test various edge cases that could cause issues

        // Empty input
        let result = PrometheusMetrics::parse("");
        assert!(result.is_ok());
        assert!(result.unwrap().metrics.is_empty());

        // Only comments
        let result = PrometheusMetrics::parse("# comment\n# another comment");
        assert!(result.is_ok());
        assert!(result.unwrap().metrics.is_empty());

        // Malformed lines (missing values, extra spaces, etc.)
        let malformed_input = r#"
metric_name_no_value
metric_name_with_space 
metric_name_multiple spaces here
= value_no_name
 leading_space_metric 123
trailing_space_metric 456 
metric{label=value} 789
metric{malformed=label value} 999
"#;
        let result = PrometheusMetrics::parse(malformed_input);
        assert!(result.is_ok());
        // Should ignore malformed lines gracefully
    }

    mod proptest_tests {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            /// Test that PrometheusMetrics::parse never panics on arbitrary input
            #[test]
            fn prometheus_parse_never_panics(input in ".*") {
                // This should never panic, even on completely malformed input
                let _result = PrometheusMetrics::parse(&input);
            }

            /// Test PrometheusMetrics::parse with more structured but still arbitrary input
            #[test]
            fn prometheus_parse_structured_input(
                lines in prop::collection::vec(".*", 0..20)
            ) {
                let input = lines.join("\n");
                let _result = PrometheusMetrics::parse(&input);
            }
        }
    }
}
