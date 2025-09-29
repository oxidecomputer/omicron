// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Configuration parameters to Nexus that are usually only known
//! at deployment time.

use crate::PostgresConfigWithUrl;
use anyhow::anyhow;
use camino::{Utf8Path, Utf8PathBuf};
use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use nexus_types::deployment::ReconfiguratorConfig;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::NEXUS_TECHPORT_EXTERNAL_PORT;
use omicron_common::address::RACK_PREFIX;
use omicron_common::api::internal::shared::SwitchLocation;
use omicron_uuid_kinds::OmicronZoneUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::DeserializeFromStr;
use serde_with::DisplayFromStr;
use serde_with::DurationSeconds;
use serde_with::SerializeDisplay;
use serde_with::serde_as;
use std::collections::HashMap;
use std::fmt;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::time::Duration;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct NexusConfig {
    /// Configuration parameters known at compile-time.
    #[serde(flatten)]
    pub pkg: PackageConfig,

    /// A variety of configuration parameters only known at deployment time.
    pub deployment: DeploymentConfig,
}

impl NexusConfig {
    /// Load a `Config` from the given TOML file
    ///
    /// This config object can then be used to create a new `Nexus`.
    /// The format is described in the README.
    pub fn from_file<P: AsRef<Utf8Path>>(path: P) -> Result<Self, LoadError> {
        let path = path.as_ref();
        let file_contents = std::fs::read_to_string(path)
            .map_err(|e| (path.to_path_buf(), e))?;
        let config_parsed: Self = toml::from_str(&file_contents)
            .map_err(|e| (path.to_path_buf(), e))?;
        Ok(config_parsed)
    }
}

#[derive(Debug)]
pub struct LoadError {
    pub path: Utf8PathBuf,
    pub kind: LoadErrorKind,
}

#[derive(Debug)]
pub struct InvalidTunable {
    pub tunable: String,
    pub message: String,
}

impl std::fmt::Display for InvalidTunable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "invalid \"{}\": \"{}\"", self.tunable, self.message)
    }
}
impl std::error::Error for InvalidTunable {}

#[derive(Debug)]
pub enum LoadErrorKind {
    Io(std::io::Error),
    Parse(toml::de::Error),
    InvalidTunable(InvalidTunable),
}

impl From<(Utf8PathBuf, std::io::Error)> for LoadError {
    fn from((path, err): (Utf8PathBuf, std::io::Error)) -> Self {
        LoadError { path, kind: LoadErrorKind::Io(err) }
    }
}

impl From<(Utf8PathBuf, toml::de::Error)> for LoadError {
    fn from((path, err): (Utf8PathBuf, toml::de::Error)) -> Self {
        LoadError { path, kind: LoadErrorKind::Parse(err) }
    }
}

impl std::error::Error for LoadError {}

impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.kind {
            LoadErrorKind::Io(e) => {
                write!(f, "read \"{}\": {}", self.path, e)
            }
            LoadErrorKind::Parse(e) => {
                write!(f, "parse \"{}\": {}", self.path, e.message())
            }
            LoadErrorKind::InvalidTunable(inner) => {
                write!(f, "invalid tunable \"{}\": {}", self.path, inner)
            }
        }
    }
}

impl std::cmp::PartialEq<std::io::Error> for LoadError {
    fn eq(&self, other: &std::io::Error) -> bool {
        if let LoadErrorKind::Io(e) = &self.kind {
            e.kind() == other.kind()
        } else {
            false
        }
    }
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum Database {
    FromDns,
    FromUrl {
        #[serde_as(as = "DisplayFromStr")]
        #[schemars(with = "String")]
        url: PostgresConfigWithUrl,
    },
}

/// The mechanism Nexus should use to contact the internal DNS servers.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InternalDns {
    /// Nexus should infer the DNS server addresses from this subnet.
    ///
    /// This is a more common usage for production.
    FromSubnet { subnet: Ipv6Subnet<RACK_PREFIX> },
    /// Nexus should use precisely the following address.
    ///
    /// This is less desirable in production, but can give value
    /// in test scenarios.
    FromAddress { address: SocketAddr },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, JsonSchema)]
pub struct DeploymentConfig {
    /// Uuid of the Nexus instance
    pub id: OmicronZoneUuid,
    /// Uuid of the Rack where Nexus is executing.
    pub rack_id: Uuid,
    /// Port on which the "techport external" dropshot server should listen.
    /// This dropshot server copies _most_ of its config from
    /// `dropshot_external` (so that it matches TLS, etc.), but builds its
    /// listening address by combining `dropshot_internal`'s IP address with
    /// this port.
    ///
    /// We use `serde(default = ...)` to ensure we don't break any serialized
    /// configs that were created before this field was added. In production we
    /// always expect this port to be constant, but we need to be able to
    /// override it when running tests.
    #[schemars(skip)]
    #[serde(default = "default_techport_external_server_port")]
    pub techport_external_server_port: u16,
    /// Dropshot configuration for the external API server.
    #[schemars(skip)] // TODO we're protected against dropshot changes
    pub dropshot_external: ConfigDropshotWithTls,
    /// Dropshot configuration for internal API server.
    #[schemars(skip)] // TODO we're protected against dropshot changes
    pub dropshot_internal: ConfigDropshot,
    /// Dropshot configuration for lockstep API server.
    #[schemars(skip)] // TODO we're protected against dropshot changes
    pub dropshot_lockstep: ConfigDropshot,
    /// Describes how Nexus should find internal DNS servers
    /// for bootstrapping.
    pub internal_dns: InternalDns,
    /// DB configuration.
    pub database: Database,
    /// External DNS servers Nexus can use to resolve external hosts.
    pub external_dns_servers: Vec<IpAddr>,
    /// Configuration for HTTP clients to external services.
    #[serde(default)]
    pub external_http_clients: ExternalHttpClientConfig,
}

fn default_techport_external_server_port() -> u16 {
    NEXUS_TECHPORT_EXTERNAL_PORT
}

impl DeploymentConfig {
    /// Load a `DeploymentConfig` from the given TOML file
    ///
    /// This config object can then be used to create a new `Nexus`.
    /// The format is described in the README.
    pub fn from_file<P: AsRef<Utf8Path>>(path: P) -> Result<Self, LoadError> {
        let path = path.as_ref();
        let file_contents = std::fs::read_to_string(path)
            .map_err(|e| (path.to_path_buf(), e))?;
        let config_parsed: Self = toml::from_str(&file_contents)
            .map_err(|e| (path.to_path_buf(), e))?;
        Ok(config_parsed)
    }
}

/// Thin wrapper around `ConfigDropshot` that adds a boolean for enabling TLS
///
/// The configuration for TLS consists of the list of TLS certificates used.
/// This is dynamic, driven by what's in CockroachDB.  That's why we only need a
/// boolean here.  (If in the future we want to configure other things about
/// TLS, this could be extended.)
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ConfigDropshotWithTls {
    /// Regular Dropshot configuration parameters
    #[serde(flatten)]
    pub dropshot: ConfigDropshot,
    /// Whether TLS is enabled (default: false)
    #[serde(default)]
    pub tls: bool,
}

// By design, we require that all config properties be specified (i.e., we don't
// use `serde(default)`).

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct AuthnConfig {
    /// allowed authentication schemes for external HTTP server
    pub schemes_external: Vec<SchemeName>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ConsoleConfig {
    pub static_dir: Utf8PathBuf,
    /// how long a session can be idle before expiring
    pub session_idle_timeout_minutes: u32,
    /// how long a session can exist before expiring
    pub session_absolute_timeout_minutes: u32,
}

/// Options to tweak database schema changes.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SchemaConfig {
    pub schema_dir: Utf8PathBuf,
}

/// Optional configuration for the timeseries database.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct TimeseriesDbConfig {
    /// The native TCP address of the ClickHouse server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<SocketAddr>,
}

/// Configuration for the `Dendrite` dataplane daemon.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DpdConfig {
    pub address: SocketAddr,
}

/// Configuration for the `Dendrite` dataplane daemon.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct MgdConfig {
    pub address: SocketAddr,
}

// A deserializable type that does no validation on the tunable parameters.
#[derive(Clone, Debug, Deserialize, PartialEq)]
struct UnvalidatedTunables {
    max_vpc_ipv4_subnet_prefix: u8,
    load_timeout: Option<std::time::Duration>,
}

/// Configuration for HTTP clients to external services.
#[derive(
    Clone, Debug, Default, Deserialize, PartialEq, Serialize, JsonSchema,
)]
pub struct ExternalHttpClientConfig {
    /// If present, bind all TCP connections for external HTTP clients on the
    /// specified interface name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interface: Option<String>,
}

/// Tunable configuration parameters, intended for use in test environments or
/// other situations in which experimentation / tuning is valuable.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(try_from = "UnvalidatedTunables")]
pub struct Tunables {
    /// The maximum prefix size supported for VPC Subnet IPv4 subnetworks.
    ///
    /// Note that this is the maximum _prefix_ size, which sets the minimum size
    /// of the subnet.
    pub max_vpc_ipv4_subnet_prefix: u8,

    /// How long should we attempt to loop until the schema matches?
    ///
    /// If "None", nexus loops forever during initialization.
    pub load_timeout: Option<std::time::Duration>,
}

// Convert from the unvalidated tunables, verifying each parameter as needed.
impl TryFrom<UnvalidatedTunables> for Tunables {
    type Error = InvalidTunable;

    fn try_from(unvalidated: UnvalidatedTunables) -> Result<Self, Self::Error> {
        Tunables::validate_ipv4_prefix(unvalidated.max_vpc_ipv4_subnet_prefix)?;
        Ok(Tunables {
            max_vpc_ipv4_subnet_prefix: unvalidated.max_vpc_ipv4_subnet_prefix,
            load_timeout: unvalidated.load_timeout,
        })
    }
}

/// Minimum prefix size supported in IPv4 VPC Subnets.
///
/// NOTE: This is the minimum _prefix_, which sets the maximum subnet size.
pub const MIN_VPC_IPV4_SUBNET_PREFIX: u8 = 8;

/// The number of reserved addresses at the beginning of a subnet range.
pub const NUM_INITIAL_RESERVED_IP_ADDRESSES: usize = 5;

impl Tunables {
    fn validate_ipv4_prefix(prefix: u8) -> Result<(), InvalidTunable> {
        let absolute_max: u8 = 32_u8
            .checked_sub(
                // Always need space for the reserved Oxide addresses, including the
                // broadcast address at the end of the subnet.
                ((NUM_INITIAL_RESERVED_IP_ADDRESSES + 1) as f32)
                .log2() // Subnet size to bit prefix.
                .ceil() // Round up to a whole number of bits.
                as u8,
            )
            .expect("Invalid absolute maximum IPv4 subnet prefix");
        if prefix >= MIN_VPC_IPV4_SUBNET_PREFIX && prefix <= absolute_max {
            Ok(())
        } else {
            Err(InvalidTunable {
                tunable: String::from("max_vpc_ipv4_subnet_prefix"),
                message: format!(
                    "IPv4 subnet prefix must be in the range [0, {}], found: {}",
                    absolute_max, prefix,
                ),
            })
        }
    }
}

/// The maximum prefix size by default.
///
/// There are 6 Oxide reserved IP addresses, 5 at the beginning for DNS and the
/// like, and the broadcast address at the end of the subnet. This size provides
/// room for 2 ** 6 - 6 = 58 IP addresses, which seems like a reasonable size
/// for the smallest subnet that's still useful in many contexts.
pub const MAX_VPC_IPV4_SUBNET_PREFIX: u8 = 26;

impl Default for Tunables {
    fn default() -> Self {
        Tunables {
            max_vpc_ipv4_subnet_prefix: MAX_VPC_IPV4_SUBNET_PREFIX,
            load_timeout: None,
        }
    }
}

/// Background task configuration
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BackgroundTaskConfig {
    /// configuration for internal DNS background tasks
    pub dns_internal: DnsTasksConfig,
    /// configuration for external DNS background tasks
    pub dns_external: DnsTasksConfig,
    /// configuration for metrics producer garbage collection background task
    pub metrics_producer_gc: MetricsProducerGcConfig,
    /// configuration for external endpoint list watcher
    pub external_endpoints: ExternalEndpointsConfig,
    /// configuration for nat table garbage collector
    pub nat_cleanup: NatCleanupConfig,
    /// configuration for inventory tasks
    pub inventory: InventoryConfig,
    /// configuration for support bundle collection
    pub support_bundle_collector: SupportBundleCollectorConfig,
    /// configuration for physical disk adoption tasks
    pub physical_disk_adoption: PhysicalDiskAdoptionConfig,
    /// configuration for decommissioned disk cleaner task
    pub decommissioned_disk_cleaner: DecommissionedDiskCleanerConfig,
    /// configuration for phantom disks task
    pub phantom_disks: PhantomDiskConfig,
    /// configuration for blueprint related tasks
    pub blueprints: BlueprintTasksConfig,
    /// configuration for service zone nat sync task
    pub sync_service_zone_nat: SyncServiceZoneNatConfig,
    /// configuration for the bfd manager task
    pub bfd_manager: BfdManagerConfig,
    /// configuration for the switch port settings manager task
    pub switch_port_settings_manager: SwitchPortSettingsManagerConfig,
    /// configuration for region replacement starter task
    pub region_replacement: RegionReplacementConfig,
    /// configuration for region replacement driver task
    pub region_replacement_driver: RegionReplacementDriverConfig,
    /// configuration for instance watcher task
    pub instance_watcher: InstanceWatcherConfig,
    /// configuration for instance updater task
    pub instance_updater: InstanceUpdaterConfig,
    /// configuration for instance reincarnation task
    pub instance_reincarnation: InstanceReincarnationConfig,
    /// configuration for service VPC firewall propagation task
    pub service_firewall_propagation: ServiceFirewallPropagationConfig,
    /// configuration for v2p mapping propagation task
    pub v2p_mapping_propagation: V2PMappingPropagationConfig,
    /// configuration for abandoned VMM reaper task
    pub abandoned_vmm_reaper: AbandonedVmmReaperConfig,
    /// configuration for saga recovery task
    pub saga_recovery: SagaRecoveryConfig,
    /// configuration for lookup region port task
    pub lookup_region_port: LookupRegionPortConfig,
    /// configuration for region snapshot replacement starter task
    pub region_snapshot_replacement_start: RegionSnapshotReplacementStartConfig,
    /// configuration for region snapshot replacement garbage collection
    pub region_snapshot_replacement_garbage_collection:
        RegionSnapshotReplacementGarbageCollectionConfig,
    /// configuration for region snapshot replacement step task
    pub region_snapshot_replacement_step: RegionSnapshotReplacementStepConfig,
    /// configuration for region snapshot replacement finisher task
    pub region_snapshot_replacement_finish:
        RegionSnapshotReplacementFinishConfig,
    /// configuration for TUF artifact replication task
    pub tuf_artifact_replication: TufArtifactReplicationConfig,
    /// configuration for read-only region replacement start task
    pub read_only_region_replacement_start:
        ReadOnlyRegionReplacementStartConfig,
    /// configuration for webhook dispatcher task
    pub alert_dispatcher: AlertDispatcherConfig,
    /// configuration for webhook deliverator task
    pub webhook_deliverator: WebhookDeliveratorConfig,
    /// configuration for SP ereport ingester task
    pub sp_ereport_ingester: SpEreportIngesterConfig,
    /// configuration for multicast group reconciler task
    pub multicast_group_reconciler: MulticastGroupReconcilerConfig,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DnsTasksConfig {
    /// period (in seconds) for periodic activations of the background task that
    /// reads the latest DNS configuration from the database
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs_config: Duration,

    /// period (in seconds) for periodic activations of the background task that
    /// reads the latest list of DNS servers from the database
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs_servers: Duration,

    /// period (in seconds) for periodic activations of the background task that
    /// propagates the latest DNS configuration to the latest set of DNS servers
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs_propagation: Duration,

    /// maximum number of concurrent DNS server updates
    pub max_concurrent_server_updates: usize,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MetricsProducerGcConfig {
    /// period (in seconds) for periodic activations of the background task that
    /// garbage collects metrics producers whose leases have expired
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExternalEndpointsConfig {
    /// period (in seconds) for periodic activations of this background task
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,
    // Other policy around the TLS certificates could go here (e.g.,
    // allow/disallow wildcard certs, don't serve expired certs, etc.)
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SupportBundleCollectorConfig {
    /// period (in seconds) for periodic activations of this background task
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,

    /// A toggle to disable support bundle collection
    ///
    /// Default: Off
    #[serde(default)]
    pub disable: bool,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PhysicalDiskAdoptionConfig {
    /// period (in seconds) for periodic activations of this background task
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,

    /// A toggle to disable automated disk adoption.
    ///
    /// Default: Off
    #[serde(default)]
    pub disable: bool,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DecommissionedDiskCleanerConfig {
    /// period (in seconds) for periodic activations of this background task
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,

    /// A toggle to disable automated disk cleanup
    ///
    /// Default: Off
    #[serde(default)]
    pub disable: bool,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NatCleanupConfig {
    /// period (in seconds) for periodic activations of this background task
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BfdManagerConfig {
    /// period (in seconds) for periodic activations of this background task
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SyncServiceZoneNatConfig {
    /// period (in seconds) for periodic activations of this background task
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SwitchPortSettingsManagerConfig {
    /// Interval (in seconds) for periodic activations of this background task.
    /// This task is also activated on-demand when any of the switch port settings
    /// api endpoints are called.
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,
}
#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InventoryConfig {
    /// period (in seconds) for periodic activations of this background task
    ///
    /// Each activation fetches information about all hardware and software in
    /// the system and inserts it into the database.  This generates a moderate
    /// amount of data.
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,

    /// maximum number of past collections to keep in the database
    ///
    /// This is a very coarse mechanism to keep the system from overwhelming
    /// itself with inventory data.
    pub nkeep: u32,

    /// disable inventory collection altogether
    ///
    /// This is an emergency lever for support / operations.  It should never be
    /// necessary.
    pub disable: bool,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PhantomDiskConfig {
    /// period (in seconds) for periodic activations of this background task
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BlueprintTasksConfig {
    /// period (in seconds) for periodic activations of the background task that
    /// reads the latest target blueprint from the database
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs_load: Duration,

    /// period (in seconds) for periodic activations of the background task that
    /// plans and updates the target blueprint
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs_plan: Duration,

    /// period (in seconds) for periodic activations of the background task that
    /// executes the latest target blueprint
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs_execute: Duration,

    /// period (in seconds) for periodic activations of the background task that
    /// reconciles the latest blueprint and latest inventory collection into
    /// Reconfigurator rendezvous tables
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs_rendezvous: Duration,

    /// period (in seconds) for periodic activations of the background task that
    /// collects the node IDs of CockroachDB zones
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs_collect_crdb_node_ids: Duration,

    /// period (in seconds) for periodic activations of the background task that
    /// reads the reconfigurator config from the database
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs_load_reconfigurator_config: Duration,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RegionReplacementConfig {
    /// period (in seconds) for periodic activations of this background task
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InstanceWatcherConfig {
    /// period (in seconds) for periodic activations of this background task
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InstanceUpdaterConfig {
    /// period (in seconds) for periodic activations of this background task
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,

    /// disable background checks for instances in need of updates.
    ///
    /// This config is intended for use in testing, and should generally not be
    /// enabled in real life.
    ///
    /// Default: Off
    #[serde(default)]
    pub disable: bool,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InstanceReincarnationConfig {
    /// period (in seconds) for periodic activations of this background task
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,

    /// disable background checks for instances in need of updates.
    ///
    /// This is an emergency lever for support / operations. It should only be
    /// necessary if something has gone extremely wrong.
    ///
    /// Default: Off
    #[serde(default)]
    pub disable: bool,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ServiceFirewallPropagationConfig {
    /// period (in seconds) for periodic activations of this background task
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct V2PMappingPropagationConfig {
    /// period (in seconds) for periodic activations of this background task
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AbandonedVmmReaperConfig {
    /// period (in seconds) for periodic activations of this background task
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SagaRecoveryConfig {
    /// period (in seconds) for periodic activations of this background task
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RegionReplacementDriverConfig {
    /// period (in seconds) for periodic activations of this background task
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LookupRegionPortConfig {
    /// period (in seconds) for periodic activations of this background task
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RegionSnapshotReplacementStartConfig {
    /// period (in seconds) for periodic activations of this background task
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RegionSnapshotReplacementGarbageCollectionConfig {
    /// period (in seconds) for periodic activations of this background task
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RegionSnapshotReplacementStepConfig {
    /// period (in seconds) for periodic activations of this background task
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RegionSnapshotReplacementFinishConfig {
    /// period (in seconds) for periodic activations of this background task
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TufArtifactReplicationConfig {
    /// period (in seconds) for periodic activations of this background task
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,
    /// The number of sleds that artifacts must be present on before a local
    /// copy of a repo's artifacts is dropped.
    pub min_sled_replication: usize,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReadOnlyRegionReplacementStartConfig {
    /// period (in seconds) for periodic activations of this background task
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AlertDispatcherConfig {
    /// period (in seconds) for periodic activations of this background task
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WebhookDeliveratorConfig {
    /// period (in seconds) for periodic activations of this background task
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,

    /// duration after which another Nexus' lease on a delivery attempt is
    /// considered expired.
    ///
    /// this is tuneable to allow testing lease expiration without having to
    /// wait a long time.
    #[serde(default = "WebhookDeliveratorConfig::default_lease_timeout_secs")]
    pub lease_timeout_secs: u64,

    /// backoff period for the first retry of a failed delivery attempt.
    ///
    /// this is tuneable to allow testing delivery retries without having to
    /// wait a long time.
    #[serde(default = "WebhookDeliveratorConfig::default_first_retry_backoff")]
    pub first_retry_backoff_secs: u64,

    /// backoff period for the second retry of a failed delivery attempt.
    ///
    /// this is tuneable to allow testing delivery retries without having to
    /// wait a long time.
    #[serde(
        default = "WebhookDeliveratorConfig::default_second_retry_backoff"
    )]
    pub second_retry_backoff_secs: u64,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SpEreportIngesterConfig {
    /// period (in seconds) for periodic activations of this background task
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,

    /// disable ereport collection altogether
    ///
    /// This is an emergency lever for support / operations.  It should never be
    /// necessary.
    ///
    /// Default: Off
    #[serde(default)]
    pub disable: bool,
}

impl Default for SpEreportIngesterConfig {
    fn default() -> Self {
        Self { period_secs: Duration::from_secs(30), disable: false }
    }
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MulticastGroupReconcilerConfig {
    /// period (in seconds) for periodic activations of the background task that
    /// reconciles multicast group state with dendrite switch configuration
    #[serde_as(as = "DurationSeconds<u64>")]
    pub period_secs: Duration,
}

impl Default for MulticastGroupReconcilerConfig {
    fn default() -> Self {
        Self { period_secs: Duration::from_secs(60) }
    }
}

/// TODO: remove this when multicast is implemented end-to-end.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct MulticastConfig {
    /// Whether multicast functionality is enabled or not.
    ///
    /// When false, multicast API calls remain accessible but no actual
    /// multicast operations occur (no switch programming, reconciler disabled).
    /// Instance sagas will skip multicast operations. This allows gradual
    /// rollout and testing of multicast configuration.
    ///
    /// Default: false (experimental feature, disabled by default)
    #[serde(default)]
    pub enabled: bool,
}

/// Configuration for a nexus server
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PackageConfig {
    /// Console-related tunables
    pub console: ConsoleConfig,
    /// Server-wide logging configuration.
    pub log: ConfigLogging,
    /// Authentication-related configuration
    pub authn: AuthnConfig,
    /// Timeseries database configuration.
    #[serde(default)]
    pub timeseries_db: TimeseriesDbConfig,
    /// Describes how to handle and perform schema changes.
    #[serde(default)]
    pub schema: Option<SchemaConfig>,
    /// Tunable configuration for testing and experimentation
    #[serde(default)]
    pub tunables: Tunables,
    /// `Dendrite` dataplane daemon configuration
    #[serde(default)]
    pub dendrite: HashMap<SwitchLocation, DpdConfig>,
    /// Maghemite mgd daemon configuration
    #[serde(default)]
    pub mgd: HashMap<SwitchLocation, MgdConfig>,
    /// Initial reconfigurator config
    ///
    /// We use this hook to disable reconfigurator automation in the test suite
    #[serde(default)]
    pub initial_reconfigurator_config: Option<ReconfiguratorConfig>,
    /// Background task configuration
    pub background_tasks: BackgroundTaskConfig,
    /// Multicast feature configuration
    #[serde(default)]
    pub multicast: MulticastConfig,
    /// Default Crucible region allocation strategy
    pub default_region_allocation_strategy: RegionAllocationStrategy,
}

/// List of supported external authn schemes
///
/// Note that the authn subsystem doesn't know about this type.  It allows
/// schemes to be called whatever they want.  This is just to provide a set of
/// allowed values for configuration.
#[derive(
    Clone, Copy, Debug, DeserializeFromStr, Eq, PartialEq, SerializeDisplay,
)]
pub enum SchemeName {
    Spoof,
    SessionCookie,
    AccessToken,
}

impl std::str::FromStr for SchemeName {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "spoof" => Ok(SchemeName::Spoof),
            "session_cookie" => Ok(SchemeName::SessionCookie),
            "access_token" => Ok(SchemeName::AccessToken),
            _ => Err(anyhow!("unsupported authn scheme: {:?}", s)),
        }
    }
}

impl std::fmt::Display for SchemeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            SchemeName::Spoof => "spoof",
            SchemeName::SessionCookie => "session_cookie",
            SchemeName::AccessToken => "access_token",
        })
    }
}

impl WebhookDeliveratorConfig {
    const fn default_lease_timeout_secs() -> u64 {
        60 // one minute
    }

    const fn default_first_retry_backoff() -> u64 {
        60 // one minute
    }

    const fn default_second_retry_backoff() -> u64 {
        60 * 5 // five minutes
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use nexus_types::deployment::PlannerConfig;
    use omicron_common::address::{
        CLICKHOUSE_TCP_PORT, Ipv6Subnet, RACK_PREFIX,
    };
    use omicron_common::api::internal::shared::SwitchLocation;

    use camino::{Utf8Path, Utf8PathBuf};
    use dropshot::ConfigDropshot;
    use dropshot::ConfigLogging;
    use dropshot::ConfigLoggingIfExists;
    use dropshot::ConfigLoggingLevel;
    use pretty_assertions::assert_eq;
    use std::collections::HashMap;
    use std::fs;
    use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};
    use std::str::FromStr;
    use std::time::Duration;

    /// Generates a temporary filesystem path unique for the given label.
    fn temp_path(label: &str) -> Utf8PathBuf {
        let arg0str = std::env::args().next().expect("expected process arg0");
        let arg0 = Utf8Path::new(&arg0str)
            .file_name()
            .expect("expected arg0 filename");
        let pid = std::process::id();
        let mut pathbuf = Utf8PathBuf::try_from(std::env::temp_dir())
            .expect("expected temp dir to be valid UTF-8");
        pathbuf.push(format!("{}.{}.{}", arg0, pid, label));
        pathbuf
    }

    /// Load a Config with the given string `contents`.  To exercise
    /// the full path, this function writes the contents to a file first, then
    /// loads the config from that file, then removes the file.  `label` is used
    /// as a unique string for the filename and error messages.  It should be
    /// unique for each test.
    fn read_config(
        label: &str,
        contents: &str,
    ) -> Result<NexusConfig, LoadError> {
        let pathbuf = temp_path(label);
        let path = pathbuf.as_path();
        eprintln!("writing test config {}", path);
        fs::write(path, contents).expect("write to tempfile failed");

        let result = NexusConfig::from_file(path);
        fs::remove_file(path).expect("failed to remove temporary file");
        eprintln!("{:?}", result);
        result
    }

    // Totally bogus config files (nonexistent, bad TOML syntax)

    #[test]
    fn test_config_nonexistent() {
        let error = NexusConfig::from_file(Utf8Path::new("/nonexistent"))
            .expect_err("expected config to fail from /nonexistent");
        let expected = std::io::Error::from_raw_os_error(libc::ENOENT);
        assert_eq!(error, expected);
    }

    #[test]
    fn test_config_bad_toml() {
        let error =
            read_config("bad_toml", "foo =").expect_err("expected failure");
        if let LoadErrorKind::Parse(error) = &error.kind {
            assert_eq!(error.span(), Some(5..5));
            // See https://github.com/toml-rs/toml/issues/519
            // assert_eq!(
            //     error.message(),
            //     "unexpected eof encountered at line 1 column 6"
            // );
        } else {
            panic!(
                "Got an unexpected error, expected Parse but got {:?}",
                error
            );
        }
    }

    // Empty config (special case of a missing required field, but worth calling
    // out explicitly)

    #[test]
    fn test_config_empty() {
        let error = read_config("empty", "").expect_err("expected failure");
        if let LoadErrorKind::Parse(error) = &error.kind {
            assert_eq!(error.span(), Some(0..0));
            assert_eq!(error.message(), "missing field `deployment`");
        } else {
            panic!(
                "Got an unexpected error, expected Parse but got {:?}",
                error
            );
        }
    }

    // Success case.  We don't need to retest semantics for either ConfigLogging
    // or ConfigDropshot because those are both tested within Dropshot.  If we
    // add new configuration sections of our own, we will want to test those
    // here (both syntax and semantics).
    #[test]
    fn test_valid() {
        let config = read_config(
            "valid",
            r##"
            [console]
            static_dir = "tests/static"
            session_idle_timeout_minutes = 60
            session_absolute_timeout_minutes = 480
            [authn]
            schemes_external = []
            [log]
            mode = "file"
            level = "debug"
            path = "/nonexistent/path"
            if_exists = "fail"
            [timeseries_db]
            address = "[::1]:9000"
            [tunables]
            max_vpc_ipv4_subnet_prefix = 27
            [deployment]
            id = "28b90dc4-c22a-65ba-f49a-f051fe01208f"
            rack_id = "38b90dc4-c22a-65ba-f49a-f051fe01208f"
            external_dns_servers = [ "1.1.1.1", "9.9.9.9" ]
            [deployment.external_http_clients]
            interface = "opte0"
            [deployment.dropshot_external]
            bind_address = "10.1.2.3:4567"
            default_request_body_max_bytes = 1024
            [deployment.dropshot_internal]
            bind_address = "10.1.2.3:4568"
            default_request_body_max_bytes = 1024
            [deployment.dropshot_lockstep]
            bind_address = "10.1.2.3:4569"
            default_request_body_max_bytes = 1024
            [deployment.internal_dns]
            type = "from_subnet"
            subnet.net = "::/56"
            [deployment.database]
            type = "from_dns"
            [dendrite.switch0]
            address = "[::1]:12224"
            [mgd.switch0]
            address = "[::1]:4676"
            [initial_reconfigurator_config]
            planner_enabled = true
            planner_config.add_zones_with_mupdate_override = true
            [background_tasks]
            dns_internal.period_secs_config = 1
            dns_internal.period_secs_servers = 2
            dns_internal.period_secs_propagation = 3
            dns_internal.max_concurrent_server_updates = 4
            dns_external.period_secs_config = 5
            dns_external.period_secs_servers = 6
            dns_external.period_secs_propagation = 7
            dns_external.max_concurrent_server_updates = 8
            metrics_producer_gc.period_secs = 60
            external_endpoints.period_secs = 9
            nat_cleanup.period_secs = 30
            bfd_manager.period_secs = 30
            inventory.period_secs = 10
            inventory.nkeep = 11
            inventory.disable = false
            support_bundle_collector.period_secs = 30
            physical_disk_adoption.period_secs = 30
            decommissioned_disk_cleaner.period_secs = 30
            phantom_disks.period_secs = 30
            blueprints.period_secs_load = 10
            blueprints.period_secs_plan = 60
            blueprints.period_secs_execute = 60
            blueprints.period_secs_rendezvous = 300
            blueprints.period_secs_collect_crdb_node_ids = 180
            blueprints.period_secs_load_reconfigurator_config = 5
            sync_service_zone_nat.period_secs = 30
            switch_port_settings_manager.period_secs = 30
            region_replacement.period_secs = 30
            region_replacement_driver.period_secs = 30
            instance_watcher.period_secs = 30
            instance_updater.period_secs = 30
            instance_updater.disable = false
            instance_reincarnation.period_secs = 67
            service_firewall_propagation.period_secs = 300
            v2p_mapping_propagation.period_secs = 30
            abandoned_vmm_reaper.period_secs = 60
            saga_recovery.period_secs = 60
            lookup_region_port.period_secs = 60
            region_snapshot_replacement_start.period_secs = 30
            region_snapshot_replacement_garbage_collection.period_secs = 30
            region_snapshot_replacement_step.period_secs = 30
            region_snapshot_replacement_finish.period_secs = 30
            tuf_artifact_replication.period_secs = 300
            tuf_artifact_replication.min_sled_replication = 3
            read_only_region_replacement_start.period_secs = 30
            alert_dispatcher.period_secs = 42
            webhook_deliverator.period_secs = 43
            webhook_deliverator.lease_timeout_secs = 44
            webhook_deliverator.first_retry_backoff_secs = 45
            webhook_deliverator.second_retry_backoff_secs = 46
            sp_ereport_ingester.period_secs = 47
            multicast_group_reconciler.period_secs = 60
            [default_region_allocation_strategy]
            type = "random"
            seed = 0
            "##,
        )
        .unwrap();

        assert_eq!(
            config,
            NexusConfig {
                deployment: DeploymentConfig {
                    id: "28b90dc4-c22a-65ba-f49a-f051fe01208f".parse().unwrap(),
                    rack_id: "38b90dc4-c22a-65ba-f49a-f051fe01208f"
                        .parse()
                        .unwrap(),
                    techport_external_server_port:
                        default_techport_external_server_port(),
                    dropshot_external: ConfigDropshotWithTls {
                        tls: false,
                        dropshot: ConfigDropshot {
                            bind_address: "10.1.2.3:4567"
                                .parse::<SocketAddr>()
                                .unwrap(),
                            ..Default::default()
                        }
                    },
                    dropshot_internal: ConfigDropshot {
                        bind_address: "10.1.2.3:4568"
                            .parse::<SocketAddr>()
                            .unwrap(),
                        ..Default::default()
                    },
                    dropshot_lockstep: ConfigDropshot {
                        bind_address: "10.1.2.3:4569"
                            .parse::<SocketAddr>()
                            .unwrap(),
                        ..Default::default()
                    },
                    internal_dns: InternalDns::FromSubnet {
                        subnet: Ipv6Subnet::<RACK_PREFIX>::new(
                            Ipv6Addr::LOCALHOST
                        )
                    },
                    database: Database::FromDns,
                    external_dns_servers: vec![
                        "1.1.1.1".parse().unwrap(),
                        "9.9.9.9".parse().unwrap(),
                    ],
                    external_http_clients: ExternalHttpClientConfig {
                        interface: Some("opte0".to_string()),
                    },
                },
                pkg: PackageConfig {
                    console: ConsoleConfig {
                        static_dir: "tests/static".parse().unwrap(),
                        session_idle_timeout_minutes: 60,
                        session_absolute_timeout_minutes: 480
                    },
                    authn: AuthnConfig { schemes_external: Vec::new() },
                    log: ConfigLogging::File {
                        level: ConfigLoggingLevel::Debug,
                        if_exists: ConfigLoggingIfExists::Fail,
                        path: "/nonexistent/path".into()
                    },
                    timeseries_db: TimeseriesDbConfig {
                        address: Some(SocketAddr::V6(SocketAddrV6::new(
                            Ipv6Addr::LOCALHOST,
                            CLICKHOUSE_TCP_PORT,
                            0,
                            0,
                        ))),
                    },
                    schema: None,
                    tunables: Tunables {
                        max_vpc_ipv4_subnet_prefix: 27,
                        load_timeout: None
                    },
                    dendrite: HashMap::from([(
                        SwitchLocation::Switch0,
                        DpdConfig {
                            address: SocketAddr::from_str("[::1]:12224")
                                .unwrap(),
                        }
                    )]),
                    mgd: HashMap::from([(
                        SwitchLocation::Switch0,
                        MgdConfig {
                            address: SocketAddr::from_str("[::1]:4676")
                                .unwrap(),
                        }
                    )]),
                    initial_reconfigurator_config: Some(ReconfiguratorConfig {
                        planner_enabled: true,
                        planner_config: PlannerConfig {
                            add_zones_with_mupdate_override: true,
                        },
                    }),
                    background_tasks: BackgroundTaskConfig {
                        dns_internal: DnsTasksConfig {
                            period_secs_config: Duration::from_secs(1),
                            period_secs_servers: Duration::from_secs(2),
                            period_secs_propagation: Duration::from_secs(3),
                            max_concurrent_server_updates: 4,
                        },
                        dns_external: DnsTasksConfig {
                            period_secs_config: Duration::from_secs(5),
                            period_secs_servers: Duration::from_secs(6),
                            period_secs_propagation: Duration::from_secs(7),
                            max_concurrent_server_updates: 8,
                        },
                        metrics_producer_gc: MetricsProducerGcConfig {
                            period_secs: Duration::from_secs(60)
                        },
                        external_endpoints: ExternalEndpointsConfig {
                            period_secs: Duration::from_secs(9),
                        },
                        nat_cleanup: NatCleanupConfig {
                            period_secs: Duration::from_secs(30),
                        },
                        bfd_manager: BfdManagerConfig {
                            period_secs: Duration::from_secs(30),
                        },
                        inventory: InventoryConfig {
                            period_secs: Duration::from_secs(10),
                            nkeep: 11,
                            disable: false,
                        },
                        support_bundle_collector:
                            SupportBundleCollectorConfig {
                                period_secs: Duration::from_secs(30),
                                disable: false,
                            },
                        physical_disk_adoption: PhysicalDiskAdoptionConfig {
                            period_secs: Duration::from_secs(30),
                            disable: false,
                        },
                        decommissioned_disk_cleaner:
                            DecommissionedDiskCleanerConfig {
                                period_secs: Duration::from_secs(30),
                                disable: false,
                            },
                        phantom_disks: PhantomDiskConfig {
                            period_secs: Duration::from_secs(30),
                        },
                        blueprints: BlueprintTasksConfig {
                            period_secs_load: Duration::from_secs(10),
                            period_secs_plan: Duration::from_secs(60),
                            period_secs_execute: Duration::from_secs(60),
                            period_secs_collect_crdb_node_ids:
                                Duration::from_secs(180),
                            period_secs_rendezvous: Duration::from_secs(300),
                            period_secs_load_reconfigurator_config:
                                Duration::from_secs(5)
                        },
                        sync_service_zone_nat: SyncServiceZoneNatConfig {
                            period_secs: Duration::from_secs(30)
                        },
                        switch_port_settings_manager:
                            SwitchPortSettingsManagerConfig {
                                period_secs: Duration::from_secs(30),
                            },
                        region_replacement: RegionReplacementConfig {
                            period_secs: Duration::from_secs(30),
                        },
                        region_replacement_driver:
                            RegionReplacementDriverConfig {
                                period_secs: Duration::from_secs(30),
                            },
                        instance_watcher: InstanceWatcherConfig {
                            period_secs: Duration::from_secs(30),
                        },
                        instance_updater: InstanceUpdaterConfig {
                            period_secs: Duration::from_secs(30),
                            disable: false,
                        },
                        instance_reincarnation: InstanceReincarnationConfig {
                            period_secs: Duration::from_secs(67),
                            disable: false,
                        },
                        service_firewall_propagation:
                            ServiceFirewallPropagationConfig {
                                period_secs: Duration::from_secs(300),
                            },
                        v2p_mapping_propagation: V2PMappingPropagationConfig {
                            period_secs: Duration::from_secs(30)
                        },
                        abandoned_vmm_reaper: AbandonedVmmReaperConfig {
                            period_secs: Duration::from_secs(60),
                        },
                        saga_recovery: SagaRecoveryConfig {
                            period_secs: Duration::from_secs(60),
                        },
                        lookup_region_port: LookupRegionPortConfig {
                            period_secs: Duration::from_secs(60),
                        },
                        region_snapshot_replacement_start:
                            RegionSnapshotReplacementStartConfig {
                                period_secs: Duration::from_secs(30),
                            },
                        region_snapshot_replacement_garbage_collection:
                            RegionSnapshotReplacementGarbageCollectionConfig {
                                period_secs: Duration::from_secs(30),
                            },
                        region_snapshot_replacement_step:
                            RegionSnapshotReplacementStepConfig {
                                period_secs: Duration::from_secs(30),
                            },
                        region_snapshot_replacement_finish:
                            RegionSnapshotReplacementFinishConfig {
                                period_secs: Duration::from_secs(30),
                            },
                        tuf_artifact_replication:
                            TufArtifactReplicationConfig {
                                period_secs: Duration::from_secs(300),
                                min_sled_replication: 3,
                            },
                        read_only_region_replacement_start:
                            ReadOnlyRegionReplacementStartConfig {
                                period_secs: Duration::from_secs(30),
                            },
                        alert_dispatcher: AlertDispatcherConfig {
                            period_secs: Duration::from_secs(42),
                        },
                        webhook_deliverator: WebhookDeliveratorConfig {
                            period_secs: Duration::from_secs(43),
                            lease_timeout_secs: 44,
                            first_retry_backoff_secs: 45,
                            second_retry_backoff_secs: 46,
                        },
                        sp_ereport_ingester: SpEreportIngesterConfig {
                            period_secs: Duration::from_secs(47),
                            disable: false,
                        },
                        multicast_group_reconciler:
                            MulticastGroupReconcilerConfig {
                                period_secs: Duration::from_secs(60),
                            },
                    },
                    multicast: MulticastConfig { enabled: false },
                    default_region_allocation_strategy:
                        crate::nexus_config::RegionAllocationStrategy::Random {
                            seed: Some(0)
                        }
                },
            }
        );

        let config = read_config(
            "valid",
            r##"
            [console]
            static_dir = "tests/static"
            session_idle_timeout_minutes = 60
            session_absolute_timeout_minutes = 480
            [authn]
            schemes_external = [ "spoof", "session_cookie" ]
            [log]
            mode = "file"
            level = "debug"
            path = "/nonexistent/path"
            if_exists = "fail"
            [timeseries_db]
            address = "[::1]:9000"
            [deployment]
            id = "28b90dc4-c22a-65ba-f49a-f051fe01208f"
            rack_id = "38b90dc4-c22a-65ba-f49a-f051fe01208f"
            techport_external_server_port = 12345
            external_dns_servers = [ "1.1.1.1", "9.9.9.9" ]
            [deployment.dropshot_external]
            bind_address = "10.1.2.3:4567"
            default_request_body_max_bytes = 1024
            [deployment.dropshot_internal]
            bind_address = "10.1.2.3:4568"
            default_request_body_max_bytes = 1024
            [deployment.dropshot_lockstep]
            bind_address = "10.1.2.3:4569"
            default_request_body_max_bytes = 1024
            [deployment.internal_dns]
            type = "from_subnet"
            subnet.net = "::/56"
            [deployment.database]
            type = "from_dns"
            [dendrite.switch0]
            address = "[::1]:12224"
            [background_tasks]
            dns_internal.period_secs_config = 1
            dns_internal.period_secs_servers = 2
            dns_internal.period_secs_propagation = 3
            dns_internal.max_concurrent_server_updates = 4
            dns_external.period_secs_config = 5
            dns_external.period_secs_servers = 6
            dns_external.period_secs_propagation = 7
            dns_external.max_concurrent_server_updates = 8
            metrics_producer_gc.period_secs = 60
            external_endpoints.period_secs = 9
            nat_cleanup.period_secs = 30
            bfd_manager.period_secs = 30
            inventory.period_secs = 10
            inventory.nkeep = 3
            inventory.disable = false
            support_bundle_collector.period_secs = 30
            physical_disk_adoption.period_secs = 30
            decommissioned_disk_cleaner.period_secs = 30
            phantom_disks.period_secs = 30
            blueprints.period_secs_load = 10
            blueprints.period_secs_plan = 60
            blueprints.period_secs_execute = 60
            blueprints.period_secs_rendezvous = 300
            blueprints.period_secs_collect_crdb_node_ids = 180
            blueprints.period_secs_load_reconfigurator_config = 5
            sync_service_zone_nat.period_secs = 30
            switch_port_settings_manager.period_secs = 30
            region_replacement.period_secs = 30
            region_replacement_driver.period_secs = 30
            instance_watcher.period_secs = 30
            instance_updater.period_secs = 30
            instance_reincarnation.period_secs = 67
            service_firewall_propagation.period_secs = 300
            v2p_mapping_propagation.period_secs = 30
            abandoned_vmm_reaper.period_secs = 60
            saga_recovery.period_secs = 60
            lookup_region_port.period_secs = 60
            region_snapshot_replacement_start.period_secs = 30
            region_snapshot_replacement_garbage_collection.period_secs = 30
            region_snapshot_replacement_step.period_secs = 30
            region_snapshot_replacement_finish.period_secs = 30
            tuf_artifact_replication.period_secs = 300
            tuf_artifact_replication.min_sled_replication = 3
            read_only_region_replacement_start.period_secs = 30
            alert_dispatcher.period_secs = 42
            webhook_deliverator.period_secs = 43
            sp_ereport_ingester.period_secs = 44
            multicast_group_reconciler.period_secs = 60

            [default_region_allocation_strategy]
            type = "random"
            "##,
        )
        .unwrap();

        assert_eq!(
            config.pkg.authn.schemes_external,
            vec![SchemeName::Spoof, SchemeName::SessionCookie],
        );
        assert_eq!(config.deployment.techport_external_server_port, 12345);
    }

    #[test]
    fn test_bad_authn_schemes() {
        let error = read_config(
            "bad authn.schemes_external",
            r##"
            [console]
            static_dir = "tests/static"
            session_idle_timeout_minutes = 60
            session_absolute_timeout_minutes = 480
            [authn]
            schemes_external = ["trust-me"]
            [log]
            mode = "file"
            level = "debug"
            path = "/nonexistent/path"
            if_exists = "fail"
            [timeseries_db]
            address = "[::1]:9000"
            [deployment]
            id = "28b90dc4-c22a-65ba-f49a-f051fe01208f"
            rack_id = "38b90dc4-c22a-65ba-f49a-f051fe01208f"
            external_dns_servers = [ "1.1.1.1", "9.9.9.9" ]
            [deployment.dropshot_external]
            bind_address = "10.1.2.3:4567"
            default_request_body_max_bytes = 1024
            [deployment.dropshot_internal]
            bind_address = "10.1.2.3:4568"
            default_request_body_max_bytes = 1024
            [deployment.dropshot_lockstep]
            bind_address = "10.1.2.3:4569"
            default_request_body_max_bytes = 1024
            [deployment.internal_dns]
            type = "from_subnet"
            subnet.net = "::/56"
            [deployment.database]
            type = "from_dns"
            "##,
        )
        .expect_err("expected failure");
        if let LoadErrorKind::Parse(error) = &error.kind {
            assert!(
                error
                    .message()
                    .starts_with("unsupported authn scheme: \"trust-me\""),
                "error = {}",
                error
            );
        } else {
            panic!(
                "Got an unexpected error, expected Parse but got {:?}",
                error
            );
        }
    }

    #[test]
    fn test_invalid_ipv4_prefix_tunable() {
        let error = read_config(
            "invalid_ipv4_prefix_tunable",
            r##"
            [console]
            static_dir = "tests/static"
            session_idle_timeout_minutes = 60
            session_absolute_timeout_minutes = 480
            [authn]
            schemes_external = []
            [log]
            mode = "file"
            level = "debug"
            path = "/nonexistent/path"
            if_exists = "fail"
            [timeseries_db]
            address = "[::1]:9000"
            [tunables]
            max_vpc_ipv4_subnet_prefix = 100
            [deployment]
            id = "28b90dc4-c22a-65ba-f49a-f051fe01208f"
            rack_id = "38b90dc4-c22a-65ba-f49a-f051fe01208f"
            external_dns_servers = [ "1.1.1.1", "9.9.9.9" ]
            [deployment.dropshot_external]
            bind_address = "10.1.2.3:4567"
            default_request_body_max_bytes = 1024
            [deployment.dropshot_internal]
            bind_address = "10.1.2.3:4568"
            default_request_body_max_bytes = 1024
            [deployment.dropshot_lockstep]
            bind_address = "10.1.2.3:4568"
            default_request_body_max_bytes = 1024
            [deployment.internal_dns]
            type = "from_subnet"
            subnet.net = "::/56"
            [deployment.database]
            type = "from_dns"
            "##,
        )
        .expect_err("Expected failure");
        if let LoadErrorKind::Parse(error) = &error.kind {
            assert!(error.message().starts_with(
                r#"invalid "max_vpc_ipv4_subnet_prefix": "IPv4 subnet prefix must"#,
            ));
        } else {
            panic!(
                "Got an unexpected error, expected Parse but got {:?}",
                error
            );
        }
    }

    #[test]
    fn test_repo_configs_are_valid() {
        // The example config file should be valid.
        let config_path = "../nexus/examples/config.toml";
        println!("checking {:?}", config_path);
        let example_config = NexusConfig::from_file(config_path)
            .expect("example config file is not valid");

        // The second example config file should be valid.
        let config_path = "../nexus/examples/config-second.toml";
        println!("checking {:?}", config_path);
        let _ = NexusConfig::from_file(config_path)
            .expect("second example config file is not valid");

        // The config file used for the tests should also be valid.  The tests
        // won't clear the runway anyway if this file isn't valid.  But it's
        // helpful to verify this here explicitly as well.
        let config_path = "../nexus/examples/config.toml";
        println!("checking {:?}", config_path);
        let _ = NexusConfig::from_file(config_path)
            .expect("test config file is not valid");

        // The partial config file that's used to deploy Nexus must also be
        // valid.  However, it's missing the "deployment" section because that's
        // generated at deployment time.  We'll serialize this section from the
        // example config file (loaded above), append it to the contents of this
        // file, and verify the whole thing.
        #[derive(serde::Serialize)]
        struct DummyConfig {
            deployment: DeploymentConfig,
        }
        let example_deployment = toml::to_string_pretty(&DummyConfig {
            deployment: example_config.deployment,
        })
        .unwrap();

        let nexus_config_paths = [
            "../smf/nexus/single-sled/config-partial.toml",
            "../smf/nexus/multi-sled/config-partial.toml",
        ];
        for config_path in nexus_config_paths {
            println!(
                "checking {:?} with example deployment section added",
                config_path
            );
            let mut contents = std::fs::read_to_string(config_path)
                .expect("failed to read Nexus SMF config file");
            contents.push_str(
                "\n\n\n \
            # !! content below added by test_repo_configs_are_valid()\n\
            \n\n\n",
            );
            contents.push_str(&example_deployment);
            let _: NexusConfig = toml::from_str(&contents)
                .expect("Nexus SMF config file is not valid");
        }
    }

    #[test]
    fn test_deployment_config_schema() {
        let schema = schemars::schema_for!(DeploymentConfig);
        expectorate::assert_contents(
            "../schema/deployment-config.json",
            &serde_json::to_string_pretty(&schema).unwrap(),
        );
    }
}

/// Defines a strategy for choosing what physical disks to use when allocating
/// new crucible regions.
///
/// NOTE: More strategies can - and should! - be added.
///
/// See <https://rfd.shared.oxide.computer/rfd/0205> for a more
/// complete discussion.
///
/// Longer-term, we should consider:
/// - Storage size + remaining free space
/// - Sled placement of datasets
/// - What sort of loads we'd like to create (even split across all disks
///   may not be preferable, especially if maintenance is expected)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RegionAllocationStrategy {
    /// Choose disks pseudo-randomly. An optional seed may be provided to make
    /// the ordering deterministic, otherwise the current time in nanoseconds
    /// will be used. Ordering is based on sorting the output of `md5(UUID of
    /// candidate dataset + seed)`. The seed does not need to come from a
    /// cryptographically secure source.
    Random { seed: Option<u64> },

    /// Like Random, but ensures that each region is allocated on its own sled.
    RandomWithDistinctSleds { seed: Option<u64> },
}
