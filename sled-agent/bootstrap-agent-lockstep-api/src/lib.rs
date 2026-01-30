// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Lockstep API for bootstrap agent rack initialization.
//!
//! This API handles rack initialization and reset operations. It is a lockstep
//! API as we do not expect rack initialization functions to be called during
//! and upgrade. Furthermore when rack initialization functions are called
//! it's expected that software components are on the same version.

use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv6Addr};

use dropshot::{HttpError, HttpResponseOk, RequestContext, TypedBody};
use omicron_common::address::IpRange;
use omicron_common::api::external::{AllowedSourceIps, Name, UserId};
use omicron_common::api::internal::nexus::Certificate;
use omicron_common::api::internal::shared::RackNetworkConfig;
use omicron_uuid_kinds::{RackInitUuid, RackResetUuid};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_hardware_types::Baseboard;

/// Configuration for the recovery silo created during rack setup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RecoverySiloConfig {
    pub silo_name: Name,
    pub user_name: UserId,
    pub user_password_hash: omicron_passwords::NewPasswordHash,
}

/// Describes how bootstrap addresses should be collected during RSS.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum BootstrapAddressDiscovery {
    /// Ignore all bootstrap addresses except our own.
    OnlyOurs,
    /// Ignore all bootstrap addresses except the following.
    OnlyThese { addrs: BTreeSet<Ipv6Addr> },
}

/// Rack initialization request.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
pub struct RackInitializeRequest {
    /// The set of peer_ids required to initialize trust quorum.
    ///
    /// The value is `None` if we are not using trust quorum.
    pub trust_quorum_peers: Option<Vec<Baseboard>>,

    /// Describes how bootstrap addresses should be collected during RSS.
    pub bootstrap_discovery: BootstrapAddressDiscovery,

    /// The external NTP server addresses.
    pub ntp_servers: Vec<String>,

    /// The external DNS server addresses.
    pub dns_servers: Vec<IpAddr>,

    /// Ranges of the service IP pool which may be used for internal services.
    pub internal_services_ip_pool_ranges: Vec<IpRange>,

    /// Service IP addresses on which we run external DNS servers.
    ///
    /// Each address must be present in `internal_services_ip_pool_ranges`.
    pub external_dns_ips: Vec<IpAddr>,

    /// DNS name for the DNS zone delegated to the rack for external DNS.
    pub external_dns_zone_name: String,

    /// Initial TLS certificates for the external API.
    pub external_certificates: Vec<Certificate>,

    /// Configuration of the Recovery Silo (the initial Silo).
    pub recovery_silo: RecoverySiloConfig,

    /// Initial rack network configuration.
    pub rack_network_config: RackNetworkConfig,

    /// IPs or subnets allowed to make requests to user-facing services.
    #[serde(default = "default_allowed_source_ips")]
    pub allowed_source_ips: AllowedSourceIps,
}

const fn default_allowed_source_ips() -> AllowedSourceIps {
    AllowedSourceIps::Any
}

/// Current status of any rack-level operation being performed by this bootstrap
/// agent.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RackOperationStatus {
    Initializing {
        id: RackInitUuid,
        step: RssStep,
    },
    /// `id` will be none if the rack was already initialized on startup.
    Initialized {
        id: Option<RackInitUuid>,
    },
    InitializationFailed {
        id: RackInitUuid,
        message: String,
    },
    InitializationPanicked {
        id: RackInitUuid,
    },
    Resetting {
        id: RackResetUuid,
    },
    /// `reset_id` will be None if the rack is in an uninitialized-on-startup,
    /// or Some if it is in an uninitialized state due to a reset operation
    /// completing.
    Uninitialized {
        reset_id: Option<RackResetUuid>,
    },
    ResetFailed {
        id: RackResetUuid,
        message: String,
    },
    ResetPanicked {
        id: RackResetUuid,
    },
}

/// Steps we go through during initial rack setup.
#[derive(
    Copy,
    Clone,
    Debug,
    Deserialize,
    Eq,
    Hash,
    JsonSchema,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RssStep {
    Requested,
    Starting,
    LoadExistingPlan,
    CreateSledPlan,
    InitTrustQuorum,
    NetworkConfigUpdate,
    SledInit,
    InitDns,
    ConfigureDns,
    InitNtp,
    WaitForTimeSync,
    WaitForDatabase,
    ClusterInit,
    ZonesInit,
    NexusHandoff,
}

#[dropshot::api_description]
pub trait BootstrapAgentLockstepApi {
    type Context;

    /// Get the current status of rack initialization or reset.
    #[endpoint {
        method = GET,
        path = "/rack-initialize",
    }]
    async fn rack_initialization_status(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<RackOperationStatus>, HttpError>;

    /// Initialize the rack with the provided configuration.
    #[endpoint {
        method = POST,
        path = "/rack-initialize",
    }]
    async fn rack_initialize(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<RackInitializeRequest>,
    ) -> Result<HttpResponseOk<RackInitUuid>, HttpError>;

    /// Reset the rack to an unconfigured state.
    #[endpoint {
        method = DELETE,
        path = "/rack-initialize",
    }]
    async fn rack_reset(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<RackResetUuid>, HttpError>;
}
