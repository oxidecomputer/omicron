// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Instance types for Sled Agent API versions 1-6.

use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};

use chrono::{DateTime, Utc};
use omicron_common::api::external;
use omicron_common::api::external::Generation;
use omicron_common::api::external::Hostname;
use omicron_common::api::internal::nexus::HostIdentifier;
use omicron_common::api::internal::shared::DhcpConfig;
use omicron_common::api::internal::shared::external_ip::v1::SourceNatConfig;
use omicron_common::api::internal::shared::network_interface::v1::NetworkInterface;
use omicron_uuid_kinds::{InstanceUuid, PropolisUuid};
use propolis_api_types_versions::v1::instance_spec::InstanceSpec;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Path parameters for VMM requests.
#[derive(Deserialize, JsonSchema)]
pub struct VmmPathParam {
    pub propolis_id: PropolisUuid,
}

/// Path parameters for VMM disk snapshot requests.
#[derive(Deserialize, JsonSchema)]
pub struct VmmIssueDiskSnapshotRequestPathParam {
    pub propolis_id: PropolisUuid,
    pub disk_id: Uuid,
}

/// Request body for VMM disk snapshot requests.
#[derive(Deserialize, JsonSchema)]
pub struct VmmIssueDiskSnapshotRequestBody {
    pub snapshot_id: Uuid,
}

/// Response for VMM disk snapshot requests.
#[derive(Serialize, JsonSchema)]
pub struct VmmIssueDiskSnapshotRequestResponse {
    pub snapshot_id: Uuid,
}

/// Path parameters for VPC requests.
#[derive(Deserialize, JsonSchema)]
pub struct VpcPathParam {
    pub vpc_id: Uuid,
}

/// One of the states that a VMM can be in.
#[derive(
    Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, Eq, PartialEq,
)]
#[serde(rename_all = "snake_case")]
pub enum VmmState {
    /// The VMM is initializing and has not started running guest CPUs yet.
    Starting,
    /// The VMM has finished initializing and may be running guest CPUs.
    Running,
    /// The VMM is shutting down.
    Stopping,
    /// The VMM's guest has stopped, and the guest will not run again, but the
    /// VMM process may not have released all of its resources yet.
    Stopped,
    /// The VMM is being restarted or its guest OS is rebooting.
    Rebooting,
    /// The VMM is part of a live migration.
    Migrating,
    /// The VMM process reported an internal failure.
    Failed,
    /// The VMM process has been destroyed and its resources have been released.
    Destroyed,
}

/// The dynamic runtime properties of an individual VMM process.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VmmRuntimeState {
    /// The last state reported by this VMM.
    pub state: VmmState,
    /// The generation number for this VMM's state.
    #[serde(rename = "gen")]
    pub generation: Generation,
    /// Timestamp for the VMM's state.
    pub time_updated: DateTime<Utc>,
}

/// A wrapper type containing a sled's total knowledge of the state of a VMM.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SledVmmState {
    /// The most recent state of the sled's VMM process.
    pub vmm_state: VmmRuntimeState,

    /// The current state of any inbound migration to this VMM.
    pub migration_in: Option<MigrationRuntimeState>,

    /// The state of any outbound migration from this VMM.
    pub migration_out: Option<MigrationRuntimeState>,
}

#[derive(Copy, Clone, Debug, Default)]
pub struct Migrations<'state> {
    pub migration_in: Option<&'state MigrationRuntimeState>,
    pub migration_out: Option<&'state MigrationRuntimeState>,
}

/// An update from a sled regarding the state of a migration, indicating the
/// role of the VMM whose migration state was updated.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct MigrationRuntimeState {
    pub migration_id: Uuid,
    pub state: MigrationState,
    #[serde(rename = "gen")]
    pub generation: Generation,

    /// Timestamp for the migration state update.
    pub time_updated: DateTime<Utc>,
}

/// The state of an instance's live migration.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Deserialize,
    Serialize,
    JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum MigrationState {
    /// The migration has not started for this VMM.
    #[default]
    Pending,
    /// The migration is in progress.
    InProgress,
    /// The migration has failed.
    Failed,
    /// The migration has completed.
    Completed,
}

/// The body of a request to ensure that a instance and VMM are known to a sled
/// agent.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct InstanceEnsureBody {
    /// The virtual hardware configuration this virtual machine should have when
    /// it is started.
    pub vmm_spec: VmmSpec,

    /// Information about the sled-local configuration that needs to be
    /// established to make the VM's virtual hardware fully functional.
    pub local_config: InstanceSledLocalConfig,

    /// The initial VMM runtime state for the VMM being registered.
    pub vmm_runtime: VmmRuntimeState,

    /// The ID of the instance for which this VMM is being created.
    pub instance_id: InstanceUuid,

    /// The ID of the migration in to this VMM, if this VMM is being
    /// ensured is part of a migration in. If this is `None`, the VMM is not
    /// being created due to a migration.
    pub migration_id: Option<Uuid>,

    /// The address at which this VMM should serve a Propolis server API.
    pub propolis_addr: SocketAddr,

    /// Metadata used to track instance statistics.
    pub metadata: InstanceMetadata,
}

/// Describes sled-local configuration that a sled-agent must establish to make
/// the instance's virtual hardware fully functional.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct InstanceSledLocalConfig {
    pub hostname: Hostname,
    pub nics: Vec<NetworkInterface>,
    pub source_nat: SourceNatConfig,
    /// Zero or more external IP addresses (either floating or ephemeral),
    /// provided to an instance to allow inbound connectivity.
    pub ephemeral_ip: Option<IpAddr>,
    pub floating_ips: Vec<IpAddr>,
    pub firewall_rules: Vec<ResolvedVpcFirewallRule>,
    pub dhcp_config: DhcpConfig,
}

/// Metadata used to track statistics about an instance.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct InstanceMetadata {
    pub silo_id: Uuid,
    pub project_id: Uuid,
}

/// Specifies the virtual hardware configuration of a new Propolis VMM in the
/// form of a Propolis instance specification.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct VmmSpec(pub InstanceSpec);

/// VPC firewall rule after object name resolution has been performed by Nexus
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct ResolvedVpcFirewallRule {
    pub status: external::VpcFirewallRuleStatus,
    pub direction: external::VpcFirewallRuleDirection,
    pub targets: Vec<NetworkInterface>,
    pub filter_hosts: Option<HashSet<HostIdentifier>>,
    pub filter_ports: Option<Vec<external::L4PortRange>>,
    pub filter_protocols: Option<Vec<VpcFirewallRuleProtocol>>,
    pub action: external::VpcFirewallRuleAction,
    pub priority: external::VpcFirewallRulePriority,
}

/// The protocols that may be specified in a firewall rule's filter.
//
// This is the version of the enum without `Icmp6`, for versions up through
// `ADD_DUAL_STACK_SHARED_NETWORK_INTERFACES`.
#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type", content = "value")]
pub enum VpcFirewallRuleProtocol {
    Tcp,
    Udp,
    Icmp(Option<external::VpcFirewallIcmpFilter>),
    // TODO: OPTE does not yet permit further L4 protocols. (opte#609)
    // Other(u16),
}

/// The body of a request to move a previously-ensured instance into a specific
/// runtime state.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct VmmPutStateBody {
    /// The state into which the instance should be driven.
    pub state: VmmStateRequested,
}

/// The response sent from a request to move an instance into a specific runtime
/// state.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct VmmPutStateResponse {
    /// The current runtime state of the instance after handling the request to
    /// change its state. If the instance's state did not change, this field is
    /// `None`.
    pub updated_runtime: Option<SledVmmState>,
}

/// Requestable running state of an Instance.
///
/// A subset of [`omicron_common::api::external::InstanceState`].
#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum VmmStateRequested {
    /// Run this instance by migrating in from a previous running incarnation of
    /// the instance.
    MigrationTarget(InstanceMigrationTargetParams),
    /// Start the instance if it is not already running.
    Running,
    /// Stop the instance.
    Stopped,
    /// Immediately reset the instance, as though it had stopped and immediately
    /// began to run again.
    Reboot,
}

/// The response sent from a request to unregister an instance.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct VmmUnregisterResponse {
    /// The current state of the instance after handling the request to
    /// unregister it. If the instance's state did not change, this field is
    /// `None`.
    pub updated_runtime: Option<SledVmmState>,
}

/// Parameters used when directing Propolis to initialize itself via live
/// migration.
#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceMigrationTargetParams {
    /// The address of the Propolis server that will serve as the migration
    /// source.
    pub src_propolis_addr: SocketAddr,
}

/// Used to dynamically update external IPs attached to an instance.
#[derive(
    Copy, Clone, Debug, Eq, PartialEq, Hash, Deserialize, JsonSchema, Serialize,
)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum InstanceExternalIpBody {
    Ephemeral(IpAddr),
    Floating(IpAddr),
}
