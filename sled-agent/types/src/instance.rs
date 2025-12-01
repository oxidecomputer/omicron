// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Common instance-related types.

use std::{
    fmt,
    net::{IpAddr, SocketAddr},
};

use omicron_common::api::{
    external::Hostname,
    internal::{
        nexus::{SledVmmState, VmmRuntimeState},
        shared::{
            DelegatedZvol, DhcpConfig, NetworkInterface,
            ResolvedVpcFirewallRule, SourceNatConfig,
        },
    },
};
use omicron_uuid_kinds::InstanceUuid;
use propolis_client::instance_spec::{
    ComponentV0, CrucibleStorageBackend, FileStorageBackend, SpecKey,
    VirtioNetworkBackend,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_agent_types_migrations as migrations;
use uuid::Uuid;

// These types must be updated when a new version is introduced
pub type InstanceEnsureBody = migrations::latest::InstanceEnsureBody;

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

impl fmt::Display for VmmStateRequested {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.label())
    }
}

impl VmmStateRequested {
    fn label(&self) -> &str {
        match self {
            VmmStateRequested::MigrationTarget(_) => "migrating in",
            VmmStateRequested::Running => "running",
            VmmStateRequested::Stopped => "stopped",
            VmmStateRequested::Reboot => "reboot",
        }
    }

    /// Returns true if the state represents a stopped Instance.
    pub fn is_stopped(&self) -> bool {
        match self {
            VmmStateRequested::MigrationTarget(_) => false,
            VmmStateRequested::Running => false,
            VmmStateRequested::Stopped => true,
            VmmStateRequested::Reboot => false,
        }
    }
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
