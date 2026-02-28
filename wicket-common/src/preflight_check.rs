// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::net::IpAddr;

use dpd_client::types::PortId;
use oxide_update_engine_types::spec::AsError;
use oxide_update_engine_types::spec::EngineSpec;
use oxnet::IpNet;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum UplinkPreflightTerminalError {
    #[error("invalid port name: {0}")]
    InvalidPortName(String),
    #[error("failed to connect to dpd to check for current configuration")]
    GetCurrentConfig(#[source] DpdError),
    #[error("uplink already configured - is rack already initialized?")]
    UplinkAlreadyConfigured,
    #[error("failed to create port {port_id:?}")]
    ConfigurePort {
        #[source]
        err: DpdError,
        port_id: PortId,
    },
    #[error("failed to remove host OS route {destination} -> {nexthop}: {err}")]
    RemoveHostRoute { err: String, destination: IpNet, nexthop: IpAddr },
    #[error("failed to remove uplink SMF property {property:?}: {err}")]
    RemoveSmfProperty { property: String, err: String },
    #[error("failed to refresh uplink service config: {0}")]
    RefreshUplinkSmf(String),
    #[error("failed to clear settings for port {port_id:?}")]
    UnconfigurePort {
        #[source]
        err: DpdError,
        port_id: PortId,
    },
}

impl AsError for UplinkPreflightTerminalError {
    fn as_error(&self) -> &(dyn std::error::Error + 'static) {
        self
    }
}

type DpdError = dpd_client::Error<dpd_client::types::Error>;

#[derive(JsonSchema)]
pub enum UplinkPreflightCheckSpec {}

impl EngineSpec for UplinkPreflightCheckSpec {
    fn spec_name() -> String {
        "UplinkPreflightCheckSpec".to_owned()
    }
    type Component = String;
    type StepId = UplinkPreflightStepId;
    type StepMetadata = ();
    type ProgressMetadata = String;
    type CompletionMetadata = Vec<String>;
    type SkippedMetadata = ();
    type Error = UplinkPreflightTerminalError;

    fn rust_type_info()
    -> Option<oxide_update_engine_types::schema::RustTypeInfo> {
        Some(oxide_update_engine_types::schema::RustTypeInfo {
            crate_name: "wicket-common",
            version: "0.1.0",
            path: "wicket_common::preflight_check\
                   ::UplinkPreflightCheckSpec",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "id", rename_all = "snake_case")]
pub enum UplinkPreflightStepId {
    ConfigureSwitch,
    WaitForL1Link,
    ConfigureAddress,
    ConfigureRouting,
    CheckExternalDnsConnectivity,
    CheckExternalNtpConnectivity,
    CleanupRouting,
    CleanupAddress,
    CleanupL1,
}

oxide_update_engine::define_update_engine!(pub UplinkPreflightCheckSpec);
oxide_update_engine_types::define_update_engine_types!(pub UplinkPreflightCheckSpec);
