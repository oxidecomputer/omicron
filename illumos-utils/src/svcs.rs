// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for reporting SMF services' status.

use crate::ExecutionError;
#[cfg(target_os = "illumos")]
use crate::PFEXEC;
#[cfg(target_os = "illumos")]
use crate::execute_async;
#[cfg(target_os = "illumos")]
use crate::zone::SVCS;

use chrono::DateTime;
use chrono::NaiveDateTime;
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use slog::Logger;
use slog::{error, info};
use std::fmt::Display;
#[cfg(target_os = "illumos")]
use tokio::process::Command;

/// Wraps commands for interacting with interfaces.
pub struct Svcs {}

impl Svcs {
    /// Lists SMF services that are enabled but not running
    #[cfg(target_os = "illumos")]
    pub async fn in_maintenance(
        log: &Logger,
    ) -> Result<Vec<SvcInMaintenance>, ExecutionError> {
        let mut cmd = Command::new(PFEXEC);
        let cmd = cmd.args(&[SVCS, "-Za", "-H", "-o", "state,fmri,zone"]);
        let output = execute_async(cmd).await?;
        Ok(SvcInMaintenance::parse(log, &output.stdout))
    }

    #[cfg(not(target_os = "illumos"))]
    pub async fn in_maintenance(
        log: &Logger,
    ) -> Result<Vec<SvcInMaintenance>, ExecutionError> {
        info!(log, "OS not illumos, will not check state of SMF services");
        Ok(vec![])
    }
}

/// Each service instance is always in a well-defined state based on its
/// dependencies, the results of the execution of its methods, and its potential
/// contracts events. See <https://illumos.org/man/7/smf> for more information.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum SvcState {
    /// Initial state for all service instances.
    Uninitailized,
    /// The instance is enabled, but not yet running or available to run.
    Offline,
    /// The instance is enabled and running or is available to run.
    Online,
    /// The instance is enabled and running or available to run. It is, however,
    /// functioning at a limited capacity in comparison to normal operation.
    Degraded,
    /// The instance is enabled, but not able to run.
    Maintenance,
    /// The instance is disabled.
    Disabled,
    /// Represents a legacy instance that is not managed by the service
    /// management facility.
    LegacyRun,
    /// We were unable to determine the state of the service instance.
    Unknown,
}

impl From<String> for SvcState {
    fn from(value: String) -> Self {
        match value.as_str() {
            "Uninitailized" | "uninitailized" => SvcState::Uninitailized,
            "Offline" | "offline" => SvcState::Offline,
            "Online" | "online" => SvcState::Online,
            "Degraded" | "degraded" => SvcState::Degraded,
            "Maintenance" | "maintenance" => SvcState::Maintenance,
            "Disabled" | "disabled" => SvcState::Disabled,
            "Legacy Run" | "legacy run" | "Legacy run" | "legacy_run"
            | "legacy-run" => SvcState::LegacyRun,
            _ => SvcState::Unknown,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
/// Information about an SMF service that is enabled but not running
pub struct SvcInMaintenance {
    fmri: String,
    zone: String,
    //state: SvcState,
    //// TODO-K: Do I need a deserialiser like parse_cockroach_cli_timestamp?
    //state_since: Option<DateTime<Utc>>,
    //reason: String,
    //impact: String,
    //additional_info: Vec<String>,
}

impl SvcInMaintenance {
    // These methods are only used when the target OS is "illumos". They are not
    // marked as a configuration option based on target OS because they are not
    // Illumos specific themselves. We mark them as unused instead.
    #[allow(dead_code)]
    // TODO-K: Remove pub?
    pub fn new() -> SvcInMaintenance {
        SvcInMaintenance { fmri: String::new(), zone: String::new() }
    }

    #[allow(dead_code)]
    fn parse(log: &Logger, data: &[u8]) -> Vec<SvcInMaintenance> {
        let mut svcs = vec![];
        if data.is_empty() {
            return svcs;
        }

        // Example of the reponse from running `svcs -Za -H -o state,fmri,zone`
        //
        // legacy_run     lrc:/etc/rc2_d/S20sysetup                          global
        // maintenance    svc:/site/fake-service:default                     global
        // disabled       svc:/network/tcpkey:default                        global
        // disabled       svc:/system/omicron/baseline:default               global
        // online         svc:/milestone/sysconfig:default                   global
        let s = String::from_utf8_lossy(data);
        let lines = s.trim().lines();
        for line in lines {
            let line = line.trim();
            let mut svc = line.split_whitespace();

            if let Some(state) = svc.next() {
                // Only attempt to parse a service that is in maintenance.
                match SvcState::from(state.to_string()) {
                    SvcState::Maintenance => {
                        // This is a new service, wipe the slate clean
                        let mut current_svc = SvcInMaintenance::new();
                        if let Some(fmri) = svc.next() {
                            current_svc.fmri = fmri.to_string()
                        } else {
                            error!(
                                log,
                                "unable to parse; output line missing FMRI: {line}",
                            );
                        }

                        if let Some(zone) = svc.next() {
                            current_svc.zone = zone.to_string()
                        } else {
                            error!(
                                log,
                                "unable to parse; output line missing zone: {line}",
                            );
                        }

                        // We add a service even if we were only partially able to
                        // parse it. If there is something in maintenance we want to
                        // include it in inventory. This means there is something
                        // going on and someone should take a look.
                        svcs.push(current_svc.clone());
                    }
                    // If there is a weird state let's log it.
                    SvcState::Unknown => {
                        info!(
                            log,
                            "output from 'svcs' contains a service with an unknown \
                        state: {}",
                            state
                        )
                    }
                    _ => (),
                }
            }
        }
        svcs
    }
}

impl Display for SvcInMaintenance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let SvcInMaintenance { fmri, zone } = self;

        write!(f, "FMRI: {}", fmri)?;
        write!(f, "zone: {}", zone)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slog::Drain;
    use slog::o;
    use slog_term::FullFormat;
    use slog_term::PlainDecorator;
    use slog_term::TestStdoutWriter;

    fn log() -> slog::Logger {
        let decorator = PlainDecorator::new(TestStdoutWriter);
        let drain = FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        slog::Logger::root(drain, o!())
    }

    #[test]
    fn test_svc_in_maintenance_parse_success() {
        let output = r#"legacy_run     lrc:/etc/rc2_d/S89PRESERVE                         global
maintenance    svc:/site/fake-service:default                     global
disabled       svc:/network/tcpkey:default                        global
maintenance    svc:/system/omicron/baseline:default               global
online         svc:/milestone/sysconfig:default                   global
"#;

        let log = log();
        let services = SvcInMaintenance::parse(&log, output.as_bytes());

        // We want to make sure we only have two entries
        assert_eq!(services.len(), 2);

        assert_eq!(
            services[0],
            SvcInMaintenance {
                fmri: "svc:/site/fake-service:default".to_string(),
                zone: "global".to_string(),
            }
        );

        assert_eq!(
            services[1],
            SvcInMaintenance {
                fmri: "svc:/system/omicron/baseline:default".to_string(),
                zone: "global".to_string(),
            }
        );
    }

    #[test]
    fn test_svc_in_maintenance_none_success() {
        let output = r#"legacy_run     lrc:/etc/rc2_d/S89PRESERVE                         global
online         svc:/site/fake-service:default                     global
disabled       svc:/network/tcpkey:default                        global
online         svc:/system/omicron/baseline:default               global
online         svc:/milestone/sysconfig:default                   global
"#;

        let log = log();
        let services = SvcInMaintenance::parse(&log, output.as_bytes());

        assert_eq!(services.len(), 0);
    }

    #[test]
    fn test_svc_in_maintenance_empty_success() {
        let output = r#""#;

        let log = log();
        let services = SvcInMaintenance::parse(&log, output.as_bytes());

        assert_eq!(services.len(), 0);
    }

    #[test]
    fn test_svc_in_maintenance_parse_unknown_zone_fail() {
        let output = r#"maintenance    svc:/site/fake-service:default
"#;

        let log = log();
        let services = SvcInMaintenance::parse(&log, output.as_bytes());

        // We want to make sure we have an entry even if we're missing the zone
        assert_eq!(services.len(), 1);

        assert_eq!(
            services[0],
            SvcInMaintenance {
                fmri: "svc:/site/fake-service:default".to_string(),
                zone: "".to_string(),
            }
        );
    }

    #[test]
    fn test_svc_in_maintenance_parse_unknown_info_fail() {
        let output = r#"maintenance
"#;

        let log = log();
        let services = SvcInMaintenance::parse(&log, output.as_bytes());

        // We want to make sure we have an entry even if we're missing all information
        assert_eq!(services.len(), 1);

        assert_eq!(
            services[0],
            SvcInMaintenance { fmri: "".to_string(), zone: "".to_string() }
        );
    }

    #[test]
    fn test_svc_in_maintenance_parse_unknown_state_fail() {
        let output = r#"Barnacles!
"#;

        let log = log();
        let services = SvcInMaintenance::parse(&log, output.as_bytes());

        assert_eq!(services.len(), 0);
    }
}
