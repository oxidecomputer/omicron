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
    /// Lists SMF services that are enabled but not online
    #[cfg(target_os = "illumos")]
    pub async fn enabled_not_online(
        log: &Logger,
    ) -> Result<SvcsResult, ExecutionError> {
        let mut cmd = Command::new(PFEXEC);
        let cmd = cmd.args(&[SVCS, "-Za", "-H", "-o", "state,fmri,zone"]);
        info!(log, "Retrieving SMF services");
        let output = execute_async(cmd).await?;
        let svcs_result =
            SvcsResult::parse(log, &output.stdout).retain_enabled_not_online();
        info!(log, "Successfully retrieved SMF services");
        Ok(svcs_result)
    }

    #[cfg(not(target_os = "illumos"))]
    pub async fn enabled_not_online(
        log: &Logger,
    ) -> Result<SvcsResult, ExecutionError> {
        info!(log, "OS not illumos, will not check state of SMF services");
        let svcs_result = SvcsResult::new();
        Ok(svcs_result)
    }
}

/// Lists services if any, and the time the sample was collected
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct SvcsResult {
    pub services: Vec<Svc>,
    pub errors: Vec<String>,
    pub time_of_status: DateTime<Utc>,
}

impl SvcsResult {
    pub fn new() -> Self {
        Self { services: vec![], errors: vec![], time_of_status: Utc::now() }
    }

    #[cfg_attr(not(target_os = "illumos"), allow(dead_code))]
    fn parse(log: &Logger, data: &[u8]) -> Self {
        let mut services = vec![];
        let mut errors = vec![];
        if data.is_empty() {
            return Self { services, errors, time_of_status: Utc::now() };
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

            if let Some(svc_state) = svc.next() {
                // Only parse services that are in a known SMF service state.
                let state = SvcState::from(svc_state.to_string());
                match &state {
                    SvcState::Maintenance
                    | SvcState::Degraded
                    | SvcState::LegacyRun
                    | SvcState::Disabled
                    | SvcState::Offline
                    | SvcState::Online
                    | SvcState::Uninitialized => {
                        let fmri = if let Some(fmri) = svc.next() {
                            fmri.to_string()
                        } else {
                            errors.push(format!(
                                "Unexpected output line: {line}"
                            ));
                            error!(
                                log,
                                "unable to parse; output line missing FMRI:";
                                "line" => line,
                            );
                            continue;
                        };

                        let zone = if let Some(zone) = svc.next() {
                            zone.to_string()
                        } else {
                            errors.push(format!(
                                "Unexpected output line: {line}"
                            ));
                            error!(
                                log,
                                "unable to parse; output line missing zone:";
                                "line" => line,
                            );
                            continue;
                        };

                        services.push(Svc { fmri, zone, state });
                    }
                    // If there is a weird state let's log it.
                    SvcState::Unknown => {
                        errors.push(format!(
                            "Found a service with an unknown state: {line}"
                        ));
                        info!(
                            log,
                            "output from 'svcs' contains a service with an \
                            unknown state: {state}",
                        )
                    }
                }
            }
        }
        Self { services, errors, time_of_status: Utc::now() }
    }

    // This small method is only a wrapper for retaining the services that are
    // in a state that is enabled but not online. It is currently not possible
    // to test the `enabled_not_online()` method directly because it runs the
    // `svcs` command on the machine it is running on, so we split this small
    // part out to test that we are retaining the correct services
    #[cfg_attr(not(target_os = "illumos"), allow(dead_code))]
    fn retain_enabled_not_online(mut self) -> Self {
        self.services.retain(|svc| {
            // legacy_run is included here because this state doesn't really say
            // anythging about whether a service is running or not. It just
            // states that this is a service that isn't managed by SMF
            !matches!(
                svc.state,
                SvcState::Online | SvcState::Disabled | SvcState::LegacyRun
            )
        });
        self
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
    Uninitialized,
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
            "uninitialized" => SvcState::Uninitialized,
            "offline" => SvcState::Offline,
            "online" => SvcState::Online,
            "degraded" => SvcState::Degraded,
            "maintenance" => SvcState::Maintenance,
            "disabled" => SvcState::Disabled,
            "legacy_run" => SvcState::LegacyRun,
            _ => SvcState::Unknown,
        }
    }
}

impl Display for SvcState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = match self {
            SvcState::Uninitialized => "uninitialized",
            SvcState::Offline => "offline",
            SvcState::Online => "online",
            SvcState::Degraded => "degraded",
            SvcState::Maintenance => "maintenance",
            SvcState::Disabled => "disabled",
            SvcState::LegacyRun => "legacy_run",
            SvcState::Unknown => "unknown",
        };

        write!(f, "{state}")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
/// Information about an SMF service that is enabled but not running
pub struct Svc {
    fmri: String,
    zone: String,
    state: SvcState,
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
    fn test_svc_parse_success() {
        let output = r#"online         svc:/milestone/sysconfig:default                   global
offline        svc:/system/manifest-import:default              global
maintenance    svc:/site/fake-service:default                   global
disabled       svc:/network/tcpkey:default                      global
degraded       svc:/system/filesystem/minimal:default           global
legacy_run     lrc:/etc/rc2_d/S89PRESERVE                       global
uninitialized  svc:/system/early-manifest-import:default        global
"#;

        let log = log();
        let result = SvcsResult::parse(&log, output.as_bytes());

        assert_eq!(result.services.len(), 7);
        assert_eq!(result.errors.len(), 0);
        assert_eq!(
            result.services[0],
            Svc {
                fmri: "svc:/milestone/sysconfig:default".to_string(),
                zone: "global".to_string(),
                state: SvcState::Online,
            }
        );
        assert_eq!(
            result.services[1],
            Svc {
                fmri: "svc:/system/manifest-import:default".to_string(),
                zone: "global".to_string(),
                state: SvcState::Offline,
            }
        );
        assert_eq!(
            result.services[2],
            Svc {
                fmri: "svc:/site/fake-service:default".to_string(),
                zone: "global".to_string(),
                state: SvcState::Maintenance,
            }
        );
        assert_eq!(
            result.services[3],
            Svc {
                fmri: "svc:/network/tcpkey:default".to_string(),
                zone: "global".to_string(),
                state: SvcState::Disabled,
            }
        );
        assert_eq!(
            result.services[4],
            Svc {
                fmri: "svc:/system/filesystem/minimal:default".to_string(),
                zone: "global".to_string(),
                state: SvcState::Degraded,
            }
        );
        assert_eq!(
            result.services[5],
            Svc {
                fmri: "lrc:/etc/rc2_d/S89PRESERVE".to_string(),
                zone: "global".to_string(),
                state: SvcState::LegacyRun,
            }
        );
        assert_eq!(
            result.services[6],
            Svc {
                fmri: "svc:/system/early-manifest-import:default".to_string(),
                zone: "global".to_string(),
                state: SvcState::Uninitialized,
            }
        );
    }

    #[test]
    fn test_svc_parse_empty_success() {
        let output = r#""#;

        let log = log();
        let result = SvcsResult::parse(&log, output.as_bytes());

        assert_eq!(result.services.len(), 0);
        assert_eq!(result.errors.len(), 0);
    }

    #[test]
    fn test_svc_parse_no_fmri_fail() {
        let output = r#"online         svc:/milestone/sysconfig:default                   global
online
disabled       svc:/network/tcpkey:default                      global
"#;

        let log = log();
        let result = SvcsResult::parse(&log, output.as_bytes());

        assert_eq!(result.services.len(), 2);
        assert_eq!(
            result.services[0],
            Svc {
                fmri: "svc:/milestone/sysconfig:default".to_string(),
                zone: "global".to_string(),
                state: SvcState::Online,
            }
        );
        assert_eq!(
            result.services[1],
            Svc {
                fmri: "svc:/network/tcpkey:default".to_string(),
                zone: "global".to_string(),
                state: SvcState::Disabled,
            }
        );
        assert_eq!(
            result.errors,
            vec!["Unexpected output line: online".to_string()]
        );
    }

    #[test]
    fn test_svc_parse_no_zone_fail() {
        let output = r#"online         svc:/milestone/sysconfig:default                   global
offline        svc:/system/manifest-import:default
disabled       svc:/network/tcpkey:default                      global
"#;

        let log = log();
        let result = SvcsResult::parse(&log, output.as_bytes());

        assert_eq!(result.services.len(), 2);
        assert_eq!(
            result.services[0],
            Svc {
                fmri: "svc:/milestone/sysconfig:default".to_string(),
                zone: "global".to_string(),
                state: SvcState::Online,
            }
        );
        assert_eq!(
            result.services[1],
            Svc {
                fmri: "svc:/network/tcpkey:default".to_string(),
                zone: "global".to_string(),
                state: SvcState::Disabled,
            }
        );
        assert_eq!(
            result.errors,
            vec![
                "Unexpected output line: offline        svc:/system/manifest-import:default"
                    .to_string()
            ]
        );
    }

    #[test]
    fn test_svc_parse_unknown_state_fail() {
        let output = r#"online         svc:/milestone/sysconfig:default                   global
Barnacles!     svc:/site/fake-service:default                   global
disabled       svc:/network/tcpkey:default                      global
"#;

        let log = log();
        let result = SvcsResult::parse(&log, output.as_bytes());

        assert_eq!(result.services.len(), 2);
        assert_eq!(
            result.services[0],
            Svc {
                fmri: "svc:/milestone/sysconfig:default".to_string(),
                zone: "global".to_string(),
                state: SvcState::Online,
            }
        );
        assert_eq!(
            result.services[1],
            Svc {
                fmri: "svc:/network/tcpkey:default".to_string(),
                zone: "global".to_string(),
                state: SvcState::Disabled,
            }
        );
        assert_eq!(
            result.errors,
            vec![
                "Found a service with an unknown state: Barnacles!     svc:/site/fake-service:default                   global"
                    .to_string()
            ]
        );
    }

    #[test]
    fn test_retain_enabled_not_online() {
        let mk_svc = |i: usize, state: SvcState| Svc {
            fmri: format!("svc:/site/fake-service-{i}:default"),
            zone: "global".to_string(),
            state,
        };

        let services = vec![
            mk_svc(0, SvcState::Online),
            mk_svc(1, SvcState::Online),
            mk_svc(2, SvcState::Offline),
            mk_svc(3, SvcState::Degraded),
            mk_svc(4, SvcState::Disabled),
            mk_svc(5, SvcState::Disabled),
            mk_svc(6, SvcState::LegacyRun),
            mk_svc(7, SvcState::Unknown),
            mk_svc(8, SvcState::Maintenance),
            mk_svc(9, SvcState::Maintenance),
            mk_svc(10, SvcState::Uninitialized),
        ];
        let result = SvcsResult {
            services,
            errors: vec!["some error".to_string()],
            time_of_status: Utc::now(),
        }
        .retain_enabled_not_online();

        assert_eq!(result.errors, vec!["some error".to_string()]);
        assert_eq!(
            result.services,
            vec![
                mk_svc(2, SvcState::Offline),
                mk_svc(3, SvcState::Degraded),
                mk_svc(7, SvcState::Unknown),
                mk_svc(8, SvcState::Maintenance),
                mk_svc(9, SvcState::Maintenance),
                mk_svc(10, SvcState::Uninitialized),
            ]
        );
    }
}
