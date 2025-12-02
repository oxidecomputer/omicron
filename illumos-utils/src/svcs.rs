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
use slog::info;
use std::fmt::Display;
#[cfg(target_os = "illumos")]
use tokio::process::Command;

/// Wraps commands for interacting with interfaces.
pub struct Svcs {}

impl Svcs {
    /// Lists SMF services that are enabled but not running
    #[cfg(target_os = "illumos")]
    pub async fn enabled_not_running(
        log: &Logger,
    ) -> Result<Vec<SvcNotRunning>, ExecutionError> {
        let mut cmd = Command::new(PFEXEC);
        let cmd = cmd.args(&[SVCS, "-Zx"]);
        let output = execute_async(cmd).await?;
        SvcNotRunning::parse(log, &output.stdout)
    }

    #[cfg(not(target_os = "illumos"))]
    pub async fn enabled_not_running(
        log: &Logger,
    ) -> Result<Vec<SvcNotRunning>, ExecutionError> {
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
pub struct SvcNotRunning {
    fmri: String,
    zone: String,
    state: SvcState,
    // TODO-K: Do I need a deserialiser like parse_cockroach_cli_timestamp?
    state_since: Option<DateTime<Utc>>,
    reason: String,
    impact: String,
    additional_info: Vec<String>,
}

impl SvcNotRunning {
    // These methods are only used when the target OS is "illumos". They are not
    // marked as a configuration option based on target OS because they are not
    // Illumos specific themselves. We mark them as unused instead.
    #[allow(dead_code)]
    fn new() -> SvcNotRunning {
        SvcNotRunning {
            fmri: String::new(),
            zone: String::new(),
            state: SvcState::Unknown,
            state_since: None,
            reason: String::new(),
            impact: String::new(),
            additional_info: vec![],
        }
    }

    #[allow(dead_code)]
    fn parse(log: &Logger, data: &[u8]) -> Vec<SvcNotRunning> {
        let mut svcs = vec![];
        if data.is_empty() {
            return svcs;
        }
        // The reponse we get from running `svcs -Zxv` is a free-form text.
        // Example:
        //
        // svc:/site/fake-service:default (?)
        //   Zone: global
        //  State: maintenance since Mon Nov 24 06:57:19 2025
        // Reason: Restarting too quickly.
        //    See: http://illumos.org/msg/SMF-8000-L5
        //    See: /var/svc/log/site-fake-service:default.log
        // Impact: This service is not running.
        //
        // svc:/system/omicron/baseline:default (Omicron brand baseline generation)
        //   Zone: global
        //  State: maintenance since Mon Nov 24 05:39:49 2025
        // Reason: Start method failed repeatedly, last died on Killed (9).
        //    See: http://illumos.org/msg/SMF-8000-KS
        //    See: man -M /usr/share/man -s 7 omicron1
        //    See: /var/svc/log/system-omicron-baseline:default.log
        // Impact: This service is not running.
        let s = String::from_utf8_lossy(data);
        let mut current_svc = SvcNotRunning::new();
        let lines = s.trim().lines();
        for line in lines {
            let line = line.trim();
            if line.starts_with("svc:") {
                // This is a new service, wipe the slate clean
                current_svc = SvcNotRunning::new();
                // We remove the text inside the parenthesis that is not part
                // of the fmri. As we are already checking that the line starts
                // with "svc:" there should be no risk of there being nothing
                // in the line
                if let Some(fmri) = line.split_whitespace().next() {
                    current_svc.fmri = fmri.to_string()
                };
            } else {
                // We don't return errors if data is missing from a line.
                // We want to collect as much information as we can from
                // every service in the response.
                if let Some((key, value)) = line.split_once(": ") {
                    match key.trim() {
                        "Zone" => current_svc.zone = value.to_string(),
                        "State" => {
                            if let Some(state) = value.split_whitespace().next()
                            {
                                current_svc.state =
                                    SvcState::from(state.to_string())
                            };

                            if let Some((_, state_since)) =
                                value.split_once("since ")
                            {
                                let naive = match NaiveDateTime::parse_from_str(
                                    state_since,
                                    "%a %b %d %H:%M:%S %Y",
                                ) {
                                    Ok(t) => t,
                                    Err(e) => {
                                        info!(
                                            log,
                                            "unable to parse service instance \
                                            state-since datetime {}: {}",
                                            state_since,
                                            e
                                        );
                                        continue;
                                    }
                                };

                                current_svc.state_since =
                                    Some(DateTime::from_naive_utc_and_offset(
                                        naive, Utc,
                                    ));
                            }
                        }
                        "Reason" => current_svc.reason = value.to_string(),
                        "See" => {
                            current_svc.additional_info.push(value.to_string())
                        }
                        "Impact" => {
                            current_svc.impact = value.to_string();
                            // This should be the last line for each service
                            // https://github.com/illumos/illumos-gate/blob/master/usr/src/cmd/svc/svcs/explain.c#L2070-L2097
                            //
                            // Push the service to the services vector.
                            svcs.push(current_svc.clone());
                        }
                        _ => {
                            info!(
                                log,
                                "unable to parse key due to unknown format: \
                                {key}"
                            );
                        }
                    }
                } else {
                    if !line.is_empty() {
                        info!(
                            log,
                            "unable to parse line due to unknown format: {line}",
                        );
                    }
                }
            }
        }
        svcs
    }
}

impl Display for SvcNotRunning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let SvcNotRunning {
            fmri,
            zone,
            state,
            state_since,
            reason,
            impact,
            additional_info,
        } = self;

        write!(f, "FMRI: {}", fmri)?;
        write!(f, "zone: {}", zone)?;
        write!(f, "state: {:?}", state)?;
        write!(f, "state since: {:?}", state_since)?;
        write!(f, "reason: {}", reason)?;
        for info in additional_info {
            write!(f, "see: {}", info)?;
        }
        write!(f, "impact: {}", impact)
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
    fn test_svc_not_running_parse_success() {
        let output = r#"svc:/site/fake-service:default
  Zone: global
 State: maintenance since Mon Nov 24 06:57:19 2025
Reason: Restarting too quickly.
   See: http://illumos.org/msg/SMF-8000-L5
   See: /var/svc/log/site-fake-service:default.log
Impact: This service is not running.

svc:/system/omicron/baseline:default (Omicron brand baseline generation)
  Zone: global
 State: maintenance since Mon Nov 24 05:39:49 2025
Reason: Start method failed repeatedly, last died on Killed (9).
   See: http://illumos.org/msg/SMF-8000-KS
   See: man -M /usr/share/man -s 7 omicron1
   See: /var/svc/log/system-omicron-baseline:default.log
Impact: This service is not running."#;

        let log = log();
        let services = SvcNotRunning::parse(&log, output.as_bytes());

        // We want to make sure we only have two entries
        assert_eq!(services.len(), 2);

        assert_eq!(
            services[0],
            SvcNotRunning {
                fmri: "svc:/site/fake-service:default".to_string(),
                zone: "global".to_string(),
                state: SvcState::Maintenance,
                state_since: Some(DateTime::from_naive_utc_and_offset(
                    NaiveDateTime::parse_from_str(
                        "Mon Nov 24 06:57:19 2025",
                        "%a %b %d %H:%M:%S %Y",
                    )
                    .unwrap(),
                    Utc,
                )),
                reason: "Restarting too quickly.".to_string(),
                additional_info: vec![
                    "http://illumos.org/msg/SMF-8000-L5".to_string(),
                    "/var/svc/log/site-fake-service:default.log".to_string(),
                ],
                impact: "This service is not running.".to_string(),
            }
        );

        assert_eq!(
            services[1],
            SvcNotRunning {
                fmri: "svc:/system/omicron/baseline:default".to_string(),
                zone: "global".to_string(),
                state: SvcState::Maintenance,
                state_since: Some(DateTime::from_naive_utc_and_offset(
                    NaiveDateTime::parse_from_str(
                        "Mon Nov 24 05:39:49 2025",
                        "%a %b %d %H:%M:%S %Y",
                    )
                    .unwrap(),
                    Utc,
                )),
                reason:
                    "Start method failed repeatedly, last died on Killed (9)."
                        .to_string(),
                additional_info: vec![
                    "http://illumos.org/msg/SMF-8000-KS".to_string(),
                    "man -M /usr/share/man -s 7 omicron1".to_string(),
                    "/var/svc/log/system-omicron-baseline:default.log"
                        .to_string(),
                ],
                impact: "This service is not running.".to_string(),
            }
        );
    }

    #[test]
    fn test_svc_not_running_parse_state_since_fail() {
        let output = r#"svc:/site/fake-service:default
  Zone: global
 State: maintenance since Mon Not 24 06:57:19 2025
Reason: Restarting too quickly.
   See: http://illumos.org/msg/SMF-8000-L5
   See: /var/svc/log/site-fake-service:default.log
Impact: This service is not running.
"#;

        let log = log();
        let services = SvcNotRunning::parse(&log, output.as_bytes());

        // We want to make sure we have an entry even if we weren't able to
        // parse the timestamp.
        assert_eq!(services.len(), 1);

        assert_eq!(
            services[0],
            SvcNotRunning {
                fmri: "svc:/site/fake-service:default".to_string(),
                zone: "global".to_string(),
                state: SvcState::Maintenance,
                state_since: None,
                reason: "Restarting too quickly.".to_string(),
                additional_info: vec![
                    "http://illumos.org/msg/SMF-8000-L5".to_string(),
                    "/var/svc/log/site-fake-service:default.log".to_string(),
                ],
                impact: "This service is not running.".to_string(),
            }
        );
    }

    #[test]
    fn test_svc_not_running_parse_unknown_format_fail() {
        let output = r#"svc:/site/fake-service:default
  Zone: global
 State: maintenance since Mon Nov 24 06:57:19 2025
Reason: Restarting too quickly.
   See: http://illumos.org/msg/SMF-8000-L5
   See: /var/svc/log/site-fake-service:default.log
   Bob: Barnacles!
   What?
Impact: This service is not running.
"#;

        let log = log();
        let services = SvcNotRunning::parse(&log, output.as_bytes());

        // We want to make sure we have an entry even if we weren't able to
        // parse two lines.
        assert_eq!(services.len(), 1);

        assert_eq!(
            services[0],
            SvcNotRunning {
                fmri: "svc:/site/fake-service:default".to_string(),
                zone: "global".to_string(),
                state: SvcState::Maintenance,
                state_since: Some(DateTime::from_naive_utc_and_offset(
                    NaiveDateTime::parse_from_str(
                        "Mon Nov 24 06:57:19 2025",
                        "%a %b %d %H:%M:%S %Y",
                    )
                    .unwrap(),
                    Utc,
                )),
                reason: "Restarting too quickly.".to_string(),
                additional_info: vec![
                    "http://illumos.org/msg/SMF-8000-L5".to_string(),
                    "/var/svc/log/site-fake-service:default.log".to_string(),
                ],
                impact: "This service is not running.".to_string(),
            }
        );
    }
}
