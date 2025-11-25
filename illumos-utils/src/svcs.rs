// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for reporting SMF services' status.

use crate::ExecutionError;
use crate::PFEXEC;
use crate::execute_async;
use crate::zone::SVCS;

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::fmt::Display;
use tokio::process::Command;

/// Wraps commands for interacting with interfaces.
pub struct Svcs {}

impl Svcs {
    /// Lists SMF services in maintenance
    // TODO-K: Do not return a string
    // TODO-K: change to not running?
    pub async fn enabled_not_running()
    -> Result<Vec<SvcNotRunning>, ExecutionError> {
        let mut cmd = Command::new(PFEXEC);
        let cmd = cmd.args(&[SVCS, "-Zxv"]);
        let output = execute_async(cmd).await?;
        // TODO-K: handle stderr and acutally parse the output
        SvcNotRunning::parse(&output.stdout)
    }
}

// TODO-K: Write enum for possible states

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
/// Information about an SMF service that is enabled but not running
pub struct SvcNotRunning {
    fmri: String,
    zone: String,
    state: String,
    reason: String,
    impact: String,
    additional_info: Vec<String>,
}

// TODO-K: new struct SvcsNot runnig with a IdOrdMap? Parse fn can lived there

impl SvcNotRunning {
    fn new() -> SvcNotRunning {
        SvcNotRunning {
            fmri: "".to_string(),
            zone: "".to_string(),
            state: "".to_string(),
            reason: "".to_string(),
            impact: "".to_string(),
            additional_info: vec![],
        }
    }
    // TODO-K: Should probably add a logger here to print out the data
    // in case the output is not in the format we expect it to be
    fn parse(data: &[u8]) -> Result<Vec<SvcNotRunning>, ExecutionError> {
        let mut svcs = vec![];
        // TODO-K: handle the case where we get an empty line or whatever
        if data.is_empty() {
            return Ok(svcs);
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
                if let Some((key, value)) = line.split_once(": ") {
                    match key.trim() {
                        "Zone" => current_svc.zone = value.to_string(),
                        // TODO-K: Only add if state is maintenance? Or add any
                        // state and decice on a layer above if we want to only
                        // keep maintenance ones
                        "State" => current_svc.state = value.to_string(),
                        "Reason" => current_svc.reason = value.to_string(),
                        "See" => {
                            current_svc.additional_info.push(value.to_string())
                        }
                        "Impact" => {
                            current_svc.impact = value.to_string();
                            // This should be the last line for each service, add
                            // the service to the vector.
                            svcs.push(current_svc.clone());
                        }
                        // TODO-K: Should this really be an error or should I just log?
                        _ => {
                            return Err(ExecutionError::ParseFailure(format!(
                                "Failed to parse: {}",
                                key
                            )));
                        }
                    }
                }
                // TODO-K: If none, should I log the line if not empty?
            }
        }
        Ok(svcs)
    }
}

impl Display for SvcNotRunning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FMRI: {}", self.fmri)?;
        write!(f, "zone: {}", self.zone)?;
        write!(f, "state: {}", self.state)?;
        write!(f, "reason: {}", self.reason)?;
        for info in &self.additional_info {
            write!(f, "see: {}", info)?;
        }
        write!(f, "impact: {}", self.impact)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_svc_not_running_parse() {
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

        let services = SvcNotRunning::parse(output.as_bytes()).unwrap();

        // We want to make sure we only have two entries
        assert_eq!(services.len(), 2);

        assert_eq!(
            services[0],
            SvcNotRunning {
                fmri: "svc:/site/fake-service:default".to_string(),
                zone: "global".to_string(),
                state: "maintenance since Mon Nov 24 06:57:19 2025".to_string(),
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
                state: "maintenance since Mon Nov 24 05:39:49 2025".to_string(),
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
}
