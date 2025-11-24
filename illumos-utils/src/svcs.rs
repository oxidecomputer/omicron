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
    pub async fn in_maintenance() -> Result<String, ExecutionError> {
        let mut cmd = Command::new(PFEXEC);
        let cmd = cmd.args(&[SVCS, "-Zxv"]);
        let output = execute_async(cmd).await?;
        // TODO-K: handle stderr and acutally parse the output
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
/// Information about an SMF service in maintenance
pub struct SvcInMaintenance {
    fmri: String,
    zone: String,
    state: String,
    reason: String,
    impact: String,
    additional_info: String,
}

impl SvcInMaintenance {
    fn new() -> SvcInMaintenance {
        SvcInMaintenance {
            fmri: "".to_string(),
            zone: "".to_string(),
            state: "".to_string(),
            reason: "".to_string(),
            impact: "".to_string(),
            additional_info: "".to_string(),
        }
    }
    // TODO-K: Should probably add a logger here to print out the data
    // in case the output is not in the format we expect it to be
    fn parse(data: &[u8]) -> Result<Vec<SvcInMaintenance>, ExecutionError> {
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
        let mut current_svc = SvcInMaintenance::new();
        let lines = s.trim().lines();
        for line in lines {
            // TODO-K: Account for the empty line between services
            if line.starts_with("svc:") {
                // This is a new service, wipe the slate clean
                current_svc = SvcInMaintenance::new();
                // TODO-K: perhaps remove the stuff between the parenthesis that
                // is not part of the fmri?
                current_svc.fmri = line.to_string();
            } else {
                // TODO-K: get rid of the unwrap
                let (key, value) = line.split_once(": ").unwrap();
                match key {
                    "Zone" => current_svc.zone = value.to_string(),
                    "State" => current_svc.state = value.to_string(),
                    "Reason" => current_svc.reason = value.to_string(),
                    // TODO-K: I know this is wrong, find a better way
                    "See" => current_svc.additional_info.push_str(value),
                    "Impact" => {
                        current_svc.impact = value.to_string();
                        // This should be the last line for each service, add
                        // the service to the vector.
                        svcs.push(current_svc.clone());
                    }
                    // TODO-K: Should this really be an error or should I just log?
                    _ => return Err(ExecutionError::ParseFailure("OHNO".to_string()))
                }
            }
        }
        Ok(svcs)
    }
}

impl Display for SvcInMaintenance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FMRI: {}", self.fmri)?;
        write!(f, "zone: {}", self.zone)?;
        write!(f, "state: {}", self.state)?;
        write!(f, "reason: {}", self.reason)?;
        write!(f, "impact: {}", self.impact)?;
        write!(f, "see: {}", self.additional_info)
    }
}
