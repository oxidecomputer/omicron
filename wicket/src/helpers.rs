// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utility functions.

use std::env::VarError;

use anyhow::{bail, Context};
use wicket_common::rack_update::{UpdateSimulatedResult, UpdateTestError};

pub(crate) fn get_update_test_error(
    env_var: &str,
) -> Result<Option<UpdateTestError>, anyhow::Error> {
    // 30 seconds should always be enough to cause a timeout. (The default
    // timeout for progenitor is 15 seconds, and in wicket we set an even
    // shorter timeout.)
    const DEFAULT_TEST_TIMEOUT_SECS: u64 = 30;

    let test_error = match std::env::var(env_var) {
        Ok(v) if v == "fail" => Some(UpdateTestError::Fail),
        Ok(v) if v == "timeout" => {
            Some(UpdateTestError::Timeout { secs: DEFAULT_TEST_TIMEOUT_SECS })
        }
        Ok(v) if v.starts_with("timeout:") => {
            // Extended start_timeout syntax with a custom
            // number of seconds.
            let suffix = v.strip_prefix("timeout:").unwrap();
            match suffix.parse::<u64>() {
                Ok(secs) => Some(UpdateTestError::Timeout { secs }),
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!(
                            "could not parse {env_var} \
                             in the form `timeout:<secs>`: {v}"
                        )
                    });
                }
            }
        }
        Ok(value) => {
            bail!("unrecognized value for {env_var}: {value}");
        }
        Err(VarError::NotPresent) => None,
        Err(VarError::NotUnicode(value)) => {
            bail!("invalid Unicode for {env_var}: {}", value.to_string_lossy());
        }
    };
    Ok(test_error)
}

pub(crate) fn get_update_simulated_result(
    env_var: &str,
) -> Result<Option<UpdateSimulatedResult>, anyhow::Error> {
    let result = match std::env::var(env_var) {
        Ok(v) if v == "success" => Some(UpdateSimulatedResult::Success),
        Ok(v) if v == "warning" => Some(UpdateSimulatedResult::Warning),
        Ok(v) if v == "skipped" => Some(UpdateSimulatedResult::Skipped),
        Ok(v) if v == "failure" => Some(UpdateSimulatedResult::Failure),
        Ok(value) => {
            bail!("unrecognized value for {env_var}: {value}");
        }
        Err(VarError::NotPresent) => None,
        Err(VarError::NotUnicode(value)) => {
            bail!("invalid Unicode for {env_var}: {}", value.to_string_lossy());
        }
    };

    Ok(result)
}
