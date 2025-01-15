// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{
    cmd::output::{OutputOpts, Styles},
    spec::Environment,
    FAILURE_EXIT_CODE, NEEDS_UPDATE_EXIT_CODE,
};
use anyhow::Result;
use std::process::ExitCode;

#[derive(Clone, Copy, Debug)]
pub(crate) enum NewCheckResult {
    Success,
    NeedsUpdate,
    Failures,
}

impl NewCheckResult {
    pub(crate) fn to_exit_code(self) -> ExitCode {
        match self {
            NewCheckResult::Success => ExitCode::SUCCESS,
            NewCheckResult::NeedsUpdate => NEEDS_UPDATE_EXIT_CODE.into(),
            NewCheckResult::Failures => FAILURE_EXIT_CODE.into(),
        }
    }
}

pub(crate) fn new_check_impl(
    env: &Environment,
    output: &OutputOpts,
) -> Result<NewCheckResult> {
    let mut styles = Styles::default();
    if output.use_color(supports_color::Stream::Stderr) {
        styles.colorize();
    }

    let (combined, warnings) =
        crate::combined::CombinedApis::load(&env.openapi_dir)?;
    for w in warnings {
        eprintln!("warning: {:#}", w);
    }
    let problems = combined.problems();
    if !problems.is_empty() {
        eprintln!("FOUND PROBLEMS: {}", problems.len());
        for p in combined.problems() {
            eprintln!("problem: {}", p);
        }
        Ok(NewCheckResult::Failures)
    } else {
        eprintln!("no immediate problems found");
        Ok(NewCheckResult::Success)
    }
}
