// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{
    apis::ManagedApis,
    environment::{BlessedSource, Environment, GeneratedSource},
    output::{
        display_load_problems, display_resolution, headers::*, CheckResult,
        OutputOpts, Styles,
    },
    resolved::Resolved,
    FAILURE_EXIT_CODE, NEEDS_UPDATE_EXIT_CODE,
};
use std::process::ExitCode;

impl CheckResult {
    pub(crate) fn to_exit_code(self) -> ExitCode {
        match self {
            CheckResult::Success => ExitCode::SUCCESS,
            CheckResult::NeedsUpdate => NEEDS_UPDATE_EXIT_CODE.into(),
            CheckResult::Failures => FAILURE_EXIT_CODE.into(),
        }
    }
}

pub(crate) fn check_impl(
    env: &Environment,
    blessed_source: &BlessedSource,
    generated_source: &GeneratedSource,
    output: &OutputOpts,
) -> anyhow::Result<CheckResult> {
    let mut styles = Styles::default();
    if output.use_color(supports_color::Stream::Stderr) {
        styles.colorize();
    }

    eprintln!("{:>HEADER_WIDTH$}", SEPARATOR);
    let apis = ManagedApis::all()?;
    let generated = generated_source.load(&apis, &styles)?;
    display_load_problems(&generated.warnings, &generated.errors, &styles)?;
    let local_files = env.local_source.load(&apis, &styles)?;
    display_load_problems(&local_files.warnings, &local_files.errors, &styles)?;
    let blessed = blessed_source.load(&apis, &styles)?;
    display_load_problems(&blessed.warnings, &blessed.errors, &styles)?;

    let resolved =
        Resolved::new(env, &apis, &blessed, &generated, &local_files);

    eprintln!("{:>HEADER_WIDTH$}", SEPARATOR);
    display_resolution(env, &apis, &resolved, &styles)
}
