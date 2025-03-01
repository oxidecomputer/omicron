// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{
    FAILURE_EXIT_CODE, NEEDS_UPDATE_EXIT_CODE,
    apis::ManagedApis,
    environment::{BlessedSource, Environment, GeneratedSource},
    output::{
        CheckResult, OutputOpts, Styles, display_load_problems,
        display_resolution, headers::*,
    },
    resolved::Resolved,
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

    let (generated, errors) = generated_source.load(&apis, &styles)?;
    display_load_problems(&errors, &styles)?;

    let (local_files, errors) = env.local_source.load(&apis, &styles)?;
    display_load_problems(&errors, &styles)?;

    let (blessed, errors) = blessed_source.load(&apis, &styles)?;
    display_load_problems(&errors, &styles)?;

    let resolved =
        Resolved::new(env, &apis, &blessed, &generated, &local_files);

    eprintln!("{:>HEADER_WIDTH$}", SEPARATOR);
    display_resolution(env, &apis, &resolved, &styles)
}

#[cfg(test)]
mod test {
    use crate::cmd::check::*;
    use crate::cmd::dispatch::*;
    use crate::environment::Environment;
    use std::process::ExitCode;

    #[test]
    fn check_apis_up_to_date() -> Result<ExitCode, anyhow::Error> {
        let env = Environment::new(None)?;
        let blessed_source = BlessedSource::try_from(BlessedSourceArgs {
            blessed_from_git: None,
            blessed_from_dir: None,
        })?;
        let generated_source =
            GeneratedSource::try_from(GeneratedSourceArgs {
                generated_from_dir: None,
            })?;
        let output = OutputOpts { color: clap::ColorChoice::Auto };

        let result =
            check_impl(&env, &blessed_source, &generated_source, &output)?;
        Ok(result.to_exit_code())
    }
}
