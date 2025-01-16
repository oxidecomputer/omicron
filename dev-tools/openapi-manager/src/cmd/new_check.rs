// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{
    cmd::output::{headers, OutputOpts, Styles},
    combined::{ApiSpecFileWhich, CheckStale},
    spec::Environment,
    FAILURE_EXIT_CODE, NEEDS_UPDATE_EXIT_CODE,
};
use anyhow::{Context, Result};
use owo_colors::OwoColorize;
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
    } else {
        eprintln!("no immediate problems found");
    }

    let mut nstale = 0;
    for api in combined.apis() {
        let api_ident = api.api().ident();

        for api_version in api.versions() {
            let check = api_version.check(env).with_context(|| {
                format!(
                    "checking API {} version {}",
                    api_ident,
                    api_version.version()
                )
            })?;

            // XXX-dap
            print!("API {} version {}: ", api_ident, api_version.version(),);

            if check.total_errors() == 0 {
                println!("{}", headers::FRESH.style(styles.success_header));
                continue;
            }

            println!("{}", headers::STALE.style(styles.failure_header));
            for (which, check_stale) in check.iter_errors() {
                nstale += 1;
                println!(
                    "    {} {}",
                    match which {
                        ApiSpecFileWhich::Openapi => "openapi document",
                        ApiSpecFileWhich::Extra(path) => path.as_str(),
                    },
                    match check_stale {
                        CheckStale::New =>
                            headers::NEW.style(styles.failure_header),
                        CheckStale::Modified { .. } =>
                            headers::MODIFIED.style(styles.failure_header),
                    }
                );
            }
        }
    }

    if !problems.is_empty() {
        Ok(NewCheckResult::Failures)
    } else if nstale > 0 {
        Ok(NewCheckResult::NeedsUpdate)
    } else {
        Ok(NewCheckResult::Success)
    }
}
