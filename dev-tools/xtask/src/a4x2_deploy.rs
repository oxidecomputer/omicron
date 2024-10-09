// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Subcommand: cargo xtask a4x2-deploy

// use crate::common::run_subcmd;
use anyhow::{Result, bail};
use camino::{Utf8Path, Utf8PathBuf};
use clap::Parser;
use serde::Deserialize;
use std::env;
use std::fs;
use std::io::{Read, Write};
use walkdir::WalkDir;
use xshell::{cmd, Shell};

#[derive(Parser)]
pub struct A4x2DeployArgs {
    /// Execute omicron live tests
    #[clap(long)]
    live_tests: bool,

    /// Execute omicron end-to-end tests
    #[clap(long)]
    end_to_end_tests: bool,

    /// Are we running in CI or not?
    #[clap(long)]
    ci: bool,
}


pub fn run_cmd(args: A4x2DeployArgs) -> Result<()> {
    let sh = Shell::new()?;

    let cargo =
        std::env::var("CARGO").unwrap_or_else(|_| String::from("cargo"));

    // XXX does this make sense as a working directory?
    let home_dir = Utf8PathBuf::from(std::env::var("HOME")?);
    let work_dir = if args.ci {
        Utf8PathBuf::from("/work/a4x2-deploy")
    } else {
        home_dir.join(".cache/a4x2-deploy")
    };

    // Delete any results from previous runs. We don't mind if there's errors.
    fs::remove_dir_all(&work_dir).ok();

    // Create work dir
    fs::create_dir_all(&work_dir)?;

    // Output. Maybe in CI we want this to be /out
    let out_dir = work_dir.join("a4x2-deploy-out");
    fs::create_dir_all(&out_dir)?;

    // a4x2 dir. will be created by the unpack step
    let a4x2_dir = work_dir.join("a4x2-package-out");

    // Unpack a4x2-package-out.tgz
    {
        cmd!(sh, "banner 'unpack'").run()?;
        let _popdir = sh.push_dir(&work_dir);
        let tgz_path = if args.ci {
            unimplemented!()
        } else {
            home_dir.join(".cache/a4x2-package/a4x2-package-out.tgz")
        };

        if !tgz_path.try_exists()? {
            bail!("a4x2-package bundle does not exist at {}, did you run `cargo xtask a4x2-package`?", tgz_path);
        }

        cmd!(sh, "tar -xvzf {tgz_path}").run()?;

        if !a4x2_dir.try_exists()? {
            bail!("extracting a4x2-package bundle did not result in a4x2-package-out/ existing");
        }
    }

    // Download a4x2 deps
    // XXX these don't need to be run every time on dev machines and
    // should be gated behind a flag, probably. but not --ci, because it is a
    // handy shortcut to be able to do this when you need to update the things
    // it sets up (or set them up for the first time).
    {
        cmd!(sh, "banner 'prepare'").run()?;

        // XXX necessary in CI?
        cmd!(sh, "pfexec pkg update").run()?;

        exec_remote_script(&sh, "https://raw.githubusercontent.com/oxidecomputer/falcon/main/setup-base-images.sh")?;
        exec_remote_script(&sh, "https://raw.githubusercontent.com/oxidecomputer/falcon/main/get-ovmf.sh")?;

        cmd!(sh, "bash {a4x2_dir}/config/install-arista-image.sh").run()?;
    }

    if args.end_to_end_tests {
        cmd!(sh, "banner 'end to end'").run()?;
    }

    if args.live_tests {
        cmd!(sh, "banner 'live tests'").run()?;
    }

    Ok(())
}


/// effectively curl | bash, but reads the full script before running bash
fn exec_remote_script(sh: &Shell, url: &str) -> Result<()> {
    // this could use reqwest instead honestly
    let script = cmd!(sh, "curl -sSfL --retry 10 {url}").read()?;
    cmd!(sh, "bash")
        .stdin(&script)
        .run()?;
    Ok(())
}