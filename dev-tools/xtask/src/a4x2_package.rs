// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Subcommand: cargo xtask a4x2-package

use crate::common::run_subcmd;
use anyhow::{Context, Result, ensure};
use camino::{Utf8Path, Utf8PathBuf};
use clap::Parser;
use serde::Deserialize;
use std::fs;
use std::env;
use std::process::{Command, Output, ExitStatus};
use std::ffi::OsStr;

#[derive(Parser)]
pub struct A4x2PackageArgs {
    /// Bundle omicron live tests into the output tarball
    #[clap(long)]
    live_tests: bool,

    /// Bundle omicron end-to-end-tests package into the output tarball
    #[clap(long)]
    end_to_end_tests: bool,

    /// Are we running in CI or not?
    #[clap(long)]
    ci: bool,
}

#[derive(Deserialize, Debug)]
struct NextestConfig {
    nextest_version: NextestConfigVersion,
}

#[derive(Deserialize, Debug)]
struct NextestConfigVersion {
    // required: String,
    recommended: String,
}

pub fn run_cmd(args: A4x2PackageArgs) -> Result<()> {
    // XXX does this make sense as a working directory?
    let work_dir =
        Utf8PathBuf::from(format!("{}/.cache/a4x2-package", &std::env::var("HOME")?));

    // Delete any results from previous runs. We don't mind if there's errors.
    let _ = fs::remove_dir_all(&work_dir);

    // Create work dir
    fs::create_dir_all(&work_dir)?;

    // Clone of omicron that we can modify without causing trouble for anyone
    let omicron_dir = work_dir.join("omicron");
    fs::create_dir_all(&omicron_dir)?;

    // Clone local working tree into work dir. This is nabbed from
    // buildomat-at-home
    {
        let pwd = env::current_dir()?;


        let mut treeish = trim_stdout(
            &Command::new("git")
                .args(["stash", "create"])
                .current_dir(&pwd)
                .succeed_output()?,
        )?;
        if treeish.is_empty() {
            treeish = trim_stdout(
                &Command::new("git")
                    .args(["rev-parse", "HEAD"])
                    .current_dir(&pwd)
                    .succeed_output()?,
            )?;
        }

        let mut cmd = Command::new("git");
        cmd
            .arg("-C")
            .arg(&omicron_dir)
            .arg("init");
        run_subcmd(cmd)?;

        let mut cmd = Command::new("git");
        cmd
            .arg("-C")
            .arg(&omicron_dir)
            .args(["remote", "add", "origin"])
            .arg(&pwd);
        run_subcmd(cmd)?;

        let mut cmd = Command::new("git");
        cmd
            .arg("-C")
            .arg(&omicron_dir)
            .args(["fetch", "origin"])
            .arg(&treeish);
        run_subcmd(cmd)?;

        let mut cmd = Command::new("git");
        cmd
            .arg("-C")
            .arg(&omicron_dir)
            .arg("checkout")
            .arg(&treeish);
        run_subcmd(cmd)?;

        // For it to be useful we need to do this
        let mut cmd = Command::new("./tools/install_builder_prerequisites.sh");
        cmd
            .current_dir(&omicron_dir)
            .arg("-yp");
        run_subcmd(cmd)?;
    }

    // Testbed stuff
    {
        let testbed_dir = work_dir.join("testbed");
        let a4x2_dir = testbed_dir.join("a4x2");
        // Fow now I'm going to clone testbed locally
        let mut clone_testbed_cmd = Command::new("git");
        clone_testbed_cmd
            .args([
                "clone", "https://github.com/oxidecomputer/testbed"
            ])
            .arg(&testbed_dir);
        run_subcmd(clone_testbed_cmd)?;

        let mut a4x2_build_cmd = Command::new("cargo");
        a4x2_build_cmd
            .current_dir(&a4x2_dir)
            .args([
                "build", "--release"
            ]);
        run_subcmd(a4x2_build_cmd)?;

        // XXX this modifies current working directory, which im not thrilled
        // about. in CI this doesn't matter, but outside CI, what should we do?
        // We could try to save the files (but then we need to make sure we
        // always restore them when we fail), or we could copy/clone the omicron
        // dir somewhere else and then work from that copy. buildomat-at-home
        // has logic to do this apparently.
        let mut a4x2_package_cmd = Command::new("./config/build-packages.sh");
            a4x2_package_cmd
                .current_dir(&a4x2_dir)
                .env("OMICRON", &omicron_dir);
        run_subcmd(a4x2_package_cmd)?;
    }

    if args.end_to_end_tests {
        // XXX
        unimplemented!("didnt do this yet");
    }

    // Generate a bundle with the live tests included and ready to go.
    if args.live_tests {
        // We need nextest available, both to generate the test bundle and to
        // include in the output tarball to execute the bundle on a4x2.
        if args.ci {
            // In CI, we need to download nextest
            let nextest_config =
                read_nextest_toml(&Utf8PathBuf::from(".config/nextest.toml"))?;

            // XXX rust this. just copying from build-and-test.sh for now to
            // get going.
            let mut dl_nextest_cmd = Command::new("bash");
            dl_nextest_cmd.args([
                "-c", &format!("curl -sSfL --retry 10 https://get.nexte.st/{}/illumos | gunzip | tar -xvf - -C ~/.cargo/bin", nextest_config.nextest_version.recommended)
            ]);
            run_subcmd(dl_nextest_cmd)?;
        } else {
            // Outside of CI, we let error messages take their course, and it's
            // up to the user to figure it out.
        }

        // Build and generate the live tests bundle
        let cargo =
            std::env::var("CARGO").unwrap_or_else(|_| String::from("cargo"));
        let mut live_tests_cmd = Command::new(&cargo);
        live_tests_cmd.args(["xtask", "live-tests"]);

        // generates
        // - target/live-tests-archive.tgz
        run_subcmd(live_tests_cmd)?;
    }

    println!("hi :)");

    Ok(())
}

fn read_nextest_toml(path: &Utf8Path) -> Result<NextestConfig> {
    let config_str = fs::read_to_string(path)?;
    toml::from_str(&config_str).with_context(|| format!("parse {:?}", path))
}

fn trim_stdout(output: &Output) -> Result<String> {
    Ok(std::str::from_utf8(&output.stdout)?.trim().to_owned())
}

pub(crate) trait CommandExt {
    fn succeed(&mut self) -> Result<()>;
    fn succeed_output(&mut self) -> Result<Output>;
    fn to_string(&self) -> String;
}

impl CommandExt for Command {
    fn succeed(&mut self) -> Result<()> {
        let status = self.status()?;
        check(self, status)
    }

    fn succeed_output(&mut self) -> Result<Output> {
        let output = self.output()?;
        eprintln!("scream {}", String::from_utf8_lossy(&output.stderr));
        check(self, output.status)?;
        Ok(output)
    }

    fn to_string(&self) -> String {
        shell_words::join(
            std::iter::once(self.get_program())
                .chain(self.get_args())
                .map(OsStr::to_string_lossy),
        )
    }
}


fn check(command: &Command, status: ExitStatus) -> Result<()> {
    ensure!(
        status.success(),
        "`{}` failed with {}",
        command.to_string(),
        status
    );
    Ok(())
}
