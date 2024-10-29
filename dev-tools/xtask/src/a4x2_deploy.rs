// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Subcommand: cargo xtask a4x2-deploy

// use crate::common::run_subcmd;
use anyhow::{anyhow, bail, Result};
use camino::{Utf8Path, Utf8PathBuf};
use clap::Parser;
use serde::Deserialize;
use serde_json::Value;
use std::env;
use std::fs;
use std::io::{Read, Write};
use walkdir::WalkDir;
use xshell::{cmd, Shell};
use std::{thread, time};

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

    // TODO falcon env var for /rpool/falcon
    // let falcon_dataset = Utf8PathBuf::from("rpool/falcon");

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
    sh.change_dir(&work_dir);

    // Output. Maybe in CI we want this to be /out
    let out_dir = work_dir.join("a4x2-deploy-out");
    fs::create_dir_all(&out_dir)?;

    // a4x2 dir. will be created by the unpack step
    let a4x2_dir = work_dir.join("a4x2-package-out");

    // Unpack a4x2-package-out.tgz
    {
        cmd!(sh, "banner 'unpack'").run()?;
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

        // XXX unpack deduped cargo bay
        cmd!(sh, "tar -cf sled-common.tar -C {a4x2_dir}/cargo-bay/sled-common ./").run()?;
        cmd!(sh, "tar -xf sled-common.tar -C {a4x2_dir}/cargo-bay/g0").run()?;
        cmd!(sh, "tar -xf sled-common.tar -C {a4x2_dir}/cargo-bay/g1").run()?;
        cmd!(sh, "tar -xf sled-common.tar -C {a4x2_dir}/cargo-bay/g2").run()?;
        cmd!(sh, "tar -xf sled-common.tar -C {a4x2_dir}/cargo-bay/g3").run()?;
    }

    // Download a4x2 deps
    // XXX these don't need to be run every time on dev machines and
    // should be gated behind a flag, probably. but not --ci, because it is a
    // handy shortcut to be able to do this when you need to update the things
    // it sets up (or set them up for the first time).
    {
        cmd!(sh, "banner 'prepare'").run()?;

        // XXX necessary in CI?
        // note - exit code 4 if no updates available
        // cmd!(sh, "pfexec pkg update").ok();

        // XXX are these necessary?
        exec_remote_script(&sh, "https://raw.githubusercontent.com/oxidecomputer/falcon/main/get-ovmf.sh")?;
        exec_remote_script(&sh, "https://raw.githubusercontent.com/oxidecomputer/falcon/main/get-propolis.sh")?;

        // Generate an ssh key we will use to log into the sleds.
        cmd!(sh, "ssh-keygen -t ed25519 -N '' -f a4x2-ssh-key").run()?;

        // Copy the public side into the cargo bay
        sh.copy_file(
            "a4x2-ssh-key.pub",
            a4x2_dir.join("cargo-bay/g0/root_authorized_keys"),
        )?;
        sh.copy_file(
            "a4x2-ssh-key.pub",
            a4x2_dir.join("cargo-bay/g1/root_authorized_keys"),
        )?;
        sh.copy_file(
            "a4x2-ssh-key.pub",
            a4x2_dir.join("cargo-bay/g2/root_authorized_keys"),
        )?;
        sh.copy_file(
            "a4x2-ssh-key.pub",
            a4x2_dir.join("cargo-bay/g3/root_authorized_keys"),
        )?;
    }

    // Launch a4x2. This can fail. In fact it fails quite a bit... enough that
    // perhaps it's owed at least one retry.
    {
        let _popdir = sh.push_dir(a4x2_dir);
        try_launch_a4x2(&sh)?;
        // XXX teardown, but only outside of CI.
    }

    if args.end_to_end_tests {
        cmd!(sh, "banner 'end to end'").run()?;
    }

    if args.live_tests {
        cmd!(sh, "banner 'live tests'").run()?;
    }

    Ok(())
}

/// launching a4x2 is rather flakey. So we extract out that process here, so we
/// can try it a few times if we need. We return a result indicating whether
/// bringup was:
/// - unambiguously a failure
///   - ex. control plane never came up, we know its a bug
/// - umambiguously a flake
///   - ex. a4x2 VMs never finished launching.
/// - ambiguously a failure
///   - ex. control plane never came up, we dont know why.
///   - ex. sled VMs never finished launching
/// - unambiguously a success
///
/// Ambiguous failures are the ones we may be interested in retrying- but not
/// before collecting any evidence we can about the failure to try to track it
/// down. And additionally, we can decide whether or not we do actually want to
/// retry at all from the side calling this.
///
/// If we don't retry, we can still use this info to categorize our bugs as
/// known or not. Ultimately, all flakes are bugs, it's just a matter of whether
/// we know the cause, and whether it affects prod.
///
/// All failures are ambiguous unless we've written a case to prove one way or
/// another that it wasn't.
fn try_launch_a4x2(sh: &Shell) -> Result<()> {
    cmd!(sh, "pfexec ./a4x2 launch").run()?;

    // XXX I hope there's a better way to do this
    let ce_addr_json =
        cmd!(sh, "./a4x2 exec ce 'ip -4 -j addr show enp0s10'").read()?;

    // Translated from this jq query:
    // .[0].addr_info[] | select(.dynamic == true) | .local
    let ce_addr_json: Value = serde_json::from_str(&ce_addr_json)?;
    let customer_edge_addr = &ce_addr_json[0]["addr_info"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["dynamic"] == Value::Bool(true))
        .ok_or(anyhow!("failed to find customer edge addr"))?["local"]
        .as_str()
        .unwrap();

    // XXX Im told that pinging the gateway from inside the sleds is no longer
    // necessary so I'm leaving it out for now. We'll see if that's true.

    // Add route. XXX store this to tear it down later
    cmd!(sh, "pfexec route add 198.51.100.0/24 {customer_edge_addr}").run()?;


    // Not sure why this IP is fixed
    let api_url = "http://198.51.100.23";
    println!("polling control plane for signs of life for up to 20 minutes");
    cmd!(sh, "date").run()?;

    // XXX timeout this
    let mut retries = 40;
    while retries > 0 && cmd!(sh, "curl -s -m 5 {api_url}").run().is_err() {
        retries -= 1;
        thread::sleep(time::Duration::from_secs(30));
    }

    if retries == 0 {
        bail!("timed out waiting for control plane");
    }


    Ok(())
}

/// effectively curl | bash, but reads the full script before running bash
fn exec_remote_script(sh: &Shell, url: &str) -> Result<()> {
    // this could use reqwest instead honestly
    let script = cmd!(sh, "curl -sSfL --retry 10 {url}").read()?;
    cmd!(sh, "bash").stdin(&script).run()?;
    Ok(())
}
