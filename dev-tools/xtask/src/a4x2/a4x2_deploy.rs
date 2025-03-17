// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Subcommand: cargo xtask a4x2-deploy

use super::cmd;
use anyhow::{Context, Result, anyhow, bail};
use camino::{Utf8Path, Utf8PathBuf};
use clap::{Args, Parser, Subcommand};
use fs_err as fs;
use serde::Deserialize;
use std::env;
use std::process::{Command, Stdio};
use std::{thread, time};
use xshell::{Cmd, Shell};

/// Args for sshing in without checking/storing the remote host key. Every time
/// we start a4x2, it will have new keys.
static INSECURE_SSH_ARGS: [&str; 8] = [
    "-o",
    "StrictHostKeyChecking no",
    "-o",
    "UserKnownHostsFile /dev/null",
    "-o",
    "GlobalKnownHostsFile /dev/null",
    "-o",
    "LogLevel error",
];

/// This subnet is set by testbed for accessing the control plane. In the future
/// this should be configurable.
static DEFAULT_OMICRON_SUBNET: &str = "198.51.100.0/24";

/// This nexus address is set by testbed for accessing the control plane.
static DEFAULT_OMICRON_NEXUS_ADDR: &str = "198.51.100.23";

#[derive(Parser)]
pub struct A4x2DeployArgs {
    #[command(subcommand)]
    command: DeployCommand,
}

#[derive(Subcommand, Clone)]
pub enum DeployCommand {
    /// Download and install the propolis hypervisor and bootcode. Pre-requisite
    /// for launching a4x2.
    InstallPropolis,

    /// Start a4x2, deploy a control plane to it, and then leave it running.
    /// Destroys any existing a4x2 deployment, unless --ensure is used.
    Start(StartArgs),

    /// Stop a4x2 that was previously launched with the `start` subcommand.
    Stop,

    /// Execute live-tests previously built by `xtask a4x2 package`.
    RunLiveTests,

    /// Collect logs from a4x2, and services within the sleds.
    CollectEvidence,

    /// Query the current state of a4x2, including node access information and
    /// whether the control plane is accessible.
    Status,
}

#[derive(Args, Clone)]
pub struct StartArgs {
    /// Path to an a4x2-package bundle generated by a4x2-package xtask.
    #[clap(long, default_value_t = Utf8PathBuf::from(super::DEFAULT_A4X2_PKG_PATH))]
    package: Utf8PathBuf,

    /// Makes the command idempotent. By default, `start` will destroy any
    /// existing a4x2 deployment for easy development iteration. With --ensure,
    /// the command will *ensure* a4x2 is running, starting it if needed, but
    /// leaving an existing deployment alone.
    #[clap(long)]
    ensure: bool,
}

struct Environment {
    /// Directory within which we will unpack the a4x2 package and launch a4x2
    work_dir: Utf8PathBuf,

    /// Directory in `work_dir` containing a4x2
    a4x2_dir: Utf8PathBuf,

    /// Falcon state directory
    falcon_dir: Utf8PathBuf,

    /// Directory in `work_dir` where logs are written by collect-evidence
    out_dir: Utf8PathBuf,
}
pub fn run_cmd(args: A4x2DeployArgs) -> Result<()> {
    let sh = Shell::new()?;

    let env = {
        let pwd = Utf8PathBuf::try_from(env::current_dir()?)?;
        let work_dir = pwd.join("target/a4x2/deploy");

        // a4x2 dir. will be created by the unpack step
        let a4x2_dir = work_dir.join(super::A4X2_PACKAGE_DIR_NAME);

        // falcon dir. created by a4x2
        let falcon_dir = a4x2_dir.join(".falcon");

        // Output logs go here
        let out_dir = work_dir.join("output-logs");

        Environment { work_dir, a4x2_dir, falcon_dir, out_dir }
    };

    match args.command {
        DeployCommand::Start(args) => {
            // Teardown previous deploy if it exists, before wiping the data
            // for it.
            if env.falcon_dir.try_exists()? {
                if args.ensure {
                    // ensure arg says: make sure a4x2 is in theory deployed.
                    // right now we will use the presence of .falcon to indicate
                    // that, but that can be an incorrect if the system rebooted
                    // since a4x2 was deployed.
                    eprintln!(".falcon directory exists; assuming a4x2 is up");
                    return Ok(());
                } else {
                    teardown_a4x2(&sh, &env)
                        .context("stopping old a4x2 deployment")?;
                }
            }

            // Delete all state from previous a4x2 invocation, so nothing leaks
            if env.work_dir.try_exists()? {
                fs::remove_dir_all(&env.work_dir)?;
            }

            // Create work dir
            fs::create_dir_all(&env.work_dir)?;
            sh.change_dir(&env.work_dir);

            let a4x2_package_tar = args
                .package
                .canonicalize_utf8()
                .context("canonicalizing a4x2 package path")?;

            unpack_a4x2(&sh, &env, &a4x2_package_tar)
                .context("unpacking packaged a4x2")?;
            prepare_to_launch_a4x2(&sh, &env)
                .context("final cargo bay preparations for a4x2")?;

            // Launch a4x2. This can fail. In fact it fails quite a bit...
            // enough that perhaps it's owed at least one retry? But we do not
            // retry right now.
            //
            // We capture the error so we can tear down a4x2 gracefully. Note
            // that in the case of running tests, we will produce an error if
            // launching a4x2 succeeds but the tests themselves fail.
            try_launch_a4x2(&sh, &env).context("launching a4x2")?;

            // If we are leaving a4x2 up, we ought to be nice and
            // print some information to the user so they can get
            // into the system
            print_a4x2_access_info(&sh, &env);
        }
        DeployCommand::Stop => teardown_a4x2(&sh, &env)?,
        DeployCommand::Status => {
            print_a4x2_access_info(&sh, &env);
        }
        DeployCommand::RunLiveTests => run_live_tests(&sh, &env)?,
        DeployCommand::InstallPropolis => install_propolis(&sh)?,
        DeployCommand::CollectEvidence => collect_evidence(&sh, &env)?,
    }

    Ok(())
}

/// Download an appropriate version of propolis and OVMF code
fn install_propolis(sh: &Shell) -> Result<()> {
    let tmp = sh.create_temp_dir()?;
    let _popdir = sh.push_dir(tmp.path());

    // Download OVMF
    cmd!(sh, "curl")
        .arg("--remote-name")
        .arg("--location")
        .arg("https://oxide-falcon-assets.s3.us-west-2.amazonaws.com/OVMF_CODE.fd")
        .run()?;
    cmd!(sh, "pfexec mkdir -p /var/ovmf").run()?;
    cmd!(sh, "pfexec cp OVMF_CODE.fd /var/ovmf/OVMF_CODE.fd").run()?;

    // Download Propolis
    // XXX falcon `get-propolis.sh` downloads this old version of propolis, but
    // it does not work when running on latest helios as a host. The latest
    // propolis doesn't either, because falcon uses it in a way it doesn't
    // support anymore (?). Right now the correct propolis is in catacomb in
    // `/staff/mike/propolis-server`, which we cannot readily download here.
    //
    // I'm not sure if there's a tracking issue for this, but I will make sure
    // there is one before this PR goes in, and get a stable link so we can
    // automatically download the hotpatched propolis at least. So consider the
    // comment below and printlns NOT FINAL and something that should be
    // resolved BEFORE this code is merged.
    //
    // This is the code we *would* run, and will, once we have a URL to a
    // working propolis:
    // cmd!(sh, "curl")
    //     .arg("--remote-name")
    //     .arg("--location")
    //     .arg("https://buildomat.eng.oxide.computer/wg/0/artefact/01J8TPA802A5SHWPY5K05ZH2J3/ucXqZK2nT2qxxgI0vY5iygOCdEwguBoe9fk3cFrm4IOs9TLX/01J8TPAVRJDHEYKPEG07AHG51H/01J8TQ0QB5M510N99RWD7X55HT/propolis-server")
    //     .run()?;
    // cmd!(sh, "chmod +x propolis-server").run()?;
    // cmd!(sh, "pfexec mv propolis-server /usr/bin").run()?;
    //
    // Instead, I am just doing this for now:
    eprintln!("We cannot currently download the correct propolis for you.");
    eprintln!("Please retrieve `/staff/mike/propolis-server` from");
    eprintln!("`catacomb.eng.oxide.computer` and place it in `/usr/bin`.");
    eprintln!();
    eprintln!("scp catacomb.eng.oxide.computer:/staff/mike/propolis-server /tmp/propolis-server");
    eprintln!("chmod +x /tmp/propolis-server");
    eprintln!("pfexec mv /tmp/propolis-server /usr/bin");

    Ok(())
}

fn unpack_a4x2(
    sh: &Shell,
    env: &Environment,
    tgz_path: &Utf8Path,
) -> Result<()> {
    cmd!(sh, "banner 'unpack'").run()?;
    let a4x2_dir = &env.a4x2_dir;

    // Remove previous execution
    if a4x2_dir.try_exists()? {
        fs::remove_dir_all(a4x2_dir)?;
    }

    if !tgz_path.try_exists()? {
        bail!(
            "a4x2-package bundle does not exist at {}, did you run `cargo xtask a4x2 package`?",
            tgz_path
        );
    }

    cmd!(sh, "tar -xvzf {tgz_path}").run()?;

    if !a4x2_dir.try_exists()? {
        bail!(
            "extracting a4x2-package bundle did not result in a4x2-package-out/ existing"
        );
    }

    // sled-common.tar contains files that were deduplicated (by sha256 hash)
    // between the cargo bays. A file is deduplicated only if it is present and
    // the same in *all* bays, so we unconditonally extract to all bays here.

    cmd!(sh, "tar -cf sled-common.tar -C {a4x2_dir}/cargo-bay/sled-common ./")
        .run()?;
    cmd!(sh, "tar -xf sled-common.tar -C {a4x2_dir}/cargo-bay/g0").run()?;
    cmd!(sh, "tar -xf sled-common.tar -C {a4x2_dir}/cargo-bay/g1").run()?;
    cmd!(sh, "tar -xf sled-common.tar -C {a4x2_dir}/cargo-bay/g2").run()?;
    cmd!(sh, "tar -xf sled-common.tar -C {a4x2_dir}/cargo-bay/g3").run()?;

    Ok(())
}

fn prepare_to_launch_a4x2(sh: &Shell, env: &Environment) -> Result<()> {
    cmd!(sh, "banner 'prepare'").run()?;

    // Generate an ssh key we will use to log into the sleds.
    cmd!(sh, "ssh-keygen -t ed25519 -N '' -f a4x2-ssh-key").run()?;
    let mut authorized_keys = vec![sh.read_file("a4x2-ssh-key.pub")?];

    // Attempt to collect the user's pubkeys, so they can connect more easily.
    // Because this is purely a convenience, we should not generate any errors
    // during this step. We don't need the user's pubkeys to proceed, so the
    // state of their .ssh folder should not be load-bearing.
    if let Ok(home_dir) = env::var("HOME") {
        let home_dir = Utf8PathBuf::from(home_dir);

        // Attempt to read authorized_keys- user may not have their pubkeys
        // on this device, if it is a device they typically only remote to.
        if let Ok(keys) =
            fs::read_to_string(home_dir.join(".ssh/authorized_keys"))
        {
            authorized_keys.extend(keys.lines().map(|s| s.to_string()));
        }

        // Attempt to scan .ssh directory for additional pubkeys.
        if let Ok(dir_ents) = fs::read_dir(home_dir.join(".ssh")) {
            // Collect contents of `.pub` files within .ssh directory.
            for ent in dir_ents {
                // Reject unreadable directory entries
                let Ok(ent) = ent else {
                    continue;
                };

                // Reject non-utf8 filenames
                let Ok(fname) = ent.file_name().into_string() else {
                    continue;
                };

                // Allow only .pub files - most likely to be public keys
                if !fname.ends_with(".pub") {
                    continue;
                }

                // Reject unreadable files
                let Ok(key) = fs::read_to_string(ent.path()) else {
                    continue;
                };

                // SSH pubkeys all start with `ssh-`
                if !key.starts_with("ssh-") {
                    continue;
                }

                // At this point we have read a key, we are confident it is a
                // pubkey. Add it to the authorized keys
                authorized_keys.push(key);
            }
        }
    }

    // Write the public keys into the cargo bay
    let authorized_keys = authorized_keys.join("\n");
    sh.write_file(
        env.a4x2_dir.join("cargo-bay/g0/root_authorized_keys"),
        &authorized_keys,
    )?;
    sh.write_file(
        env.a4x2_dir.join("cargo-bay/g1/root_authorized_keys"),
        &authorized_keys,
    )?;
    sh.write_file(
        env.a4x2_dir.join("cargo-bay/g2/root_authorized_keys"),
        &authorized_keys,
    )?;
    sh.write_file(
        env.a4x2_dir.join("cargo-bay/g3/root_authorized_keys"),
        &authorized_keys,
    )?;

    Ok(())
}

fn try_launch_a4x2(sh: &Shell, env: &Environment) -> Result<()> {
    let _popdir = sh.push_dir(&env.a4x2_dir);

    // - First, launch a4x2.
    // - Then chown falcon files to the executing user regardless of whether
    //   a4x2 succeeded, so we are cargo-cleanable, even if a4x2 failed.
    // - Then inspect the result of launching a4x2.
    //
    // This is not ideal circumstances. It would be nice if falcon created files
    // owned by the user running `pfexec`, rather than by root. Revisit this if
    // these files stop being created by root.
    {
        let uid = cmd!(sh, "/usr/bin/id -u").read()?;
        let gid = cmd!(sh, "/usr/bin/id -g").read()?;
        let a4x2_launch_result =
            cmd!(sh, "pfexec ./a4x2 launch").run().context("launching a4x2");

        // these may or may not exist depending on how far along a4x2 got
        // before hitting an error.
        if env.falcon_dir.try_exists()? {
            cmd!(sh, "pfexec chown -R {uid}:{gid} .falcon/").run()?;
        }
        if env.a4x2_dir.join("debug.out").try_exists()? {
            cmd!(sh, "pfexec chown    {uid}:{gid} debug.out").run()?;
        }

        a4x2_launch_result?;
    }

    let ce_addr_json = cmd!(sh, "./a4x2 exec ce 'ip -4 -j addr show enp0s10'")
        .read()
        .context("querying customer edge router's external ip address")?;

    // Translated from this jq query:
    // .[0].addr_info[] | select(.dynamic == true) | .local
    #[derive(Deserialize)]
    struct Link {
        addr_info: Vec<AddrInfo>,
    }
    #[derive(Deserialize)]
    struct AddrInfo {
        dynamic: Option<bool>,
        local: String,
    }
    let ce_addr_info: Vec<Link> = serde_json::from_str(&ce_addr_json)?;
    let customer_edge_addr = &ce_addr_info
        .get(0)
        .with_context(|| {
            format!("customer edge router has no adresses: {ce_addr_json}")
        })?
        .addr_info
        .iter()
        .find(|v| matches!(v.dynamic, Some(true)))
        .with_context(|| {
            format!(
                "customer edge router has no dynamic addresses: {ce_addr_json}"
            )
        })?
        .local;

    // Add a route so we can access the omicron subnet through the customer
    // edge router. This is necessary to be able to reach nexus from outside the
    // virtual machines. Presently this is required, because we use nexus's
    // responses to API requests as an indicator of control plane liveness.
    // If we check liveness through a different method, it may still be good
    // to leave this in, either unconditionally or gated by a command line flag,
    // so the user can interact with the control plane.
    cmd!(
        sh,
        "pfexec route -n add {DEFAULT_OMICRON_SUBNET} {customer_edge_addr}"
    )
    .run()
    .context(
        "adding route to omicron subnet through the customer edge router VM",
    )?;

    let api_url = DEFAULT_OMICRON_NEXUS_ADDR;

    // Timeout = (retries / 2) minutes
    // This is an arbitrary timeout. Should it be configurable? Skippable?
    let mut retries = 40;
    println!(
        "polling control plane @ {api_url} for signs of life for up to {} minutes",
        retries * 30 / 60
    );

    // Print the date for the logs' benefit
    let _ = cmd!(sh, "date").run();

    // The important thing here is to do an HTTP request with timeout to the
    // control plane API endpoint. If the server is up, we'll get the page you'd
    // expect for an unauthenticated user. If not, then the request will either
    // - fail
    // - stall
    //
    // Stalling is why we do a timeout, and we need to handle timeout both at
    // the TCP level and the HTTP level. `curl --max-time 5` will hard-out after
    // 5 seconds no matter what's going on.
    //
    // That 5 seconds is arbitrary, but it's been working well over in
    // rackletteadm, from which this logic is copied.
    while retries > 0
        && cmd!(sh, "curl --silent --fail --show-error --max-time 5 {api_url}")
            .run()
            .is_err()
    {
        let eta = retries * 30 / 60;
        eprintln!("Poll failed. Will poll for {eta} more minutes.");

        retries -= 1;
        thread::sleep(time::Duration::from_secs(25));

        if retries % 5 == 0 {
            let _ = cmd!(sh, "date").run();
        }
    }

    if retries == 0 {
        bail!("timed out waiting for control plane");
    } else {
        eprintln!("control plane is up:");
        let _ = cmd!(sh, "date").run();
    }

    Ok(())
}

fn run_live_tests(sh: &Shell, env: &Environment) -> Result<()> {
    cmd!(sh, "banner 'live tests'").run()?;

    // Ensure a4x2 state directory exists.
    if !env.falcon_dir.try_exists()? {
        bail!(
            ".falcon state directory not found; a4x2 assumed not running. Please start a4x2 with `cargo xtask a4x2 deploy start`"
        );
    }

    let _popdir = sh.push_dir(&env.a4x2_dir);

    // Bring various file path constants into scope for use below
    use super::{
        LIVE_TEST_BUNDLE_DIR, LIVE_TEST_BUNDLE_NAME, LIVE_TEST_BUNDLE_SCRIPT,
    };

    // Send live tests to sled 0
    scp_to_node(
        sh,
        env,
        "g0",
        &Utf8Path::new(LIVE_TEST_BUNDLE_NAME),
        &Utf8Path::new("/zone/oxz_switch/root/root"),
    )?
    .run()?;

    // NOTE! Below we do need to do string-escaping, because we are shuffling
    // some scripts around and pushing them into shells on remote systems

    // This script will execute within the switch zone. If you want any change
    // in functionality for the test runner, update run-live-tests over in
    // a4x2_package.rs. Don't add it here!
    let switch_zone_script = format!(
        r#"
            set -euxo pipefail
            tar xvzf {0}
            cd {1}
            bash ./{2}
        "#,
        escape_shell_arg(LIVE_TEST_BUNDLE_NAME),
        escape_shell_arg(LIVE_TEST_BUNDLE_DIR),
        escape_shell_arg(LIVE_TEST_BUNDLE_SCRIPT),
    );

    // This script will execute within the global zone, and launch the switch
    // zone script.
    let ssh_script =
        format!("zlogin oxz_switch {}", escape_shell_arg(&switch_zone_script));

    // NOTE! No more escaping, we are back to the comfort of
    // std::process::Command from now on.

    // Unpack and execute the live tests
    let ssh_cmd = ssh_into_node(sh, env, "g0")?.arg(ssh_script);
    let live_tests_status = Command::from(ssh_cmd)
        .stdin(Stdio::null())
        .status()?
        .code()
        .with_context(|| "SSH command exited by signal unexpectedly")?;

    match live_tests_status {
        0 => Ok(()),

        // SSH returns exit code 255 if any connection error occurred
        255 => Err(anyhow!(
            "SSH connection to a4x2 failed, no tests were run (exit 255)"
        )),

        // The rest of these are nextest exit codes
        // https://github.com/nextest-rs/nextest/blob/main/nextest-metadata/src/exit_codes.rs

        // TEST_RUN_FAILED
        100 => Err(anyhow!(
            "Test suite completed with some test failures. (exit 100)"
        )),

        // REQUIRED_VERSION_NOT_MET
        92 => Err(anyhow!(
            "Nextest version too old; please update nextest. (exit 92)"
        )),

        // Catch-all for errors we don't expect to encounter.
        e => Err(anyhow!(
            "Unhandled exit code from running live tests. (exit {})",
            e
        )),
    }
}

fn teardown_a4x2(sh: &Shell, env: &Environment) -> Result<()> {
    let _popdir = sh.push_dir(&env.a4x2_dir);

    cmd!(sh, "pfexec ./a4x2 destroy").run()?;

    // Destroy the route we added (and any stale ones laying around).
    //
    // Each time we run `route get` we will get one gateway. If multiple routes
    // have been added for this IP (by, say, multiple spinup commands without
    // teardowns in between), then we will need to do this multiple times to
    // fully tear down all of them. But, I don't like unbounded loops, so we
    // will only make a reasonable number of attempts.
    const MAX_TRIES: usize = 25;
    for _ in 0..MAX_TRIES {
        let mut route_cmd = cmd!(sh, "route -n get {DEFAULT_OMICRON_SUBNET}");

        // We get an error code when there is no route (and thus, nothing for
        // us to delete!)
        route_cmd.set_ignore_status(true);

        let route = route_cmd.read()?;
        let mut gateway: Option<&str> = None;
        let mut not_in_table = false;

        // Output parsing
        for ln in route.lines() {
            // We succesfully confirmed no routes exist anymore.
            if ln.contains("not in table") {
                not_in_table = true;
            }

            // A route exists and we need to delete it
            if ln.contains("gateway") {
                gateway = Some(
                    ln.split_whitespace().nth(1).with_context(|| {
                        format!("teardown_a4x2: could not get gateway for a4x2 route from line `{ln}`")
                    })?
                );
            }
        }

        match gateway {
            // not_in_table: positive confirmation that there are no routes
            // None gateway: negative confirmation that there are no routes
            // This is the goal state: all routes gone and we know it.
            None if not_in_table => return Ok(()),

            // Some(gateway): positive confirmation that there is a route
            // !not_in_table: negative confirmation that there is a route
            // We have a route to delete, and so we delete it.
            Some(gateway) if !not_in_table => {
                cmd!(
                    sh,
                    "pfexec route -n delete {DEFAULT_OMICRON_SUBNET} {gateway}"
                )
                .run()?;

                // No break/return, we want another round in the loop to clear
                // more routes
            }

            // Anything else is unexpected
            _ => {
                bail!("Unexpected output from route get: `{}`", route);
            }
        }
    }

    // It's not really an error to be here, just means the user had a lot of
    // routes. So we will warn, but move on
    eprintln!(
        "Warning: deleted {MAX_TRIES} routes to the omicron subnet, but still more remain. That's a lot of routes!"
    );
    Ok(())
}

/// Collect a4x2 logs, both from the falcon directory and from services within
/// the sleds.
fn collect_evidence(sh: &Shell, env: &Environment) -> Result<()> {
    let out_dir = &env.out_dir;

    // avoid stale logs, one of the worst problems to unknowingly have while
    // debugging
    if out_dir.try_exists()? {
        fs::remove_dir_all(out_dir)?;
    }

    // Collect falcon logs.
    let falcon_out = out_dir.join("falcon");
    fs::create_dir_all(&falcon_out)?;
    for ent in fs::read_dir(&env.falcon_dir)? {
        let ent = ent?;

        let fname = ent.file_name();
        let Some(fname) = fname.to_str() else {
            continue;
        };

        // ignore the PID/PORT files, those are just tracking how to talk to the
        // currently running propolis processes.
        if !fname.ends_with(".pid") && !fname.ends_with(".port") {
            fs::copy(ent.path(), falcon_out.join(fname))?;
        }
    }

    // Each sled gets an output directory for its service logs
    let sleds = [
        ("g0", out_dir.join("g0")),
        ("g1", out_dir.join("g1")),
        ("g2", out_dir.join("g2")),
        ("g3", out_dir.join("g3")),
    ];

    // Collect sled logs
    for (sled, sled_dir) in &sleds {
        // Permit errors when collecting sled logs, so we can lossily collect
        // logs even if some sleds are unreachable
        collect_sled_logs(sh, env, sled, sled_dir).unwrap_or_else(|e| {
            eprintln!("Errors collecting logs from sled {sled}: {e}");
        });
    }

    Ok(())
}

/// Collect logs from the given sled VM, and place them in [sled_dir]
fn collect_sled_logs(
    sh: &Shell,
    env: &Environment,
    sled: &str,
    sled_dir: &Utf8Path,
) -> Result<()> {
    // This script will run in the global zone of each sled VM to collect logs.
    // It generates a tar stream of logs from the system.
    const LOG_COLLECTION_SCRIPT: &str = r#"
        # we are specifically not setting exit-on-error, so we can lossily get
        # logs even if the presence of errors
        set -x

        # generate file list of logs. each command in this subshell should print
        # file paths to stdout
        (
            # global zone logs.
            svcs -L sled-agent mg-ddm

            # zone service logs
            /opt/oxide/oxlog/oxlog zones \
                | xargs -n1 /opt/oxide/oxlog/oxlog logs \
                    --current --archived --extra
        ) > /tmp/list-of-logs.txt

        # tar all the files listed, stream tarfile to stdout
        # don't use gzip, because we can't stream decompress it
        #   (https://www.illumos.org/issues/15228)
        # should not matter since the transfer is local anyway.
        tar cEf - -I /tmp/list-of-logs.txt
    "#;

    fs::create_dir_all(sled_dir)?;

    // Run tar on remote sled
    let ssh_cmd = ssh_into_node(sh, env, sled)?.arg(LOG_COLLECTION_SCRIPT);
    let mut ssh_cmd = Command::from(ssh_cmd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .spawn()
        .context("executing log collection script")?;

    // should be infallible because we set to piped above
    let tar_stream = ssh_cmd.stdout.take().unwrap();

    // Unpack tar locally
    Command::from(cmd!(sh, "tar xf - -C {sled_dir}"))
        .stdin(Stdio::from(tar_stream))
        .stdout(Stdio::null())
        .status()
        .map_err(|err| anyhow!(err))
        .and_then(|status| {
            let code = status.code().unwrap();
            if code == 0 {
                Ok(())
            } else {
                Err(anyhow!("tar failed: {}", code))
            }
        })
        .context("unpacking logs locally")?;
    ssh_cmd.wait()?;

    Ok(())
}

fn print_a4x2_access_info(sh: &Shell, env: &Environment) {
    let a4x2_dir = &env.a4x2_dir;

    // This is best effort. If we can't get the node IPs, it's up to the reader
    // to decide on a course of action. Knowing that we can't get the node IPs
    // is itself useful information, and the stderr from trying should be
    // visible in the scrollback.
    let g0ip = get_node_ip(sh, env, "g0").unwrap_or("Unknown!".to_string());
    let g1ip = get_node_ip(sh, env, "g1").unwrap_or("Unknown!".to_string());
    let g2ip = get_node_ip(sh, env, "g2").unwrap_or("Unknown!".to_string());
    let g3ip = get_node_ip(sh, env, "g3").unwrap_or("Unknown!".to_string());

    let ssh_key_path = env.work_dir.join("a4x2-ssh-key");

    // pour one out for enjoyers of clean indentation
    println!(
        r#"=== A4x2 Access Information ===

The following commands can be run from the a4x2 workdir:

    Connect to a virtual sled's serial console:
    ./a4x2 serial <g0|g1|g2|g3>

    Run a command on a sled, over the serial console:
    ./a4x2 exec <g0|g1|g2|g3> 'command'

Consult ./a4x2 --help for additional a4x2 functionality.

NOTE: you *MUST* `cd` into the a4x2 workdir before using a4x2.

a4x2 workdir: {a4x2_dir}

---

If network setup succeeded, you should be able to ssh/scp into the sleds as
root. Use the ssh key at:

    {ssh_key_path}

i.e.

    ssh -i {ssh_key_path} root@<ip>

We have also attempted to add your pubkeys from ~/.ssh, so you may be able to
connect in without specifying an ssh key.

Virtual Sled IP addresses:
- g0: {g0ip}
- g1: {g1ip}
- g2: {g2ip}
- g3: {g3ip}

Control plane IP address:
- {DEFAULT_OMICRON_NEXUS_ADDR}

"#
    );
}

/// Get the IP address of a node, so we can connect or ssh into it
fn get_node_ip(sh: &Shell, env: &Environment, node: &str) -> Result<String> {
    let _popdir = sh.push_dir(&env.a4x2_dir);

    // We are parsing output that looks like this:
    // > pfexec ./a4x2 exec g0 ipadm
    // ADDROBJ           TYPE     STATE        ADDR
    // lo0/v4            static   ok           127.0.0.1/8
    // vioif3/v4         dhcp     ok           172.16.254.203/24
    // lo0/v6            static   ok           ::1/128
    // vioif1/ll         addrconf ok           fe80::aa40:25ff:fe00:1%vioif1/10
    // vioif2/ll         addrconf ok           fe80::aa40:25ff:fe00:2%vioif2/10
    // bootstrap0/ll     addrconf ok           fe80::8:20ff:fe05:1288%bootstrap0/10
    // bootstrap0/bootstrap6 static ok         fdb0:a840:2500:1::1/64
    // underlay0/ll      addrconf ok           fe80::8:20ff:fe02:9bb3%underlay0/10
    // underlay0/sled6   static   ok           fd00:1122:3344:101::1/64
    // underlay0/internaldns0 static ok        fd00:1122:3344:1::2/64
    //
    // We are trying to extract the IPv4 address for the DHCP-allocated IP
    // address, in this case "vioif3/v4". This is the IP address on the user's
    // local LAN, accessible from the machine running a4x2, and potentially
    // other devices on the same local network.

    let ipadm = cmd!(sh, "pfexec ./a4x2 exec {node} ipadm")
        .read()
        .with_context(|| format!("Failed to execute ipadm on {node}. a4x2 may not be running. If you rebooted since the last time you launched a4x2, please relaunch it."))?;
    for ln in ipadm.lines() {
        // Match the dhcp line
        if ln.contains("dhcp") {
            // Take the "ADDR" column
            let ipv4 = ln.split_whitespace()
                .nth(3)
                .with_context(|| format!("get_host_ip: could not extract IP for node {node} from line {ln}"))?;

            // Strip the subnet suffix by splitting on the "/" and taking
            // everything to the left of it.
            let ipv4 = ipv4.split("/")
                .nth(0)
                .with_context(|| format!("get_host_ip: could not extract IP for node {node} from line {ln}"))?;

            // Return the address since we have now matched the line
            return Ok(ipv4.to_string());
        }
    }

    // Loop above here will return early if we successfully found the IP
    bail!("get_host_ip: could not locate IP for node {node}.");
}

fn ssh_into_node<'a>(
    sh: &'a Shell,
    env: &'a Environment,
    node: &str,
) -> Result<Cmd<'a>> {
    let ssh_key = env.work_dir.join("a4x2-ssh-key");
    let ip = get_node_ip(&sh, env, node)?;
    let ssh_host = format!("root@{ip}");

    Ok(cmd!(sh, "ssh -i {ssh_key} {INSECURE_SSH_ARGS...} {ssh_host}"))
}

fn scp_to_node<'a>(
    sh: &'a Shell,
    env: &'a Environment,
    node: &str,
    local_path: &'a Utf8Path,
    remote_path: &'a Utf8Path,
) -> Result<Cmd<'a>> {
    let ssh_key = env.work_dir.join("a4x2-ssh-key");
    let ip = get_node_ip(&sh, env, node)?;
    let ssh_host = format!("root@{ip}");

    Ok(cmd!(
        sh,
        "scp -i {ssh_key} {INSECURE_SSH_ARGS...} {local_path} {ssh_host}:{remote_path}"
    ))
}

/// Escape a string so it will be interpreted as a single argument when placed
/// into a shell script.
fn escape_shell_arg(s: &str) -> String {
    // The escape method we use here is very simple, but like many shell things,
    // looks confusing.
    //
    // Single-quote strings in shells are literal strings. There are *no*
    // escape characters, no ways of terminating the string, except for the
    // final terminating right-hand single quote. This means any character can
    // be safely placed into a single quote string- except single quotes.
    //
    // To escape single quotes, we use the sequence:
    //     '"'"'
    // This reads as
    // - terminate previous single-quote string.
    // - create a double-quote string, containing one single-quote.
    // - open a new single-quote string.
    //
    // Adjacent strings that are not separated by spaces are considered part of
    // the same word during tokenization.
    //
    // This is portable across POSIX-compliant shells and can be nested.

    // 1. Escape single-quotes within the string
    let s = s.replace("'", "'\"'\"'");

    // 2. Enclose the string within single-quotes
    let s = ["'", &s, "'"].join("");

    s
}
