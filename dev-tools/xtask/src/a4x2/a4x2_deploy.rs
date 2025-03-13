// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Subcommand: cargo xtask a4x2-deploy

use super::cmd;
use anyhow::{Context, Result, bail};
use camino::Utf8PathBuf;
use clap::{Args, Parser, Subcommand};
use fs_err as fs;
use serde::Deserialize;
use std::env;
use std::{thread, time};
use xshell::Shell;

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

/// default path used when none provided on command line
static DEFAULT_A4X2_PKG_PATH: &str = "out/a4x2-package-out.tgz";

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
    /// Start a4x2, deploy a control plane to it, and then leave it running.
    Start(StartArgs),

    /// Stop a4x2 that was previously launched with the `start` subcommand.
    Stop,

    /// Start a4x2, run tests, and then stop a4x2 unless you request otherwise.
    RunTests(RunTestsArgs),

    /// Query the current state of a4x2, including node access information and
    /// whether the control plane is accessible.
    Status,
}

#[derive(Args, Clone)]
pub struct StartArgs {
    /// Path to an a4x2-package bundle generated by a4x2-package xtask.
    #[clap(long, default_value_t = Utf8PathBuf::from(DEFAULT_A4X2_PKG_PATH))]
    package: Utf8PathBuf,
}

#[derive(Args, Clone)]
pub struct RunTestsArgs {
    /// Execute omicron live tests
    #[clap(long)]
    live_tests: bool,

    /// Leave a4x2 running after tests. Useful for investigating test
    /// failures.
    #[clap(long)]
    leave_running: bool,

    /// Path to an a4x2-package bundle generated by a4x2-package xtask.
    #[clap(long, default_value_t = Utf8PathBuf::from(DEFAULT_A4X2_PKG_PATH))]
    package: Utf8PathBuf,
}

struct Environment {
    /// Path to a4x2 package generated by the a4x2-package xtask
    a4x2_package_tar: Utf8PathBuf,

    /// Directory within which we will unpack the a4x2 package and launch a4x2
    work_dir: Utf8PathBuf,

    /// Directory in `work_dir` containing a4x2
    a4x2_dir: Utf8PathBuf,

    /// Directory in `work_dir` where presently nothing actually happens because
    /// we don't have any output artifacts yet lol YYY/XXX
    out_dir: Utf8PathBuf,
}

pub fn run_cmd(args: A4x2DeployArgs) -> Result<()> {
    let sh = Shell::new()?;

    let env = {
        let a4x2_package_tar = match &args.command {
            DeployCommand::Start(args) => args.package.clone(),
            DeployCommand::RunTests(args) => args.package.clone(),

            // Unused by other commands, so harmless to fill in with a default
            _ => Utf8PathBuf::from(DEFAULT_A4X2_PKG_PATH),
        };
        let a4x2_package_tar = a4x2_package_tar.canonicalize_utf8()?;

        let home_dir = Utf8PathBuf::from(env::var("HOME")?);
        let work_dir = home_dir.join(".cache/a4x2-deploy");

        // a4x2 dir. will be created by the unpack step
        let a4x2_dir = work_dir.join("a4x2-package-out");

        // Output. Maybe in CI we want this to be /out
        let out_dir = work_dir.join("a4x2-deploy-out");

        Environment { a4x2_package_tar, work_dir, a4x2_dir, out_dir }
    };

    match args.command {
        DeployCommand::Stop => teardown_a4x2(&sh, &env)?,
        DeployCommand::Status => {
            // TODO: expand?
            print_a4x2_access_info(&sh, &env);
        }
        DeployCommand::Start(_) | DeployCommand::RunTests(_) => {
            // Teardown previous deploy if it exists, before wiping the data
            // for it. If this errors, we assume the deploy doesn't exist
            // and carry on.
            let result = teardown_a4x2(&sh, &env);
            eprintln!("teardown result: {:?}", result);
            eprintln!("continuing regardless of whether there were errors");

            // Delete any results from previous runs. We don't mind if
            // there's errors. This needs to run as root in case there are
            // artifacts owned by root left around from the deploy.
            cmd!(sh, "pfexec rm -rf").arg(&env.work_dir).run()?;

            // Create work dir
            fs::create_dir_all(&env.out_dir)?;
            fs::create_dir_all(&env.work_dir)?;
            sh.change_dir(&env.work_dir);

            unpack_a4x2(&sh, &env)?;
            prepare_to_launch_a4x2(&sh, &env)?;

            // Launch a4x2. This can fail. In fact it fails quite a bit...
            // enough that perhaps it's owed at least one retry? But we do not
            // retry right now.
            //
            // We capture the error so we can tear down a4x2 gracefully. Note
            // that in the case of running tests, we will produce an error if
            // launching a4x2 succeeds but the tests themselves fail.
            let result =
                try_launch_a4x2(&sh, &env).and_then(|_| match &args.command {
                    DeployCommand::RunTests(test_args) => {
                        run_tests(&sh, &env, &test_args)
                    }
                    _ => Ok(()),
                });

            // TODO: Collect evidence!!! Try to retrieve the sled-agent logs
            // (and perhaps others?), to help diagnose a4x2 launch failures, and
            // provide context alongside live tests.

            // We do not tear down if we are just running the start command, or
            // if the user requested to leave a4x2 up after running the tests.
            match args.command {
                DeployCommand::RunTests(RunTestsArgs {
                    leave_running: false,
                    ..
                }) => {
                    // We ignore any error here, because the result we
                    // actually want to produce is whether tests passed
                    //
                    // But also, only tear down if tests succeeded
                    if result.is_ok() {
                        let _ = teardown_a4x2(&sh, &env);
                    } else {
                        print_a4x2_access_info(&sh, &env);
                        println!(
                            "errors occurred. we are NOT stopping a4x2, to avoid destroying evidence"
                        );
                        println!(
                            "Use `cargo xtask a4x2 deploy stop` to stop a4x2 manually"
                        );
                    }
                }

                _ => {
                    // If we are leaving a4x2 up, we ought to be nice and
                    // print some information to the user so they can get
                    // into the system
                    print_a4x2_access_info(&sh, &env);
                }
            }

            // Unwrap the launch/tests error now, if there was one
            // TODO: print something confirming success instead of letting no
            // output here indicate success.
            result?;
        }
    }

    Ok(())
}

fn unpack_a4x2(sh: &Shell, env: &Environment) -> Result<()> {
    cmd!(sh, "banner 'unpack'").run()?;
    let tgz_path = &env.a4x2_package_tar;

    if !tgz_path.try_exists()? {
        bail!(
            "a4x2-package bundle does not exist at {}, did you run `cargo xtask a4x2-package`?",
            tgz_path
        );
    }

    cmd!(sh, "tar -xvzf {tgz_path}").run()?;

    if !env.a4x2_dir.try_exists()? {
        bail!(
            "extracting a4x2-package bundle did not result in a4x2-package-out/ existing"
        );
    }

    let a4x2_dir = &env.a4x2_dir;
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

    // TODO could move these into the a4x2-package bundle.
    // mostly of matter of having a consistent version of propolis.
    exec_remote_script(
        &sh,
        "https://raw.githubusercontent.com/oxidecomputer/falcon/main/get-ovmf.sh",
    )?;
    exec_remote_script(&sh, "https://raw.githubusercontent.com/oxidecomputer/falcon/main/get-propolis.sh")?;

    // Generate an ssh key we will use to log into the sleds.
    cmd!(sh, "ssh-keygen -t ed25519 -N '' -f a4x2-ssh-key").run()?;
    let mut authorized_keys = vec![sh.read_file("a4x2-ssh-key.pub")?];

    // Attempt to collect the user's pubkeys, so they can connect more easily.
    // Because this is purely a convenience, we should not generate any errors
    // during this step. We don't need the user's pubkeys to proceed, so the
    // state of their .ssh folder should not be load-bearing.
    env::var("HOME")
        .ok()
        .and_then(|home_dir| {
            // Attempt to traverse .ssh directory
            let home_dir = Utf8PathBuf::from(home_dir);
            fs::read_dir(home_dir.join(".ssh")).ok()
        })
        .map(|dir_ents| {
            let user_keys = dir_ents
                .filter_map(|ent| {
                    // Collect contents of `.pub` files within .ssh directory.
                    // Ignore files that aren't pubkeys, unreadable files,
                    // non-utf8 files.
                    ent.ok()
                        .and_then(|ent| {
                            ent.file_name().into_string().ok().and_then(
                                |fname| {
                                    if fname.ends_with(".pub") {
                                        return Some(ent.path());
                                    }
                                    None
                                },
                            )
                        })
                        .and_then(|path| fs::read(path).ok())
                        .and_then(|contents| String::from_utf8(contents).ok())
                })
                .filter(|key| {
                    // Extra safety to make sure we're only looking at
                    // probably-correctly-formed ssh pubkeys.
                    key.starts_with("ssh-")
                });

            // Add them all to our list of pubkeys
            for key in user_keys {
                authorized_keys.push(key);
            }
        });

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

    cmd!(sh, "pfexec ./a4x2 launch").run()?;

    // XXX Is there a better way to do this?
    let ce_addr_json =
        cmd!(sh, "./a4x2 exec ce 'ip -4 -j addr show enp0s10'").read()?;

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

    cmd!(
        sh,
        "pfexec route -n add {DEFAULT_OMICRON_SUBNET} {customer_edge_addr}"
    )
    .run()?;

    // Not sure how this IP is fixed, but it is
    let api_url = DEFAULT_OMICRON_NEXUS_ADDR;
    println!(
        "polling control plane @ {api_url} for signs of life for up to 25 minutes"
    );

    // Print the date for the logs' benefit
    let _ = cmd!(sh, "date").run();

    // Timeout = (retries / 2) minutes
    // XXX this is an arbitrary timeout. Should it be configurable? Skippable?
    let mut retries = 40;

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

fn run_tests(sh: &Shell, env: &Environment, args: &RunTestsArgs) -> Result<()> {
    // TODO: support running end to end tests
    if args.live_tests {
        cmd!(sh, "banner 'live tests'").run()?;
        let _popdir = sh.push_dir(&env.a4x2_dir);
        let g0ip = get_node_ip(&sh, env, "g0")?;

        let ssh_host = format!("root@{g0ip}");
        let mut ssh_args_owned = vec!["-i", "../a4x2-ssh-key"];
        ssh_args_owned.extend_from_slice(&INSECURE_SSH_ARGS);
        let ssh_args = &ssh_args_owned;
        use super::{
            LIVE_TEST_BUNDLE_DIR, LIVE_TEST_BUNDLE_NAME,
            LIVE_TEST_BUNDLE_SCRIPT,
        };

        cmd!(sh, "scp {ssh_args...} {LIVE_TEST_BUNDLE_NAME} {ssh_host}:/zone/oxz_switch/root/root").run()?;

        // If you want any change in functionality for the test runner, update
        // run-live-tests over in a4x2_package.rs. Don't add it here!
        //
        // The weird replace() is for quote-escaping, as we shove this into a
        // single-quote string right below when creating the script to run over
        // ssh.
        let switch_zone_script = format!(
            r#"
            set -euxo pipefail
            tar xvzf '{LIVE_TEST_BUNDLE_NAME}'
            cd '{LIVE_TEST_BUNDLE_DIR}'
            ./'{LIVE_TEST_BUNDLE_SCRIPT}'
        "#
        )
        .replace("'", "'\"'\"'");

        let remote_script =
            format!("zlogin oxz_switch bash -c '{switch_zone_script}'");

        // Will error if the live tests fail. This is desired.
        cmd!(sh, "ssh {ssh_args...} {ssh_host} {remote_script}").run()?;
    }

    Ok(())
}

fn teardown_a4x2(sh: &Shell, env: &Environment) -> Result<()> {
    let _popdir = sh.push_dir(&env.a4x2_dir);

    // destroy a4x2 stuff
    cmd!(sh, "pfexec ./a4x2 destroy").run()?;

    // destroy the route we added (and any stale ones laying around) Each time
    // we run `route get` we will get one gateway. If multiple routes have been
    // added for this IP (by, say, multiple spinup commands without teardowns in
    // between), then we will need to do this multiple times to fully tear down
    // all of them. But, I don't like unbounded loops, so we will do at max 10,
    // which is a number that seems unlikely enough to reach, to me.
    for _ in 0..10 {
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
            None if not_in_table => {
                break;
            }

            // Some(gateway): positive confirmation that there is a route
            // !not_in_table: negative confirmation that there is a route
            // We have a route to delete, and so we delete it.
            Some(gateway) if !not_in_table => {
                cmd!(
                    sh,
                    "pfexec route -n delete {DEFAULT_OMICRON_SUBNET} {gateway}"
                )
                .run()?;

                break;
            }

            // Anything else is unexpected
            _ => {
                bail!("Unexpected output from route get: `{}`", route);
            }
        }
    }

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

If network setup succeeded, you should also be able to ssh/scp into the sleds as
root. Use the ssh key at:

    {ssh_key_path}

i.e.

    ssh -i {ssh_key_path} root@<ip>

Virtual Sled IP addresses:
- g0: {g0ip}
- g1: {g1ip}
- g2: {g2ip}
- g3: {g3ip}

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
    let ipadm = cmd!(sh, "pfexec ./a4x2 exec {node} ipadm").read()?;
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
    bail!("get_host_ip: could not locate IP for node {node}");
}

/// effectively curl | bash, but reads the full script before running bash
fn exec_remote_script(sh: &Shell, url: &str) -> Result<()> {
    let script = cmd!(
        sh,
        "curl --silent --fail --show-error --location --retry 10 {url}"
    )
    .read()?;
    cmd!(sh, "bash").stdin(&script).run()?;
    Ok(())
}
