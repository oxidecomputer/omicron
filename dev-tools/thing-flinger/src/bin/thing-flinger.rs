// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utility for deploying Omicron to remote machines

use omicron_package::{parse, BuildCommand, DeployCommand};

use camino::{Utf8Path, Utf8PathBuf};
use std::collections::{BTreeMap, BTreeSet};
use std::process::Command;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use crossbeam::thread::{self, ScopedJoinHandle};
use serde_derive::Deserialize;
use thiserror::Error;

// A server on which omicron source should be compiled into packages.
#[derive(Deserialize, Debug)]
struct Builder {
    server: String,
    omicron_path: Utf8PathBuf,
}

// A server on which an omicron package is deployed.
#[derive(Deserialize, Debug, Eq, PartialEq)]
struct Server {
    username: String,
    addr: String,
}

#[derive(Deserialize, Debug)]
struct Deployment {
    rss_server: String,
    staging_dir: Utf8PathBuf,
    servers: BTreeSet<String>,
}

#[derive(Debug, Deserialize)]
struct Config {
    omicron_path: Utf8PathBuf,
    builder: Builder,
    servers: BTreeMap<String, Server>,
    deployment: Deployment,

    #[serde(default)]
    rss_config_path: Option<Utf8PathBuf>,

    #[serde(default)]
    debug: bool,
}

impl Config {
    fn release_arg(&self) -> &str {
        if self.debug {
            ""
        } else {
            "--release"
        }
    }

    fn deployment_servers(&self) -> impl Iterator<Item = &Server> {
        self.servers.iter().filter_map(|(name, s)| {
            if self.deployment.servers.contains(name) {
                Some(s)
            } else {
                None
            }
        })
    }
}

fn parse_into_set(src: &str) -> Result<BTreeSet<String>, &'static str> {
    Ok(src.split_whitespace().map(|s| s.to_owned()).collect())
}

#[derive(Debug, Subcommand)]
enum SubCommand {
    /// Run the given command on the given servers, or all servers if none are
    /// specified.
    ///
    /// Be careful!
    Exec {
        /// The command to run
        #[clap(short, long, action)]
        cmd: String,

        /// The servers to run the command on
        #[clap(short, long, value_parser = parse_into_set)]
        servers: Option<BTreeSet<String>>,
    },

    /// Install necessary prerequisites on the "builder" server and all "deploy"
    /// servers.
    InstallPrereqs,

    /// Sync our local source to the build host
    Sync,

    /// Runs a command on the "builder" server.
    #[clap(name = "build", subcommand)]
    Builder(BuildCommand),

    /// Runs a command on all the "deploy" servers.
    #[clap(subcommand)]
    Deploy(DeployCommand),

    /// Create an overlay directory tree for each deployment server
    ///
    /// Each directory tree contains unique files for the given server that will
    /// be populated in the svc/pkg dir.
    ///
    /// This is a separate subcommand so that we can reconstruct overlays
    /// without rebuilding or repackaging.
    Overlay,
}

#[derive(Debug, Parser)]
#[clap(
    name = "thing-flinger",
    about = "A tool for synchronizing packages and configs between machines"
)]
struct Args {
    /// The path to the deployment manifest TOML file
    #[clap(
        short,
        long,
        help = "Path to deployment manifest toml file",
        action
    )]
    config: Utf8PathBuf,

    #[clap(
        short,
        long,
        help = "The name of the build target to use for this command"
    )]
    target: String,

    /// The output directory, where artifacts should be built and staged
    #[clap(long = "artifacts", default_value = "out/")]
    artifact_dir: Utf8PathBuf,

    #[clap(subcommand)]
    subcommand: SubCommand,
}

/// Errors which can be returned when executing subcommands
#[derive(Error, Debug)]
enum FlingError {
    #[error("Servers not listed in configuration: {0:?}")]
    InvalidServers(Vec<String>),

    /// Failed to rsync omicron to build host
    #[error("Failed to sync {src} with {dst}")]
    FailedSync { src: String, dst: String },

    /// The given path must be absolute
    #[error("Path for {field} must be absolute")]
    NotAbsolutePath { field: &'static str },
}

// How should `ssh_exec` be run?
enum SshStrategy {
    // Forward agent and source .profile
    Forward,

    // Don't forward agent, but source .profile
    NoForward,

    // Don't forward agent and don't source .profile
    NoForwardNoProfile,
}

impl SshStrategy {
    fn forward_agent(&self) -> bool {
        match self {
            SshStrategy::Forward => true,
            _ => false,
        }
    }

    fn source_profile(&self) -> bool {
        match self {
            SshStrategy::Forward | &SshStrategy::NoForward => true,
            _ => false,
        }
    }
}

// TODO: run in parallel when that option is given
fn do_exec(
    config: &Config,
    cmd: String,
    servers: Option<BTreeSet<String>>,
) -> Result<()> {
    if let Some(ref servers) = servers {
        validate_servers(servers, &config.servers)?;

        for name in servers {
            let server = &config.servers[name];
            ssh_exec(&server, &cmd, SshStrategy::NoForward)?;
        }
    } else {
        for (_, server) in config.servers.iter() {
            ssh_exec(&server, &cmd, SshStrategy::NoForward)?;
        }
    }
    Ok(())
}

// start an `rsync` command with args common to all our uses
fn rsync_common() -> Command {
    let mut cmd = Command::new("rsync");
    cmd.arg("-az")
        .arg("-e")
        .arg("ssh -o StrictHostKeyChecking=no")
        .arg("--delete")
        .arg("--progress")
        .arg("--out-format")
        .arg("File changed: %o %t %f");
    cmd
}

fn do_sync(config: &Config) -> Result<()> {
    let builder =
        config.servers.get(&config.builder.server).ok_or_else(|| {
            FlingError::InvalidServers(vec![config.builder.server.clone()])
        })?;

    // For rsync to copy from the source appropriately we must guarantee a
    // trailing slash.
    let src = format!(
        "{}/",
        config.omicron_path.canonicalize_utf8().with_context(|| format!(
            "could not canonicalize {}",
            config.omicron_path
        ))?
    );
    let dst = format!(
        "{}@{}:{}",
        builder.username, builder.addr, config.builder.omicron_path
    );

    println!("Synchronizing source files to: {}", dst);
    let mut cmd = rsync_common();

    // exclude build and development environment artifacts
    cmd.arg("--exclude")
        .arg("target/")
        .arg("--exclude")
        .arg("*.vdev")
        .arg("--exclude")
        .arg("*.swp")
        .arg("--exclude")
        .arg(".git/")
        .arg("--exclude")
        .arg("out/");

    // exclude `config-rss.toml`, which needs to be sent to only one target
    // system. we handle this in `do_overlay` below.
    cmd.arg("--exclude").arg("**/config-rss.toml");

    // finish with src/dst
    cmd.arg(&src).arg(&dst);
    let status =
        cmd.status().context(format!("Failed to run command: ({:?})", cmd))?;
    if !status.success() {
        return Err(FlingError::FailedSync { src, dst }.into());
    }

    Ok(())
}

fn copy_to_deployment_staging_dir(
    config: &Config,
    src: String,
    description: &str,
) -> Result<()> {
    let partial_cmd = || {
        let mut cmd = rsync_common();
        cmd.arg("--relative");
        cmd.arg(&src);
        cmd
    };

    // A function for each deployment server to run in parallel
    let fns = config.deployment_servers().map(|server| {
        || {
            let dst = format!(
                "{}@{}:{}",
                server.username, server.addr, config.deployment.staging_dir
            );
            let mut cmd = partial_cmd();
            cmd.arg(&dst);
            let status = cmd
                .status()
                .context(format!("Failed to run command: ({:?})", cmd))?;
            if !status.success() {
                return Err(
                    FlingError::FailedSync { src: src.clone(), dst }.into()
                );
            }
            Ok(())
        }
    });

    let named_fns = config.deployment.servers.iter().zip(fns);
    run_in_parallel(description, named_fns);

    Ok(())
}

fn rsync_config_needed_for_tools(config: &Config) -> Result<()> {
    let src = format!(
        // the `./` here is load-bearing; it interacts with `--relative` to tell
        // rsync to create `smf/sled-agent` but none of its parents
        "{}/./smf/sled-agent/",
        config.omicron_path.canonicalize_utf8().with_context(|| format!(
            "could not canonicalize {}",
            config.omicron_path
        ))?
    );

    copy_to_deployment_staging_dir(config, src, "Copy smf/sled-agent dir")
}

fn rsync_tools_dir_to_deployment_servers(config: &Config) -> Result<()> {
    // we need to rsync `./tools/*` to each of the deployment targets (the
    // "builder" already has it via `do_sync()`), and then run `pfexec
    // tools/install_prerequisites.sh` on each system.
    let src = format!(
        // the `./` here is load-bearing; it interacts with `--relative` to tell
        // rsync to create `tools` but none of its parents
        "{}/./tools/",
        config.omicron_path.canonicalize_utf8().with_context(|| format!(
            "could not canonicalize {}",
            config.omicron_path
        ))?
    );
    copy_to_deployment_staging_dir(config, src, "Copy tools dir")
}

fn do_install_prereqs(config: &Config) -> Result<()> {
    rsync_config_needed_for_tools(config)?;
    rsync_tools_dir_to_deployment_servers(config)?;
    install_rustup_on_deployment_servers(config);
    create_virtual_hardware_on_deployment_servers(config);
    create_external_tls_cert_on_builder(config)?;

    // Create a set of servers to install prereqs to
    let builder = &config.servers[&config.builder.server];
    let build_server = (builder, &config.builder.omicron_path);
    let all_servers = std::iter::once(build_server).chain(
        config.deployment_servers().filter_map(|server| {
            // Don't duplicate the builder
            if server.addr != builder.addr {
                Some((server, &config.deployment.staging_dir))
            } else {
                None
            }
        }),
    );

    let server_names = std::iter::once(&config.builder.server).chain(
        config
            .deployment
            .servers
            .iter()
            .filter(|s| **s != config.builder.server),
    );

    // Install functions to run in parallel on each server
    let fns = all_servers.map(|(server, root_path)| {
        || {
            // -y: assume yes instead of prompting
            // -p: skip check that deps end up in $PATH
            let (script, script_type) = if *server == *builder {
                ("install_builder_prerequisites.sh -y -p", "builder")
            } else {
                ("install_runner_prerequisites.sh -y", "runner")
            };

            let cmd = format!(
                "cd {} && mkdir -p out && pfexec ./tools/{}",
                root_path.clone(),
                script
            );
            println!(
                "Install {} prerequisites on {}",
                script_type, server.addr
            );
            ssh_exec(server, &cmd, SshStrategy::NoForward)
        }
    });

    let named_fns = server_names.zip(fns);
    run_in_parallel("Install prerequisites", named_fns);

    Ok(())
}

fn create_external_tls_cert_on_builder(config: &Config) -> Result<()> {
    let builder = &config.servers[&config.builder.server];
    let cmd = format!(
        "cd {} && ./tools/create_self_signed_cert.sh",
        config.builder.omicron_path,
    );
    ssh_exec(&builder, &cmd, SshStrategy::NoForward)
}

fn create_virtual_hardware_on_deployment_servers(config: &Config) {
    let cmd = format!(
        "cd {} && pfexec ./tools/create_virtual_hardware.sh",
        config.deployment.staging_dir
    );
    let fns = config.deployment_servers().map(|server| {
        || {
            println!("Create virtual hardware on {}", server.addr);
            ssh_exec(server, &cmd, SshStrategy::NoForward)
        }
    });

    let named_fns = config.deployment.servers.iter().zip(fns);
    run_in_parallel("Create virtual hardware", named_fns);
}

fn install_rustup_on_deployment_servers(config: &Config) {
    let cmd = "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y";
    let fns = config.deployment_servers().map(|server| {
        || ssh_exec(server, cmd, SshStrategy::NoForwardNoProfile)
    });

    let named_fns = config.deployment.servers.iter().zip(fns);
    run_in_parallel("Install rustup", named_fns);
}

// Build omicron-package and omicron-deploy on the builder
//
// We need to build omicron-deploy for overlay file generation
fn do_build_minimal(config: &Config) -> Result<()> {
    let server = &config.servers[&config.builder.server];
    let cmd = format!(
        "cd {} && cargo build {} -p {} -p {}",
        config.builder.omicron_path,
        config.release_arg(),
        "omicron-package",
        "omicron-deploy"
    );
    ssh_exec(&server, &cmd, SshStrategy::NoForward)
}

fn do_package(config: &Config, artifact_dir: Utf8PathBuf) -> Result<()> {
    let builder = &config.servers[&config.builder.server];

    // We use a bash login shell to get a proper environment, so we have a path to
    // postgres, and $DEP_PQ_LIBDIRS is filled in. This is required for building
    // nexus.
    //
    // See https://github.com/oxidecomputer/omicron/blob/8757ec542ea4ffbadd6f26094ed4ba357715d70d/rpaths/src/lib.rs
    let cmd = format!(
        "bash -lc \
            'cd {} && \
             cargo run {} --bin omicron-package -- package --out {}'",
        config.builder.omicron_path,
        config.release_arg(),
        artifact_dir,
    );

    ssh_exec(&builder, &cmd, SshStrategy::NoForward)
}

fn do_dot(_config: &Config) -> Result<()> {
    anyhow::bail!("\"dot\" command is not supported for thing-flinger");
}

fn do_check(config: &Config) -> Result<()> {
    let builder = &config.servers[&config.builder.server];

    let cmd = format!(
        "bash -lc \
            'cd {} && \
             cargo run {} --bin omicron-package -- check'",
        config.builder.omicron_path,
        config.release_arg(),
    );

    ssh_exec(&builder, &cmd, SshStrategy::NoForward)
}

fn do_uninstall(config: &Config) -> Result<()> {
    let builder = &config.servers[&config.builder.server];
    for server in config.deployment_servers() {
        copy_omicron_package_binary_to_staging(config, builder, server)?;

        // Run `omicron-package uninstall` on the deployment server
        let cmd = format!(
            "cd {} && pfexec ./omicron-package uninstall",
            config.deployment.staging_dir,
        );
        println!("$ {}", cmd);
        ssh_exec(&server, &cmd, SshStrategy::Forward)?;
    }
    Ok(())
}

fn do_clean(
    config: &Config,
    artifact_dir: Utf8PathBuf,
    install_dir: Utf8PathBuf,
) -> Result<()> {
    let mut deployment_src = Utf8PathBuf::from(&config.deployment.staging_dir);
    deployment_src.push(&artifact_dir);
    let builder = &config.servers[&config.builder.server];
    for server in config.deployment_servers() {
        copy_omicron_package_binary_to_staging(config, builder, server)?;

        // Run `omicron-package uninstall` on the deployment server
        let cmd = format!(
            "cd {} && pfexec ./omicron-package clean --in {} --out {}",
            config.deployment.staging_dir, deployment_src, install_dir,
        );
        println!("$ {}", cmd);
        ssh_exec(&server, &cmd, SshStrategy::Forward)?;
    }
    Ok(())
}

fn run_in_parallel<'a, F>(op: &str, cmds: impl Iterator<Item = (&'a String, F)>)
where
    F: FnOnce() -> Result<()> + Send,
{
    thread::scope(|s| {
        let named_handles: Vec<(_, ScopedJoinHandle<'_, Result<()>>)> = cmds
            .map(|(server_name, f)| (server_name, s.spawn(|_| f())))
            .collect();

        // Join all the handles and print the install status
        for (server_name, handle) in named_handles {
            match handle.join() {
                Ok(Ok(())) => {
                    println!("{} completed for server: {}", op, server_name)
                }
                Ok(Err(e)) => {
                    println!(
                        "{} failed for server: {} with error: {}",
                        op, server_name, e
                    )
                }
                Err(_) => {
                    println!(
                        "{} failed for server: {}. Thread panicked.",
                        op, server_name
                    )
                }
            }
        }
    })
    .unwrap();
}

fn do_install(
    config: &Config,
    artifact_dir: &Utf8Path,
    install_dir: &Utf8Path,
) {
    let builder = &config.servers[&config.builder.server];
    let mut pkg_dir = Utf8PathBuf::from(&config.builder.omicron_path);
    pkg_dir.push(artifact_dir);

    let fns = config.deployment.servers.iter().map(|server_name| {
        (server_name, || {
            single_server_install(
                config,
                &artifact_dir,
                &install_dir,
                pkg_dir.as_str(),
                builder,
                server_name,
            )
        })
    });

    run_in_parallel("Install", fns);
}

fn do_overlay(config: &Config) -> Result<()> {
    let builder = &config.servers[&config.builder.server];
    let mut root_path = Utf8PathBuf::from(&config.builder.omicron_path);
    // TODO: This needs to match the artifact_dir in `package`
    root_path.push("out/overlay");

    // Build a list of directories for each server to be deployed and tag which
    // one is the server to run RSS; e.g., for servers ["foo", "bar", "baz"]
    // with root_path "/my/path", we produce
    // [
    //     "/my/path/foo/sled-agent/pkg",
    //     "/my/path/bar/sled-agent/pkg",
    //     "/my/path/baz/sled-agent/pkg",
    // ]
    // As we're doing so, record which directory is the one for the server that
    // will run RSS.
    let mut rss_server_dir = None;

    for server_name in &config.deployment.servers {
        let mut dir = root_path.clone();
        dir.push(server_name);
        dir.push("sled-agent/pkg");
        if *server_name == config.deployment.rss_server {
            rss_server_dir = Some(dir.clone());
            break;
        }
    }

    // we know exactly one of the servers matches `rss_server` from our config
    // validation, so we can unwrap here
    let rss_server_dir = rss_server_dir.unwrap();

    overlay_rss_config(builder, config, &rss_server_dir)?;

    Ok(())
}

fn overlay_rss_config(
    builder: &Server,
    config: &Config,
    rss_server_dir: &Utf8Path,
) -> Result<()> {
    // Sync `config-rss.toml` to the directory for the RSS server on the
    // builder.
    let src = if let Some(src) = &config.rss_config_path {
        src.clone()
    } else {
        config.omicron_path.join("smf/sled-agent/non-gimlet/config-rss.toml")
    };
    let dst = format!(
        "{}@{}:{}/config-rss.toml",
        builder.username, builder.addr, rss_server_dir
    );

    let mut cmd = rsync_common();
    cmd.arg(&src).arg(&dst);

    let status =
        cmd.status().context(format!("Failed to run command: ({:?})", cmd))?;
    if !status.success() {
        return Err(FlingError::FailedSync { src: src.to_string(), dst }.into());
    }

    Ok(())
}

fn single_server_install(
    config: &Config,
    artifact_dir: &Utf8Path,
    install_dir: &Utf8Path,
    pkg_dir: &str,
    builder: &Server,
    server_name: &str,
) -> Result<()> {
    let server = &config.servers[server_name];

    println!(
        "COPYING packages from builder ({}) -> deploy server ({})",
        builder.addr, server_name
    );
    copy_package_artifacts_to_staging(config, pkg_dir, builder, server)?;

    println!(
        "COPYING deploy tool from builder ({}) -> deploy server ({})",
        builder.addr, server_name
    );
    copy_omicron_package_binary_to_staging(config, builder, server)?;

    println!(
        "COPYING manifest from builder ({}) -> deploy server ({})",
        builder.addr, server_name
    );
    copy_package_manifest_to_staging(config, builder, server)?;

    println!("UNPACKING packages on deploy server ({})", server_name);
    run_omicron_package_unpack_from_staging(
        config,
        server,
        &artifact_dir,
        &install_dir,
    )?;

    println!(
        "COPYING overlay files from builder ({}) -> deploy server ({})",
        builder.addr, server_name
    );
    copy_overlay_files_to_staging(
        config,
        pkg_dir,
        builder,
        server,
        server_name,
    )?;

    println!("INSTALLING overlay files into the install directory of the deploy server ({})", server_name);
    install_overlay_files_from_staging(config, server, &install_dir)?;

    println!("STARTING services on the deploy server ({})", server_name);
    run_omicron_package_activate_from_staging(config, server, &install_dir)
}

// Copy package artifacts as a result of `omicron-package package` from the
// builder to the deployment server staging directory.
//
// This staging directory acts as an intermediate location where
// packages may reside prior to being installed.
fn copy_package_artifacts_to_staging(
    config: &Config,
    pkg_dir: &str,
    builder: &Server,
    destination: &Server,
) -> Result<()> {
    let cmd = format!(
        "rsync -avz -e 'ssh -o StrictHostKeyChecking=no' \
            --include 'out/' \
            --include 'out/*.tar' \
            --include 'out/*.tar.gz' \
            --exclude '*' \
            {} {}@{}:{}",
        pkg_dir,
        destination.username,
        destination.addr,
        config.deployment.staging_dir
    );
    println!("$ {}", cmd);
    ssh_exec(builder, &cmd, SshStrategy::Forward)
}

fn copy_omicron_package_binary_to_staging(
    config: &Config,
    builder: &Server,
    destination: &Server,
) -> Result<()> {
    let mut bin_path = Utf8PathBuf::from(&config.builder.omicron_path);
    bin_path.push(format!(
        "target/{}/omicron-package",
        if config.debug { "debug" } else { "release" }
    ));
    let cmd = format!(
        "rsync -avz {} {}@{}:{}",
        bin_path,
        destination.username,
        destination.addr,
        config.deployment.staging_dir
    );
    println!("$ {}", cmd);
    ssh_exec(builder, &cmd, SshStrategy::Forward)
}

fn copy_package_manifest_to_staging(
    config: &Config,
    builder: &Server,
    destination: &Server,
) -> Result<()> {
    let mut path = Utf8PathBuf::from(&config.builder.omicron_path);
    path.push("package-manifest.toml");
    let cmd = format!(
        "rsync {} {}@{}:{}",
        path,
        destination.username,
        destination.addr,
        config.deployment.staging_dir
    );
    println!("$ {}", cmd);
    ssh_exec(builder, &cmd, SshStrategy::Forward)
}

fn run_omicron_package_activate_from_staging(
    config: &Config,
    destination: &Server,
    install_dir: &Utf8Path,
) -> Result<()> {
    // Run `omicron-package activate` on the deployment server
    let cmd = format!(
        "cd {} && pfexec ./omicron-package activate --out {}",
        config.deployment.staging_dir, install_dir,
    );

    println!("$ {}", cmd);
    ssh_exec(destination, &cmd, SshStrategy::Forward)
}

fn run_omicron_package_unpack_from_staging(
    config: &Config,
    destination: &Server,
    artifact_dir: &Utf8Path,
    install_dir: &Utf8Path,
) -> Result<()> {
    let mut deployment_src = Utf8PathBuf::from(&config.deployment.staging_dir);
    deployment_src.push(&artifact_dir);

    // Run `omicron-package unpack` on the deployment server
    let cmd = format!(
        "cd {} && pfexec ./omicron-package unpack --in {} --out {}",
        config.deployment.staging_dir, deployment_src, install_dir,
    );

    println!("$ {}", cmd);
    ssh_exec(destination, &cmd, SshStrategy::Forward)
}

fn copy_overlay_files_to_staging(
    config: &Config,
    pkg_dir: &str,
    builder: &Server,
    destination: &Server,
    destination_name: &str,
) -> Result<()> {
    let cmd = format!(
        "rsync -avz {}/overlay/{}/ {}@{}:{}/overlay/",
        pkg_dir,
        destination_name,
        destination.username,
        destination.addr,
        config.deployment.staging_dir
    );
    println!("$ {}", cmd);
    ssh_exec(builder, &cmd, SshStrategy::Forward)
}

fn install_overlay_files_from_staging(
    config: &Config,
    destination: &Server,
    install_dir: &Utf8Path,
) -> Result<()> {
    let cmd = format!(
        "pfexec cp -r {}/overlay/* {}",
        config.deployment.staging_dir, install_dir
    );
    println!("$ {}", cmd);
    ssh_exec(&destination, &cmd, SshStrategy::NoForward)
}

fn ssh_exec(
    server: &Server,
    remote_cmd: &str,
    strategy: SshStrategy,
) -> Result<()> {
    let remote_cmd = if strategy.source_profile() {
        // Source .profile, so we have access to cargo. Rustup installs knowledge
        // about the cargo path here.
        String::from(". $HOME/.profile && ") + remote_cmd
    } else {
        remote_cmd.into()
    };

    let mut cmd = Command::new("ssh");
    if strategy.forward_agent() {
        cmd.arg("-A");
    }
    cmd.arg("-o")
        .arg("StrictHostKeyChecking=no")
        .arg("-l")
        .arg(&server.username)
        .arg(&server.addr)
        .arg(&remote_cmd);

    // If the builder is the same as the client, this will likely not be set,
    // as the keys will reside on the builder.
    if let Some(auth_sock) = std::env::var_os("SSH_AUTH_SOCK") {
        cmd.env("SSH_AUTH_SOCK", auth_sock);
    }
    let exit_status = cmd
        .status()
        .context(format!("Failed to run {} on {}", remote_cmd, server.addr))?;
    if !exit_status.success() {
        anyhow::bail!("Command failed: {}", exit_status);
    }

    Ok(())
}

fn validate_servers(
    chosen: &BTreeSet<String>,
    all: &BTreeMap<String, Server>,
) -> Result<(), FlingError> {
    let all = all.keys().cloned().collect();
    let diff: Vec<String> = chosen.difference(&all).cloned().collect();
    if !diff.is_empty() {
        Err(FlingError::InvalidServers(diff))
    } else {
        Ok(())
    }
}

fn validate_absolute_path(
    path: &Utf8Path,
    field: &'static str,
) -> Result<(), FlingError> {
    if path.is_absolute() || path.starts_with("$HOME") {
        Ok(())
    } else {
        Err(FlingError::NotAbsolutePath { field })
    }
}

fn validate(config: &Config) -> Result<(), FlingError> {
    validate_absolute_path(&config.omicron_path, "omicron_path")?;
    validate_absolute_path(
        &config.builder.omicron_path,
        "builder.omicron_path",
    )?;
    validate_absolute_path(
        &config.deployment.staging_dir,
        "deployment.staging_dir",
    )?;

    validate_servers(
        &BTreeSet::from([
            config.builder.server.clone(),
            config.deployment.rss_server.clone(),
        ]),
        &config.servers,
    )
}

fn main() -> Result<()> {
    let args = Args::try_parse()?;
    let config = parse::<_, Config>(args.config)?;

    validate(&config)?;

    match args.subcommand {
        SubCommand::Exec { cmd, servers } => {
            do_exec(&config, cmd, servers)?;
        }
        SubCommand::Sync => do_sync(&config)?,
        SubCommand::InstallPrereqs => do_install_prereqs(&config)?,
        SubCommand::Builder(BuildCommand::Target { .. }) => {
            todo!("Setting target not supported through thing-flinger")
        }
        SubCommand::Builder(BuildCommand::Package) => {
            do_package(&config, args.artifact_dir)?;
        }
        SubCommand::Builder(BuildCommand::Stamp { .. }) => {
            anyhow::bail!("Distributed package stamping not supported")
        }
        SubCommand::Builder(BuildCommand::Check) => do_check(&config)?,
        SubCommand::Builder(BuildCommand::Dot) => {
            do_dot(&config)?;
        }
        SubCommand::Deploy(DeployCommand::Install { install_dir }) => {
            do_build_minimal(&config)?;
            do_install(&config, &args.artifact_dir, &install_dir);
        }
        SubCommand::Deploy(DeployCommand::Uninstall) => {
            do_build_minimal(&config)?;
            do_uninstall(&config)?;
        }
        SubCommand::Deploy(DeployCommand::Clean { install_dir }) => {
            do_build_minimal(&config)?;
            do_clean(&config, args.artifact_dir, install_dir)?;
        }
        // TODO: It doesn't really make sense to allow the user direct access
        // to these low level operations in thing-flinger. Should we not use
        // the DeployCommand from omicron-package directly?
        SubCommand::Deploy(_) => anyhow::bail!("Unsupported action"),
        SubCommand::Overlay => do_overlay(&config)?,
    }
    Ok(())
}
