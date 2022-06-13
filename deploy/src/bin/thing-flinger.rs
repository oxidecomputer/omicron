// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utility for deploying Omicron to remote machines

use omicron_package::{parse, BuildCommand, DeployCommand};

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
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
    omicron_path: PathBuf,
}

// A server on which an omicron package is deployed.
#[derive(Deserialize, Debug)]
struct Server {
    username: String,
    addr: String,
}

#[derive(Deserialize, Debug)]
struct Deployment {
    rss_server: String,
    staging_dir: PathBuf,
}

#[derive(Debug, Deserialize)]
struct Config {
    omicron_path: PathBuf,
    builder: Builder,
    servers: BTreeMap<String, Server>,
    deployment: Deployment,

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
}

fn parse_into_set(src: &str) -> BTreeSet<String> {
    src.split_whitespace().map(|s| s.to_owned()).collect()
}

#[derive(Debug, Subcommand)]
enum SubCommand {
    /// Run the given command on the given servers, or all servers if none are
    /// specified.
    ///
    /// Be careful!
    Exec {
        /// The command to run
        #[clap(short, long)]
        cmd: String,

        /// The servers to run the command on
        #[clap(short, long, parse(from_str = parse_into_set))]
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
    #[clap(short, long, help = "Path to deployment manifest toml file")]
    config: PathBuf,

    #[clap(subcommand)]
    subcommand: SubCommand,
}

/// Errors which can be returned when executing subcommands
#[derive(Error, Debug)]
enum FlingError {
    #[error("Servers not listed in configuration: {0:?}")]
    InvalidServers(Vec<String>),

    /// The parameter should be the name of the argument that could not be
    /// properly converted to a string.
    #[error("{0} is not valid UTF-8")]
    BadString(String),

    /// Failed to rsync omicron to build host
    #[error("Failed to sync {src} with {dst}")]
    FailedSync { src: String, dst: String },

    /// The given path must be absolute
    #[error("Path for {field} must be absolute")]
    NotAbsolutePath { field: &'static str },
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
            ssh_exec(&server, &cmd, false)?;
        }
    } else {
        for (_, server) in config.servers.iter() {
            ssh_exec(&server, &cmd, false)?;
        }
    }
    Ok(())
}

// start an `rsync` command with args common to all our uses
fn rsync_common() -> Command {
    let mut cmd = Command::new("rsync");
    cmd.arg("-az")
        .arg("-e")
        .arg("ssh")
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
        config
            .omicron_path
            .canonicalize()
            .with_context(|| format!(
                "could not canonicalize {}",
                config.omicron_path.display()
            ))?
            .to_string_lossy()
    );
    let dst = format!(
        "{}@{}:{}",
        builder.username,
        builder.addr,
        config.builder.omicron_path.to_str().unwrap()
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

fn do_install_prereqs(config: &Config) -> Result<()> {
    // we need to rsync `./tools/*` to each of the deployment targets (the
    // "builder" already has it via `do_sync()`), and then run `pfxec
    // tools/install_prerequisites.sh` on each system.
    let src = format!(
        // the `./` here is load-bearing; it interacts with `--relative` to tell
        // rsync to create `tools` but none of its parents
        "{}/./tools/",
        config
            .omicron_path
            .canonicalize()
            .with_context(|| format!(
                "could not canonicalize {}",
                config.omicron_path.display()
            ))?
            .to_string_lossy()
    );
    let partial_cmd = || {
        let mut cmd = rsync_common();
        cmd.arg("--relative");
        cmd.arg(&src);
        cmd
    };

    for server in config.servers.values() {
        let dst = format!(
            "{}@{}:{}",
            server.username,
            server.addr,
            config.deployment.staging_dir.to_str().unwrap()
        );
        let mut cmd = partial_cmd();
        cmd.arg(&dst);
        let status = cmd
            .status()
            .context(format!("Failed to run command: ({:?})", cmd))?;
        if !status.success() {
            return Err(FlingError::FailedSync { src, dst }.into());
        }
    }

    // run install_prereqs on each server
    let builder = &config.servers[&config.builder.server];
    let build_server = (builder, &config.builder.omicron_path);
    let all_servers = std::iter::once(build_server).chain(
        config.servers.iter().filter_map(|(name, server)| {
            // skip running prereq installing on a deployment target if it is
            // also the builder, because we're already running it on the builder
            if *name == config.builder.server {
                None
            } else {
                Some((server, &config.deployment.staging_dir))
            }
        }),
    );

    for (server, root_path) in all_servers {
        // -y: assume yes instead of prompting
        // -p: skip check that deps end up in $PATH
        let cmd = format!(
            "cd {} && mkdir -p out && pfexec ./tools/install_prerequisites.sh -y -p",
            root_path.display()
        );
        println!("install prerequisites on {}", server.addr);
        ssh_exec(server, &cmd, false)?;
    }

    Ok(())
}

// Build omicron-package and omicron-deploy on the builder
//
// We need to build omicron-deploy for overlay file generation
fn do_build_minimal(config: &Config) -> Result<()> {
    let server = &config.servers[&config.builder.server];
    let cmd = format!(
        "cd {} && cargo build {} -p {} -p {}",
        config.builder.omicron_path.to_string_lossy(),
        config.release_arg(),
        "omicron-package",
        "omicron-deploy"
    );
    ssh_exec(&server, &cmd, false)
}

fn do_package(config: &Config, artifact_dir: PathBuf) -> Result<()> {
    let builder = &config.servers[&config.builder.server];
    let artifact_dir = artifact_dir
        .to_str()
        .ok_or_else(|| FlingError::BadString("artifact_dir".to_string()))?;

    // We use a bash login shell to get a proper environment, so we have a path to
    // postgres, and $DEP_PQ_LIBDIRS is filled in. This is required for building
    // nexus.
    //
    // See https://github.com/oxidecomputer/omicron/blob/8757ec542ea4ffbadd6f26094ed4ba357715d70d/rpaths/src/lib.rs
    let cmd = format!(
        "bash -lc \
            'cd {} && \
             cargo run {} --bin omicron-package -- package --out {}'",
        config.builder.omicron_path.to_string_lossy(),
        config.release_arg(),
        &artifact_dir,
    );

    ssh_exec(&builder, &cmd, false)
}

fn do_check(config: &Config) -> Result<()> {
    let builder = &config.servers[&config.builder.server];

    let cmd = format!(
        "bash -lc \
            'cd {} && \
             cargo run {} --bin omicron-package -- check'",
        config.builder.omicron_path.to_string_lossy(),
        config.release_arg(),
    );

    ssh_exec(&builder, &cmd, false)
}

fn do_uninstall(
    config: &Config,
    artifact_dir: PathBuf,
    install_dir: PathBuf,
) -> Result<()> {
    let mut deployment_src = PathBuf::from(&config.deployment.staging_dir);
    deployment_src.push(&artifact_dir);
    let builder = &config.servers[&config.builder.server];
    for server in config.servers.values() {
        copy_omicron_package_binary_to_staging(config, builder, server)?;

        // Run `omicron-package uninstall` on the deployment server
        let cmd = format!(
            "cd {} && pfexec ./omicron-package uninstall --in {} --out {}",
            config.deployment.staging_dir.to_string_lossy(),
            deployment_src.to_string_lossy(),
            install_dir.to_string_lossy()
        );
        println!("$ {}", cmd);
        ssh_exec(&server, &cmd, true)?;
    }
    Ok(())
}

fn do_install(config: &Config, artifact_dir: &Path, install_dir: &Path) {
    let builder = &config.servers[&config.builder.server];
    let mut pkg_dir = PathBuf::from(&config.builder.omicron_path);
    pkg_dir.push(artifact_dir);
    let pkg_dir = pkg_dir.to_string_lossy();
    let pkg_dir = &pkg_dir;

    thread::scope(|s| {
        let mut handles =
            Vec::<(String, ScopedJoinHandle<'_, Result<()>>)>::new();

        // Spawn a thread for each server install
        for server_name in config.servers.keys() {
            handles.push((
                server_name.to_owned(),
                s.spawn(move |_| -> Result<()> {
                    single_server_install(
                        config,
                        &artifact_dir,
                        &install_dir,
                        &pkg_dir,
                        builder,
                        server_name,
                    )
                }),
            ));
        }

        // Join all the handles and print the install status
        for (server_name, handle) in handles {
            match handle.join() {
                Ok(Ok(())) => {
                    println!("Install completed for server: {}", server_name)
                }
                Ok(Err(e)) => {
                    println!(
                        "Install failed for server: {} with error: {}",
                        server_name, e
                    )
                }
                Err(_) => {
                    println!(
                        "Install failed for server: {}. Thread panicked.",
                        server_name
                    )
                }
            }
        }
    })
    .unwrap();
}

fn do_overlay(config: &Config) -> Result<()> {
    let builder = &config.servers[&config.builder.server];
    let mut root_path = PathBuf::from(&config.builder.omicron_path);
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
    let sled_agent_dirs = config
        .servers
        .keys()
        .map(|server_name| {
            let mut dir = root_path.clone();
            dir.push(server_name);
            dir.push("sled-agent/pkg");
            if *server_name == config.deployment.rss_server {
                rss_server_dir = Some(dir.clone());
            }
            dir
        })
        .collect::<Vec<_>>();

    // we know exactly one of the servers matches `rss_server` from our config
    // validation, so we can unwrap here
    let rss_server_dir = rss_server_dir.unwrap();

    overlay_sled_agent(builder, config, &sled_agent_dirs)?;
    overlay_rss_config(builder, config, &rss_server_dir)?;

    Ok(())
}

fn overlay_sled_agent(
    builder: &Server,
    config: &Config,
    sled_agent_dirs: &[PathBuf],
) -> Result<()> {
    // Send SSH command to create directories on builder and generate secret
    // shares.

    // TODO do we need any escaping here? this will definitely break if any dir
    // names have spaces
    let dirs = sled_agent_dirs
        .iter()
        .map(|dir| format!(" --directories {}", dir.display()))
        .collect::<String>();

    let cmd = format!(
        "sh -c 'for dir in {}; do mkdir -p $dir; done' && \
            cd {} && \
            cargo run {} --bin sled-agent-overlay-files -- {}",
        dirs,
        config.builder.omicron_path.to_string_lossy(),
        config.release_arg(),
        dirs
    );
    ssh_exec(builder, &cmd, false)
}

fn overlay_rss_config(
    builder: &Server,
    config: &Config,
    rss_server_dir: &Path,
) -> Result<()> {
    // Sync `config-rss.toml` to the directory for the RSS server on the
    // builder.
    let src = config.omicron_path.join("smf/sled-agent/config-rss.toml");
    let dst = format!(
        "{}@{}:{}",
        builder.username,
        builder.addr,
        rss_server_dir.display()
    );

    let mut cmd = rsync_common();
    cmd.arg(&src).arg(&dst);

    let status =
        cmd.status().context(format!("Failed to run command: ({:?})", cmd))?;
    if !status.success() {
        return Err(FlingError::FailedSync {
            src: src.to_string_lossy().to_string(),
            dst,
        }
        .into());
    }

    Ok(())
}

fn single_server_install(
    config: &Config,
    artifact_dir: &Path,
    install_dir: &Path,
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

    println!("INSTALLING packages on deploy server ({})", server_name);
    run_omicron_package_install_from_staging(
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

    println!("RESTARTING services on the deploy server ({})", server_name);
    restart_services(server)
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
        config.deployment.staging_dir.to_string_lossy()
    );
    println!("$ {}", cmd);
    ssh_exec(builder, &cmd, true)
}

fn copy_omicron_package_binary_to_staging(
    config: &Config,
    builder: &Server,
    destination: &Server,
) -> Result<()> {
    let mut bin_path = PathBuf::from(&config.builder.omicron_path);
    bin_path.push(format!(
        "target/{}/omicron-package",
        if config.debug { "debug" } else { "release" }
    ));
    let cmd = format!(
        "rsync -avz {} {}@{}:{}",
        bin_path.to_string_lossy(),
        destination.username,
        destination.addr,
        config.deployment.staging_dir.to_string_lossy()
    );
    println!("$ {}", cmd);
    ssh_exec(builder, &cmd, true)
}

fn copy_package_manifest_to_staging(
    config: &Config,
    builder: &Server,
    destination: &Server,
) -> Result<()> {
    let mut path = PathBuf::from(&config.builder.omicron_path);
    path.push("package-manifest.toml");
    let cmd = format!(
        "rsync {} {}@{}:{}",
        path.to_string_lossy(),
        destination.username,
        destination.addr,
        config.deployment.staging_dir.to_string_lossy()
    );
    println!("$ {}", cmd);
    ssh_exec(builder, &cmd, true)
}

fn run_omicron_package_install_from_staging(
    config: &Config,
    destination: &Server,
    artifact_dir: &Path,
    install_dir: &Path,
) -> Result<()> {
    let mut deployment_src = PathBuf::from(&config.deployment.staging_dir);
    deployment_src.push(&artifact_dir);

    // Run `omicron-package install` on the deployment server
    let cmd = format!(
        "cd {} && pfexec ./omicron-package install --in {} --out {}",
        config.deployment.staging_dir.to_string_lossy(),
        deployment_src.to_string_lossy(),
        install_dir.to_string_lossy()
    );
    println!("$ {}", cmd);
    ssh_exec(destination, &cmd, true)
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
        config.deployment.staging_dir.to_string_lossy()
    );
    println!("$ {}", cmd);
    ssh_exec(builder, &cmd, true)
}

fn install_overlay_files_from_staging(
    config: &Config,
    destination: &Server,
    install_dir: &Path,
) -> Result<()> {
    let cmd = format!(
        "pfexec cp -r {}/overlay/* {}",
        config.deployment.staging_dir.to_string_lossy(),
        install_dir.to_string_lossy()
    );
    println!("$ {}", cmd);
    ssh_exec(&destination, &cmd, false)
}

// For now, we just restart sled-agent, as that's the only service with an
// overlay file.
fn restart_services(destination: &Server) -> Result<()> {
    ssh_exec(destination, "svcadm restart sled-agent", false)
}

fn ssh_exec(
    server: &Server,
    remote_cmd: &str,
    forward_agent: bool,
) -> Result<()> {
    // Source .profile, so we have access to cargo. Rustup installs knowledge
    // about the cargo path here.
    let remote_cmd = String::from(". $HOME/.profile && ") + remote_cmd;
    let auth_sock = std::env::var("SSH_AUTH_SOCK")?;
    let mut cmd = Command::new("ssh");
    if forward_agent {
        cmd.arg("-A");
    }
    cmd.arg("-o")
        .arg("StrictHostKeyChecking=no")
        .arg("-l")
        .arg(&server.username)
        .arg(&server.addr)
        .arg(&remote_cmd);
    cmd.env("SSH_AUTH_SOCK", auth_sock);
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
    path: &Path,
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
        SubCommand::Builder(BuildCommand::Package { artifact_dir }) => {
            do_package(&config, artifact_dir)?;
        }
        SubCommand::Builder(BuildCommand::Check) => do_check(&config)?,
        SubCommand::Deploy(DeployCommand::Install {
            artifact_dir,
            install_dir,
        }) => {
            do_build_minimal(&config)?;
            do_install(&config, &artifact_dir, &install_dir);
        }
        SubCommand::Deploy(DeployCommand::Uninstall {
            artifact_dir,
            install_dir,
        }) => {
            do_build_minimal(&config)?;
            do_uninstall(&config, artifact_dir, install_dir)?;
        }
        SubCommand::Overlay => do_overlay(&config)?,
    }
    Ok(())
}
