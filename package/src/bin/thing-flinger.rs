// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utility for deploying Omicron to remote machines

use omicron_package::{parse, SubCommand as PackageSubCommand};

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, Context, Result};
use crossbeam::thread::{self, ScopedJoinHandle};
use serde_derive::Deserialize;
use structopt::StructOpt;
use thiserror::Error;

#[derive(Deserialize, Debug)]
struct Builder {
    pub server: String,
    pub omicron_path: PathBuf,
    pub git_treeish: String,
}

// A server on which an omicron package is deployed
#[derive(Deserialize, Debug)]
struct Server {
    pub username: String,
    pub addr: String,
}

#[derive(Deserialize, Debug)]
struct Deployment {
    pub servers: BTreeSet<String>,
    pub rack_secret_threshold: usize,
    pub staging_dir: PathBuf,
}

#[derive(Debug, Deserialize)]
struct Config {
    pub local_source: PathBuf,
    pub builder: Builder,
    pub servers: BTreeMap<String, Server>,
    pub deployment: Deployment,
}

fn parse_into_set(src: &str) -> BTreeSet<String> {
    src.split_whitespace().map(|s| s.to_owned()).collect()
}

#[derive(Debug, StructOpt)]
enum SubCommand {
    /// Run the given command on the given servers, or all servers if none are
    /// given.
    ///
    /// Be careful!
    Exec {
        /// The command to run
        #[structopt(short, long)]
        cmd: String,

        /// The servers to run the command on
        #[structopt(short, long, parse(from_str = parse_into_set))]
        servers: Option<BTreeSet<String>>,
    },

    /// Sync our local source to the build host
    Sync,

    /// Build omicron-package and everything needed to run thing-flinger
    /// commands on the build host.
    ///
    /// Package always builds everything, but it can be set in release mode, and
    /// we expect the existing tools to run from 'target/debug'. Additionally,
    // you can't run `Package` until you have actually built `omicron-package`,
    // which `BuildMinimal` does.
    BuildMinimal,

    /// Use all subcommands from omicron-package
    #[structopt(flatten)]
    Package(PackageSubCommand),

    /// Create an overlay directory tree for each deployment server
    ///
    /// Each directory tree contains unique files for the given server that will
    /// be populated in the svc/pkg dir.
    ///
    /// This is a separate subcommand so that we can reconstruct overlays
    /// without rebuilding or repackaging.
    Overlay,
}

#[derive(Debug, StructOpt)]
#[structopt(name = "thing-flinger")]
struct Args {
    /// The path to the deployment manifest TOML file
    #[structopt(short, long, help = "Path to deployment manifest toml file")]
    config: PathBuf,

    #[structopt(subcommand)]
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

fn do_sync(config: &Config) -> Result<()> {
    let server =
        config.servers.get(&config.builder.server).ok_or_else(|| {
            FlingError::InvalidServers(vec![config.builder.server.clone()])
        })?;

    // For rsync to copy from the source appropriately we must guarantee a
    // trailing slash.
    let src =
        format!("{}/", config.local_source.canonicalize()?.to_string_lossy());
    let dst = format!(
        "{}@{}:{}",
        server.username,
        server.addr,
        config.builder.omicron_path.to_str().unwrap()
    );
    let mut cmd = Command::new("rsync");
    cmd.arg("-az")
        .arg("-e")
        .arg("ssh")
        .arg("--delete")
        .arg("--progress")
        .arg("--exclude")
        .arg("target/")
        .arg("--exclude")
        .arg("out/")
        .arg("--exclude")
        .arg("cockroachdb/")
        .arg("--exclude")
        .arg("clickhouse/")
        .arg("--exclude")
        .arg("*.swp")
        .arg("--out-format")
        .arg("File changed: %o %t %f")
        .arg(&src)
        .arg(&dst);
    let status =
        cmd.status().context(format!("Failed to run command: ({:?})", cmd))?;
    if !status.success() {
        return Err(FlingError::FailedSync { src, dst }.into());
    }

    Ok(())
}

// Build omicron-package and omicron-sled-agent on the builder
//
// We need to build omicron-sled-agent for overlay file generation
fn do_build_minimal(config: &Config) -> Result<()> {
    let server = &config.servers[&config.builder.server];
    let cmd = format!(
        "cd {} && git checkout {} && cargo build -p {} -p {}",
        config.builder.omicron_path.to_string_lossy(),
        config.builder.git_treeish,
        "omicron-package",
        "omicron-sled-agent"
    );
    ssh_exec(&server, &cmd, false)
}

fn do_package(config: &Config, artifact_dir: PathBuf) -> Result<()> {
    let server = &config.servers[&config.builder.server];
    let mut cmd = String::new();
    let cmd_path = "./target/debug/omicron-package";
    let artifact_dir = artifact_dir
        .to_str()
        .ok_or_else(|| FlingError::BadString("artifact_dir".to_string()))?;

    // We use a bash login shell to get a proper environment, so we have a path to
    // postgres, and $DEP_PQ_LIBDIRS is filled in. This is required for building
    // nexus.
    //
    // See https://github.com/oxidecomputer/omicron/blob/8757ec542ea4ffbadd6f26094ed4ba357715d70d/rpaths/src/lib.rs
    write!(
        &mut cmd,
        "bash -lc 'cd {} && git checkout {} && {} package --out {}'",
        config.builder.omicron_path.to_string_lossy(),
        config.builder.git_treeish,
        cmd_path,
        &artifact_dir,
    )?;

    ssh_exec(&server, &cmd, false)
}

fn do_check(config: &Config) -> Result<()> {
    let server = &config.servers[&config.builder.server];
    let mut cmd = String::new();
    let cmd_path = "./target/debug/omicron-package";

    write!(
        &mut cmd,
        "bash -lc 'cd {} && git checkout {} && {} check'",
        config.builder.omicron_path.to_string_lossy(),
        config.builder.git_treeish,
        cmd_path,
    )?;

    ssh_exec(&server, &cmd, false)
}

fn do_uninstall(
    config: &Config,
    artifact_dir: PathBuf,
    install_dir: PathBuf,
) -> Result<()> {
    let mut deployment_src = PathBuf::from(&config.deployment.staging_dir);
    deployment_src.push(&artifact_dir);
    for server_name in &config.deployment.servers {
        let server = &config.servers[server_name];
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
        for server_name in &config.deployment.servers {
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
    let mut root_path = PathBuf::from(&config.builder.omicron_path);
    // TODO: This needs to match the artifact_dir in `package`
    root_path.push("out/overlay");
    let server_dirs = dir_per_deploy_server(config, &root_path);
    let server = &config.servers[&config.builder.server];
    overlay_sled_agent(&server, config, &server_dirs)
}

fn overlay_sled_agent(
    server: &Server,
    config: &Config,
    server_dirs: &[PathBuf],
) -> Result<()> {
    let sled_agent_dirs: Vec<PathBuf> = server_dirs
        .iter()
        .map(|dir| {
            let mut dir = PathBuf::from(dir);
            dir.push("sled-agent/pkg");
            dir
        })
        .collect();

    // Create directories on builder
    let dirs = dir_string(&sled_agent_dirs);
    let cmd = format!("sh -c 'for dir in {}; do mkdir -p $dir; done'", dirs);

    let cmd = format!(
        "{} && cd {} && ./target/debug/sled-agent-overlay-files \
            --threshold {} --directories {}",
        cmd,
        config.builder.omicron_path.to_string_lossy(),
        config.deployment.rack_secret_threshold,
        dirs
    );
    ssh_exec(server, &cmd, false)
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

    copy_package_artifacts_to_staging(config, pkg_dir, builder, server)?;
    copy_omicron_package_binary_to_staging(config, builder, server)?;
    copy_package_manifest_to_staging(config, builder, server)?;
    run_omicron_package_from_staging(
        config,
        server,
        &artifact_dir,
        &install_dir,
    )?;
    copy_overlay_files_to_staging(
        config,
        pkg_dir,
        builder,
        server,
        server_name,
    )?;
    install_overlay_files_from_staging(config, server, &install_dir)?;
    restart_services(server)
}

// Copy package artifacts as a result of `omicron-package package` from the
// builder to the deployment server staging directory.
fn copy_package_artifacts_to_staging(
    config: &Config,
    pkg_dir: &str,
    builder: &Server,
    destination: &Server,
) -> Result<()> {
    let cmd = format!(
        "rsync -avz -e 'ssh -o StrictHostKeyChecking=no' \
                    --exclude overlay/ {} {}@{}:{}",
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
    bin_path.push("target/debug/omicron-package");
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

fn run_omicron_package_from_staging(
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

fn dir_string(dirs: &[PathBuf]) -> String {
    dirs.iter().map(|dir| dir.to_string_lossy().to_string() + " ").collect()
}

fn dir_per_deploy_server(config: &Config, root: &Path) -> Vec<PathBuf> {
    config
        .deployment
        .servers
        .iter()
        .map(|server_dir| {
            let mut dir = PathBuf::from(root);
            dir.push(server_dir);
            dir
        })
        .collect()
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
    cmd.status()
        .context(format!("Failed to run {} on {}", remote_cmd, server.addr))?;

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
    validate_absolute_path(&config.local_source, "local_source")?;
    validate_absolute_path(
        &config.builder.omicron_path,
        "builder.omicron_path",
    )?;
    validate_absolute_path(
        &config.deployment.staging_dir,
        "deployment.staging_dir",
    )?;

    validate_servers(&config.deployment.servers, &config.servers)?;

    validate_servers(
        &BTreeSet::from([config.builder.server.clone()]),
        &config.servers,
    )
}

fn main() -> Result<()> {
    let args = Args::from_args_safe().map_err(|err| anyhow!(err))?;
    let config = parse::<_, Config>(args.config)?;

    validate(&config)?;

    match args.subcommand {
        SubCommand::Exec { cmd, servers } => {
            do_exec(&config, cmd, servers)?;
        }
        SubCommand::Sync => do_sync(&config)?,
        SubCommand::BuildMinimal => do_build_minimal(&config)?,
        SubCommand::Package(PackageSubCommand::Package { artifact_dir }) => {
            do_package(&config, artifact_dir)?;
        }
        SubCommand::Package(PackageSubCommand::Install {
            artifact_dir,
            install_dir,
        }) => {
            do_install(&config, &artifact_dir, &install_dir);
        }
        SubCommand::Package(PackageSubCommand::Uninstall {
            artifact_dir,
            install_dir,
        }) => {
            do_uninstall(&config, artifact_dir, install_dir)?;
        }
        SubCommand::Package(PackageSubCommand::Check) => do_check(&config)?,
        SubCommand::Overlay => do_overlay(&config)?,
    }
    Ok(())
}
