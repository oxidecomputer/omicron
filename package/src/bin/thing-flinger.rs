use omicron_package::{parse, SubCommand as PackageSubCommand};

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, Context, Result};
use serde_derive::Deserialize;
use structopt::StructOpt;
use thiserror::Error;

#[derive(Deserialize, Debug)]
struct Builder {
    pub server: String,
    pub omicron_path: String,
    git_treeish: String,
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
    pub staging_dir: String,
}

#[derive(Debug, Deserialize)]
struct Config {
    pub local_source: String,
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

// TODO: run in parallel when that option is given
fn do_exec(
    config: &Config,
    cmd: String,
    servers: Option<BTreeSet<String>>,
) -> Result<()> {
    if servers.is_some() {
        validate_servers(&servers.as_ref().unwrap(), &config.servers)?;

        for name in &servers.unwrap() {
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
    let src = config.local_source.clone() + "/";
    let dst = format!(
        "{}@{}:{}",
        server.username, server.addr, config.builder.omicron_path
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
        config.builder.omicron_path,
        config.builder.git_treeish,
        "omicron-package",
        "omicron-sled-agent"
    );
    ssh_exec(&server, &cmd, false)
}

fn do_package(
    config: &Config,
    artifact_dir: PathBuf,
    release: bool,
) -> Result<()> {
    let server = &config.servers[&config.builder.server];
    let mut cmd = String::new();
    let mut release_flag = "";
    if release {
        release_flag = "--release";
    }
    let cmd_path = "./target/debug/omicron-package";
    let artifact_dir = artifact_dir
        .to_str()
        .ok_or_else(|| FlingError::BadString("artifact_dir".to_string()))?;

    write!(
        &mut cmd,
        "cd {} && git checkout {} && {} package --out {} {}",
        config.builder.omicron_path,
        config.builder.git_treeish,
        cmd_path,
        &artifact_dir,
        release_flag
    )?;

    ssh_exec(&server, &cmd, false)
}

fn do_check(config: &Config) -> Result<()> {
    let server = &config.servers[&config.builder.server];
    let mut cmd = String::new();
    let cmd_path = "./target/debug/omicron-package";

    write!(
        &mut cmd,
        "cd {} && git checkout {} && {} check",
        config.builder.omicron_path, config.builder.git_treeish, cmd_path,
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
            "cd ~/{} && pfexec ./omicron-package uninstall --in ~/{} --out {}",
            config.deployment.staging_dir,
            deployment_src.to_string_lossy(),
            install_dir.to_string_lossy()
        );
        println!("$ {}", cmd);
        ssh_exec(&server, &cmd, true)?;
    }
    Ok(())
}

fn do_install(
    config: &Config,
    artifact_dir: PathBuf,
    install_dir: PathBuf,
) -> Result<()> {
    let builder = &config.servers[&config.builder.server];
    let mut src_dir = PathBuf::from(&config.builder.omicron_path);
    src_dir.push(artifact_dir.as_path());
    let src_dir = src_dir.to_string_lossy();

    for server_name in &config.deployment.servers {
        let server = &config.servers[server_name];

        copy_package_artifacts_to_staging(config, &src_dir, builder, server)?;
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
            &src_dir,
            builder,
            server,
            server_name,
        )?;
        install_overlay_files_from_staging(config, server, &install_dir)?;
        restart_services(server)?;
    }
    Ok(())
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
        config.builder.omicron_path,
        config.deployment.rack_secret_threshold,
        dirs
    );
    ssh_exec(server, &cmd, false)
}

// Copy package artifacts as a result of `omicron-package package` from the
// builder to the deployment server staging directory.
fn copy_package_artifacts_to_staging(
    config: &Config,
    src_dir: &str,
    builder: &Server,
    dest_server: &Server,
) -> Result<()> {
    let cmd = format!(
        "rsync -avz -e 'ssh -o StrictHostKeyChecking=no' \
                    --exclude overlay/ {} {}@{}:~/{}",
        src_dir,
        dest_server.username,
        dest_server.addr,
        config.deployment.staging_dir
    );
    println!("$ {}", cmd);
    ssh_exec(builder, &cmd, true)
}

fn copy_omicron_package_binary_to_staging(
    config: &Config,
    builder: &Server,
    dest_server: &Server,
) -> Result<()> {
    let mut bin_path = PathBuf::from(&config.builder.omicron_path);
    bin_path.push("target/debug/omicron-package");
    let cmd = format!(
        "rsync -avz {} {}@{}:~/{}",
        bin_path.to_string_lossy(),
        dest_server.username,
        dest_server.addr,
        config.deployment.staging_dir
    );
    println!("$ {}", cmd);
    ssh_exec(builder, &cmd, true)
}

fn copy_package_manifest_to_staging(
    config: &Config,
    builder: &Server,
    dest_server: &Server,
) -> Result<()> {
    let mut path = PathBuf::from(&config.builder.omicron_path);
    path.push("package-manifest.toml");
    let cmd = format!(
        "rsync {} {}@{}:~/{}",
        path.to_string_lossy(),
        dest_server.username,
        dest_server.addr,
        config.deployment.staging_dir
    );
    println!("$ {}", cmd);
    ssh_exec(builder, &cmd, true)
}

fn run_omicron_package_from_staging(
    config: &Config,
    dest_server: &Server,
    artifact_dir: &Path,
    install_dir: &Path,
) -> Result<()> {
    let mut deployment_src = PathBuf::from(&config.deployment.staging_dir);
    deployment_src.push(&artifact_dir);

    // Run `omicron-package install` on the deployment server
    let cmd = format!(
        "cd ~/{} && pfexec ./omicron-package install --in ~/{} --out {}",
        config.deployment.staging_dir,
        deployment_src.to_string_lossy(),
        install_dir.to_string_lossy()
    );
    println!("$ {}", cmd);
    ssh_exec(dest_server, &cmd, true)
}

fn copy_overlay_files_to_staging(
    config: &Config,
    src_dir: &str,
    builder: &Server,
    dest_server: &Server,
    dest_server_name: &str,
) -> Result<()> {
    let cmd = format!(
        "rsync -avz {}/overlay/{}/ {}@{}:~/{}/overlay/",
        src_dir,
        dest_server_name,
        dest_server.username,
        dest_server.addr,
        config.deployment.staging_dir
    );
    println!("$ {}", cmd);
    ssh_exec(builder, &cmd, true)
}

fn install_overlay_files_from_staging(
    config: &Config,
    dest_server: &Server,
    install_dir: &Path,
) -> Result<()> {
    let cmd = format!(
        "pfexec cp -r ~/{}/overlay/* {}",
        config.deployment.staging_dir,
        install_dir.to_string_lossy()
    );
    println!("$ {}", cmd);
    ssh_exec(&dest_server, &cmd, false)
}

// For now, we just restart sled-agent, as that's the only service with an
// overlay file.
fn restart_services(dest_server: &Server) -> Result<()> {
    ssh_exec(dest_server, "svcadm restart sled-agent", false)
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
    let remote_cmd = String::from(". ~/.profile && ") + remote_cmd;
    let auth_sock = std::env::var("SSH_AUTH_SOCK")?;
    let mut cmd = Command::new("ssh");
    if forward_agent {
        cmd.arg("-A");
    }
    cmd.arg("-o")
        .arg("StrictHostKeyChecking=no")
        .arg("-l")
        .arg(&server.username)
        .arg("-t")
        .arg(&server.addr)
        .arg(&remote_cmd);
    cmd.env("SSH_AUTH_SOCK", auth_sock);
    cmd.status()
        .context(format!("Failed to run {} on {}", remote_cmd, server.addr))?;

    Ok(())
}

fn main() -> Result<()> {
    let args = Args::from_args_safe().map_err(|err| anyhow!(err))?;
    let config = parse::<_, Config>(args.config)?;

    validate_servers(&config.deployment.servers, &config.servers)?;
    validate_servers(
        &BTreeSet::from([config.builder.server.clone()]),
        &config.servers,
    )?;

    match args.subcommand {
        SubCommand::Exec { cmd, servers } => {
            do_exec(&config, cmd, servers)?;
        }
        SubCommand::Sync => do_sync(&config)?,
        SubCommand::BuildMinimal => do_build_minimal(&config)?,
        SubCommand::Package(PackageSubCommand::Package {
            artifact_dir,
            release,
        }) => {
            do_package(&config, artifact_dir, release)?;
        }
        SubCommand::Package(PackageSubCommand::Install {
            artifact_dir,
            install_dir,
        }) => {
            do_install(&config, artifact_dir, install_dir)?;
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
