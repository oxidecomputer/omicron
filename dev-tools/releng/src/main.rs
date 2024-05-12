// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod cmd;
mod job;

use std::sync::Arc;
use std::time::Instant;

use anyhow::bail;
use anyhow::Context;
use anyhow::Result;
use camino::Utf8PathBuf;
use chrono::Utc;
use clap::Parser;
use fs_err::tokio as fs;
use omicron_zone_package::config::Config;
use once_cell::sync::Lazy;
use semver::Version;
use slog::debug;
use slog::error;
use slog::info;
use slog::Drain;
use slog::Logger;
use slog_term::FullFormat;
use slog_term::TermDecorator;
use tokio::process::Command;

use crate::cmd::CommandExt;
use crate::job::Jobs;

/// The base version we're currently building. Build information is appended to
/// this later on.
///
/// Under current policy, each new release is a major version bump, and
/// generally referred to only by the major version (e.g. 8.0.0 is referred
/// to as "v8", "version 8", or "release 8" to customers). The use of semantic
/// versioning is mostly to hedge for perhaps wanting something more granular in
/// the future.
const BASE_VERSION: Version = Version::new(8, 0, 0);

#[derive(Debug, Clone, Copy)]
enum InstallMethod {
    /// Unpack the tarball to `/opt/oxide/<service-name>`, and install
    /// `pkg/manifest.xml` (if it exists) to
    /// `/lib/svc/manifest/site/<service-name>.xml`.
    Install,
    /// Copy the tarball to `/opt/oxide/<service-name>.tar.gz`.
    Bundle,
}

/// Packages to install or bundle in the host OS image.
const HOST_IMAGE_PACKAGES: [(&str, InstallMethod); 7] = [
    ("mg-ddm-gz", InstallMethod::Install),
    ("omicron-sled-agent", InstallMethod::Install),
    ("overlay", InstallMethod::Bundle),
    ("oxlog", InstallMethod::Install),
    ("propolis-server", InstallMethod::Bundle),
    ("pumpkind-gz", InstallMethod::Install),
    ("switch-asic", InstallMethod::Bundle),
];
/// Packages to install or bundle in the recovery (trampoline) OS image.
const RECOVERY_IMAGE_PACKAGES: [(&str, InstallMethod); 2] = [
    ("installinator", InstallMethod::Install),
    ("mg-ddm-gz", InstallMethod::Install),
];

const HELIOS_REPO: &str = "https://pkg.oxide.computer/helios/2/dev/";
const OPTE_VERSION: &str = include_str!("../../../tools/opte_version");

static WORKSPACE_DIR: Lazy<Utf8PathBuf> = Lazy::new(|| {
    // $CARGO_MANIFEST_DIR is at `.../omicron/dev-tools/releng`
    let mut dir =
        Utf8PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect(
            "$CARGO_MANIFEST_DIR is not set; run this via `cargo xtask releng`",
        ));
    dir.pop();
    dir.pop();
    dir
});

#[derive(Parser)]
/// Run the Oxide release engineering process and produce a TUF repo that can be
/// used to update a rack.
///
/// For more information, see `docs/releng.adoc` in the Omicron repository.
///
/// Note that `--host-dataset` and `--recovery-dataset` must be set to different
/// values to build the two OS images in parallel. This is strongly recommended.
struct Args {
    /// ZFS dataset to use for `helios-build` when building the host image
    #[clap(long, default_value_t = Self::default_dataset("host"))]
    host_dataset: String,

    /// ZFS dataset to use for `helios-build` when building the recovery
    /// (trampoline) image
    #[clap(long, default_value_t = Self::default_dataset("recovery"))]
    recovery_dataset: String,

    /// Path to a Helios repository checkout (default: "helios" in the same
    /// directory as "omicron")
    #[clap(long, default_value_t = Self::default_helios_dir())]
    helios_dir: Utf8PathBuf,

    /// Ignore the current HEAD of the Helios repository checkout
    #[clap(long)]
    ignore_helios_origin: bool,

    /// Output dir for TUF repo and log files
    #[clap(long, default_value_t = Self::default_output_dir())]
    output_dir: Utf8PathBuf,
}

impl Args {
    fn default_dataset(name: &str) -> String {
        format!(
            "rpool/images/{}/{}",
            std::env::var("LOGNAME").expect("$LOGNAME is not set"),
            name
        )
    }

    fn default_helios_dir() -> Utf8PathBuf {
        WORKSPACE_DIR
            .parent()
            .expect("omicron is presumably not cloned at /")
            .join("helios")
    }

    fn default_output_dir() -> Utf8PathBuf {
        WORKSPACE_DIR.join("out/releng")
    }
}

fn main() -> Result<()> {
    let args = Args::parse();

    let decorator = TermDecorator::new().build();
    let drain = FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = Logger::root(drain, slog::o!());

    // Change the working directory to the workspace root.
    info!(logger, "changing working directory to {}", *WORKSPACE_DIR);
    std::env::set_current_dir(&*WORKSPACE_DIR)
        .context("failed to change working directory to workspace root")?;

    // Unset `$CARGO*` and `$RUSTUP_TOOLCHAIN`, which will interfere with
    // various tools we're about to run. (This needs to come _after_ we read
    // from `WORKSPACE_DIR` as it relies on `$CARGO_MANIFEST_DIR`.)
    for (name, _) in std::env::vars_os() {
        if name
            .to_str()
            .map(|s| s.starts_with("CARGO") || s == "RUSTUP_TOOLCHAIN")
            .unwrap_or(false)
        {
            debug!(logger, "unsetting {:?}", name);
            std::env::remove_var(name);
        }
    }

    // Now that we're done mucking about with our environment (something that's
    // not necessarily safe in multi-threaded programs), create a Tokio runtime
    // and call `do_run`.
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(do_run(logger, args))
}

async fn do_run(logger: Logger, args: Args) -> Result<()> {
    let commit = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .ensure_stdout(&logger)
        .await?
        .trim()
        .to_owned();

    let mut version = BASE_VERSION.clone();
    // Differentiate between CI and local builds.
    version.pre =
        if std::env::var_os("CI").is_some() { "0.ci" } else { "0.local" }
            .parse()?;
    // Set the build metadata to the current commit hash.
    let mut build = String::with_capacity(14);
    build.push_str("git");
    build.extend(commit.chars().take(11));
    version.build = build.parse()?;
    info!(logger, "version: {}", version);

    let manifest = Arc::new(omicron_zone_package::config::parse_manifest(
        &fs::read_to_string(WORKSPACE_DIR.join("package-manifest.toml"))
            .await?,
    )?);

    // PREFLIGHT ==============================================================
    let mut preflight_ok = true;

    for (package, _) in HOST_IMAGE_PACKAGES
        .into_iter()
        .chain(RECOVERY_IMAGE_PACKAGES.into_iter())
    {
        if !manifest.packages.contains_key(package) {
            error!(
                logger,
                "package {} to be installed in the OS image \
                is not listed in the package manifest",
                package
            );
            preflight_ok = false;
        }
    }

    // Ensure the Helios checkout exists
    if args.helios_dir.exists() {
        if !args.ignore_helios_origin {
            // check that our helios clone is up to date
            Command::new("git")
                .arg("-C")
                .arg(&args.helios_dir)
                .args(["fetch", "--no-write-fetch-head", "origin", "master"])
                .ensure_success(&logger)
                .await?;
            let stdout = Command::new("git")
                .arg("-C")
                .arg(&args.helios_dir)
                .args(["rev-parse", "HEAD", "origin/master"])
                .ensure_stdout(&logger)
                .await?;
            let mut lines = stdout.lines();
            let first =
                lines.next().context("git-rev-parse output was empty")?;
            if !lines.all(|line| line == first) {
                error!(
                    logger,
                    "helios checkout at {0} is out-of-date; run \
                    `git pull -C {0}`, or run omicron-releng with \
                    --ignore-helios-origin or --helios-path",
                    shell_words::quote(args.helios_dir.as_str())
                );
                preflight_ok = false;
            }
        }
    } else {
        info!(logger, "cloning helios to {}", args.helios_dir);
        Command::new("git")
            .args(["clone", "https://github.com/oxidecomputer/helios.git"])
            .arg(&args.helios_dir)
            .ensure_success(&logger)
            .await?;
    }

    // Check that the omicron1 brand is installed
    if !Command::new("pkg")
        .args(["verify", "-q", "/system/zones/brand/omicron1/tools"])
        .is_success(&logger)
        .await?
    {
        error!(
            logger,
            "the omicron1 brand is not installed; install it with \
            `pfexec pkg install /system/zones/brand/omicron1/tools`"
        );
        preflight_ok = false;
    }

    // Check that the datasets for helios-image to use exist
    for (dataset, option) in [
        (&args.host_dataset, "--host-dataset"),
        (&args.recovery_dataset, "--recovery-dataset"),
    ] {
        if !Command::new("zfs")
            .arg("list")
            .arg(dataset)
            .is_success(&logger)
            .await?
        {
            error!(
                logger,
                "the dataset {0} does not exist; run `pfexec zfs create \
                -p {0}`, or specify a different one with {1}",
                shell_words::quote(dataset),
                option
            );
            preflight_ok = false;
        }
    }

    if !preflight_ok {
        bail!("some preflight checks failed");
    }

    fs::create_dir_all(&args.output_dir).await?;

    // DEFINE JOBS ============================================================
    let tempdir = camino_tempfile::tempdir()
        .context("failed to create temporary directory")?;
    let mut jobs = Jobs::new(&logger, &args.output_dir);

    jobs.push_command(
        "helios-setup",
        Command::new("ptime")
            .args(["-m", "gmake", "setup"])
            .current_dir(&args.helios_dir)
            // ?!?!
            // somehow, the Makefile does not see a new `$(PWD)` without this.
            .env("PWD", &args.helios_dir)
            // Setting `BUILD_OS` to no makes setup skip repositories we don't
            // need for building the OS itself (we are just building an image
            // from an already-built OS).
            .env("BUILD_OS", "no"),
    );

    jobs.push_command(
        "omicron-package",
        Command::new("ptime").args([
            "-m",
            "cargo",
            "build",
            "--locked",
            "--release",
            "--bin",
            "omicron-package",
        ]),
    );
    let omicron_package = WORKSPACE_DIR.join("target/release/omicron-package");

    macro_rules! os_image_jobs {
        (
            target_name: $target_name:literal,
            target_args: $target_args:expr,
            proto_packages: $proto_packages:expr,
            image_prefix: $image_prefix:literal,
            image_build_args: $image_build_args:expr,
            image_dataset: $image_dataset:expr,
        ) => {
            jobs.push_command(
                concat!($target_name, "-target"),
                Command::new(&omicron_package)
                    .args(["--target", $target_name, "target", "create"])
                    .args($target_args),
            )
            .after("omicron-package");

            jobs.push_command(
                concat!($target_name, "-package"),
                Command::new(&omicron_package).args([
                    "--target",
                    $target_name,
                    "package",
                ]),
            )
            .after(concat!($target_name, "-target"));

            let proto_dir = tempdir.path().join("proto").join($target_name);
            jobs.push(
                concat!($target_name, "-proto"),
                build_proto_area(
                    WORKSPACE_DIR.join("out"),
                    proto_dir.clone(),
                    &$proto_packages,
                    manifest.clone(),
                ),
            )
            .after(concat!($target_name, "-package"));

            // The ${os_short_commit} token will be expanded by `helios-build`
            let image_name = format!(
                "{} {}/${{os_short_commit}} {}",
                $image_prefix,
                commit.chars().take(7).collect::<String>(),
                Utc::now().format("%Y-%m-%d %H:%M")
            );

            jobs.push_command(
                concat!($target_name, "-image"),
                Command::new("ptime")
                    .arg("-m")
                    .arg(args.helios_dir.join("helios-build"))
                    .arg("experiment-image")
                    .arg("-o") // output directory for image
                    .arg(args.output_dir.join($target_name))
                    .arg("-p") // use an external package repository
                    .arg(format!("helios-dev={}", HELIOS_REPO))
                    .arg("-F") // pass extra image builder features
                    .arg(format!("optever={}", OPTE_VERSION.trim()))
                    .arg("-P") // include all files from extra proto area
                    .arg(proto_dir.join("root"))
                    .arg("-N") // image name
                    .arg(image_name)
                    .arg("-s") // tempdir name suffix
                    .arg($target_name)
                    .args($image_build_args)
                    .current_dir(&args.helios_dir)
                    .env("IMAGE_DATASET", &$image_dataset),
            )
            .after("helios-setup")
            .after(concat!($target_name, "-proto"));
        };
    }

    os_image_jobs! {
        target_name: "host",
        target_args: [
            "--image",
            "standard",
            "--machine",
            "gimlet",
            "--switch",
            "asic",
            "--rack-topology",
            "multi-sled"
        ],
        proto_packages: HOST_IMAGE_PACKAGES,
        image_prefix: "ci",
        image_build_args: ["-B"],
        image_dataset: args.host_dataset,
    }
    os_image_jobs! {
        target_name: "recovery",
        target_args: ["--image", "trampoline"],
        proto_packages: RECOVERY_IMAGE_PACKAGES,
        image_prefix: "recovery",
        image_build_args: ["-R"],
        image_dataset: args.recovery_dataset,
    }

    // Build the recovery target after we build the host target. Only one
    // of these will build at a time since Cargo locks its target directory;
    // since host-package and host-image both take longer than their recovery
    // counterparts, this should be the fastest option to go first.
    jobs.select("recovery-package").after("host-package");
    if args.host_dataset == args.recovery_dataset {
        // If the datasets are the same, we can't parallelize these.
        jobs.select("recovery-image").after("host-image");
    }

    // RUN JOBS ===============================================================
    let start = Instant::now();
    jobs.run_all().await?;
    debug!(
        logger,
        "all jobs completed in {:?}",
        Instant::now().saturating_duration_since(start)
    );

    // fs::create_dir_all(host_proto.path().join("root/root"))?;
    // fs::write(
    //     host_proto.path().join("root/root/.profile"),
    //     "# Add opteadm, ddadm, oxlog to PATH\n\
    //     export PATH=$PATH:/opt/oxide/opte/bin:/opt/oxide/mg-ddm:/opt/oxide/oxlog\n"
    // )?;

    Ok(())
}

async fn build_proto_area(
    package_dir: Utf8PathBuf,
    proto_dir: Utf8PathBuf,
    packages: &'static [(&'static str, InstallMethod)],
    manifest: Arc<Config>,
) -> Result<()> {
    let opt_oxide = proto_dir.join("root/opt/oxide");
    let manifest_site = proto_dir.join("root/lib/svc/manifest/site");
    fs::create_dir_all(&opt_oxide).await?;

    for &(package_name, method) in packages {
        let package =
            manifest.packages.get(package_name).expect("checked in preflight");
        match method {
            InstallMethod::Install => {
                let path = opt_oxide.join(&package.service_name);
                fs::create_dir(&path).await?;

                let cloned_path = path.clone();
                let cloned_package_dir = package_dir.to_owned();
                tokio::task::spawn_blocking(move || -> Result<()> {
                    let mut archive = tar::Archive::new(std::fs::File::open(
                        cloned_package_dir
                            .join(package_name)
                            .with_extension("tar"),
                    )?);
                    archive.unpack(cloned_path).with_context(|| {
                        format!("failed to extract {}.tar.gz", package_name)
                    })?;
                    Ok(())
                })
                .await??;

                let smf_manifest = path.join("pkg").join("manifest.xml");
                if smf_manifest.exists() {
                    fs::create_dir_all(&manifest_site).await?;
                    fs::rename(
                        smf_manifest,
                        manifest_site
                            .join(&package.service_name)
                            .with_extension("xml"),
                    )
                    .await?;
                }
            }
            InstallMethod::Bundle => {
                fs::copy(
                    package_dir.join(format!("{}.tar.gz", package_name)),
                    opt_oxide.join(format!("{}.tar.gz", package.service_name)),
                )
                .await?;
            }
        }
    }

    Ok(())
}
