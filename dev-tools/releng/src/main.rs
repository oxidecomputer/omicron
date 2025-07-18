// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod cmd;
mod helios;
mod hubris;
mod job;
mod tuf;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use chrono::Utc;
use clap::Parser;
use fs_err::tokio as fs;
use omicron_zone_package::config::Config;
use omicron_zone_package::config::PackageName;
use semver::Version;
use slog::Drain;
use slog::Logger;
use slog::debug;
use slog::error;
use slog::info;
use slog_term::FullFormat;
use slog_term::TermDecorator;
use tokio::sync::Semaphore;

use crate::cmd::Command;
use crate::job::Jobs;

/// The base version we're currently building. Build information is appended to
/// this later on.
///
/// Under current policy, each new release is a major version bump, and
/// generally referred to only by the major version (e.g. 8.0.0 is referred
/// to as "v8", "version 8", or "release 8" to customers). The use of semantic
/// versioning is mostly to hedge for perhaps wanting something more granular in
/// the future.
const BASE_VERSION: Version = Version::new(16, 0, 0);

const RETRY_ATTEMPTS: usize = 3;

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
const HOST_IMAGE_PACKAGES: [(&PackageName, InstallMethod); 8] = [
    (&PackageName::new_const("mg-ddm-gz"), InstallMethod::Install),
    (&PackageName::new_const("omicron-sled-agent"), InstallMethod::Install),
    (&PackageName::new_const("overlay"), InstallMethod::Bundle),
    (&PackageName::new_const("oxlog"), InstallMethod::Install),
    (&PackageName::new_const("propolis-server"), InstallMethod::Bundle),
    (&PackageName::new_const("pumpkind-gz"), InstallMethod::Install),
    (&PackageName::new_const("crucible-dtrace"), InstallMethod::Install),
    (&PackageName::new_const("switch-asic"), InstallMethod::Bundle),
];
/// Packages to install or bundle in the recovery (trampoline) OS image.
const RECOVERY_IMAGE_PACKAGES: [(&PackageName, InstallMethod); 2] = [
    (&PackageName::new_const("installinator"), InstallMethod::Install),
    (&PackageName::new_const("mg-ddm-gz"), InstallMethod::Install),
];
const TUF_PACKAGES: [&PackageName; 12] = [
    &PackageName::new_const("clickhouse_keeper"),
    &PackageName::new_const("clickhouse_server"),
    &PackageName::new_const("clickhouse"),
    &PackageName::new_const("cockroachdb"),
    &PackageName::new_const("crucible-pantry-zone"),
    &PackageName::new_const("crucible-zone"),
    &PackageName::new_const("external-dns"),
    &PackageName::new_const("internal-dns"),
    &PackageName::new_const("nexus"),
    &PackageName::new_const("ntp"),
    &PackageName::new_const("oximeter"),
    &PackageName::new_const("probe"),
];

const HELIOS_REPO: &str = "https://pkg.oxide.computer/helios/2/dev/";

static WORKSPACE_DIR: LazyLock<Utf8PathBuf> = LazyLock::new(|| {
    // $CARGO_MANIFEST_DIR is at `.../omicron/dev-tools/releng`
    let mut dir =
        Utf8PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect(
            "$CARGO_MANIFEST_DIR is not set; run this via `cargo xtask releng`",
        ));
    dir.pop();
    dir.pop();
    dir
});

/// Run the Oxide release engineering process and produce a TUF repo that can be
/// used to update a rack.
///
/// For more information, see `docs/releng.adoc` in the Omicron repository.
///
/// Note that `--host-dataset` and `--recovery-dataset` must be set to different
/// values to build the two OS images in parallel. This is strongly recommended.
#[derive(Parser)]
#[command(name = "cargo xtask releng", bin_name = "cargo xtask releng")]
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

    /// Path to the directory containing the rustup proxy `bin/cargo` (usually
    /// set by Cargo)
    #[clap(long, env = "CARGO_HOME")]
    cargo_home: Option<Utf8PathBuf>,

    /// Path to the git binary
    #[clap(long, env = "GIT", default_value = "git")]
    git_bin: Utf8PathBuf,

    /// Path to a pre-built omicron-package binary (skips building if set)
    #[clap(long, env = "OMICRON_PACKAGE")]
    omicron_package_bin: Option<Utf8PathBuf>,

    /// Build the helios OS image from local sources.
    #[clap(long)]
    helios_local: bool,

    /// Extra manifest to be merged with the rest of the repo
    #[clap(long)]
    extra_manifest: Option<Utf8PathBuf>,

    /// Extra helios-dev origin to be passed along to helios-build
    #[clap(long)]
    extra_origin: Option<String>,

    /// Create and use an `omicron-ci-incorporation` package during the image
    /// build. The incorporation can then be reused during branching to pin
    /// packages in the image to the same version.
    ///
    /// This option does nothing if the `[helios]` table is present in
    /// tools/pins.toml.
    #[clap(long, conflicts_with("helios_local"))]
    mkincorp: bool,
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

#[expect(
    clippy::disallowed_macros,
    reason = "this is a dev-tool, and avoiding a dependency on \
     `omicron-runtime` helps minimize compile time."
)]
#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let decorator = TermDecorator::new().build();
    let drain = FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = Logger::root(drain, slog::o!());

    // Change the working directory to the workspace root.
    debug!(logger, "changing working directory to {}", *WORKSPACE_DIR);
    std::env::set_current_dir(&*WORKSPACE_DIR)
        .context("failed to change working directory to workspace root")?;

    // Determine the target directory.
    let metadata = cargo_metadata::MetadataCommand::new()
        .no_deps()
        .exec()
        .context("failed to get cargo metadata")?;
    let target_dir = metadata.target_directory;

    // We build everything in Omicron with $CARGO, but we need to use the rustup
    // proxy for Cargo when outside Omicron.
    let rustup_cargo = match &args.cargo_home {
        Some(path) => path.join("bin/cargo"),
        None => Utf8PathBuf::from("cargo"),
    };
    // `var_os` here is deliberate: if CARGO is set to a non-UTF-8 path we
    // shouldn't do something confusing as a fallback.
    let cargo = match std::env::var_os("CARGO") {
        Some(path) => Utf8PathBuf::try_from(std::path::PathBuf::from(path))
            .context("$CARGO is not valid UTF-8")?,
        None => rustup_cargo.clone(),
    };

    // Read pins.toml.
    let pins = omicron_pins::Pins::read_from_dir(&metadata.workspace_root)?;

    let permits = Arc::new(Semaphore::new(
        std::thread::available_parallelism()
            .context("couldn't get available parallelism")?
            .into(),
    ));

    let commit = Command::new(&args.git_bin)
        .args(["rev-parse", "HEAD"])
        .ensure_stdout(&logger)
        .await?
        .trim()
        .to_owned();

    let mut version = BASE_VERSION.clone();
    // Differentiate between CI and local builds. We use `0.word` as the
    // prerelease field because it comes before `alpha`.
    version.pre =
        if std::env::var_os("CI").is_some() { "0.ci" } else { "0.local" }
            .parse()?;
    // Set the build metadata to the current commit hash.
    let mut build = String::with_capacity(14);
    build.push_str("git");
    build.extend(commit.chars().take(11));
    version.build = build.parse()?;
    let version_str = version.to_string();
    info!(logger, "version: {}", version_str);

    let manifest = Arc::new(omicron_zone_package::config::parse_manifest(
        &fs::read_to_string(WORKSPACE_DIR.join("package-manifest.toml"))
            .await?,
    )?);
    let opte_version =
        fs::read_to_string(WORKSPACE_DIR.join("tools/opte_version")).await?;

    let client = reqwest::ClientBuilder::new()
        .connect_timeout(Duration::from_secs(15))
        .timeout(Duration::from_secs(120))
        .tcp_keepalive(Duration::from_secs(60))
        .build()
        .context("failed to build reqwest client")?;

    // PREFLIGHT ==============================================================
    let mut preflight_ok = true;

    for package in HOST_IMAGE_PACKAGES
        .into_iter()
        .chain(RECOVERY_IMAGE_PACKAGES)
        .map(|(package, _)| package)
        .chain(TUF_PACKAGES)
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

    // Ensure the Helios checkout exists. If the directory exists and is
    // non-empty, check that the commit is correct.
    let helios_commit = if args.helios_dir.exists()
        && fs::read_dir(&args.helios_dir).await?.next_entry().await?.is_some()
    {
        let helios_commit = git_resolve_commit(
            &args.git_bin,
            &args.helios_dir,
            "HEAD",
            &logger,
        )
        .await?;
        if !args.ignore_helios_origin {
            if let Some(helios) = &pins.helios {
                if helios_commit != helios.commit {
                    error!(
                        logger,
                        "helios checkout at {0} is not at pinned commit {1}; \
                        checkout the correct commit, or run omicron-releng \
                        with --ignore-helios-origin or --helios-dir",
                        shell_words::quote(args.helios_dir.as_str()),
                        shell_words::quote(&helios.commit),
                    );
                    preflight_ok = false;
                }
            } else {
                // check that our helios clone is up to date
                Command::new(&args.git_bin)
                    .arg("-C")
                    .arg(&args.helios_dir)
                    // HEAD in a remote repository refers to the default
                    // branch, even if the default branch is renamed.
                    // `--no-write-fetch-head` avoids modifying FETCH_HEAD.
                    .args(["fetch", "--no-write-fetch-head", "origin", "HEAD"])
                    .ensure_success(&logger)
                    .await?;
                let upstream_commit = git_resolve_commit(
                    &args.git_bin,
                    &args.helios_dir,
                    "origin/HEAD",
                    &logger,
                )
                .await?;
                if helios_commit != upstream_commit {
                    error!(
                        logger,
                        "helios checkout at {0} is out-of-date; run \
                        `git pull -C {0}`, or run omicron-releng with \
                        --ignore-helios-origin or --helios-dir",
                        shell_words::quote(args.helios_dir.as_str())
                    );
                    preflight_ok = false;
                }
            }
        }
        helios_commit
    } else {
        info!(logger, "cloning helios to {}", args.helios_dir);
        Command::new(&args.git_bin)
            .args(["clone", "https://github.com/oxidecomputer/helios.git"])
            .arg(&args.helios_dir)
            .ensure_success(&logger)
            .await?;
        if let Some(helios) = &pins.helios {
            info!(
                logger,
                "checking out {} to {}", args.helios_dir, helios.commit,
            );
            Command::new(&args.git_bin)
                .arg("-C")
                .arg(&args.helios_dir)
                .args(["checkout", &helios.commit])
                .ensure_success(&logger)
                .await?;
            helios.commit.clone()
        } else {
            git_resolve_commit(&args.git_bin, &args.helios_dir, "HEAD", &logger)
                .await?
        }
    };

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
    let mut jobs = Jobs::new(&logger, permits.clone(), &args.output_dir);

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
            .env("BUILD_OS", "no")
            .env_remove("CARGO")
            .env_remove("RUSTUP_TOOLCHAIN"),
    );

    // Record the commit of helios.git and everything helios-setup cloned
    {
        let git_bin = args.git_bin.clone();
        let output_dir = args.output_dir.clone();
        let helios_dir = args.helios_dir.clone();
        let logger = logger.clone();
        jobs.push("helios-record", async move {
            let projects_dir = helios_dir.join("projects");
            let mut projects = BTreeMap::new();
            let mut read_dir = fs::read_dir(&projects_dir).await?;
            while let Some(entry) = read_dir.next_entry().await? {
                let name =
                    Utf8PathBuf::try_from(PathBuf::from(entry.file_name()))?;
                let commit = git_resolve_commit(
                    &git_bin,
                    &projects_dir.join(&name),
                    "HEAD",
                    &logger,
                )
                .await?;
                projects.insert(name, commit);
            }

            let json = format!(
                "{:#}\n",
                serde_json::json!({
                    "helios": helios_commit,
                    "projects": projects,
                })
            );
            fs::write(output_dir.join("helios.json"), json).await?;
            Ok(())
        })
        .after("helios-setup");
    }

    let omicron_package = if let Some(path) = &args.omicron_package_bin {
        // omicron-package is provided, so don't build it.
        jobs.push("omicron-package", std::future::ready(Ok(())));
        path.clone()
    } else {
        jobs.push_command(
            "omicron-package",
            Command::new("ptime").args([
                "-m",
                cargo.as_str(),
                "build",
                "--locked",
                "--release",
                "--bin",
                "omicron-package",
            ]),
        );
        target_dir.join("release/omicron-package")
    };

    // Generate `omicron-package stamp` jobs for a list of packages as a nested
    // `Jobs`. Returns the selector for the outer job.
    //
    // (This could be a function but the resulting function would have too many
    // confusable arguments.)
    macro_rules! stamp_packages {
        ($name:expr, $target:expr, $packages:expr) => {{
            let mut stamp_jobs =
                Jobs::new(&logger, permits.clone(), &args.output_dir);
            for package in $packages {
                stamp_jobs.push_command(
                    format!("stamp-{}", package),
                    Command::new(&omicron_package)
                        .args([
                            "--target",
                            $target.as_str(),
                            "--artifacts",
                            $target.artifacts_path(&args).as_str(),
                            "stamp",
                            package.as_str(),
                            &version_str,
                        ])
                        .env_remove("CARGO_MANIFEST_DIR"),
                );
            }
            jobs.push($name, stamp_jobs.run_all())
        }};
    }

    let incorp_version = format!("{}.0.0.0", version.major);
    if args.mkincorp {
        let action = if let Some(helios) = &pins.helios {
            helios::Action::Passthru { version: helios.incorporation.clone() }
        } else {
            helios::Action::Generate { version: incorp_version.clone() }
        };
        helios::push_incorporation_jobs(
            &mut jobs,
            &logger,
            &args.output_dir,
            action,
        )
        .await?;
    } else {
        jobs.push("helios-incorp", std::future::ready(Ok(())));
    }

    for target in [Target::Host, Target::Recovery] {
        let artifacts_path = target.artifacts_path(&args);

        // omicron-package target create
        jobs.push_command(
            format!("{}-target", target),
            Command::new(&omicron_package)
                .args([
                    "--target",
                    target.as_str(),
                    "--artifacts",
                    artifacts_path.as_str(),
                    "target",
                    "create",
                    "--preset",
                    target.as_str(),
                ])
                // Note: Do not override the preset by adding arguments like
                // `-m`/`--machine` here, or anywhere else in the releng
                // tooling! All release targets must be configured entirely via
                // the `target.preset` table in `package-manifest.toml`.
                .env_remove("CARGO_MANIFEST_DIR"),
        )
        .after("omicron-package");

        // omicron-package package
        jobs.push_command(
            format!("{}-package", target),
            Command::new(&omicron_package)
                .args([
                    "--target",
                    target.as_str(),
                    "--artifacts",
                    artifacts_path.as_str(),
                    "package",
                ])
                .env_remove("CARGO_MANIFEST_DIR"),
        )
        .after(format!("{}-target", target));

        // omicron-package stamp
        stamp_packages!(
            format!("{}-stamp", target),
            target,
            target.proto_package_names()
        )
        .after(format!("{}-package", target));

        // [build proto dir, to be overlaid into disk image]
        let proto_dir = tempdir.path().join("proto").join(target.as_str());
        jobs.push(
            format!("{}-proto", target),
            build_proto_area(
                artifacts_path,
                proto_dir.clone(),
                target.proto_packages(),
                manifest.clone(),
            ),
        )
        .after(format!("{}-stamp", target));

        // The ${os_short_commit} token will be expanded by `helios-build`
        let image_name = format!(
            "{} {}/${{os_short_commit}} {}",
            target.image_prefix(),
            commit.chars().take(7).collect::<String>(),
            Utc::now().format("%Y-%m-%d %H:%M")
        );

        let mut image_cmd = Command::new("ptime")
            .arg("-m")
            .arg(args.helios_dir.join("helios-build"))
            .arg("experiment-image")
            .arg("-o") // output directory for image
            .arg(args.output_dir.join(format!("os-{}", target)))
            .arg("-F") // pass extra image builder features
            .arg(format!("optever={}", opte_version.trim()))
            .arg("-P") // include all files from extra proto area
            .arg(proto_dir.join("root"))
            .arg("-N") // image name
            .arg(image_name)
            .arg("-s") // tempdir name suffix
            .arg(target.as_str())
            .args(target.image_build_args())
            .current_dir(&args.helios_dir)
            .env(
                "IMAGE_DATASET",
                match target {
                    Target::Host => &args.host_dataset,
                    Target::Recovery => &args.recovery_dataset,
                },
            )
            .env_remove("CARGO")
            .env_remove("RUSTUP_TOOLCHAIN");

        if let Some(extra_origin) = &args.extra_origin {
            image_cmd = image_cmd
                .arg("-p")
                .arg(format!("{}={extra_origin}", helios::PUBLISHER));
        }

        if let Some(helios) = &pins.helios {
            image_cmd = image_cmd.arg("-F").arg(format!(
                "extra_packages+=/{}@{}",
                helios::INCORP_NAME,
                helios.incorporation
            ));
        } else if args.mkincorp {
            image_cmd = image_cmd
                .arg("-F")
                .arg(format!(
                    "extra_packages+=/{}@{incorp_version}",
                    helios::INCORP_NAME
                ))
                .arg("-p")
                .arg(format!(
                    "{}=file://{}",
                    helios::PUBLISHER,
                    args.output_dir
                        .canonicalize_utf8()
                        .with_context(|| format!(
                            "failed to canonicalize {}",
                            args.output_dir
                        ))?
                        .join(helios::ARCHIVE_PATH)
                ));
        }

        if !args.helios_local {
            image_cmd = image_cmd
                .arg("-p") // use an external package repository
                .arg(format!("helios-dev={HELIOS_REPO}"))
        }

        // helios-build experiment-image
        jobs.push_command(format!("{}-image", target), image_cmd)
            .after("helios-setup")
            .after("helios-incorp")
            .after(format!("{}-proto", target));
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

    // Set up /root/.profile in the host OS image.
    jobs.push(
        "host-profile",
        host_add_root_profile(tempdir.path().join("proto/host/root/root")),
    )
    .after("host-proto");
    jobs.select("host-image").after("host-profile");

    stamp_packages!("tuf-stamp", Target::Host, TUF_PACKAGES)
        .after("host-stamp")
        .after("recovery-stamp");

    // Run `cargo xtask verify-libraries --release`. (This was formerly run in
    // the build-and-test Buildomat job, but this fits better here where we've
    // already built most of the binaries.)
    jobs.push_command(
        "verify-libraries",
        Command::new(&cargo).args(["xtask", "verify-libraries", "--release"]),
    )
    .after("host-package")
    .after("recovery-package");

    for (name, base_url) in [
        ("staging", "https://permslip-staging.corp.oxide.computer"),
        ("production", "https://signer-us-west.corp.oxide.computer"),
    ] {
        jobs.push(
            format!("hubris-{}", name),
            hubris::fetch_hubris_artifacts(
                logger.clone(),
                base_url,
                client.clone(),
                WORKSPACE_DIR.join(format!("tools/permslip_{}", name)),
                args.output_dir.join(format!("hubris-{}", name)),
            ),
        );
    }

    jobs.push(
        "tuf-repo",
        tuf::build_tuf_repo(
            logger.clone(),
            args.output_dir.clone(),
            version,
            manifest,
            args.extra_manifest,
        ),
    )
    .after("tuf-stamp")
    .after("host-image")
    .after("recovery-image")
    .after("hubris-staging")
    .after("hubris-production");

    // RUN JOBS ===============================================================
    let start = Instant::now();
    jobs.run_all().await?;
    info!(
        logger,
        "all jobs completed in {:?}",
        Instant::now().saturating_duration_since(start)
    );
    Ok(())
}

#[derive(Clone, Copy)]
enum Target {
    Host,
    Recovery,
}

impl Target {
    fn as_str(self) -> &'static str {
        match self {
            Target::Host => "host",
            Target::Recovery => "recovery",
        }
    }

    fn artifacts_path(self, args: &Args) -> Utf8PathBuf {
        match self {
            Target::Host => WORKSPACE_DIR.join("out"),
            Target::Recovery => {
                args.output_dir.join(format!("artifacts-{}", self))
            }
        }
    }

    fn proto_packages(
        self,
    ) -> &'static [(&'static PackageName, InstallMethod)] {
        match self {
            Target::Host => &HOST_IMAGE_PACKAGES,
            Target::Recovery => &RECOVERY_IMAGE_PACKAGES,
        }
    }

    fn proto_package_names(self) -> impl Iterator<Item = &'static PackageName> {
        self.proto_packages().iter().map(|(name, _)| *name)
    }

    fn image_prefix(self) -> &'static str {
        match self {
            Target::Host => "ci",
            Target::Recovery => "recovery",
        }
    }

    fn image_build_args(self) -> &'static [&'static str] {
        match self {
            Target::Host => &[
                "-B", // include omicron1 brand
            ],
            Target::Recovery => &[
                "-R", // recovery image
            ],
        }
    }
}

impl std::fmt::Display for Target {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

async fn build_proto_area(
    mut package_dir: Utf8PathBuf,
    proto_dir: Utf8PathBuf,
    packages: &'static [(&'static PackageName, InstallMethod)],
    manifest: Arc<Config>,
) -> Result<()> {
    let opt_oxide = proto_dir.join("root/opt/oxide");
    let manifest_site = proto_dir.join("root/lib/svc/manifest/site");
    fs::create_dir_all(&opt_oxide).await?;

    // use the stamped packages
    package_dir.push("versioned");

    for &(package_name, method) in packages {
        let package =
            manifest.packages.get(package_name).expect("checked in preflight");
        match method {
            InstallMethod::Install => {
                let path = opt_oxide.join(package.service_name.as_str());
                fs::create_dir(&path).await?;

                let cloned_path = path.clone();
                let cloned_package_dir = package_dir.to_owned();
                tokio::task::spawn_blocking(move || -> Result<()> {
                    let mut archive = tar::Archive::new(fs_err::File::open(
                        cloned_package_dir
                            .join(package_name.as_str())
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
                            .join(package.service_name.as_str())
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

async fn host_add_root_profile(host_proto_root: Utf8PathBuf) -> Result<()> {
    fs::create_dir_all(&host_proto_root).await?;
    fs::write(
        host_proto_root.join(".profile"),
        "# Add opteadm, ddadm, oxlog to PATH\n\
        export PATH=$PATH:/opt/oxide/opte/bin:/opt/oxide/mg-ddm:/opt/oxide/oxlog\n",
    ).await?;
    Ok(())
}

async fn git_resolve_commit(
    git_bin: &Utf8Path,
    repo: &Utf8Path,
    what: &str,
    logger: &Logger,
) -> Result<String> {
    Ok(Command::new(git_bin)
        .args(["-C", repo.as_str(), "rev-parse", "--verify"])
        .arg(format!("{what}^{{commit}}"))
        .ensure_stdout(&logger)
        .await?
        .trim()
        .to_owned())
}
