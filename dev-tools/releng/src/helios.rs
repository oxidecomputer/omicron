// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::fmt::Display;
use std::sync::LazyLock;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use fs_err::tokio as fs;
use fs_err::tokio::File;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_ord_map::Entry;
use iddqd::id_upcast;
use regex::Regex;
use serde::Deserialize;
use slog::Logger;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;

use crate::HELIOS_PKGREPO;
use crate::Jobs;
use crate::cmd::Command;

pub const INCORP_NAME: &str =
    "consolidation/oxide/omicron-release-incorporation";
const MANIFEST_PATH: &str = "incorporation.p5m";
const REPO_PATH: &str = "incorporation";
pub const ARCHIVE_PATH: &str = "incorporation.p5p";

pub const PUBLISHER: &str = "helios";
pub const OSRELEASE: &str = "5.11";

pub(crate) enum Action {
    Generate { version: String },
    Passthru { version: String },
}

pub(crate) async fn push_incorporation_jobs(
    jobs: &mut Jobs,
    logger: &Logger,
    output_dir: &Utf8Path,
    action: Action,
) -> Result<()> {
    let manifest_path = output_dir.join(MANIFEST_PATH);
    let repo_path = output_dir.join(REPO_PATH);
    let archive_path = output_dir.join(ARCHIVE_PATH);

    fs::remove_dir_all(&repo_path).await.or_else(ignore_not_found)?;
    fs::remove_file(&archive_path).await.or_else(ignore_not_found)?;

    match action {
        Action::Generate { version } => {
            jobs.push(
                "incorp-manifest",
                generate_incorporation_manifest(
                    logger.clone(),
                    manifest_path.clone(),
                    version,
                ),
            );
        }
        Action::Passthru { version } => {
            jobs.push(
                "incorp-manifest",
                passthru_incorporation_manifest(
                    logger.clone(),
                    manifest_path.clone(),
                    version,
                ),
            );
        }
    }

    jobs.push_command(
        "incorp-fmt",
        Command::new("pkgfmt").args(["-u", "-f", "v2", manifest_path.as_str()]),
    )
    .after("incorp-manifest");

    jobs.push_command(
        "incorp-create",
        Command::new("pkgrepo").args(["create", repo_path.as_str()]),
    );

    let path_args = ["-s", repo_path.as_str()];
    jobs.push_command(
        "incorp-publisher",
        Command::new("pkgrepo")
            .arg("add-publisher")
            .args(&path_args)
            .arg(PUBLISHER),
    )
    .after("incorp-create");

    jobs.push_command(
        "incorp-pkgsend",
        Command::new("pkgsend")
            .arg("publish")
            .args(&path_args)
            .arg(manifest_path),
    )
    .after("incorp-fmt")
    .after("incorp-publisher");

    jobs.push_command(
        "helios-incorp",
        Command::new("pkgrecv")
            .args(path_args)
            .args(["-a", "-d", archive_path.as_str()])
            .args(["-m", "latest", "-v", "*"]),
    )
    .after("incorp-pkgsend");

    Ok(())
}

#[derive(Debug, Clone, Copy, Deserialize)]
struct Package<'a> {
    #[serde(borrow, flatten)]
    fmri: Fmri<'a>,
    flags: Flags,
}

impl IdOrdItem for Package<'_> {
    type Key<'a>
        = &'a str
    where
        Self: 'a;

    fn key(&self) -> Self::Key<'_> {
        self.fmri.name
    }

    id_upcast!();
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq)]
struct Fmri<'a> {
    name: &'a str,
    release: &'a str,
    osrelease: &'a str,
    branch: &'a str,
    timestamp: Option<&'a str>,
}

impl<'a> Fmri<'a> {
    fn new(s: &'a str) -> Result<Self> {
        static RE: LazyLock<Regex> = LazyLock::new(|| {
            // NOTE: This is not a perfectly-accurate FMRI regex; it's intending
            // to balance readability while avoiding ambiguity.
            Regex::new(
                r"(?x)
                ^(?:(?:pkg:)?(?://[[:alnum:]_.+-]*/|/))?
                (?<name>[[:alnum:]][[:alnum:]/_.+-]*)
                @(?<release>[0-9][0-9.]*)
                (?:,(?<osrelease>[0-9][0-9.]*))?
                -(?<branch>[0-9][0-9.]*)
                (?::(?<timestamp>[0-9]{8}T[0-9]{6}Z$))?$",
            )
            .unwrap()
        });

        let Some(captures) = RE.captures(s) else {
            bail!("invalid FMRI: {s}");
        };
        Ok(Self {
            name: captures.name("name").unwrap().as_str(),
            release: captures.name("release").unwrap().as_str(),
            osrelease: captures
                .name("osrelease")
                .map(|m| m.as_str())
                .unwrap_or(OSRELEASE),
            branch: captures.name("branch").unwrap().as_str(),
            timestamp: captures.name("timestamp").map(|m| m.as_str()),
        })
    }

    fn matches(self, other: Self) -> bool {
        self.name == other.name
            && self.release == other.release
            && self.osrelease == other.osrelease
            && self.branch == other.branch
            && (self.timestamp.is_none()
                || other.timestamp.is_none()
                || self.timestamp == other.timestamp)
    }
}

impl Display for Fmri<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "pkg:/{}", self.name)?;
        write!(f, "@{},{}-{}", self.release, self.osrelease, self.branch)?;
        if let Some(timestamp) = self.timestamp {
            write!(f, ":{timestamp}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct Flags {
    obsolete: bool,
}

impl<'de> Deserialize<'de> for Flags {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = <&str>::deserialize(deserializer)?;
        if s.len() == 3 {
            Ok(Self { obsolete: s.ends_with('o') })
        } else {
            Ok(Self::default())
        }
    }
}

async fn incorporation_fmri(logger: &Logger, release: &str) -> Result<String> {
    let stdout = Command::new("pkg")
        .args(["list", "-g", HELIOS_PKGREPO, "-H"])
        .args(["-o", "branch", "-n", "release/name"])
        .ensure_stdout(&logger)
        .await?;
    let fmri = Fmri {
        name: INCORP_NAME,
        release,
        osrelease: OSRELEASE,
        branch: stdout.trim(),
        timestamp: None,
    };
    Ok(fmri.to_string())
}

async fn generate_incorporation_manifest(
    logger: Logger,
    path: Utf8PathBuf,
    version: String,
) -> Result<()> {
    let mut manifest = BufWriter::new(File::create(path).await?);
    let fmri = incorporation_fmri(&logger, &version).await?;
    let preamble = format!(
        r#"set name=pkg.fmri value={fmri}
set name=pkg.summary value="Incorporation to constrain software delivered in Omicron Release V{version} images"
set name=info.classification value="org.opensolaris.category.2008:Meta Packages/Incorporations"
set name=variant.opensolaris.zone value=global value=nonglobal
"#
    );
    manifest.write_all(preamble.as_bytes()).await?;

    let stdout = Command::new("pkg")
        .args(["list", "-g", HELIOS_PKGREPO, "-n", "-F", "json"])
        .args(["-o", "name,release,osrelease,branch,timestamp,flags", "*"])
        .ensure_stdout(&logger)
        .await?;
    let mut packages: IdOrdMap<Package> = serde_json::from_str(&stdout)
        .context("failed to parse pkgrepo output")?;

    // Don't pin to a past version of our own incorporation.
    packages.remove(INCORP_NAME);
    // Don't pin opte; that's separately pinned in `tools/opte_version`.
    packages.remove("driver/network/opte");

    // Obsolete packages cannot be installed, so we filter them from the
    // generated incorporation.
    packages.retain(|package| !package.flags.obsolete);

    // Get the list of `type=incorporate` dependencies from `consolidation/*`
    // packages, and remove those FMRIs from our own incorporation. This
    // reduces the amount of modifications necessary to backport changes like
    // osnet-incorporation.
    let consolidations = packages
        .iter()
        .filter(|package| package.fmri.name.starts_with("consolidation/"))
        .map(|package| package.fmri.to_string());
    let stdout = Command::new("pkg")
        .args(["contents", "-g", HELIOS_PKGREPO, "-H", "-o", "fmri"])
        .args(["-t", "depend", "-a", "type=incorporate"])
        .args(consolidations)
        .ensure_stdout(&logger)
        .await?;
    for line in stdout.lines() {
        let fmri = Fmri::new(line)?;
        if let Entry::Occupied(entry) = packages.entry(fmri.name)
            && fmri.matches(entry.get().fmri)
        {
            entry.remove();
        }
    }

    for package in packages {
        let line = format!("depend type=incorporate fmri={}\n", package.fmri);
        manifest.write_all(line.as_bytes()).await?;
    }

    manifest.shutdown().await?;
    Ok(())
}

async fn passthru_incorporation_manifest(
    logger: Logger,
    path: Utf8PathBuf,
    version: String,
) -> Result<()> {
    let fmri = incorporation_fmri(&logger, &version).await?;
    let stdout = Command::new("pkgrepo")
        .args(["contents", "-m", "-s", HELIOS_PKGREPO, &fmri])
        .ensure_stdout(&logger)
        .await?;
    fs::write(&path, stdout).await?;
    Ok(())
}

fn ignore_not_found(err: std::io::Error) -> Result<(), std::io::Error> {
    if err.kind() == std::io::ErrorKind::NotFound { Ok(()) } else { Err(err) }
}
