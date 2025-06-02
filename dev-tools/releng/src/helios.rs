// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Context;
use anyhow::Result;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use fs_err::tokio as fs;
use fs_err::tokio::File;
use serde::Deserialize;
use slog::Logger;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;

use crate::HELIOS_REPO;
use crate::Jobs;
use crate::cmd::Command;

pub const INCORP_NAME: &str =
    "consolidation/oxide/omicron-release-incorporation";
const MANIFEST_PATH: &str = "incorporation.p5m";
const REPO_PATH: &str = "incorporation";
pub const ARCHIVE_PATH: &str = "incorporation.p5p";

pub const PUBLISHER: &str = "helios-dev";

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

async fn generate_incorporation_manifest(
    logger: Logger,
    path: Utf8PathBuf,
    version: String,
) -> Result<()> {
    #[derive(Deserialize, PartialEq, Eq, PartialOrd, Ord)]
    struct Package {
        fmri: String,
    }

    let mut manifest = BufWriter::new(File::create(path).await?);
    let preamble = format!(
        r#"set name=pkg.fmri value=pkg://{PUBLISHER}/{INCORP_NAME}@{version},5.11
set name=pkg.summary value="Incorporation to constrain software delivered in Omicron Release V{version} images"
set name=info.classification value="org.opensolaris.category.2008:Meta Packages/Incorporations"
set name=variant.opensolaris.zone value=global value=nonglobal
"#
    );
    manifest.write_all(preamble.as_bytes()).await?;

    let stdout = Command::new("pkg")
        .args(["list", "-g", HELIOS_REPO, "-F", "json"])
        .args(["-o", "fmri", "-n", "*"])
        .ensure_stdout(&logger)
        .await?;
    let packages: Vec<Package> = serde_json::from_str(&stdout)
        .context("failed to parse pkgrepo output")?;
    let prefix = format!("pkg://{PUBLISHER}/");
    for package in packages {
        let Some(partial) = package.fmri.strip_prefix(&prefix) else {
            continue;
        };
        let Some((package, _)) = partial.split_once('@') else {
            continue;
        };
        if package == INCORP_NAME || package == "driver/network/opte" {
            continue;
        }
        let line = format!("depend type=incorporate fmri=pkg:/{partial}\n");
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
    let stdout = Command::new("pkgrepo")
        .args(["contents", "-m", "-s", HELIOS_REPO])
        .arg(format!("pkg://{PUBLISHER}/{INCORP_NAME}@{version},5.11"))
        .ensure_stdout(&logger)
        .await?;
    fs::write(&path, stdout).await?;
    Ok(())
}

fn ignore_not_found(err: std::io::Error) -> Result<(), std::io::Error> {
    if err.kind() == std::io::ErrorKind::NotFound { Ok(()) } else { Err(err) }
}
