// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::error::Error as _;
use std::fmt::Write;
use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use anyhow::ensure;
use camino::Utf8PathBuf;
use chrono::Duration;
use chrono::Timelike;
use chrono::Utc;
use fs_err::tokio as fs;
use fs_err::tokio::File;
use omicron_zone_package::config::Config;
use semver::Version;
use slog::Logger;
use tufaceous_artifact::ArtifactHash;
use tufaceous_artifact::ArtifactVersion;
use tufaceous_artifact::KnownArtifactKind;
use tufaceous_lib::Key;
use tufaceous_lib::assemble::ArtifactManifest;
use tufaceous_lib::assemble::DeserializedArtifactData;
use tufaceous_lib::assemble::DeserializedArtifactSource;
use tufaceous_lib::assemble::DeserializedControlPlaneZoneSource;
use tufaceous_lib::assemble::DeserializedManifest;
use tufaceous_lib::assemble::OmicronRepoAssembler;
use tufaceous_v2::RepositoryLoader;
use update_common::artifacts::{
    ArtifactsWithPlan, ControlPlaneZonesMode, VerificationMode,
};

pub(crate) async fn build_tuf_repo(
    logger: Logger,
    output_dir: Utf8PathBuf,
    version: Version,
    package_manifest: Arc<Config>,
    extra_manifest: Option<Utf8PathBuf>,
    threads: usize,
) -> Result<()> {
    let repo_path = output_dir.join("repo.zip");
    let sha256_path = output_dir.join("repo.zip.sha256.txt");

    // We currently go about this somewhat strangely; the old release
    // engineering process produced a Tufaceous manifest, and (the now very many
    // copies of) the TUF repo download-and-unpack script we use expects to be
    // able to download a manifest. So we build up a `DeserializedManifest`,
    // write it to disk, and then turn it into an `ArtifactManifest` to actually
    // build the repo.

    let artifact_version =
        version.to_string().parse::<ArtifactVersion>().with_context(|| {
            format!("failed to parse artifact version from {}", version)
        })?;

    // Start a new manifest by loading the Hubris staging manifest.
    let mut manifest = DeserializedManifest::from_path(
        &output_dir.join("hubris-staging/manifest.toml"),
    )
    .context("failed to open intermediate hubris staging manifest")?;
    // Set the version.
    manifest.system_version = version;

    // Load the Hubris production manifest and merge it in.
    let hubris_production = DeserializedManifest::from_path(
        &output_dir.join("hubris-production/manifest.toml"),
    )
    .context("failed to open intermediate hubris production manifest")?;
    for (kind, artifacts) in hubris_production.artifacts {
        manifest.artifacts.entry(kind).or_default().extend(artifacts);
    }

    if let Some(path) = extra_manifest {
        let m = DeserializedManifest::from_path(&path)
            .context("failed to open extra manifest")?;
        for (kind, artifacts) in m.artifacts {
            manifest.artifacts.entry(kind).or_default().extend(artifacts);
        }
    }

    // Add the OS images.
    manifest.artifacts.insert(
        KnownArtifactKind::Host,
        vec![DeserializedArtifactData {
            name: "host".to_string(),
            version: artifact_version.clone(),
            source: DeserializedArtifactSource::File {
                path: output_dir.join("os-host/os.tar.gz"),
            },
        }],
    );
    manifest.artifacts.insert(
        KnownArtifactKind::Trampoline,
        vec![DeserializedArtifactData {
            name: "trampoline".to_string(),
            version: artifact_version.clone(),
            source: DeserializedArtifactSource::File {
                path: output_dir.join("os-recovery/os.tar.gz"),
            },
        }],
    );

    // Add the control plane zones.
    let mut zones = Vec::new();
    for package in crate::TUF_PACKAGES {
        zones.push(DeserializedControlPlaneZoneSource::File {
            file_name: Some(format!(
                "{}.tar.gz",
                package_manifest
                    .packages
                    .get(package)
                    .expect("checked in preflight")
                    .service_name
            )),
            path: crate::WORKSPACE_DIR
                .join("out/versioned")
                .join(format!("{}.tar.gz", package)),
        });
    }

    manifest.artifacts.insert(
        KnownArtifactKind::ControlPlane,
        vec![DeserializedArtifactData {
            name: "control-plane".to_string(),
            version: artifact_version.clone(),
            source: DeserializedArtifactSource::CompositeControlPlane { zones },
        }],
    );

    // Serialize the manifest out.
    fs::write(
        output_dir.join("manifest.toml"),
        toml::to_string_pretty(&manifest)?.into_bytes(),
    )
    .await?;

    // Convert the manifest.
    let manifest = ArtifactManifest::from_deserialized(&output_dir, manifest)?;
    manifest.verify_all_semver()?;
    manifest.verify_all_present()?;
    // Assemble the repo.
    let keys = vec![Key::generate_ed25519()?];
    let expiry = Utc::now().with_nanosecond(0).unwrap() + Duration::weeks(1);
    OmicronRepoAssembler::new(
        &logger,
        manifest,
        keys,
        expiry,
        true,
        repo_path.clone(),
    )
    .build()
    .await?;

    // Load and check the repository with Tufaceous v2
    let repo = RepositoryLoader::new()
        .v1_compatibility(true)
        .unsafe_blindly_trust_repo()
        .compute_archive_sha256(true)
        .load_zip_path(repo_path.clone(), &logger)
        .await?;
    let sha256 =
        *repo.archive_sha256().context("archive sha256 was not calculated")?;
    fs::write(sha256_path, format!("{}\n", hex::encode(sha256))).await?;

    repo.verify_targets(threads).await?;

    let mut problems = String::new();
    for problem in repo.check_problems().await? {
        write!(&mut problems, "\n- {problem}")?;
        let mut source = problem.source();
        while let Some(s) = source {
            write!(&mut problems, ": {s}")?;
            source = s.source();
        }
    }
    ensure!(problems.is_empty(), "found compatibility problems:{problems}",);

    // Check that we haven't stepped on any rakes by attempting to generate a
    // plan from the zip
    for mode in [ControlPlaneZonesMode::Split, ControlPlaneZonesMode::Composite]
    {
        let file = File::open(&repo_path).await?.into_std().await;
        let repo_hash = ArtifactHash(sha256);
        let _ = ArtifactsWithPlan::from_zip(
            file,
            None,
            repo_hash,
            mode,
            VerificationMode::BlindlyTrustAnything,
            &logger,
        )
        .await
        .with_context(|| {
            format!("error reading generated TUF repo ({mode:?} control plane)")
        })?;
    }

    Ok(())
}
