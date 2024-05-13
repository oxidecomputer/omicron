// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use camino::Utf8PathBuf;
use chrono::Duration;
use chrono::Timelike;
use chrono::Utc;
use fs_err::tokio as fs;
use fs_err::tokio::File;
use omicron_common::api::external::SemverVersion;
use omicron_common::api::internal::nexus::KnownArtifactKind;
use omicron_zone_package::config::Config;
use semver::Version;
use sha2::Digest;
use sha2::Sha256;
use slog::Logger;
use tokio::io::AsyncReadExt;
use tufaceous_lib::assemble::ArtifactManifest;
use tufaceous_lib::assemble::DeserializedArtifactData;
use tufaceous_lib::assemble::DeserializedArtifactSource;
use tufaceous_lib::assemble::DeserializedControlPlaneZoneSource;
use tufaceous_lib::assemble::DeserializedManifest;
use tufaceous_lib::assemble::OmicronRepoAssembler;
use tufaceous_lib::Key;

pub(crate) async fn build_tuf_repo(
    logger: Logger,
    output_dir: Utf8PathBuf,
    version: Version,
    package_manifest: Arc<Config>,
) -> Result<()> {
    // We currently go about this somewhat strangely; the old release
    // engineering process produced a Tufaceous manifest, and (the now very many
    // copies of) the TUF repo download-and-unpack script we use expects to be
    // able to download a manifest. So we build up a `DeserializedManifest`,
    // write it to disk, and then turn it into an `ArtifactManifest` to actually
    // build the repo.

    // Start a new manifest by loading the Hubris staging manifest.
    let mut manifest = DeserializedManifest::from_path(
        &output_dir.join("hubris-staging/manifest.toml"),
    )
    .context("failed to open intermediate hubris staging manifest")?;
    // Set the version.
    manifest.system_version = SemverVersion(version);

    // Load the Hubris production manifest and merge it in.
    let hubris_production = DeserializedManifest::from_path(
        &output_dir.join("hubris-production/manifest.toml"),
    )
    .context("failed to open intermediate hubris production manifest")?;
    for (kind, artifacts) in hubris_production.artifacts {
        manifest.artifacts.entry(kind).or_default().extend(artifacts);
    }

    // Add the OS images.
    manifest.artifacts.insert(
        KnownArtifactKind::Host,
        vec![DeserializedArtifactData {
            name: "host".to_string(),
            version: manifest.system_version.clone(),
            source: DeserializedArtifactSource::File {
                path: output_dir.join("os-host/os.tar.gz"),
            },
        }],
    );
    manifest.artifacts.insert(
        KnownArtifactKind::Trampoline,
        vec![DeserializedArtifactData {
            name: "trampoline".to_string(),
            version: manifest.system_version.clone(),
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
            version: manifest.system_version.clone(),
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
    manifest.verify_all_present()?;
    // Assemble the repo.
    let keys = vec![Key::generate_ed25519()];
    let expiry = Utc::now().with_nanosecond(0).unwrap() + Duration::weeks(1);
    OmicronRepoAssembler::new(
        &logger,
        manifest,
        keys,
        expiry,
        output_dir.join("repo.zip"),
    )
    .build()
    .await?;
    // Generate the checksum file.
    let mut hasher = Sha256::new();
    let mut buf = [0; 8192];
    let mut file = File::open(output_dir.join("repo.zip")).await?;
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    fs::write(
        output_dir.join("repo.zip.sha256.txt"),
        format!("{}\n", hex::encode(&hasher.finalize())),
    )
    .await?;

    Ok(())
}
