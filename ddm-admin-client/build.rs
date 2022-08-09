// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use anyhow::bail;
use anyhow::Context;
use anyhow::Result;
use omicron_zone_package::config::Config;
use omicron_zone_package::config::ExternalPackageSource;
use quote::quote;
use std::env;
use std::fs;
use std::path::Path;

fn main() -> Result<()> {
    // Find the current maghemite repo commit from our package manifest.
    let manifest = fs::read("../package-manifest.toml")
        .context("failed to read ../package-manifest.toml")?;
    println!("cargo:rerun-if-changed=../package-manifest.toml");

    let config: Config = toml::de::from_slice(&manifest)
        .context("failed to parse ../package-manifest.toml")?;
    let maghemite = config
        .external_packages
        .get("maghemite")
        .context("missing maghemite package in ../package-manifest.toml")?;
    let commit = match &maghemite.source {
        ExternalPackageSource::Manual => {
            bail!("maghemite external package must have type `prebuilt`")
        }
        ExternalPackageSource::Prebuilt { commit, .. } => commit,
    };

    // Report a relatively verbose error if we haven't downloaded the requisite
    // openapi spec.
    let local_path = format!("../out/downloads/ddm-admin-{commit}.json");
    if !Path::new(&local_path).exists() {
        bail!("{local_path} doesn't exist; rerun `tools/ci_download_maghemite_openapi` (after updating `tools/maghemite_openapi_version` if the maghemite commit in package-manifest.toml has changed)");
    }
    println!("cargo:rerun-if-changed={local_path}");

    let spec = {
        let bytes = fs::read(&local_path)
            .with_context(|| format!("failed to read {local_path}"))?;
        serde_json::from_slice(&bytes).with_context(|| {
            format!("failed to parse {local_path} as openapi spec")
        })?
    };

    let content = progenitor::Generator::new(
        progenitor::GenerationSettings::new()
            .with_inner_type(quote!(slog::Logger))
            .with_pre_hook(quote! {
                |log: &slog::Logger, request: &reqwest::Request| {
                    slog::debug!(log, "client request";
                        "method" => %request.method(),
                        "uri" => %request.url(),
                        "body" => ?&request.body(),
                    );
                }
            })
            .with_post_hook(quote! {
                |log: &slog::Logger, result: &Result<_, _>| {
                    slog::debug!(log, "client response"; "result" => ?result);
                }
            }),
    )
    .generate_text(&spec)
    .with_context(|| {
        format!("failed to generate progenitor client from {local_path}")
    })?;

    let out_file =
        Path::new(&env::var("OUT_DIR").expect("OUT_DIR env var not set"))
            .join("ddm-admin-client.rs");

    fs::write(&out_file, content).with_context(|| {
        format!("failed to write client to {}", out_file.display())
    })?;

    Ok(())
}
