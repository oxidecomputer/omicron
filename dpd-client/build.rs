// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

// use anyhow::bail;
use anyhow::Context;
use anyhow::Result;
// use omicron_zone_package::config::Config;
// use omicron_zone_package::package::PackageSource;
use quote::quote;
use std::env;
use std::fs;
use std::path::Path;

fn main() -> Result<()> {
    // TODO: pull latest published dendrite specification
    // We currently have to pull the dendrite openapi specification
    // because dendrite is a private repository. We're not pulling the
    // latest specification because the updated API and binary require
    // some rework in Nexus. For the meantime, we are building from a
    // local copy of the older api specification (and running against
    // an older version of dendrite).

    // Find the current dendrite repo commit from our package manifest.
    // let manifest = fs::read("../package-manifest.toml")
    //     .context("failed to read ../package-manifest.toml")?;
    // println!("cargo:rerun-if-changed=../package-manifest.toml");

    // let config: Config = toml::de::from_slice(&manifest)
    //     .context("failed to parse ../package-manifest.toml")?;

    // let dendrite = config
    //     .packages
    //     .get("dendrite-softnpu")
    //     .context("missing dendrite package in ../package-manifest.toml")?;

    // let local_path = match &dendrite.source {
    //     PackageSource::Prebuilt { commit, .. } => {
    //         // Report a relatively verbose error if we haven't downloaded the requisite
    //         // openapi spec.
    //         let local_path = format!("../out/downloads/dpd-{commit}.json");
    //         if !Path::new(&local_path).exists() {
    //             bail!("{local_path} doesn't exist; rerun `tools/ci_download_dpd_openapi` (after updating `tools/dpd_openapi_version` if the dendrite commit in package-manifest.toml has changed)");
    //         }
    //         println!("cargo:rerun-if-changed={local_path}");
    //         local_path
    //     }

    //     PackageSource::Manual => {
    //         let local_path = "../out/downloads/dpd-manual.json".to_string();
    //         if !Path::new(&local_path).exists() {
    //             bail!("{local_path} doesn't exist, please copy manually built dpd.json there!");
    //         }
    //         println!("cargo:rerun-if-changed={local_path}");
    //         local_path
    //     }

    //     _ => {
    //         bail!("dendrite external package must have type `prebuilt` or `manual`")
    //     }
    // };
    let local_path = "./dpd.json".to_string();

    let spec = {
        let bytes = fs::read(&local_path)
            .with_context(|| format!("failed to read {local_path}"))?;
        serde_json::from_slice(&bytes).with_context(|| {
            format!("failed to parse {local_path} as openapi spec")
        })?
    };

    let content = progenitor::Generator::new(
        progenitor::GenerationSettings::new()
            .with_inner_type(quote!(ClientState))
            .with_pre_hook(quote! {
                |state: &crate::ClientState, request: &reqwest::Request| {
                    slog::debug!(state.log, "client request";
                        "method" => %request.method(),
                        "uri" => %request.url(),
                        "body" => ?&request.body(),
                    );
                }
            })
            .with_post_hook(quote! {
                |state: &crate::ClientState, result: &Result<_, _>| {
                    slog::debug!(state.log, "client response"; "result" => ?result);
                }
            }),
    )
    .generate_text(&spec)
    .with_context(|| {
        format!("failed to generate progenitor client from {local_path}")
    })?;

    let out_file =
        Path::new(&env::var("OUT_DIR").expect("OUT_DIR env var not set"))
            .join("dpd-client.rs");

    fs::write(&out_file, content).with_context(|| {
        format!("failed to write client to {}", out_file.display())
    })?;

    Ok(())
}
