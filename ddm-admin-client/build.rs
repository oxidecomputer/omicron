// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use anyhow::Context;
use anyhow::Result;
use quote::quote;
use std::env;
use std::fs;
use std::path::Path;

fn main() -> Result<()> {
    // TODO-cleanup: Pull hash from package-manifest.toml and add a
    // rerun-if-changed rule
    let commit = "05bd6e627c78b95166e3d258aab8edb4494e8ee0";

    let url = format!("https://buildomat.eng.oxide.computer/public/file/oxidecomputer/maghemite/openapi/{commit}/ddm-admin.json");

    let spec = reqwest::blocking::get(&url)
        .with_context(|| format!("failed to download openapi spec from {url}"))?
        .json()
        .with_context(|| format!("failed to parse {url} as openapi spec"))?;

    let content = progenitor::Generator::new()
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
        })
        .generate_text(&spec)
        .with_context(|| {
            format!("failed to generate progenitor client from {url}")
        })?;

    let out_file =
        Path::new(&env::var("OUT_DIR").expect("OUT_DIR env var not set"))
            .join("ddm-admin-client.rs");

    fs::write(&out_file, content).with_context(|| {
        format!("failed to write client to {}", out_file.display())
    })?;

    Ok(())
}
