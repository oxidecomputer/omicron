// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2025 Oxide Computer Company
//
// TODO: remove
// This code is only required at the moment because the source repo
// for `Dendrite` is not yet public. Once `Dendrite` has been made
// public, we can remove this and point the services in `omicron`
// that require the `dpd-client` library to the `Dendrite` repo.

use anyhow::bail;
use anyhow::Context;
use anyhow::Result;
use omicron_zone_package::config::Config;
use omicron_zone_package::config::PackageName;
use omicron_zone_package::package::PackageSource;
use quote::quote;
use std::env;
use std::fs;
use std::path::Path;
use typify::TypeSpaceImpl;

const DENDRITE_ASIC_PACKAGE: PackageName =
    PackageName::new_const("dendrite-asic");

fn main() -> Result<()> {
    // Find the current dendrite repo commit from our package manifest.
    let manifest = fs::read_to_string("../../package-manifest.toml")
        .context("failed to read ../../package-manifest.toml")?;
    println!("cargo:rerun-if-changed=../../package-manifest.toml");

    let config: Config = toml::from_str(&manifest)
        .context("failed to parse ../../package-manifest.toml")?;

    let dendrite = config
        .packages
        .get(&DENDRITE_ASIC_PACKAGE)
        .context("missing dendrite package in ../../package-manifest.toml")?;

    let local_path = match &dendrite.source {
        PackageSource::Prebuilt { commit, .. } => {
            // Report a relatively verbose error if we haven't downloaded the
            // requisite openapi spec.
            let local_path =
                env::var("DPD_OPENAPI_PATH").unwrap_or_else(|_| {
                    format!("../../out/downloads/dpd-{commit}.json")
                });
            if !Path::new(&local_path).exists() {
                bail!("{local_path} doesn't exist; rerun `cargo xtask download dendrite-openapi` (after updating `tools/dendrite_openapi_version` if the dendrite commit in package-manifest.toml has changed)");
            }
            println!("cargo:rerun-if-changed={local_path}");
            local_path
        }

        PackageSource::Manual => {
            let local_path = "../../out/downloads/dpd-manual.json".to_string();
            if !Path::new(&local_path).exists() {
                bail!("{local_path} doesn't exist, please copy manually built dpd.json there!");
            }
            println!("cargo:rerun-if-changed={local_path}");
            local_path
        }

        _ => {
            bail!("dendrite external package must have type `prebuilt` or `manual`")
        }
    };

    let spec = {
        let bytes = fs::read(&local_path)
            .with_context(|| format!("failed to read {local_path}"))?;
        serde_json::from_slice(&bytes).with_context(|| {
            format!("failed to parse {local_path} as openapi spec")
        })?
    };

    let code = progenitor::Generator::new(
        progenitor::GenerationSettings::new()
            .with_inner_type(quote!{ ClientState })
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
            })
            .with_derive("PartialEq")
            // NOTE: This should all go away when we can open-source the real
            // `dpd-client` from the repo itself. These are used to ensure that
            // we pick up things like the display implementations.
            .with_replacement(
                "ActiveCableMediaInterfaceId",
                "transceiver_decode::ActiveCableMediaInterfaceId",
                std::iter::once(TypeSpaceImpl::Display)
            )
            .with_replacement(
                "ApplicationDescriptor", "transceiver_decode::ApplicationDescriptor",
                std::iter::once(TypeSpaceImpl::Display)
            )
            .with_replacement(
                "BaseTMediaInterfaceId", "transceiver_decode::BaseTMediaInterfaceId",
                std::iter::once(TypeSpaceImpl::Display))
            .with_replacement(
                "CmisDatapath", "transceiver_decode::CmisDatapath",
                std::iter::once(TypeSpaceImpl::Display))
            .with_replacement(
                "CmisDatapathState", "transceiver_decode::CmisDatapathState",
                std::iter::once(TypeSpaceImpl::Display))
            .with_replacement(
                "CmisLaneStatus", "transceiver_decode::CmisLaneStatus",
                std::iter::once(TypeSpaceImpl::Display))
            .with_replacement(
                "ConnectorType", "transceiver_decode::ConnectorType",
                std::iter::once(TypeSpaceImpl::Display))
            .with_replacement(
                "ExtendedSpecificationComplianceCode", "transceiver_decode::ExtendedSpecificationComplianceCode",
                std::iter::once(TypeSpaceImpl::Display))
            .with_replacement(
                "HostElectricalInterfaceId", "transceiver_decode::HostElectricalInterfaceId",
                std::iter::once(TypeSpaceImpl::Display))
            .with_replacement(
                "Identifier", "transceiver_decode::Identifier",
                std::iter::once(TypeSpaceImpl::Display))
            .with_replacement(
                "MediaType", "transceiver_decode::MediaType",
                std::iter::once(TypeSpaceImpl::Display))
            .with_replacement(
                "MmfMediaInterfaceId", "transceiver_decode::MmfMediaInterfaceId",
                std::iter::once(TypeSpaceImpl::Display))
            .with_replacement(
                "PassiveCopperMediaInterfaceId", "transceiver_decode::PassiveCopperMediaInterfaceId",
                std::iter::once(TypeSpaceImpl::Display))
            .with_replacement(
                "Sff8636Datapath", "transceiver_decode::Sff8636Datapath",
                std::iter::once(TypeSpaceImpl::Display))
            .with_replacement(
                "SffComplianceCode", "transceiver_decode::SffComplianceCode",
                std::iter::once(TypeSpaceImpl::Display))
            .with_replacement(
                "SmfMediaInterfaceId", "transceiver_decode::SmfMediaInterfaceId",
                std::iter::once(TypeSpaceImpl::Display))
    )
    .generate_tokens(&spec)
    .with_context(|| {
        format!("failed to generate progenitor client from {local_path}")
    })?;

    let content = rustfmt_wrapper::rustfmt(code).with_context(|| {
        format!("rustfmt failed on progenitor code from {local_path}")
    })?;

    let out_file =
        Path::new(&env::var("OUT_DIR").expect("OUT_DIR env var not set"))
            .join("dpd-client.rs");

    fs::write(&out_file, content).with_context(|| {
        format!("failed to write client to {}", out_file.display())
    })?;

    Ok(())
}
