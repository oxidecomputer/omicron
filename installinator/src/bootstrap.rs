// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

//! Perform "bootstrap-agent lite" sled setup.

use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use illumos_utils::addrobj::AddrObject;
use sled_hardware::underlay;
use slog::info;
use slog::Logger;

const MG_DDM_SERVICE_FMRI: &str = "svc:/system/illumos/mg-ddm";
const MG_DDM_MANIFEST_PATH: &str = "/opt/oxide/mg-ddm/pkg/ddm/manifest.xml";

// TODO-cleanup The implementation of this function is heavily derived from
// `sled_agent::bootstrap::server::Server::start()`; consider whether we could
// find a way for them to share it.
pub(crate) async fn bootstrap_sled(log: Logger) -> Result<()> {
    // Find address objects to pass to maghemite.
    let mg_addr_objs = underlay::find_nics()
        .context("failed to find address objects for maghemite")?;
    ensure!(
        !mg_addr_objs.is_empty(),
        "underlay::find_nics() returned 0 address objects"
    );

    info!(log, "Starting mg-ddm service");
    tokio::task::spawn_blocking(|| {
        enable_mg_ddm_service_blocking(log, mg_addr_objs)
    })
    .await
    .unwrap()
}

fn enable_mg_ddm_service_blocking(
    log: Logger,
    interfaces: Vec<AddrObject>,
) -> Result<()> {
    ensure!(
        !interfaces.is_empty(),
        "Service mg-ddm requires at least one interface"
    );

    info!(log, "Importing mg-ddm service"; "path" => MG_DDM_MANIFEST_PATH);
    smf::Config::import().run(MG_DDM_MANIFEST_PATH).with_context(|| {
        format!("failed to import mg-ddm from {MG_DDM_MANIFEST_PATH}")
    })?;

    let interface_names: Vec<String> = interfaces
        .iter()
        .map(|interface| format!(r#""{}""#, interface))
        .collect();
    let property_value = format!("({})", interface_names.join(" "));
    info!(log, "Setting mg-ddm interfaces"; "interfaces" => &property_value);
    smf::Config::set_property(MG_DDM_SERVICE_FMRI)
        .run(smf::Property::new(
            smf::PropertyName::new("config", "interfaces").unwrap(),
            smf::PropertyValue::Astring(property_value),
        ))
        .context("failed to set mg-ddm config/interfaces")?;

    info!(log, "Enabling mg-ddm service");
    smf::Adm::new()
        .enable()
        .temporary()
        .run(smf::AdmSelection::ByPattern(&[MG_DDM_SERVICE_FMRI]))
        .context("failed to enable mg-ddm service")?;

    Ok(())
}
