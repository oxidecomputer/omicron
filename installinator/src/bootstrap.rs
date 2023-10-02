// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

//! Perform "bootstrap-agent lite" sled setup.

use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use ddm_admin_client::Client as DdmAdminClient;
use helios_fusion::BoxedExecutor;
use illumos_utils::addrobj::AddrObject;
use illumos_utils::dladm;
use illumos_utils::dladm::Dladm;
use illumos_utils::zone::Zones;
use omicron_common::address::Ipv6Subnet;
use sled_hardware::underlay;
use sled_hardware::underlay::BootstrapInterface;
use slog::info;
use slog::Logger;

const MG_DDM_SERVICE_FMRI: &str = "svc:/system/illumos/mg-ddm";
const MG_DDM_MANIFEST_PATH: &str = "/opt/oxide/mg-ddm/pkg/ddm/manifest.xml";

// TODO-cleanup The implementation of this function is heavily derived from
// `sled_agent::bootstrap::server::Server::start()`; consider whether we could
// find a way for them to share it.
pub(crate) async fn bootstrap_sled(
    data_links: &[String; 2],
    log: Logger,
    executor: &BoxedExecutor,
) -> Result<()> {
    // Find address objects to pass to maghemite.
    let links = underlay::find_chelsio_links(executor, data_links)
        .context("failed to find chelsio links")?;
    ensure!(
        !links.is_empty(),
        "underlay::find_chelsio_nics() returned 0 links"
    );

    let mg_addr_objs =
        underlay::ensure_links_have_global_zone_link_local_v6_addresses(
            executor, &links,
        )
        .context("failed to create address objects for maghemite")?;

    info!(log, "Starting mg-ddm service");
    {
        let log = log.clone();
        tokio::task::spawn_blocking(|| {
            enable_mg_ddm_service_blocking(log, mg_addr_objs)
        })
        .await
        .unwrap()?;
    }

    // Set up an interface for our bootstrap network.
    let bootstrap_etherstub =
        Dladm::ensure_etherstub(executor, dladm::BOOTSTRAP_ETHERSTUB_NAME)
            .context("failed to ensure bootstrap etherstub existence")?;

    let bootstrap_etherstub_vnic =
        Dladm::ensure_etherstub_vnic(executor, &bootstrap_etherstub)
            .context("failed to ensure bootstrap etherstub vnic existence")?;

    // Use the mac address of the first link to derive our bootstrap address.
    let ip = BootstrapInterface::GlobalZone
        .ip(executor, &links[0])
        .with_context(|| {
            format!("failed to derive a bootstrap prefix from {:?}", links[0])
        })?;

    Zones::ensure_has_global_zone_v6_address(
        executor,
        bootstrap_etherstub_vnic,
        ip,
        "bootstrap6",
    )
    .context("failed to create v6 address for bootstrap etherstub vnic")?;

    // Spawn a background task to notify our local ddmd of our bootstrap address
    // so it can advertise it to other sleds.
    let ddmd_client = DdmAdminClient::localhost(&log)?;
    ddmd_client.advertise_prefix(Ipv6Subnet::new(ip));

    Ok(())
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
