// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Starting the mg-ddm service.

use crate::illumos::addrobj::AddrObject;
use slog::Logger;
use thiserror::Error;

const SERVICE_FMRI: &str = "svc:/system/illumos/mg-ddm";
const MANIFEST_PATH: &str = "/opt/oxide/mg-ddm/pkg/ddm/manifest.xml";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error configuring service: {0}")]
    Config(#[from] smf::ConfigError),

    #[error("Error administering service: {0}")]
    Adm(#[from] smf::AdmError),

    #[error("Error starting service: {0}")]
    Join(#[from] tokio::task::JoinError),
}

pub async fn enable_mg_ddm_service(
    log: Logger,
    interface: AddrObject,
) -> Result<(), Error> {
    tokio::task::spawn_blocking(|| {
        enable_mg_ddm_service_blocking(log, interface)
    })
    .await?
}

fn enable_mg_ddm_service_blocking(
    log: Logger,
    interface: AddrObject,
) -> Result<(), Error> {
    // TODO-correctness Should we try to shut down / remove any existing mg-ddm
    // service first? This appears to work fine as-is on a restart of the
    // sled-agent service.
    info!(log, "Importing mg-ddm service"; "path" => MANIFEST_PATH);
    smf::Config::import().run(MANIFEST_PATH)?;

    // TODO-cleanup mg-ddm supports multiple interfaces, but `smf` currently
    // doesn't expose an equivalent of `svccfg addpropvalue`. If we need
    // multiple interfaces we'll need to extend smf.
    let interface = interface.to_string();
    info!(log, "Setting mg-ddm interface"; "interface" => interface.as_str());
    smf::Config::set_property(SERVICE_FMRI).run(smf::Property::new(
        smf::PropertyName::new("config", "interfaces").unwrap(),
        smf::PropertyValue::Astring(interface),
    ))?;

    info!(log, "Enabling mg-ddm service");
    smf::Adm::new()
        .enable()
        .temporary()
        .run(smf::AdmSelection::ByPattern(&[SERVICE_FMRI]))?;

    Ok(())
}
