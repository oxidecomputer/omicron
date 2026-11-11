// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Starting the mg-ddm service.

use illumos_utils::addrobj::AddrObject;
use slog::Logger;
use thiserror::Error;

const SERVICE_FMRI: &str = "svc:/oxide/mg-ddm";
const MANIFEST_PATH: &str = "/opt/oxide/mg-ddm/pkg/ddm/manifest.xml";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error configuring service: {0}")]
    Config(#[from] smf::ConfigError),

    #[error("Error administering service: {0}")]
    Adm(#[from] smf::AdmError),

    #[error("Error starting service: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("Argument error: {0}")]
    Argument(String),
}

pub(super) fn enable_mg_ddm_service_blocking(
    log: Logger,
    interfaces: Vec<AddrObject>,
) -> Result<(), Error> {
    if interfaces.is_empty() {
        return Err(Error::Argument(
            "Service mg-ddm requires at least one interface".to_string(),
        ));
    }

    // TODO-correctness Should we try to shut down / remove any existing mg-ddm
    // service first? This appears to work fine as-is on a restart of the
    // sled-agent service.
    info!(log, "Importing mg-ddm service"; "path" => MANIFEST_PATH);
    smf::Config::import().run(MANIFEST_PATH)?;

    let interface_names: Vec<String> = interfaces
        .iter()
        .map(|interface| format!(r#""{}""#, interface))
        .collect();
    let property_value = format!("({})", interface_names.join(" "));
    info!(log, "Setting mg-ddm interfaces"; "interfaces" => &property_value);
    smf::Config::set_property(SERVICE_FMRI).run(smf::Property::new(
        smf::PropertyName::new("config", "interfaces").unwrap(),
        smf::PropertyValue::Astring(property_value),
    ))?;

    info!(log, "Enabling mg-ddm service");
    smf::Adm::new()
        .enable()
        .temporary()
        .run(smf::AdmSelection::ByPattern(&[SERVICE_FMRI]))?;

    Ok(())
}
