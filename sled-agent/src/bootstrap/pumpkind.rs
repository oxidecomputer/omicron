// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Starting the pumpkind service.

use crate::config::Deployment;
use thiserror::Error;

const SERVICE_FMRI: &str = "svc:/oxide/pumpkind";
const MANIFEST_PATH: &str =
    "/opt/oxide/pumpkind/lib/svc/manifest/system/pumpkind.xml";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error configuring service: {0}")]
    Config(#[from] smf::ConfigError),

    #[error("Error administering service: {0}")]
    Adm(#[from] smf::AdmError),

    #[error("detecting Oxide sled")]
    Detect(#[source] anyhow::Error),

    #[error("pumpkind manifest not installed at {0}")]
    ManifestMissing(&'static str),
}

/// Import and enable pumpkind on Oxide sleds whose deployment has a physical
/// ASIC.
pub(super) fn enable_pumpkind_service(
    log: &slog::Logger,
    deployment: &Deployment,
) -> Result<(), Error> {
    if !deployment.has_physical_asic() {
        info!(log, "deployment has no physical ASIC; skipping pumpkind");
        return Ok(());
    }
    if !sled_hardware::is_oxide_sled().map_err(Error::Detect)? {
        info!(log, "not an Oxide sled; skipping pumpkind");
        return Ok(());
    }
    if !std::path::Path::new(MANIFEST_PATH).exists() {
        return Err(Error::ManifestMissing(MANIFEST_PATH));
    }

    info!(log, "Importing pumpkind service"; "path" => MANIFEST_PATH);
    smf::Config::import().run(MANIFEST_PATH)?;

    info!(log, "Enabling pumpkind service");
    smf::Adm::new()
        .enable()
        .temporary()
        .run(smf::AdmSelection::ByPattern(&[SERVICE_FMRI]))?;

    Ok(())
}
