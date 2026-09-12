// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Starting the pumpkind service.

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

    #[error("Error detecting Oxide sled: {0}")]
    Detect(anyhow::Error),
}

/// Import and enable pumpkind on Oxide sleds with the manifest installed.
pub(super) fn enable_pumpkind_service(log: &slog::Logger) -> Result<(), Error> {
    if !sled_hardware::is_oxide_sled().map_err(Error::Detect)? {
        info!(log, "not an Oxide sled; skipping pumpkind");
        return Ok(());
    }
    if !std::path::Path::new(MANIFEST_PATH).exists() {
        info!(
            log,
            "pumpkind manifest not installed; skipping";
            "path" => MANIFEST_PATH,
        );
        return Ok(());
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
