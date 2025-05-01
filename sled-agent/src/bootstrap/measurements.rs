// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functions related to management of measurement corpus
use camino::Utf8PathBuf;
use sled_storage::dataset::INSTALL_DATASET;
use sled_storage::manager::StorageHandle;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MeasurementError {
    #[error("Missing INSTALL dataset")]
    MissingInstallSet,
    #[error("io: {0}")]
    Io(std::io::Error),
}

/// Access the measurements in the install directory
pub async fn sled_new_measurement_paths(
    storage: &StorageHandle,
) -> Result<Vec<Utf8PathBuf>, MeasurementError> {
    let mut all = vec![];

    let resources = storage.get_latest_disks().await;
    let dirs: Vec<_> = resources
        .all_m2_mountpoints(INSTALL_DATASET)
        .into_iter()
        .map(|p| p.join("measurements"))
        .collect();

    if dirs.is_empty() {
        return Err(MeasurementError::MissingInstallSet);
    }

    for dir in dirs {
        match dir.read_dir_utf8() {
            Ok(iter) => {
                for entry in iter {
                    let entry = entry.map_err(MeasurementError::Io)?;
                    all.push(entry.path().into());
                }
            }
            // We purposely skip over errors here in case the
            // directory is missing. This will just end up as
            // an empty corpus set.
            Err(_) => {}
        }
    }

    Ok(all)
}
